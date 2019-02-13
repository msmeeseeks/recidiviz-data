# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================


"""Generic scraper implementation for regions using JailTracker.

All subclasses of JailtrackerScraper must implement:
- get_jailtracker_index

JailTracker exposes data via JSON endpoints, so the scraper only scrapes the
page for the session token required to access those endpoints, and the data
is then requested directly.

Scraper flow:
    1. Requests landing page and scrapes required session token from the page
        content.
    2. Requests roster in increments of _ROSTER_PAGE_SIZE.
    3. For each person on each page of the roster, sequentially requests the
        person, cases, and charges details.
    4. When all requests for an individual person are completed, passes the
        data to populate_data.
"""

import abc
import logging
import os
import re
from itertools import groupby
from typing import Optional
from typing import List

import more_itertools
from lxml import html

from recidiviz.common.constants.bond import BondType
from recidiviz.common.constants.booking import CustodyStatus, ReleaseReason
from recidiviz.common.constants.charge import ChargeClass, ChargeStatus
from recidiviz.ingest.scrape import constants
from recidiviz.ingest.scrape.base_scraper import BaseScraper
from recidiviz.ingest.extractor.json_data_extractor import JsonDataExtractor
from recidiviz.ingest.models.ingest_info import IngestInfo, Bond, Sentence, \
    Charge
from recidiviz.ingest.scrape.task_params import ScrapedData, Task


class JailTrackerRequestRateExceededError(Exception):
    """Raised if we detect that we'ree exceeded thre Jailtracker system
    request rate.
    """

    def __init__(self):
        msg = ('Exceeded the maximum number of requests per minute.'
               ' Failing to retry.')
        super().__init__(msg)


class JailTrackerScraper(BaseScraper):
    """Generic scraper for regions using JailTracker."""

    # Number of roster entries requested in a single request.
    _ROSTER_PAGE_SIZE = 25

    # URL base for all JailTracker pages and requests.
    _URL_BASE = "https://omsweb.public-safety-cloud.com/jtclientweb/"

    # Template for URL suffix used to request a specific region's landing page.
    #
    # Must be provided with 'index' field before being used.
    _LANDING_PAGE_URL_SUFFIX_TEMPLATE = "jailtracker/index/{index}"

    # Template for URL suffix used to request roster data.
    #
    # Must be provided with 'session', 'start', and 'limit' fields before being
    # used.
    _ROSTER_REQUEST_SUFFIX_TEMPLATE = (
        "(S({session}))//(S({session}))/"
        "JailTracker/GetInmates?start={start}&limit={limit}&sort=LastName&dir="
        "ASC")

    # Template for URL suffix used to request data on a specific person.
    #
    # Must be provided with 'session' and 'arrest' fields before being used.
    _PERSON_REQUEST_SUFFIX_TEMPLATE = (
        "(S({session}))//(S({session}))/"
        "JailTracker/GetInmate?arrestNo={arrest}")

    # Template for URL suffix used to request data on cases associated with a
    # specific person.
    #
    # Must be provided with 'session' and 'arrest' fields before being used.
    _CASES_REQUEST_SUFFIX_TEMPLATE = (
        "(S({session}))//(S({session}))/"
        "JailTracker/GetCases?arrestNo={arrest}")

    # Template for URL suffix used to request data on charges associated with
    # a specific person.
    #
    # Must be provided with 'session' field before being used.
    _CHARGES_REQUEST_SUFFIX_TEMPLATE = (
        "(S({session}))//(S({session}))/"
        "JailTracker/GetCharges")

    # Constant strings needed for param and data dicts.
    #
    # These are only values we're setting internally on our own data structures.
    # Strings needed to parse JailTracker's JSON responses are handled as raw
    # strings within each parsing method.

    # Key in param dict for identifier used for requesting person, cases, and
    # charges.
    _ARREST_NUMBER = "arrest_number"
    # Key in param dict for JSON object containing cases.
    _CASES = "cases"
    # Value in param dict for a request targeting the cases endpoint.
    _CASES_REQUEST = "cases_request"
    # Value in param dict for a request targeting the charges endpoint.
    _CHARGES_REQUEST = "charges_request"
    # Key in param dict for JSON object for a person.
    _PERSON = "person"
    # Value in param dict for a request targeting the person endpoint.
    _PERSON_REQUEST = "person_request"
    # Key in param dict specifying the type of endpoint requested.
    _REQUEST_TARGET = "request_target"
    # Value in param dict for a request targeting the roster endpoint.
    _ROSTER_REQUEST = "roster_request"
    # Key in param dict for session token.
    _SESSION_TOKEN = "session_token"

    def __init__(self, region_name, yaml_file=None):
        super(JailTrackerScraper, self).__init__(region_name)

        # Set initial endpoint using region-specific landing page index.
        landing_page_url_suffix = self._LANDING_PAGE_URL_SUFFIX_TEMPLATE.format(
            index=self.get_jailtracker_index())
        # A yaml file can be passed in if the fields in the child scraper
        # instance are different or expanded.
        self.yaml = yaml_file or os.path.join(
            os.path.dirname(__file__), 'jailtracker.yaml')
        self._initial_endpoint = "/".join(
            [self._URL_BASE, landing_page_url_suffix])

    @abc.abstractmethod
    def get_jailtracker_index(self):
        """Returns the index used in the JailTracker URL to request a specific
        region's landing page.

        A JailTracker landing page URL ends with: "/jailtracker/index/<INDEX>".
        This value can either be text or a number. In either case, this method
        should return the value as a string.
        """

    def get_initial_task(self) -> Task:
        # First request is for landing page which gives an HTML response.
        return Task(
            task_type=constants.TaskType.INITIAL_AND_MORE,
            endpoint=self._initial_endpoint,
            response_type=constants.ResponseType.HTML,
        )

    def get_more_tasks(self, content, task: Task) -> List[Task]:
        if self.is_initial_task(task.task_type):
            roster_request_params = \
                self._process_landing_page_and_get_next_task(content)
            if roster_request_params is None:
                return []
            return [roster_request_params]

        if task.custom[self._REQUEST_TARGET] == self._ROSTER_REQUEST:
            return self._process_roster_response_and_get_next_tasks(
                content, task)

        if task.custom[self._REQUEST_TARGET] == self._PERSON_REQUEST:
            return [self._process_person_response_and_get_next_task(
                content, task)]

        if task.custom[self._REQUEST_TARGET] == self._CASES_REQUEST:
            return [self._process_cases_response_and_get_next_task(
                content, task)]

        return []

    def find_session_token(self, content):
        """Finds the session token of the page given the content."""

        body_script = html.tostring(content, encoding='unicode')
        return re.search(r"JailTracker.Web.Settings.init\('(.*)'",
                         body_script).group(1)

    def populate_data(
            self, content, task: Task,
            ingest_info: IngestInfo) -> Optional[ScrapedData]:
        data_extractor = JsonDataExtractor(self.yaml)
        facility, parole_agency = self.extract_agencies(
            task.custom[self._PERSON]['data'])
        booking_data = self._field_val_pairs_to_dict(
            task.custom[self._PERSON]['data'])
        booking_data['booking_id'] = task.custom[self._ARREST_NUMBER]
        # Infer their release if the agency is a parole agency.
        if facility is None and parole_agency is not None:
            booking_data['Custody Status'] = CustodyStatus.RELEASED.value
            booking_data['Release Reason'] = ReleaseReason.PROBATION.value
        elif facility:
            booking_data['Facility'] = facility
        data = {
            'booking': booking_data,
            'charges': content['data'],
        }
        ingest_info = data_extractor.extract_and_populate_data(data,
                                                               ingest_info)
        self._add_cases_information(ingest_info, task.custom[self._CASES])
        self._sanitize_bond_types(ingest_info)
        ingest_info = ingest_info.prune()
        return ScrapedData(ingest_info=ingest_info, persist=True)

    def get_enum_overrides(self):
        return {
            'O': ChargeClass.PROBATION_VIOLATION,
            'CLOSED': ChargeStatus.DROPPED,
            'NO BAIL': BondType.NO_BOND,
            # This is overloaded in the bond and means there is no bond.
            'SERVING_TIME': None,
        }

    def _process_landing_page_and_get_next_task(
            self, content) -> Optional[Task]:
        """Scrapes session token from landing page and creates params for
        first roster request.

        Args:
            content: lxml html tree of landing page content

        Returns:
            Param dict for first roster request on success or -1 on failure.
        """
        try:
            session_token = self.find_session_token(content)
        except Exception as exception:
            logging.error("Error, could not parse session token from the "
                          "landing page HTML. Error: %s\nPage content:\n\n%s",
                          exception, content)
            logging.error(exception)
            return None

        roster_request_suffix = self._ROSTER_REQUEST_SUFFIX_TEMPLATE.format(
            session=session_token,
            start=0,
            limit=self._ROSTER_PAGE_SIZE)
        roster_request_endpoint = "/".join(
            [self._URL_BASE, roster_request_suffix])

        return Task(
            task_type=constants.TaskType.GET_MORE_TASKS,
            endpoint=roster_request_endpoint,
            response_type=constants.ResponseType.JSON,
            custom={
                self._REQUEST_TARGET: self._ROSTER_REQUEST,
                self._SESSION_TOKEN: session_token,
            },
        )

    def _process_roster_response_and_get_next_tasks(
            self, response, task: Task) -> List[Task]:
        """Returns next tasks from a single roster response.

        One task will be created for each person in the response. Additionally,
        if the response does not represent the end of the roster, a task will
        be created to fetch the next page of the roster.

        Args:
            response: JSON object of one page of roster
            params: dict of parameters used for last request

        Returns:
            List of param dicts, one for each task to run.
        """

        next_tasks = []

        if 'error' in response and \
           response['error'] == 'max-requests-for-timeperiod':
            raise JailTrackerRequestRateExceededError()

        max_index_present = 0
        for roster_entry in response["data"]:
            row_index = roster_entry["RowIndex"]
            if row_index > max_index_present:
                max_index_present = row_index

            arrest_number = roster_entry["ArrestNo"]

            person_request_suffix = self._PERSON_REQUEST_SUFFIX_TEMPLATE.format(
                session=task.custom[self._SESSION_TOKEN],
                arrest=arrest_number)
            person_request_endpoint = "/".join(
                [self._URL_BASE, person_request_suffix])

            next_tasks.append(Task(
                task_type=constants.TaskType.GET_MORE_TASKS,
                endpoint=person_request_endpoint,
                response_type=constants.ResponseType.JSON,
                custom={
                    self._ARREST_NUMBER: arrest_number,
                    self._REQUEST_TARGET: self._PERSON_REQUEST,
                    self._SESSION_TOKEN: task.custom[self._SESSION_TOKEN],
                },
            ))

        # If we aren't done reading the roster, request another page
        if response["totalCount"] > max_index_present:
            roster_request_suffix = self._ROSTER_REQUEST_SUFFIX_TEMPLATE.format(
                session=task.custom[self._SESSION_TOKEN],
                # max_index_present doesn't need to be incremented because the
                # returned RowIndex is 1-based while the index in the request
                # is 0-based.
                start=max_index_present,
                limit=self._ROSTER_PAGE_SIZE)
            roster_request_endpoint = "/".join(
                [self._URL_BASE, roster_request_suffix])

            # Prepend roster task for debugging purposes.
            # When running create_scraper for Jailtracker, `--lifo` flag will
            # fetch people before fetching the entire roster.
            next_tasks.insert(0, Task(
                task_type=constants.TaskType.GET_MORE_TASKS,
                endpoint=roster_request_endpoint,
                response_type=constants.ResponseType.JSON,
                custom={
                    self._REQUEST_TARGET: self._ROSTER_REQUEST,
                    self._SESSION_TOKEN: task.custom[self._SESSION_TOKEN],
                },
            ))

        return next_tasks

    def _process_person_response_and_get_next_task(
            self, response, task: Task) -> Task:
        """Creates cases request task for the person provided in the response.

        The returned params will also include the JSON person object from the
        response, so it can be combined with cases and charges once both are
        fetched.

        Args:
            response: JSON object of one person
            params: dict of parameters used for last request

        Returns:
            Dict of parameters for cases request
        """

        cases_request_suffix = self._CASES_REQUEST_SUFFIX_TEMPLATE.format(
            session=task.custom[self._SESSION_TOKEN],
            arrest=task.custom[self._ARREST_NUMBER])
        cases_request_endpoint = "/".join(
            [self._URL_BASE, cases_request_suffix])

        return Task(
            task_type=constants.TaskType.GET_MORE_TASKS,
            endpoint=cases_request_endpoint,
            response_type=constants.ResponseType.JSON,
            custom={
                self._ARREST_NUMBER: task.custom[self._ARREST_NUMBER],
                self._PERSON: response,
                self._REQUEST_TARGET: self._CASES_REQUEST,
                self._SESSION_TOKEN: task.custom[self._SESSION_TOKEN],
            },
        )

    def _process_cases_response_and_get_next_task(
            self, response, task: Task) -> Task:
        """Creates charges request task for the person provided in params.

        The returned params will also include the JSON objects for both person
        and cases, so they can be combined with charges once those are fetched.

        Args:
            response: JSON object of cases for one person
            params: dict of parameters used for last request

        Returns:
            Dict of parameters for charges request
        """

        charges_request_suffix = self._CHARGES_REQUEST_SUFFIX_TEMPLATE.format(
            session=task.custom[self._SESSION_TOKEN])
        charges_request_endpoint = "/".join(
            [self._URL_BASE, charges_request_suffix])

        return Task(
            task_type=constants.TaskType.SCRAPE_DATA,
            endpoint=charges_request_endpoint,
            response_type=constants.ResponseType.JSON,
            post_data={
                "arrestNo": task.custom[self._ARREST_NUMBER],
            },
            custom={
                self._CASES: response,
                self._ARREST_NUMBER: task.custom[self._ARREST_NUMBER],
                self._PERSON: task.custom[self._PERSON],
                self._REQUEST_TARGET: self._CHARGES_REQUEST,
                self._SESSION_TOKEN: task.custom[self._SESSION_TOKEN],
            },
        )

    def extract_agencies(self, person_data):
        """Get the list of agencies with jurisdiction over this person. There
        can be more than one agency with jurisdiction if someone is
        incarcerated but already has a parole agency assigned.
        Args:
            person_data: (list of key/value pairs) The data scraped
                from the person's detail page.
        Returns:
            A tuple where the first entry is the name of the
            residential facility with custody over this person (or
            None) and the second entry is the probation or parole
            office in charge of supervising this person (or None).
            Returns None on error.
        """

        def is_parole_agency(agency):
            return 'parole' in agency.lower()

        facility = None
        parole_agency = None

        # when multiple agencies are present, 'Field' is None for
        # agencies after the first, but they are sequential.
        last_was_agency = False  # if the last named field was an agency
        for entry in person_data:
            if (last_was_agency and entry['Field'] is None or
                    entry['Field'] in ['Agency:', 'Agencies:']):
                last_was_agency = True

                agency_name = entry['Value']
                # choose between agency type
                if is_parole_agency(agency_name):
                    # repeated names happen
                    if agency_name == parole_agency:
                        continue
                    parole_agency = agency_name
                else:
                    facility = agency_name

            else:  # Not an agency
                last_was_agency = False

        return facility, parole_agency

    def _add_cases_information(
            self, ingest_info: IngestInfo, case_content) -> None:
        """"Looks at the provided |case_content| and updates the found charge
        information based on the contents of it's corresponding case, if
        present"""
        person = more_itertools.one(ingest_info.people)
        booking = more_itertools.one(person.bookings)
        charges_by_case = {k: list(cases) for k, cases in
                           groupby(booking.charges, lambda x: x.case_number)}

        for case in case_content.get('data', []):
            case_no = case.get('CaseNo', '')
            if not case_no:
                continue

            charges = charges_by_case.get(case_no, [])
            sentence = None
            bond = None
            charge_status = None

            # Only use case-level bond information if bond amount not on charges
            # already.
            if not self._any_charge_has_bond_amount(charges):
                bond = Bond(amount=str(case.get('BondAmount', '')),
                            bond_type=case.get('BondType', ''))

            if 'Sentence' in case:
                length = case.get('Sentence')
                sentence = Sentence(min_length=length, max_length=length)

            if 'Status' in case:
                charge_status = case.get('Status')

            self._add_to_charges(bond, sentence, charge_status, charges)

    def _add_to_charges(
            self, bond: Optional[Bond], sentence: Optional[Sentence],
            charge_status: Optional[str],
            charges: List[Charge]) -> None:
        for charge in charges:
            if charge_status and not charge.status:
                charge.status = charge_status
            if bond:
                charge.bond = bond
            if sentence:
                charge.sentence = sentence

    def _any_charge_has_bond_amount(self, charges):
        for charge in charges:
            if charge.bond and charge.bond.amount and float(charge.bond.amount):
                return True
        return False

    def _sanitize_bond_types(self, ingest_info: IngestInfo) -> None:
        person = more_itertools.one(ingest_info.people)
        booking = more_itertools.one(person.bookings)
        for charge in booking.charges:
            if charge.bond and charge.bond.bond_type in {'SENTENCED',
                                                         'PROB REVOKED'}:
                if not charge.status:
                    charge.status = charge.bond.bond_type
                charge.bond.bond_type = None

    def _field_val_pairs_to_dict(self, field_val_pair):
        """Convert an array of dict entries where each dict consists of only
        the fields 'Field' and 'Value' to a dictionary with the
        values of those fields as the keys and values.
        """
        items = {}
        for pair in field_val_pair:
            field = pair['Field']
            if field is None:
                continue
            if field.endswith(':'):
                field = field[:-1]

            items[field] = pair['Value']

        return items
