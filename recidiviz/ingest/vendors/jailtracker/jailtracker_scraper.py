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
- process_record

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
import json
import logging
import re
import os
from lxml import html

from recidiviz.common.constants.booking import CustodyStatus, ReleaseReason
from recidiviz.common.constants.charge import ChargeClass
from recidiviz.ingest.extractor.json_data_extractor import JsonDataExtractor

from recidiviz.ingest import constants
from recidiviz.ingest.base_scraper import BaseScraper


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
    # Key in param dict for data dict passed to _fetch_content.
    _DATA = "post_data"
    # Key in param dict for endpoint requested.
    _ENDPOINT = "endpoint"
    # Value in data dict for an expected response type of HTML.
    _HTML = "HTML"
    # Value in data dict for an expected response type of JSON.
    _JSON = "JSON"
    # Key in param dict for JSON object for a person.
    _PERSON = "person"
    # Value in param dict for a request targeting the person endpoint.
    _PERSON_REQUEST = "person_request"
    # Key in param dict specifying the type of endpoint requested.
    _REQUEST_TARGET = "request_target"
    # Key in data dict for the expected response type.
    _RESPONSE_TYPE = "response_type"
    # Value in param dict for a request targeting the roster endpoint.
    _ROSTER_REQUEST = "roster_request"
    # Key in param dict for session token.
    _SESSION_TOKEN = "session_token"
    # Key in param dict for task type.
    _TASK_TYPE = "task_type"

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

    def get_initial_endpoint(self):
        """Returns the initial endpoint to hit on the first call."""

        return self._initial_endpoint

    def get_initial_data(self):
        """Returns the initial data to send on the first call."""

        # First request will be for landing page, which gives an HTML response.
        return {self._RESPONSE_TYPE: self._HTML}

    def get_more_tasks(self, content, params):
        """Gets more tasks based on the content and params passed in.

        Args:
            content: An lxml html tree on the first request, and a JSON
                object for all following requests.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A list of param dicts, one for each task we want to run.
        """

        task_type = params.get(self._TASK_TYPE, self.get_initial_task_type())
        next_tasks = []

        if self.is_initial_task(task_type):
            roster_request_params = \
                self._process_landing_page_and_get_next_task(content)
            if roster_request_params == -1:
                return -1
            next_tasks.append(roster_request_params)

        elif params[self._REQUEST_TARGET] == self._ROSTER_REQUEST:
            next_tasks.extend(
                self._process_roster_response_and_get_next_tasks(
                    content, params))

        elif params[self._REQUEST_TARGET] == self._PERSON_REQUEST:
            next_tasks.append(
                self._process_person_response_and_get_next_task(
                    content, params))

        elif params[self._REQUEST_TARGET] == self._CASES_REQUEST:
            next_tasks.append(
                self._process_cases_response_and_get_next_task(
                    content, params))

        return next_tasks

    def find_session_token(self, content):
        """Finds the session token of the page given the content."""

        body_script = html.tostring(content, encoding='unicode')
        return re.search(r"JailTracker.Web.Settings.init\('(.*)'",
                         body_script).group(1)

    def populate_data(self, content, params, ingest_info):
        """
        Populates the ingest info object from the content and params given

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.
            ingest_info: The IngestInfo object to populate
        """
        data_extractor = JsonDataExtractor(self.yaml)
        facility, parole_agency = self.extract_agencies(
            params[self._PERSON]['data'])
        booking_data = self._field_val_pairs_to_dict(
            params[self._PERSON]['data'])
        booking_data['booking_id'] = params[self._ARREST_NUMBER]
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
        data_extractor.extract_and_populate_data(data, ingest_info)
        return ingest_info

    def get_enum_overrides(self):
        return {
            'O': ChargeClass.PROBATION_VIOLATION,
        }

    def _process_landing_page_and_get_next_task(self, content):
        """Scrapes session token from landing page and creates params for
        first roster request.

        Args:
            content: lxml html tree of landing page content

        Returns:
            Param dict for first roster request on success or -1 on failure.
        """

        session_token = None
        try:
            session_token = self.find_session_token(content)
        except Exception as exception:
            logging.error("Error, could not parse session token from the "
                          "landing page HTML. Error: %s\nPage content:\n\n%s",
                          exception, content)
            logging.error(exception)
            return -1

        roster_request_suffix = self._ROSTER_REQUEST_SUFFIX_TEMPLATE.format(
            session=session_token,
            start=0,
            limit=self._ROSTER_PAGE_SIZE)
        roster_request_endpoint = "/".join(
            [self._URL_BASE, roster_request_suffix])

        return {
            self._DATA: {self._RESPONSE_TYPE: self._JSON},
            self._ENDPOINT: roster_request_endpoint,
            self._REQUEST_TARGET: self._ROSTER_REQUEST,
            self._SESSION_TOKEN: session_token,
            self._TASK_TYPE: constants.GET_MORE_TASKS,
        }

    def _process_roster_response_and_get_next_tasks(self, response, params):
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

        max_index_present = 0
        for roster_entry in response["data"]:
            row_index = roster_entry["RowIndex"]
            if row_index > max_index_present:
                max_index_present = row_index

            arrest_number = roster_entry["ArrestNo"]

            person_request_suffix = self._PERSON_REQUEST_SUFFIX_TEMPLATE.format(
                session=params[self._SESSION_TOKEN],
                arrest=arrest_number)
            person_request_endpoint = "/".join(
                [self._URL_BASE, person_request_suffix])

            next_tasks.append({
                self._ARREST_NUMBER: arrest_number,
                self._DATA: {self._RESPONSE_TYPE: self._JSON},
                self._ENDPOINT: person_request_endpoint,
                self._REQUEST_TARGET: self._PERSON_REQUEST,
                self._SESSION_TOKEN: params[self._SESSION_TOKEN],
                self._TASK_TYPE: constants.GET_MORE_TASKS,
            })

        # If we aren't done reading the roster, request another page
        if response["totalCount"] > max_index_present:
            roster_request_suffix = self._ROSTER_REQUEST_SUFFIX_TEMPLATE.format(
                session=params[self._SESSION_TOKEN],
                # max_index_present doesn't need to be incremented because the
                # returned RowIndex is 1-based while the index in the request
                # is 0-based.
                start=max_index_present,
                limit=self._ROSTER_PAGE_SIZE)
            roster_request_endpoint = "/".join(
                [self._URL_BASE, roster_request_suffix])

            next_tasks.append({
                self._DATA: {self._RESPONSE_TYPE: self._JSON},
                self._ENDPOINT: roster_request_endpoint,
                self._REQUEST_TARGET: self._ROSTER_REQUEST,
                self._SESSION_TOKEN: params[self._SESSION_TOKEN],
                self._TASK_TYPE: constants.GET_MORE_TASKS,
            })

        return next_tasks

    def _process_person_response_and_get_next_task(self, response, params):
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
            session=params[self._SESSION_TOKEN],
            arrest=params[self._ARREST_NUMBER])
        cases_request_endpoint = "/".join(
            [self._URL_BASE, cases_request_suffix])

        return {
            self._ARREST_NUMBER: params[self._ARREST_NUMBER],
            self._DATA: {self._RESPONSE_TYPE: self._JSON},
            self._ENDPOINT: cases_request_endpoint,
            self._PERSON: response,
            self._REQUEST_TARGET: self._CASES_REQUEST,
            self._SESSION_TOKEN: params[self._SESSION_TOKEN],
            self._TASK_TYPE: constants.GET_MORE_TASKS,
        }

    def _process_cases_response_and_get_next_task(self, response, params):
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
            session=params[self._SESSION_TOKEN])
        charges_request_endpoint = "/".join(
            [self._URL_BASE, charges_request_suffix])

        return {
            self._CASES: response,
            self._ARREST_NUMBER: params[self._ARREST_NUMBER],
            self._DATA: {
                "arrestNo": params[self._ARREST_NUMBER],
                self._RESPONSE_TYPE: self._JSON
            },
            self._ENDPOINT: charges_request_endpoint,
            self._PERSON: params[self._PERSON],
            self._REQUEST_TARGET: self._CHARGES_REQUEST,
            self._SESSION_TOKEN: params[self._SESSION_TOKEN],
            self._TASK_TYPE: constants.SCRAPE_DATA,
        }

    # Overrides method in GenericScraper to handle JSON responses.
    def _fetch_content(self, endpoint, post_data=None, json_data=None):
        """Returns the response content, either HTML or JSON.

        'post_data' is expected to contain an entry keyed on '_RESPONSE_TYPE',
        indicating whether the expected response type is HTML or JSON.

        Args:
            endpoint: the endpoint to make a request to.
            post_data: dict of parameters to pass into the request.

        Returns:
            Returns the content of the response on success or -1 on failure.
        """
        response_type = post_data.get(self._RESPONSE_TYPE, None)
        if response_type is None:
            logging.error("Missing response type for endpoint %s. Data:\n%s",
                          endpoint, post_data)
            return -1

        # Remove response type before passing on data.
        post_data.pop(self._RESPONSE_TYPE)

        if response_type == self._HTML:
            # Fall back on GenericScraper behavior.
            return super(JailTrackerScraper, self)._fetch_content(
                endpoint, post_data)
        if response_type == self._JSON:
            logging.info('Fetching json content with endpoint: %s', endpoint)
            response = self.fetch_page(endpoint, post_data=post_data or None)
            if response == -1:
                return -1
            return json.loads(response.content)

        logging.error("Unexpected response type %s for endpoint %s",
                      response_type, endpoint)
        return -1

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
