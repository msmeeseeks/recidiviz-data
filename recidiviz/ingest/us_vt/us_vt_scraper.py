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


"""Scraper implementation for people incarcerated in Vermont.

Region-specific notes:

    - Scrape the front page to get the session key
    - Make a requests to the GetInmates endpoint N records at a time
      (say 500) until all rows of the roster have been retrieved.
    - For each person on the roster, use the ArrestNo in the person's
      row to grab the GetInmate, GetCases, and GetCharges info.
    - Create the person and record objects and store them in the datastore.

"""


from copy import deepcopy
import json
import logging
import re

from lxml import html
from lxml.etree import XMLSyntaxError # pylint:disable=no-name-in-module

from google.appengine.ext.db import InternalError
from google.appengine.ext.db import Timeout, TransactionFailedError

from recidiviz.ingest import scraper_utils
from recidiviz.ingest.scraper import Scraper
from recidiviz.ingest.us_vt.us_vt_person import UsVtPerson
from recidiviz.ingest.us_vt.us_vt_record import UsVtOffense
from recidiviz.ingest.us_vt.us_vt_snapshot import \
    UsVtSnapshot
from recidiviz.models.record import Record


class UsVtScraper(Scraper):
    """Class that scrapes information on people held in Vermont facilities.

    """

    def __init__(self):

        self.front_url = '/jailtracker/index/Vermont'
        self.roster_url = ('(S({session}))//(S({session}))/JailTracker/'
                           'GetInmates?start={start}&limit={limit}'
                           '&sort=LastName&dir=ASC')
        self.person_url = ('(S({session}))//(S({session}))/JailTracker/'
                           'GetInmate?arrestNo={arrest}')
        self.cases_url = ('(S({session}))//(S({session}))/JailTracker/'
                          'GetCases?arrestNo={arrest}')
        self.charges_url = ('(S({session}))//(S({session}))/JailTracker/'
                            'GetCharges')

        super(UsVtScraper, self).__init__('us_vt')

    def get_initial_task(self):
        """
        Get the name of the first task to be run.

        Returns:
            The name of the task to run first.

        """
        return 'scrape_front_page'

    def scrape_front_page(self, params):
        """Scrapes the front page for the VT DOC site to get
        the session key. Enqueues tasks to scrape the roster.

        Args:
            params: Dict of parameters, includes:
                scrape_type: 'background' or 'snapshot'

        Returns:
            Nothing if successful, -1 if fails.

        """

        def get_session_key(html_tree):
            """Helper function to extract the session key from the html of the
            front page of the Vermont DOC site.

            """

            # The session key is in a string parameter in a javascript
            # call that initializes the jailtracker system.
            body_script = html.tostring(
                html_tree.xpath("//body/div/script")[0])
            try:
                session_key = re.search(
                    r"JailTracker.Web.Settings.init\('(.*)'",
                    body_script).group(1)
            except AttributeError:
                logging.error("Error, could not parse session key from the "
                              "front page HTML")
                return None

            return session_key

        url = '/'.join([self.get_region().base_url, self.front_url])
        front_page = self.fetch_page(url)
        if front_page == -1:
            return -1

        try:
            front_tree = html.fromstring(front_page.content)
        except XMLSyntaxError as e:
            logging.error("Error parsing front page. Error: %s\nPage:\n\n%s",
                          e, front_page.content)
            return -1

        session = get_session_key(front_tree)
        if session is None:
            return -1

        first_roster_params = {
            'session': session,
            'start': 0,
            'limit': 10,
            'scrape_type': params['scrape_type']}

        self.add_task('scrape_roster', first_roster_params)

        return None

    def scrape_roster(self, params):
        """Scrapes the roster for VT to get a list of people. Enqueues tasks
        to scrape the individual listings.

        Args:
            params: Dict of parameters, includes:
                scrape_type: 'background' or 'snapshot'
                session: (string) session key (required)
                start: (int) index of the first row of the roster to retrieve
                       (required)
                limit: (int) the number of rows to retrieve (required)

        Returns:
            Nothing if successful, -1 if fails.

        """

        url = '/'.join([self.get_region().base_url, self.roster_url]).format(
            session=params['session'], start=params['start'],
            limit=params['limit'])

        roster_response = self.fetch_page(url)

        roster = json.loads(roster_response.content)

        # for termination condition testing and to know which start
        # row to request next
        max_row_index = -1

        # add a task for each person in the roster
        for person in roster['data']:
            if person['RowIndex'] > max_row_index:
                max_row_index = person['RowIndex']

            next_person_params = {
                'session': params['session'],
                'roster_entry': person,
                'scrape_type': params['scrape_type']
            }

            self.add_task('scrape_person', next_person_params)

        # if we aren't done reading the roster, read another page
        if roster['totalCount'] > max_row_index:
            next_roster_params = {
                'start': max_row_index,  # 1- vs. 0-based indexing :(
                'limit': params['limit'],
                'session': params['session'],
                'scrape_type': params['scrape_type']}

            self.add_task('scrape_roster', next_roster_params)

        return None

    def scrape_person(self, params):
        """Scrape the information about an individual.

        Fetches the general information about an individual, and
        enqueues the task to scrape the cases information associated
        with the individual.

        Args:
            params: Dict of parameters.
                scrape_type: 'background' or 'snapshot'
                session: (string) session key (required)
                roster_entry: (dict) key/value pairs with info contained in
                    the roster entry
        """

        url = '/'.join([self.get_region().base_url, self.person_url]).format(
            session=params['session'],
            arrest=params['roster_entry']['ArrestNo'])
        person_response = self.fetch_page(url)
        person = json.loads(person_response.content)

        cases_params = {
            'session': params['session'],
            'roster_entry': params['roster_entry'],
            'person': person,
            'scrape_type': params['scrape_type']}

        self.add_task('scrape_cases', cases_params)

    def scrape_cases(self, params):
        """Scrape the case information about an individual case.

        Args:
            params: Dict of parameters.
                scrape_type: 'background' or 'snapshot'
                session: (string) session key (required)
                roster_entry: (dict) key/value pairs with info contained in
                    the roster entry
                person: (dict) key/value pairs with info on the person panel
        """

        url = '/'.join([self.get_region().base_url, self.cases_url]).format(
            session=params['session'],
            arrest=params['roster_entry']['ArrestNo'])
        cases_response = self.fetch_page(url)
        cases = json.loads(cases_response.content)

        charges_params = {
            'session': params['session'],
            'roster_entry': params['roster_entry'],
            'person': params['person'],
            'cases': cases,
            'scrape_type': params['scrape_type']}

        self.add_task('scrape_charges', charges_params)

    def scrape_charges(self, params):
        """Scrape the charge information about an individual charge.

        Args:
            params: Dict of parameters.
                scrape_type: 'background' or 'snapshot'
                session: (string) session key (required)
                roster_entry: (dict) key/value pairs with info contained in
                    the roster entry
                person: (dict) key/value pairs with info on the person panel
                cases: (dict) key/value pairs with info from each of the cases
                    from the cases list

        """

        url = '/'.join([self.get_region().base_url, self.charges_url]).format(
            session=params['session'])
        data = {'arrestNo': params['roster_entry']['ArrestNo']}

        charges_response = self.fetch_page(url, data=data)
        charges = json.loads(charges_response.content)

        self.store_record(params['roster_entry'], params['person'],
                          params['cases'], charges)

    @staticmethod
    def extract_agencies(person_data, person_id):
        """Get the list of agencies with jurisdiction over this person. There
        can be more than one agency with jurisdiction if someone is
        incarcerated but already has a parole agency assigned.

        Args:

            person_data: (list of key/value pairs) The data scraped
                from the person's detail page.

            person_id: (string) Person identifier for logging purposes.

        Returns:

            A tuple where the first entry is the name of the
            residential facility with custody over this person (or
            None) and the second entry is the probation or parole
            office in charge of supervising this person (or None).

            Returns None on error.

        """

        def is_parole_agency(agency_name):
            """Guess facility vs. community supervision agency by looking for
            the string 'parole' in the agency name.

            TODO: #122 find a better way to do this besides looking for a
            hardcoded string.

            Args:

                agency_name: (string) The name of an agency with
                    jurisdiction over a person.

            Returns:

                True if the agency is guessed to be a community
                    supervision agency, False otherwise.

            """
            return 'parole' in agency_name.lower()

        facility = None
        parole_agency = None

        # when multiple agencies are present, 'Field' is None for
        # agencies after the first, but they are sequential.
        last_was_agency = False  # if the last named field was an agency
        for entry in person_data:
            if (last_was_agency and entry['Field'] is None or entry[
                    'Field'] in ['Agency:', 'Agencies:']):  # Yes, agency
                last_was_agency = True

                agency_name = entry['Value']
                # choose between agency type
                if is_parole_agency(agency_name):
                    # repeated names happen
                    if agency_name == parole_agency:
                        continue

                    # protect against the so-far-unencountered
                    # situation of multiple distinct community
                    # supervision agencies.
                    if parole_agency is not None:
                        logging.error("Found multiple community supervision "
                                      "agencies for person id [%s]: [%s, %s]",
                                      person_id, agency_name, parole_agency)
                        return None

                    parole_agency = agency_name

                else:
                    # protect against the so-far-unencountered
                    # situation of multiple residential facilities.
                    if facility is not None:
                        logging.error("Found multiple residential facilites "
                                      "for person id [%s]: [%s, %s]",
                                      person_id, agency_name, facility)
                        return None

                    facility = agency_name

            else:  # Not an agency
                last_was_agency = False

        return (facility, parole_agency)

    def create_person(self, roster_data, person_data):
        """Make the person model entity, given data scraped from the roster
        entry and the person detail page.

        Args:

            roster_data: (list of key/value pairs) information scraped
                from the person's roster entry.

            person_data: (list of key/value pairs) information scraped
                from the person's details page.

        Returns:

            Person entity

        """
        person_id = roster_data['Jacket']
        person = UsVtPerson.get_or_insert(person_id)

        person.surname = roster_data['LastName']
        person.given_names = ' '.join([roster_data['FirstName'],
                                       roster_data['MiddleName']])

        # Note that we do not try to guess a birthday here, which may
        # lead to problems with age estimation later.
        person.age = int(person_data['Current Age'])

        # turn an 'm' or 'f' into a whole word
        if not person_data['Sex']:
            person.sex = 'unknown'
        elif person_data['Sex'].lower()[0] == 'f':
            person.sex = 'female'
        elif person_data['Sex'].lower()[0] == 'm':
            person.sex = 'male'
        else:
            person.sex = 'unknown'

        # sometimes race is indicated by letter, but more frequently
        # it's a whole word. i've not encountered other abbreviations
        # than 'w' and 'b'.
        if person_data['Race'].lower() == 'b':
            person.race = 'black'
        elif person_data['Race'].lower() == 'w':
            person.race = 'white'
        else:
            person.race = person_data['Race'].lower()

        person.person_id = person_id
        person.us_vt_person_id = person_id
        person.person_id_is_fuzzy = False
        person.region = self.get_region().region_code

        return person

    def create_record(self, person, agencies, roster_data, person_data,
                      charge_data):
        """Make the record model entity, given the person entity and data
        scraped from several pages.

        Args:

            person: (Person entity) person model entity created from
                scraped data.

            roster_data: (list of key/value pairs) information scraped
                from the person's roster entry.

            person_data: (list of key/value pairs) information scraped
                from the person's details page.

            charge_data: (list of key/value pairs) information scraped
                from the person's charge details page.

        Returns:

            Record entity

        """

        arrest_no = str(roster_data['ArrestNo'])
        record = Record.get_or_insert(arrest_no, parent=person.key)

        record.custody_date = scraper_utils.parse_date_string(
            person_data['Booking Date'], arrest_no)

        # store each charge as an offense to be added to the record
        record_offenses = []
        for charge in charge_data:
            offense = UsVtOffense(
                arresting_agency=charge['ArrestingAgency'],
                arrest_code=charge['ArrestCode'],
                arrest_date=scraper_utils.parse_date_string(
                    charge['ArrestDate'], arrest_no),
                bond_amount=charge['BondAmount'],
                bond_type=charge['BondType'],
                case_number=charge['CaseNo'],
                control_number=charge['ControlNumber'],
                court_time=scraper_utils.parse_date_string(
                    charge['CourtTime'], arrest_no),
                court_type=charge['CourtType'],
                crime_class=str(charge['ChargeId']),
                crime_description=charge['ChargeDescription'],
                crime_type=charge['CrimeType'],
                modifier=charge['Modifier'],
                number_of_counts=charge['Counts'],
                status=charge['ChargeStatus'],
                warrant_number=charge['WarrantNumber'],
                )
            record_offenses.append(offense)
        record.offense = record_offenses

        record.status = person_data['Status']
        record.release_date = roster_data['FinalReleaseDateTime']
        record.earliest_release_date = scraper_utils.parse_date_string(
            person_data['Min Release'], arrest_no)
        record.latest_release_date = scraper_utils.parse_date_string(
            person_data['Max Release'], arrest_no)
        record.parole_officer = person_data['Parole Officer']
        record.case_worker = person_data['Case Worker']

        record.sex = person.sex
        record.race = person.race
        record.region = self.get_region().region_code
        record.surname = person.surname
        record.given_names = person.given_names
        record.record_id = str(roster_data['ArrestNo'])
        record.record_id_is_fuzzy = False

        if agencies:
            record.latest_facility = agencies[0]
            record.community_supervision_agency = agencies[1]

        return record

    def store_record(self, roster_data, person_data, case_data, charge_data):
        """Store scraped data about a person. This is where information about
        a person in VT DOC custody is persisted to the datastore.

        We've scraped all incarceration details, and want to store the
        data we found. This function does some post-processing on the
        scraped data, and feeds it into the datastore in a way that
        can be indexed / queried in the future.

        Args:
            roster_data: (dict) Key/value results parsed from the roster
            person_data: (dict) Key/value results parsed from the person's page
            case_data: (dict) Key/value results parsed from the cases page
            charge_data: (dict) Key/value results parsed from the charges page

        Returns:
            Nothing if successful, -1 if fails.

        """

        def field_val_pairs_to_dict(field_val_pair):
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

        agencies = UsVtScraper.extract_agencies(person_data['data'],
                                                roster_data['Jacket'])
        person_data = field_val_pairs_to_dict(person_data['data'])
        charge_data = charge_data['data']

        # So far, all case data has been empty. Check for non-empty
        # case data and log an error so we know to investigate what
        # should be stored.
        case_data = case_data['data']
        if case_data:
            logging.error("Non-empty case data found, inspect it and "
                          "investigate storing the info it contains.\n"
                          "%r", case_data)

        # create the person and persist them
        person = self.create_person(roster_data, person_data)

        try:
            person.put()
        except (Timeout, TransactionFailedError, InternalError):
            # Datastore error - fail task to trigger queue retry + backoff
            logging.warning("Couldn't persist person: %s", person.person_id)
            return -1

        # create the record and persist it.
        record = self.create_record(
            person, agencies, roster_data, person_data, charge_data)

        try:
            record.put()
        except (Timeout, TransactionFailedError, InternalError):
            logging.warning("Couldn't persist record: %s", record.record_id)
            return -1

        # create a snapshot from the record so we only store what's
        # changed since the last scrape.
        old_record = deepcopy(record)
        new_snapshot = self.record_to_snapshot(record)
        self.compare_and_set_snapshot(old_record, new_snapshot)

        return None

    # pylint:disable=arguments-differ
    def person_id_to_record_id(self, person_id):
        """Convert provided person_id to record_id of any record for that
        person. This is the implementation of an abstract method in Scraper.

        Args:
            person_id: (string) Person ID for the person

        Returns:
            None if query returns None
            Record ID if a record is found for the person in the docket item

        """
        person = UsVtPerson.query(UsVtPerson.person_id == person_id).get()
        if not person:
            return None

        record = Record.query(ancestor=person.key).get()
        if not record:
            return None

        return record.record_id

    def record_to_snapshot(self, record):
        """Mirrors record fields into a Snapshot instance

        Takes in a new Record entity, and mirrors its fields into a
        Snapshot entity for comparison against the last-collected
        snapshot entity for this Record.

        Args:
            record: A Record object to mirror

        Returns:
            A Snapshot entity populated with the same details as the Record

        """
        snapshot = UsVtSnapshot(
            birthdate=record.birthdate,
            case_worker=record.case_worker,
            community_supervision_agency=record.community_supervision_agency,
            custody_date=record.custody_date,
            earliest_release_date=record.earliest_release_date,
            given_names=record.given_names,
            is_released=record.is_released,
            latest_facility=record.latest_facility,
            latest_release_date=record.latest_release_date,
            latest_release_type=record.latest_release_type,
            max_sentence_length=record.max_sentence_length,
            min_sentence_length=record.min_sentence_length,
            offense=record.offense,
            parent=record.key,
            parole_officer=record.parole_officer,
            race=record.race,
            region=record.region,
            release_date=record.release_date,
            sex=record.sex,
            status=record.status,
            surname=record.surname
        )

        return snapshot
