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


"""Scraper implementation for the state of New York (NYS DOCCS)

Region-specific notes:
    - DOCCS includes historical data (back to ~1920s, reliably back to ~1970s)
    - DOCCS allows for surname-only search
    - DOCCS returns the nearest match alphabetically, and then all subsequent
      names in the alphabet.
        - As a result, the us-ny names list is one name, 'aaardvark', since
          this query will return all people in the DOCCS system.
    - DOCCS attempts to de-duplicate people, and show a disambiguation page
      for multiple records it believes to be of the same person.

Background scraping procedure:
    1. A starting search page (to get session vars) --> (search, see 2)
    2. A results list (4x/page) --> (parse, see 2a - 2c)
        (2a) A list of person results --> (follow, each entry leads to 3)
        (2b) The 'next page' of results for the query --> (follow it, see 2)
        (2c) The main search page --> Reached end of the list, stop scrape
    3. EITHER
        (3a) A disambiguation page (which record would you like to see for this
             person?)
        (3b) A details page for the person, about a specific incarceration
             event
"""

import logging
import os

from lxml import html

from recidiviz.ingest import constants
from recidiviz.ingest import scraper_utils
from recidiviz.ingest.base_scraper import BaseScraper
from recidiviz.ingest.extractor.data_extractor import DataExtractor


class UsNyScraper(BaseScraper):
    """Class to scrape info from the NY state DOCCS searcher.
    """

    def __init__(self):
        self.mapping_filepath = os.path.join(os.path.dirname(__file__),
                                             'us_ny.yaml')

        self._data_fields = [
            'M13_PAGE_CLICKI',
            'M13_SEL_DINI',
            'K01',
            'K02',
            'K03',
            'K04',
            'K05',
            'K06',
            'DFH_STATE_TOKEN',
            'DFH_MAP_STATE_TOKEN',
        ]

        super(UsNyScraper, self).__init__('us_ny')

    def get_initial_endpoint(self):
        """Returns the initial endpoint to hit on the first call
        Returns:
            A string representing the initial endpoint to hit
        """
        return self.get_region().base_url

    def set_initial_vars(self, content, params):
        pass

    def get_more_tasks(self, content, params):
        task_type = params.get('task_type', self.get_initial_task_type())
        params_list = []

        if self.is_initial_task(task_type):
            params_list.append(self._get_first_search_page_params(content))
        elif self.should_get_more_tasks(task_type):
            # Search and disambiguation pages have lists of people to
            # be scraped, person pages do not. Decide here if we're
            # handling a person detail page based on whether a list of
            # people pages was found.
            person_params = self._get_person_params(content)
            if person_params:
                params_list.extend(person_params)
            else:
                params_list.append(self._get_person_passthrough_params(content))
            params_list.extend(self._get_next_page_params(content))

        # Add session variables to the post data of each params dict.
        session_vars = self._get_session_vars(content)
        for params in params_list:
            if 'data' in params:
                params['data'].update(session_vars)

        return params_list

    def _get_session_vars(self, content):
        session_vars = {
            'DFH_STATE_TOKEN': scraper_utils.get_value_from_html_tree(
                content, 'DFH_STATE_TOKEN', tag='name'),
        }

        return session_vars

    def _get_first_search_page_params(self, content):
        data = {
            'DFH_MAP_STATE_TOKEN': '',
            'M00_LAST_NAMEI': 'a',
            'M00_FIRST_NAMEI': '',
            'M00_MID_NAMEI': '',
            'M00_NAME_SUFXI': '',
            'M00_DOBCCYYI': '',
            'M00_DIN_FLD1I': '',
            'M00_DIN_FLD2I': '',
            'M00_DIN_FLD3I': '',
            'M00_NYSID_FLD1I': '',
            'M00_NYSID_FLD2I': '',
            'K01': scraper_utils.get_value_from_html_tree(
                content, 'K01', tag='name'),
        }

        action = content.xpath('//div[@id="content"]/form/@action')[0]

        params = {
            'data': data,
            'endpoint': self.get_region().base_url + action,
            'task_type': constants.GET_MORE_TASKS,
        }

        return params

    def _get_next_page_params(self, content):
        try:
            next_button = content.xpath('//div[@id="content"]/form')[-1]
        except IndexError:
            return []

        data = {
            field_name: scraper_utils.get_value_from_html_tree(
                next_button, field_name, tag='name')
            for field_name in self._data_fields
        }

        action = next_button.xpath('attribute::action')[0]
        params = {
            'data': data,
            'endpoint': self.get_region().base_url + action,
            'task_type': constants.GET_MORE_TASKS,
        }

        return [params]

    def _get_person_params(self, content):
        result_list = content.xpath('//table[@id="dinlist"]/tr/td/form')

        params_list = []
        for row in result_list:
            data = {
                field_name: scraper_utils.get_value_from_html_tree(
                    row, field_name, tag='name')
                for field_name in self._data_fields
            }

            data[row.xpath('./div/input[@type="submit"]/@name')[0]] = \
                row.xpath('./div/input[@type="submit"]/@value')[0],

            action = row.xpath('attribute::action')[0]
            result_params = {
                'data': data,
                'endpoint': self.get_region().base_url + action,
                # Even though we are looking for data here, we might
                # get a disambiguation page, so we have to pretend we
                # get more tasks in case we do.
                'task_type': constants.GET_MORE_TASKS,
            }

            params_list.append(result_params)

        return params_list

    def _get_person_passthrough_params(self, content):
        params = {
            'endpoint': None,
            'content': html.tostring(content),
        }

        return params

    def populate_data(self, content, params, ingest_info):
        data_extractor = DataExtractor(self.mapping_filepath)
        ingest_info = data_extractor.extract_and_populate_data(content,
                                                               ingest_info)
        return ingest_info


    def scrape_disambiguation(self, page_tree, query_content,
                              scrape_type, ignore_list):
        """Scraped record disambiguation page for a person in DOCCS

        In attempting to fetch a person, the scrape_person received a
        disambig page - asking which incarceration event for that person they
        wanted. This function takes that result page, parses it, and enqueues a
        new task to scrape each entry from the disambig page.

        Args:
            page_tree: lxml.html parsed object of the disambig page
            query_content: (tuple) The query we're after, in the form:
                Background scrape: ("surname", "given names")
                Snapshot scrape:   ("record_id", ["records to ignore", ...])
            scrape_type: (string) Type of scrape we're performing
            ignore_list: (list of strings) Set of record IDs not to
                follow (set if in snapshot scrape, and some records
                are outside of our metrics / time period of interest
                window)

        Returns:
            Nothing if successful, -1 if fails.
        """
        # Create an ID to group these entries with - DOCCS doesn't
        # tell us how it groups these / give us a persistent ID for
        # persons, but we want to know each of the entries scraped
        # from this page were about the same person.
        group_id = scraper_utils.generate_id(UsNyPerson)
        new_tasks = []
        department_identification_numbers = []

        # We detect the end of a normal 'background' scrape by noting
        # that we're at the end of the alphabet. In the case of
        # snapshot scrapes, that won't be the case - so we add a
        # marker into one and only one of the person scrapes that
        # signifies we should kick off the next task.
        first_task = True if scrape_type == constants.SNAPSHOT_SCRAPE else False

        # Parse the results list
        result_list = page_tree.xpath('//div[@id="content"]/table/tr/td/form')

        if not result_list:
            logging.warning("Malformed person or disambig page, failing task "
                            "to re-queue.")
            self.stop_scrape_and_maybe_resume(scrape_type)
            return None

        for row in result_list:
            result_params = {}

            result_params['group_id'] = group_id

            # Parse result row and relevant form info to follow link
            result_params['action'] = row.xpath('attribute::action')[0]
            result_params['dini'] = row.xpath(
                './div/input[@name="M12_SEL_DINI"]/@value')[0]
            result_params['k01'] = row.xpath(
                './div/input[@name="K01"]/@value')[0]
            result_params['k02'] = row.xpath(
                './div/input[@name="K02"]/@value')[0]
            result_params['k03'] = row.xpath(
                './div/input[@name="K03"]/@value')[0]
            result_params['k04'] = row.xpath(
                './div/input[@name="K04"]/@value')[0]
            result_params['k05'] = row.xpath(
                './div/input[@name="K05"]/@value')[0]
            result_params['k06'] = row.xpath(
                './div/input[@name="K06"]/@value')[0]
            result_params['token'] = row.xpath(
                './div/input[@name="DFH_STATE_TOKEN"]/@value')[0]
            result_params['map_token'] = row.xpath(
                './div/input[@name="DFH_MAP_STATE_TOKEN"]/@value')[0]
            result_params['dinx_name'] = row.xpath(
                './div/input[@type="submit"]/@name')[0]
            result_params['dinx_val'] = row.xpath(
                './div/input[@type="submit"]/@value')[0]

            department_identification_numbers.append(result_params['dinx_val'])
            new_tasks.append(result_params)

        for task_params in new_tasks:
            # The disambig page produces far more rows than are
            # visible, each with a form element you can click -
            # programmatically, these look nearly identical to actual
            # result rows.
            #
            # The only differences are
            #   a) they have empty 'value' attr in the 'submit' element /
            #     (thus are empty / invisible in the UI), and
            #   b) they go back to the search results instead of
            #     listing another record.
            #
            # To avoid the latter mucking up our parsing, we test
            # dinx_val to skip these entries.

            if task_params['dinx_val']:

                dept_id_number = task_params['dinx_val']

                # Double-check that we haven't already processed this
                # entry while scraping another disambig page for this
                # same person
                current_session = sessions.get_open_sessions(
                    self.get_region().region_code, most_recent_only=True)

                if current_session is None:
                    # The session's been closed, we should peacefully
                    # end scraping
                    logging.info("No open sessions, exiting without creating "
                                 "new tasks for disambig page results.")
                    return None

                scraped_record = ScrapedRecord.query(ndb.AND(
                    ScrapedRecord.region == self.get_region().region_code,
                    ScrapedRecord.record_id == dept_id_number,
                    ScrapedRecord.created_on > current_session.start)).get()

                if scraped_record:
                    logging.info("We already scraped record %s, skipping.",
                                 dept_id_number)
                    continue

                if dept_id_number in ignore_list:
                    logging.info(
                        "Record %s outside of snapshot range, skipping.",
                        dept_id_number)
                    continue

                # Otherwise, schedule scraping it and add it to the list
                new_scraped_record = ScrapedRecord(
                    region=self.get_region().region_code,
                    record_id=dept_id_number)
                try:
                    new_scraped_record.put()
                except (Timeout, TransactionFailedError, InternalError):
                    logging.warning("Couldn't persist ScrapedRecord entry, "
                                    "record_id: %s", dept_id_number)

                # Set the next_docket_item to True for only one person page
                task_params['next_docket_item'] = first_task
                first_task = False

                # Enqueue task to follow that link / scrape record
                task_params['content'] = query_content
                task_params[
                    'linked_records'] = department_identification_numbers
                task_params['scrape_type'] = scrape_type
                self.add_task('scrape_person', task_params)

        return None

    @staticmethod
    def gather_details(page_tree, details_page):
        """Gathers all of the details about a person and their particular
        record into a dictionary.

        Returns a dictionary containing all of the information we will need
        to save entities in the database, gathered from the given HTML page
        tree.

        Args:
            page_tree: lxml.html parsed object of the person page
            details_page: lxml.html parsed div from the person page, which
                contains all of the actual data to scrape

        Returns:
            A dict containing all of the scraped fields, or -1 if we have any
            issue parsing the page tree, i.e. it has an unexpected structure.
        """
        person_details = {}

        # Create xpath selectors for the parts of the page we want to scrape
        id_and_locale_rows = details_page[0].xpath(
            "./table[contains(@summary, 'Identifying')]/tr")
        crimes_rows = details_page[0].xpath(
            "./table[contains(@summary, 'crimes')]/tr")
        sentence_rows = details_page[0].xpath(
            "./table[contains(@summary, 'sentence')]/tr")

        # Make sure the tables on the page are what we're expecting
        expected_first_row_id = "DIN (Department Identification Number)"
        expected_first_row_crimes = "Crime"
        expected_first_row_sentences = "Aggregate Minimum Sentence"

        if not id_and_locale_rows or not crimes_rows or not sentence_rows:
            # This isn't the page we were expecting
            logging.warning("Did not find expected tables on "
                            "person page. Page received: \n%s",
                            html.tostring(page_tree, pretty_print=True))
            return -1

        actual_first_row_id = (
            id_and_locale_rows[0].xpath("./td")[0].text_content().strip())
        actual_first_row_crimes = (
            crimes_rows[0].xpath("./th")[0].text_content().strip())
        actual_first_row_sentences = (
            sentence_rows[0].xpath("./td")[0].text_content().strip())

        if (actual_first_row_id != expected_first_row_id or
                actual_first_row_crimes != expected_first_row_crimes or
                actual_first_row_sentences != expected_first_row_sentences):

            # This isn't the page we were expecting
            logging.warning("Did not find expected content in tables on "
                            "person page. Page received: \n%s",
                            html.tostring(page_tree, pretty_print=True))
            return -1

        crimes = []

        # Capture table data from each details table on the page
        for row in id_and_locale_rows:
            row_data = row.xpath('./td')
            key, value = scraper_utils.normalize_key_value_row(
                row_data)
            person_details[key] = value

        for row in crimes_rows:
            row_data = row.xpath('./td')

            # One row is the table headers / has no <td> elements
            if not row_data:
                pass
            else:
                crime_description, crime_class = (
                    scraper_utils.normalize_key_value_row(row_data))
                crime = {'crime': crime_description,
                         'class': crime_class}

                # Only add to the list if row is non-empty
                if crime['crime']:
                    crimes.append(crime)

        for row in sentence_rows:
            row_data = row.xpath('./td')
            key, value = scraper_utils.normalize_key_value_row(
                row_data)
            person_details[key] = value

            person_details['crimes'] = crimes

        return person_details

    def create_person(self, person_details):
        """Creates a Person from the details scraped from the person page.

        Instantiates a UsNyPerson entity and sets its fields based on data
        in the given dict. If a person already exists with this person's
        proposed id, then we fetch that person from the database and update it.

        This method does not actually save the updated entity to the database.
        It is transient upon return.

        Args:
            person_details: (dict) all of the scraped details to save

        Returns:
            A UsNyPerson entity ready to be saved to the database.
        """
        department_identification_numbers = []
        if 'linked_records' in person_details:
            department_identification_numbers.extend(
                person_details['linked_records'])
        else:
            department_identification_numbers.append(
                person_details['DIN (Department Identification Number)'])

        old_id = self.link_person(department_identification_numbers)

        if old_id:
            person_id = old_id
        else:
            if 'group_id' in person_details:
                # If we find no prior records, use the group_id
                # generated earlier in this scraping session as the
                # person_id, so as to tie this record to the linked
                # ones for the same person.
                person_id = person_details['group_id']
            else:
                person_id = scraper_utils.generate_id(UsNyPerson)

        person_entity_id = self.get_region().region_code + person_id
        person = UsNyPerson.get_or_insert(person_entity_id)

        # Some pre-work to massage values out of the data
        person_name = person_details['Inmate Name'].split(', ')
        person_dob = person_details['Date of Birth']
        person_age = None
        if person_dob:
            person_dob = scraper_utils.parse_date_string(person_dob, person_id)
            if person_dob:
                person_age = scraper_utils.calculate_age(person_dob)
        person_sex = person_details['Sex'].lower()
        person_race = person_details['Race / Ethnicity'].lower()

        # NY-specific fields
        person.us_ny_person_id = person_id

        # General Person fields
        if person_dob:
            person.birthdate = person_dob
        if person_age:
            person.age = person_age
        if person_sex:
            person.sex = person_sex
        if person_race:
            person.race = person_race
        person.person_id = person_id
        person.person_id_is_fuzzy = True
        person.surname = person_name[0]
        if len(person_name) > 1:
            person_given_name = person_name[1]
        else:
            person_given_name = ""
        person.given_names = person_given_name
        person.region = self.get_region().region_code

        return person

    def create_record(self, person, person_key, person_details):
        """Creates a Record from the details scraped from the person page.

        Instantiates a UsNyRecord entity and sets its fields based on data
        in the given dict. If a record already exists with this records's
        proposed id, then we fetch that record from the database and update it.

        This returns a tuple. If a record did already exist with the proposed
        id, then it is returned in its previous state as the first entry in the
        tuple. Otherwise, the first entry in the tuple is a fresh record with
        no fields set. Regardless, the second entry is the fully updated record.

        This method does not actually save the updated entity to the database.
        It is transient upon return.

        Args:
            person: (UsNyPerson) the person created from the same details
            person_key: (ndb.Key) the NDB Key of the saved person entity,
                for establishing ancestry between Person and Record
            person_details: (dict) all of the scraped details to save

        Returns:
            A tuple of (old_record, record) as described above.
        """
        person_id = person.person_id

        record_id = person_details['DIN (Department Identification Number)']

        record_entity_id = self.get_region().region_code + record_id
        record = UsNyRecord.get_or_insert(record_entity_id, parent=person_key)

        # Capture the state of the record from before we updated it
        old_record = deepcopy(record)

        # Some pre-work to massage values out of the data
        last_custody = person_details['Date Received (Current)']
        last_custody = scraper_utils.parse_date_string(last_custody, person_id)
        first_custody = person_details['Date Received (Original)']
        first_custody = scraper_utils.parse_date_string(first_custody,
                                                        person_id)
        admission_type = person_details['Admission Type']
        county_of_commit = person_details['County of Commitment']
        custody_status = person_details['Custody Status']
        released = (custody_status != "IN CUSTODY")
        min_sentence = person_details['Aggregate Minimum Sentence']
        min_sentence = self.parse_sentence_duration(min_sentence,
                                                    person_id)
        max_sentence = person_details['Aggregate Maximum Sentence']
        max_sentence = self.parse_sentence_duration(max_sentence,
                                                    person_id)
        earliest_release_date = person_details['Earliest Release Date']
        earliest_release_date = scraper_utils.parse_date_string(
            earliest_release_date, person_id)
        earliest_release_type = person_details['Earliest Release Type']
        parole_hearing_date = person_details['Parole Hearing Date']
        parole_hearing_date = scraper_utils.parse_date_string(
            parole_hearing_date, person_id)
        parole_hearing_type = person_details['Parole Hearing Type']
        parole_elig_date = person_details['Parole Eligibility Date']
        parole_elig_date = scraper_utils.parse_date_string(parole_elig_date,
                                                           person_id)
        cond_release_date = person_details['Conditional Release Date']
        cond_release_date = scraper_utils.parse_date_string(cond_release_date,
                                                            person_id)
        max_expir_date = person_details['Maximum Expiration Date']
        max_expir_date = scraper_utils.parse_date_string(max_expir_date,
                                                         person_id)
        max_expir_date_parole = (
            person_details['Maximum Expiration Date for Parole Supervision'])
        max_expir_date_parole = scraper_utils.parse_date_string(
            max_expir_date_parole, person_id)
        max_expir_date_superv = (
            person_details['Post Release Supervision Maximum Expiration Date'])
        max_expir_date_superv = scraper_utils.parse_date_string(
            max_expir_date_superv, person_id)
        parole_discharge_date = person_details['Parole Board Discharge Date']
        parole_discharge_date = scraper_utils.parse_date_string(
            parole_discharge_date, person_id)
        scraped_facility = person_details['Housing / Releasing Facility']
        last_release = (
            person_details[
                'Latest Release Date / Type (Released Inmates Only)'])
        if last_release:
            release_info = last_release.split(" ", 1)
            last_release_date = scraper_utils.parse_date_string(
                release_info[0], person_id)
            last_release_type = release_info[1]
        else:
            last_release_date = None
            last_release_type = None

        record_offenses = []
        for crime in person_details['crimes']:
            crime = Offense(
                crime_description=crime['crime'],
                crime_class=crime['class'])
            record_offenses.append(crime)

        # us_ny specific fields
        record.us_ny_record_id = record_id

        if min_sentence:
            min_sentence_duration = SentenceDuration(
                life_sentence=min_sentence['Life'],
                years=min_sentence['Years'],
                months=min_sentence['Months'],
                days=min_sentence['Days'])
        else:
            min_sentence_duration = None

        if max_sentence:
            max_sentence_duration = SentenceDuration(
                life_sentence=max_sentence['Life'],
                years=max_sentence['Years'],
                months=max_sentence['Months'],
                days=max_sentence['Days'])
        else:
            max_sentence_duration = None

        # General Record fields
        record.admission_type = admission_type
        record.birthdate = person.birthdate
        record.cond_release_date = cond_release_date
        record.county_of_commit = county_of_commit
        record.custody_date = first_custody
        record.custody_status = custody_status
        record.earliest_release_date = earliest_release_date
        record.earliest_release_type = earliest_release_type
        record.is_released = released
        record.last_custody_date = last_custody
        if last_release:
            record.latest_release_date = last_release_date
            record.latest_release_type = last_release_type
        record.latest_facility = scraped_facility
        record.max_expir_date = max_expir_date
        record.max_expir_date_parole = max_expir_date_parole
        record.max_expir_date_superv = max_expir_date_superv
        record.max_sentence_length = max_sentence_duration
        record.min_sentence_length = min_sentence_duration
        record.parole_elig_date = parole_elig_date
        record.parole_discharge_date = parole_discharge_date
        record.parole_hearing_date = parole_hearing_date
        record.parole_hearing_type = parole_hearing_type
        if record_offenses:
            record.offense = record_offenses
        record.race = person.race
        record.record_id = record_id
        record.region = self.get_region().region_code
        record.sex = person.sex
        record.surname = person.surname
        record.given_names = person.given_names

        return old_record, record

    def store_record(self, person_details):
        """Store scraped data from a results page

        We've scraped an incarceration details page, and want to store
        the data we found. This function does some post-processing on
        the scraped data, and feeds it into the datastore in a way
        that can be indexed / queried in the future.

        Args:
            person_details: (dict) Key/value results parsed from the scrape

        Returns:
            Nothing if successful, -1 if fails.

        """
        person = self.create_person(person_details)
        person_id = person.person_id

        try:
            person_key = person.put()
        except (Timeout, TransactionFailedError, InternalError):
            # Datastore error - fail task to trigger queue retry + backoff
            logging.warning("Couldn't persist person: %s", person_id)
            return -1

        old_record, record = self.create_record(person,
                                                person_key,
                                                person_details)
        record_id = record.record_id

        try:
            record.put()
        except (Timeout, TransactionFailedError, InternalError):
            logging.warning("Couldn't persist record: %s", record_id)
            return -1

        new_snapshot = self.record_to_snapshot(record)
        self.compare_and_set_snapshot(old_record, new_snapshot)

        if 'group_id' in person_details:
            logging.info("Checked record for %s %s, person %s, in group %s, "
                         "for record %s.", person.given_names, person.surname,
                         person_id, person_details['group_id'], record_id)
        else:
            logging.info("Checked record for %s %s, person %s, (no group), for"
                         " record %s.", person.given_names, person.surname,
                         person_id, record_id)

        return None

    @staticmethod
    def parse_sentence_duration(term_string, person_id):
        """Converts string describing sentence duration to
        models.SentenceDuration

        For the 'Maximum Aggregate Sentence' and 'Minimum Aggregate Sentence'
        results, the scraped string often looks like one of these:

            "00 Years, 000 Months, 000 Days",
            "04 Years, 002 Months, 000 Days",
            "LIFE Years, 999 Months, 999 Days", etc.

        There is a bit of inconsistency on number of digits or exact string.
        This function takes the string, and turns it into a dictionary with
        year/month/day values and a 'Life Sentence' boolean.

        Args:
            term_string: (string) Scraped sentence duration string
            person_id: (string) Person ID this date is for, for logging

        Returns:
            dict of values -
                'Life' (bool) whether sentence is a life term,
                'Years' (int) # years
                'Months' (int) # months
                'Days' (int) # days
            Or None, if string is empty or not parsable.

        """
        if term_string.startswith("LIFE"):

            result = {'Life': True,
                      'Years': 0,
                      'Months': 0,
                      'Days': 0}

        else:
            parsed_nums = re.findall(r'\d+', term_string)

            if not term_string:
                return None
            elif (not parsed_nums) or (len(parsed_nums) < 3):
                logging.debug("Couldn't parse term string '%s' for person: %s",
                              term_string, person_id)
                return None
            else:
                years = int(parsed_nums[0])
                months = int(parsed_nums[1])
                days = int(parsed_nums[2])

                result = {'Life': False,
                          'Years': years,
                          'Months': months,
                          'Days': days}

        return result

    @staticmethod
    def link_person(record_list):
        """Checks for prior records matching newly scraped ones, returns
        person ID

        Matches DIN (record IDs) to previously scraped records, looks
        up associated persons, then returns that person_id so we can
        update the same person rather than duplicating them.

        Args:
            record_list: (list of strings) List of DINs (record IDs) to check

        Returns:
            The found person_id from prior instance, or None if not found

        """

        for linked_record in record_list:
            query = UsNyRecord.query(UsNyRecord.record_id == linked_record)
            result = query.get()
            if result:
                prior_person_key = result.key.parent()
                prior_person = prior_person_key.get()
                person_id = prior_person.person_id

                logging.info("Found an earlier record with a person ID %s,"
                             "using that.", person_id)
                return person_id

        return None

    def stop_scrape_and_maybe_resume(self, scrape_type):
        """Stops current scrape and schedules a resume if this scrape received
        an error before scraping all of the names.

        Args:
            scrape_type: The type of the current scrape session
        """
        if self.should_resume():
            deferred.defer(self.resume_scrape, scrape_type, _countdown=60)
        self.stop_scrape([scrape_type])

    def should_resume(self):
        """Determines whether the scrape session should be resumed.

        We didn't get the page we expected while retrieving a results, person,
        or disambiguation page. We first check if we've completed the alphabet.
        On the last page, when we click 'Next 4 results', DOCCS just takes the
        user back to the main search page (which could be why trying to parse it
        like a result page failed). If we did, we just shut down the scraper
        because this is success.

        If not, we assume DOCCS has lost state and no longer knows
        what we're asking for. In this case we clean up the current
        scraping session, purge all other tasks in the queue, and kick
        off a new scraping session to get new state in DOCCS to
        continue scraping using.

        Returns:
            True if scrape should be resumed
            False otherwise
        """
        # This is a hacky check for whether we finished the
        # alphabet. The last name in DOCCS as of 11/13/2017 is
        # 'ZYTEL', who's sentenced to life and has no other crimes
        # / disambig.
        last_scraped = self.get_last_scraped(self.get_region().region_code)

        # TODO (#113): we need a more robust method for detecting
        # the end of the roster.
        if last_scraped and last_scraped[0:3] < "ZYT":

            # We haven't finished the alphabet yet. Most likely,
            # we're failing repeatedly because the server has lost
            # state (e.g., went through a maintenance period). End
            # current scrape / purge tasks, start again from where
            # we left off.
            logging.warning(
                "Server has lost state. Kicking off new scrape "
                "task for last name seen in results, and removing "
                "other tasks with old state.")
            return True

        # We've run out of names, and the 'Next 4 results'
        # button dumped us back at the original search
        # page. Log it, and end the query.

        # Note: In another region (where we search for names
        #       one-by-one), we'd iterate the background
        #       scrape docket item here.
        logging.info("Looped all results. Ending scraping session.")
        return False


    @staticmethod
    def get_last_scraped(region_code):
        """Gets the last person scraped during this scrape session.

        If the current session does not have a last scraped set, then returns
        the last scraped from the most recent session with one set.

        Args:
            region_code: Code for current region

        Returns:
            None if no open sessions
            Last scraped otherwise
        """
        current_session = sessions.get_open_sessions(
            region_code, most_recent_only=True)
        if current_session:
            last_scraped = current_session.last_scraped
            scrape_type = current_session.scrape_type
        else:
            logging.error(
                "No open sessions found! Bad state, ending scrape.")
            return None

        if not last_scraped:
            logging.error(
                "Session isn't old enough to have a last_scraped "
                "name yet, but no search results are coming back. "
                "Finding last scraped name from earlier session.")

            # Get most recent sessions, including closed ones, and find
            # the last one to have a last_scraped name in it. These will
            # come back most-recent-first.
            recent_sessions = sessions.get_recent_sessions(
                ScrapeKey(region_code, scrape_type))

            for session in recent_sessions:
                if session.last_scraped:
                    last_scraped = session.last_scraped
                    break
        return last_scraped


    def person_id_to_record_id(self, person_id):
        """Convert provided person_id to record_id of any record for that person

        The general snapshot logic creates dockets of person IDs to snapshot,
        but in the us_ny case this means 'fuzzy' person IDs, which were
        generated as a substitute for anything state-provided. Since we can't
        search DOCCS with those, we need to convert them to record IDs instead,
        which DOCCS does allow querying by.

        We only need one DOCCS record ID per person ID, because DOCCS
        will take any record ID query to the disambiguation page for
        that person if they have more records than just the one
        searched for.

        Args:
            person_id: (string) Person ID for the person

        Returns:
            None if query returns None
            Record ID if a record is found for the person in the docket item

        """
        person = UsNyPerson.query(UsNyPerson.person_id == person_id).get()
        if not person:
            return None

        record = UsNyRecord.query(ancestor=person.key).get()
        if not record:
            return None

        return record.record_id

    @staticmethod
    def record_to_snapshot(record):
        """Mirrors record fields into a Snapshot instance

        Takes in a new Record entity, and mirrors its fields into a
        Snapshot entity for comparison against the last-collected
        snapshot entity for this Record.

        Args:
            record: A Record object to mirror

        Returns:
            A Snapshot entity populated with the same details as the Record

        """
        snapshot = Snapshot(
            parent=record.key,
            admission_type=record.admission_type,
            birthdate=record.birthdate,
            cond_release_date=record.cond_release_date,
            county_of_commit=record.county_of_commit,
            custody_date=record.custody_date,
            custody_status=record.custody_status,
            earliest_release_date=record.earliest_release_date,
            earliest_release_type=record.earliest_release_type,
            is_released=record.is_released,
            last_custody_date=record.last_custody_date,
            latest_facility=record.latest_facility,
            latest_release_date=record.latest_release_date,
            latest_release_type=record.latest_release_type,
            max_expir_date=record.max_expir_date,
            max_expir_date_parole=record.max_expir_date_parole,
            max_expir_date_superv=record.max_expir_date_superv,
            max_sentence_length=record.max_sentence_length,
            min_sentence_length=record.min_sentence_length,
            offense=record.offense,
            parole_discharge_date=record.parole_discharge_date,
            parole_elig_date=record.parole_elig_date,
            parole_hearing_date=record.parole_hearing_date,
            parole_hearing_type=record.parole_hearing_type,
            race=record.race,
            region=record.region,
            sex=record.sex,
            surname=record.surname,
            given_names=record.given_names)

        return snapshot
