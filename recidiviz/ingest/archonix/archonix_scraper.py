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


"""Scraper implementation for Archonix

Region-specific notes:
    - No historical data is kept, only data on current people
    - Allows for surname only search
    - Archonix allows an age range search so if we search for age 0,
      it returns all people
    - Can search by person number, however there is also a URL parameter
      that takes us straight to the
      person and booking page, this parameter is ReferenceID
    - The person page contains the current booking info


Background scraping procedure:
    1. A starting home page GET
      (to get VIEWSTATE and EVENTVALIDATION which is needed) --> (search, see 2)
    2. A primary search with age from set to 0, this gets us a list of people
      (10x/page) --> (search, 3)
    3. A secondary search requesting 50 results per page.  It must be done in
       this order because a proper VIEWSTATE is needed, unfortunately cannot
       send a single query and get 50 results, but this is still
       less total queries to get all results. --> (parse, see 3a to 3c)
         (3a) A list of person results --> (follow, each entry leads to 3)
         (3b) The 'next page' of results for the query --> (follow it, see 4)
         (3c) When no more pages, end scrape
    4. A details page for the person, and details about the specific
       incerceration event
"""

import re

from recidiviz.ingest import constants
from recidiviz.ingest import scraper_utils
from recidiviz.ingest.generic_scraper import GenericScraper
from recidiviz.ingest.archonix.archonix_person import ArchonixPerson
from recidiviz.ingest.archonix.archonix_record import ArchonixRecord
from recidiviz.models.record import Offense


class ArchonixScraper(GenericScraper):
    """Scraper for counties using Archonix."""

    def __init__(self, region_name):
        super(ArchonixScraper, self).__init__(region_name)

        self._base_endpoint = self.get_region().base_url
        self._front_url = 'Default.aspx'
        self._initial_endpoint = '/'.join(
            [self._base_endpoint, self._front_url])
        self._required_session_vars = ['__VIEWSTATE', '__VIEWSTATEGENERATOR',
                                       '__EVENTVALIDATION']
        # Data params needed to return all people.
        self._all_people_search = {
            '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$btnBkSearch',
            'ctl00_ContentPlaceHolder1_txtBkAgeFrom_ClientState':
                '{"enabled":true,"emptyMessage":"","validationText":"0",'
                '"valueAsString":"0","lastSetTextBoxValue":"0"}'
        }
        # Data needed to search for 50 people.
        self._people_50_search = {
            '__EVENTTARGET': 'ctl00$ContentPlaceHolder1$gridResults',
            '__EVENTARGUMENT': 'FireCommand:ctl00$ContentPlaceHolder1'
                               '$gridResults$ctl00;PageSize;50',
            'ctl00_ContentPlaceHolder1_gridResults_ctl00_'
            'ctl03_ctl01_PageSizeComboBox_ClientState':
                '{"logEntries":[],"value":"50","text":"50","enabled":true,'
                '"checkedIndices":[],"checkedItemsTextOverflows":false}'

        }
        self._page_size_id = (
            'ctl00_ContentPlaceHolder1_gridResults_ctl00_'
            'ctl03_ctl01_PageSizeComboBox_Input')
        self._total_pages_regex = r'[0-9].*? items in ([0-9].*?) page'

    def _retrieve_session_vars(self, content):
        """Gets the variables necessary to complete requests.

        Args:
            content: An lxml html tree.

        Returns:
            A dict of session vars needed for the next scrape.
        """
        data = {session_var: scraper_utils.get_id_value_from_html_tree(
            content, session_var) for session_var in
            self._required_session_vars}
        # Viewstate is much large, compress it before sending it to the queue.
        # Note that we encode the compressed string to base64 so we can happily
        # dump the json.
        data['__VIEWSTATE'] = scraper_utils.compress_string(
            data['__VIEWSTATE'], level=9).encode('base64')
        return data

    def _get_all_people_params(self, content):
        """Returns the params needed to search for all people.

        Args:
            content: An lxml html tree.

        Returns:
            dictionary of data needed for the next scrape session.
        """
        params = {
            # Gets the session vars needed for this request.
            'data': self._retrieve_session_vars(content),
            # The endpoint which is to be hit next scrape.
            'endpoint': self._initial_endpoint,
            'task_type': constants.GET_MORE_TASKS,
        }
        # Set the age_from search to 0.  This returns all people.
        params['data'].update(self._all_people_search)
        return params

    def _get_page_size_50_params(self, content):
        """Returns the params needed to expand the search to 50 people per page
        instead of 10.  The site requires a correct viewstate for this to
        work hence why we don't just send this in one initial scrape session.

        Args:
            content: An lxml html tree.

        Returns:
            dictionary of data needed for the next scrape session.
        """
        params = {
            'data': self._retrieve_session_vars(content),
            'endpoint': self._initial_endpoint,
            'task_type': constants.GET_MORE_TASKS,
        }
        # Update with the data needed to search for 50 people at once.
        params['data'].update(self._people_50_search)
        return params

    def _get_person_params(self, content):
        """Returns the params needed to open the page of a specific people.

        Args:
            content: An lxml html tree.

        Returns:
            a list of dictionary data needed for the next scrape session.
        """
        params_list = []
        grid_results = content.cssselect(
            '[id=ctl00_ContentPlaceHolder1_gridResults_ctl00]')[0]
        table_body = grid_results.find('tbody')
        for row in table_body:
            href = row.find('td').find('a').get('href')
            endpoint = '/'.join([self._base_endpoint, href])
            # Extract out the reference and inmate id.
            ref_id_start = href.index('ReferenceID=') + len('ReferenceID=')
            ref_id_end = href.index('&', ref_id_start)
            inmate_id_start = href.index('InmateID=') + len('InmateID=')
            params = {
                'endpoint': endpoint,
                'task_type': constants.SCRAPE_PERSON_AND_RECORD,
                'inmate_id': href[inmate_id_start:],
                'reference_id': href[ref_id_start:ref_id_end],
            }
            params_list.append(params)
        return params_list

    def _get_next_page_if_exists(self, content):
        """Returns the params needed to scrape the next page of results.

        Args:
            content: An lxml html tree.

        Returns:
            dictionary of data needed for the next scrape session.
        """
        params_list = []
        current_page = content.cssselect(
            '[class=rgCurrentPage]')[0].text_content()
        total_pages = re.findall(
            self._total_pages_regex, content.text_content())[0]
        # There is a next page.  Lets scrape it.
        if current_page != total_pages:
            params = {
                'data': self._retrieve_session_vars(content),
                'endpoint': self._initial_endpoint,
                'task_type': constants.GET_MORE_TASKS
            }
            # Effectively this extracts out the next page button to send to
            # the post.
            params['data']['__EVENTTARGET'] = content.cssselect(
                'div.rgArrPart2')[0].find('input').get('name')
            params_list.append(params)
        return params_list

    def get_more_tasks(self, content, params):
        """
        Gets more tasks based on the content and params passed in.  This
        function should determine which task params, if any, should be
        added to the queue

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A list of params containing endpoint and task_type at minimum.
        """
        task_type = params.get('task_type', self.get_initial_task_type())
        params_list = []
        page_size = scraper_utils.get_id_value_from_html_tree(
            content, self._page_size_id)
        # If it is our first task, we know the next task must be a query to
        # return all people
        if self.is_initial_task(task_type):
            params_list.append(self._get_all_people_params(content))
        # In this case, we need to return more people so scraping can go faster.
        elif self.should_get_more_tasks(task_type) and page_size == '10':
            params_list.append(self._get_page_size_50_params(content))
        # We can start sending people to be scraped.
        elif self.should_get_more_tasks(task_type) and page_size == '50':
            # First find all people we need to scrape on this page.
            params_list.extend(self._get_person_params(content))
            # Next check to see if there is another page we can scrape.
            params_list.extend(self._get_next_page_if_exists(content))
        return params_list

    def transform_data(self, data):
        """If the child needs to transform the data in any way before it sends
        the request, it can override this function.

        Args:
            data: dict of parameters to send as data to the post request.
        """
        compression_key = '__VIEWSTATE'
        if data and compression_key in data:
            data[compression_key] = scraper_utils.decompress_string(
                data[compression_key].decode('base64'))

    def _get_scraped_value(self, content, scrape_id):
        """Convenience function to get a scraped value from a row.

        Args:
            content: An lxml html tree.
            scrape_id: the html id to scrape out of the page

        Returns:
            The value scraped from the page.
        """
        text = content.cssselect(
            '[id={}]'.format(scrape_id))[0].text_content()
        return ''.join(text.split()).split(':')[1]

    # pylint:disable=unused-argument

    def get_initial_endpoint(self):
        """Returns the initial endpoint to hit on the first call
        Returns:
            A string representing the initial endpoint to hit
        """
        return self._initial_endpoint

    def set_initial_vars(self, content, params):
        """
        Sets initial vars in the params that it will pass on to future scrapes

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.
        """
        pass

    def get_person_class(self):
        """Returns the person subclass to use for this scraper.

        Returns:
            A class representing the person DB object.
        """
        return ArchonixPerson

    def person_id_is_fuzzy(self):
        """Returns whether or not this scraper generates person ids

        Returns:
            A boolean representing whether or not the person id is fuzzy.
        """
        return False

    def record_id_is_fuzzy(self):
        """Returns whether or not this scraper generates record ids

        Returns:
            A boolean representing whether or not the record id is fuzzy.
        """
        return False

    def get_person_id(self, content, params):
        """Gets person id given a page

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A string representing the persons id
        """
        return self._get_scraped_value(
            content, 'ctl00_ContentPlaceHolder1_spnInmateNo')

    def get_given_names(self, content, params):
        """Gets the persons given names from content and params that are
        passed in.

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A string representing the persons given name
        """
        full_name = content.cssselect(
            '[id=ctl00_ContentPlaceHolder1_spnInmateName]')[
            0].text_content().strip()
        return full_name.split(',')[1].strip()

    def get_surname(self, content, params):
        """Gets the persons surname.

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A string representing the persons surname
        """
        full_name = content.cssselect(
            '[id=ctl00_ContentPlaceHolder1_spnInmateName]')[
            0].text_content().strip()
        full_name.split(',')[0].strip()
        last_name = full_name.split(',')[0].strip()

        return last_name

    def get_birthdate(self, content, params):
        """Gets person birthday given a page

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A datetime representing the persons birthdate
        """
        birthdate = self._get_scraped_value(
            content, 'ctl00_ContentPlaceHolder1_spnBirthDate')
        return scraper_utils.parse_date_string(birthdate)

    def get_age(self, content, params):
        """Gets person age given a page

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            An integer representing the persons age
        """
        age_string = self._get_scraped_value(
            content, 'ctl00_ContentPlaceHolder1_spnAge')
        return int(age_string)

    def get_sex(self, content, params):
        """Gets person sex given a page

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A string representing the persons sex
        """
        return self._get_scraped_value(
            content, 'ctl00_ContentPlaceHolder1_spnGender')

    def get_race(self, content, params):
        """Gets person race given a page

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A string representing the persons race
        """
        return self._get_scraped_value(
            content, 'ctl00_ContentPlaceHolder1_spnRace')

    def populate_extra_person_params(self, content, params, person):
        """Populates any extra params for a person.

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.
            person: The person object to populate.
        """
        person.archonix_inmate_id = params['inmate_id']

    def get_record_id(self, content, params):
        """Gets record id given a page

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A string representing the persons record id
        """
        return self._get_scraped_value(
            content, 'ctl00_ContentPlaceHolder1_spnBookingNo')

    def get_record_class(self):
        """Returns the record subclass to use for this scraper.

        Returns:
            The class representing the record DB object.
        """
        return ArchonixRecord

    def get_custody_date(self, content, params):
        """Gets custody date given a page

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A datetime representing the persons custody date
        """
        date_str = self._get_scraped_value(
            content, 'ctl00_ContentPlaceHolder1_spnBookingDtTime')
        return scraper_utils.parse_date_string(date_str[0:10])

    def get_custody_status(self, content, params):
        """Gets the record custody status given a page.

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A string representing the persons custody status.
        """
        return self._get_scraped_value(
            content, 'ctl00_ContentPlaceHolder1_spnCustodyStatus')

    def get_is_released(self, content, params):
        """Gets whether or not the person is released.

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A bool representing whether the person is released.
        """
        # This county only shows record of people that are currently in jail,
        # so if we got here they are not released.
        return False

    def get_committed_by(self, content, params):
        """Gets the name of the entity that committed the person.

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A string representing the name of the entity that committed the
            person
        """

        return self._get_scraped_value(
            content, 'ctl00_ContentPlaceHolder1_spnHoldForAgency')


    def populate_extra_record_params(self, content, params, record):
        """Populates any extra params for a record.

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.
            record: the record DB object to populate.
        """
        record.reference_id = params['reference_id']

    def get_offenses(self, content, params):
        """Gets the list of offenses relevant to the record.

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A list of Offense objects for the record.
        """
        offenses = []
        charges = content.cssselect(
            '[id=ctl00_ContentPlaceHolder1_gridCharges_ctl00]')[0]
        body = charges.find('tbody')
        for row in body:
            offense = Offense()
            offense.crime_class = row[0].text_content()
            offense.description = row[1].text_content()
            offense.case_number = row[2].text_content()
            # Note that row[3] and row[4] represent the offense date and arrest
            # date, But Archonix leaves this field blank.
            bond_str = row[5].text_content()
            # If there is no bond, it is filled with this character.
            if bond_str.isspace():
                # Bond is written in this form: $15,000.00
                offense.bond_amount = scraper_utils.currency_to_float(bond_str)
            offenses.append(offense)
        return offenses
