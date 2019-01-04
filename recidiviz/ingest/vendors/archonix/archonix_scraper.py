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


"""Scraper implementation for the Archonix vendor. This handles all Archonix
specific navigation and data extraction. All counties that use Archonix should
have their county-specific scrapers inherit from this class.

Vendor-specific notes:
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
from recidiviz.ingest.base_scraper import BaseScraper
from recidiviz.ingest.extractor.data_extractor import DataExtractor


class ArchonixScraper(BaseScraper):
    """Scraper for counties using Archonix."""

    def __init__(self, region_name, yaml_file):
        super(ArchonixScraper, self).__init__(region_name)

        self.yaml_file = yaml_file
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
        data = {session_var: scraper_utils.get_value_from_html_tree(
            content, session_var) for session_var in
                self._required_session_vars}
        # Viewstate is much large, compress it before sending it to the queue.
        data['__VIEWSTATE'] = scraper_utils.compress_string(
            data['__VIEWSTATE'], level=9)
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
            'post_data': self._retrieve_session_vars(content),
            # The endpoint which is to be hit next scrape.
            'endpoint': self._initial_endpoint,
            'task_type': constants.GET_MORE_TASKS,
        }
        # Set the age_from search to 0.  This returns all people.
        params['post_data'].update(self._all_people_search)
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
            'post_data': self._retrieve_session_vars(content),
            'endpoint': self._initial_endpoint,
            'task_type': constants.GET_MORE_TASKS,
        }
        # Update with the data needed to search for 50 people at once.
        params['post_data'].update(self._people_50_search)
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
                'task_type': constants.SCRAPE_DATA,
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
                'post_data': self._retrieve_session_vars(content),
                'endpoint': self._initial_endpoint,
                'task_type': constants.GET_MORE_TASKS
            }
            # Effectively this extracts out the next page button to send to
            # the post.
            params['post_data']['__EVENTTARGET'] = content.cssselect(
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
        page_size = scraper_utils.get_value_from_html_tree(
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

    def populate_data(self, content, params, ingest_info):
        """
        Populates the ingest info object from the content and params given

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.
            ingest_info: The IngestInfo object to populate
        """
        data_extractor = DataExtractor(self.yaml_file)
        data_extractor.extract_and_populate_data(content, ingest_info)
        return ingest_info



    def transform_post_data(self, data):
        """If the child needs to transform the data in any way before it sends
        the request, it can override this function.

        Args:
            data: dict of parameters to send as data to the post request.
        """
        compression_key = '__VIEWSTATE'
        if data and compression_key in data:
            data[compression_key] = scraper_utils.decompress_string(
                data[compression_key])

    def _get_scraped_value(self, content, scrape_id):
        """Convenience function to get a scraped value from a row.

        Args:
            content: An lxml html tree.
            scrape_id: the html id to scrape out of the page

        Returns:
            The value scraped from the page.
        """
        html_elements = content.cssselect('[id={}]'.format(scrape_id))
        if html_elements:
            text = html_elements[0].text_content()
            return ''.join(text.split()).split(':')[1]
        return None

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
