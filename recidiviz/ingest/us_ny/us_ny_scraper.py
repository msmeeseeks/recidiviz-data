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
    - DOCCS attempts to de-duplicate people, and show a disambiguation page
      for multiple records it believes to be of the same person.

Roster scraping procedure:
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
            person_params = self._get_person_params(content, params)
            if person_params:
                params_list.extend(person_params)
            else:
                params_list.append(
                    self._get_person_passthrough_params(content))
            params_list.extend(self._get_next_page_params(content))

        # Add session variables to the post data of each params dict.
        session_vars = self._get_session_vars(content)
        for one_params in params_list:
            if 'data' in one_params:
                one_params['data'].update(session_vars)

        return params_list

    def _get_session_vars(self, content):
        """Returns the session variables contained in a webpage.
        Arguments:
            content: (html tree) a webpage with session variables
        Returns:
            A dict containing just the session variables from the page.
        """
        session_vars = {
            'DFH_STATE_TOKEN': scraper_utils.get_value_from_html_tree(
                content, 'DFH_STATE_TOKEN', tag='name'),
        }

        return session_vars

    def _get_first_search_page_params(self, content):
        """Returns the parameters needed to fetch the initial search page.
        Arguments:
            content: (html tree) a webpage with the search form on it.
        Returns:
            A dict containing the params necessary to fetch the first search
            page.
        """
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
        """Returns the parameters needed to fetch the next search page.
        Arguments:
            content: (html tree) a webpage with the 'next' button on it.
        Returns:
            A dict containing the params necessary to fetch the next search
            page.
        """

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

    def _get_person_params(self, content, params):
        """Returns the parameters needed to fetch the person details page.
        Arguments:
            content: (html tree) a webpage with a table with person links on
            it. Note that this page could b either the normal search page, or
            the person disambiguation page.
        Returns:
            A dict containing the params necessary to fetch the person detail
            page.
        """
        # on the search results page, the table has an id
        result_list = content.xpath('//table[@id="dinlist"]/tr/td/form')

        # on the disambiguation page, there's no name but the table is first.
        if not result_list:
            din = params['din']

            # Find the particular DIN we were after on this click.
            result_list = [
                res for res in
                content.xpath('//table/tr/td[@headers="din"]/form')
                if res.xpath('div/input[@type="submit"]')[0].value == din
            ]

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
                'din': data['M13_SEL_DINI'],
                # Even though we are looking for data here, we might
                # get a disambiguation page, so we have to pretend we
                # get more tasks in case we do.
                'task_type': constants.GET_MORE_TASKS,
            }

            params_list.append(result_params)

        return params_list

    def _get_person_passthrough_params(self, content):
        """Returns the parameters needed to store the person details.
        Arguments:
            content: (html tree) a webpage with the person details.
        Returns:
            A dict containing the params necessary to store the person info.
        """
        params = {
            'endpoint': None,
            'content': html.tostring(content),
        }

        return params

    def populate_data(self, content, params, ingest_info):
        """Extracts data from the content passed into an ingest_info object.
        Arguments:
            content: (html tree) a webpage with the person details.
            params: (dict) parameters sent to the last task.
            ingest_info: (ingest_info object) and ingested info about this
                person from prior tasks
        Returns:
            A completely filled in ingest_info object.
        """
        data_extractor = DataExtractor(self.mapping_filepath)
        ingest_info = data_extractor.extract_and_populate_data(content,
                                                               ingest_info)
        return ingest_info
