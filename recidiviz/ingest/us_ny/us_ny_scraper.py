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
    - So we start with a search for a single 'a', which will return all the
      people in the system
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

import copy
import logging
import os

from lxml import html

from recidiviz.common.constants.charge import ChargeClass
from recidiviz.common.constants.charge import ChargeDegree
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.ingest import constants
from recidiviz.ingest import scraper_utils
from recidiviz.ingest.base_scraper import BaseScraper
from recidiviz.ingest.extractor.data_extractor import DataExtractor


class UsNyScraper(BaseScraper):
    """Class to scrape info from the NY state DOCCS searcher.
    """

    TOKEN_NAME = 'DFH_STATE_TOKEN'

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

        self._first_search_page_base_data = {
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
        }

        super(UsNyScraper, self).__init__('us_ny')

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
                    self._get_store_person_params(content))
            params_list.extend(self._get_next_page_params(content))

        # Add session variables to the post data of each params dict.
        session_vars = self._get_session_vars(content)

        for next_param in params_list:
            if 'post_data' in next_param:
                next_param['post_data'].update(session_vars)

        return params_list

    def _get_session_vars(self, content):
        """Returns the session variables contained in a webpage.
        Args:
            content: (html tree) a webpage with session variables
        Returns:
            A dict containing just the session variables from the page.
        """
        session_vars = {
            UsNyScraper.TOKEN_NAME: scraper_utils.get_value_from_html_tree(
                content, UsNyScraper.TOKEN_NAME, attribute_name='name'),
        }

        return session_vars

    def _get_post_data(self, html_tree):
        """Extracts information from an html tree and places it in a
        dictionary to be used as data in a POST.

        Args:
            html_tree: (html_tree) HTML content containing keys in
                self._data_fields
        Returns:
            A dict with keys self._data_fields and values extracted from the
            html_tree.
        """
        data = {
            field_name: scraper_utils.get_value_from_html_tree(
                html_tree, field_name, attribute_name='name')
            for field_name in self._data_fields
        }

        return data

    def _get_first_search_page_params(self, content):
        """Returns the parameters needed to fetch the initial search page.
        Args:
            content: (html tree) a webpage with the search form on it.
        Returns:
            A dict containing the params necessary to fetch the first search
            page.
        """
        post_data = copy.copy(self._first_search_page_base_data)
        post_data['K01'] = scraper_utils.get_value_from_html_tree(
            content, 'K01', attribute_name='name')

        action = content.xpath('//div[@id="content"]/form/@action')[0]

        params = {
            'post_data': post_data,
            'endpoint': self.get_region().base_url + action,
            'task_type': constants.GET_MORE_TASKS,
        }

        return params

    def _get_next_page_params(self, content):
        """Returns the parameters needed to fetch the next search page.
        Args:
            content: (html tree) a webpage with the 'next' button on it.
        Returns:
            A dict containing the params necessary to fetch the next search
            page.
        """

        try:
            next_button = content.xpath('//div[@id="content"]/form')[-1]
        except IndexError:
            # This should only occur when we've gone beyond the last
            # page and hit the first search page again.
            logging.info("End of search results reached, no further pages "
                         "to search")
            return []

        action = next_button.xpath('attribute::action')[0]
        params = {
            'post_data': self._get_post_data(next_button),
            'endpoint': self.get_region().base_url + action,
            'task_type': constants.GET_MORE_TASKS,
        }

        return [params]

    def _get_person_params(self, content, params):
        """Returns the parameters needed to fetch the person details page.
        Args:
            content: (html tree) a webpage with a table with person links on
            it. Note that this page could be either the normal search page, or
            the person disambiguation page.
        Returns:
            A dict containing the params necessary to fetch the person detail
            page.
        """
        # on the search results page, the table has an id
        result_list = content.xpath('//table[@id="dinlist"]/tr/td/form')

        # on the disambiguation page, there's no name but the table is first.
        if not result_list and 'din' in params:
            din = params['din']

            # Find the particular DIN we were after on this click.
            result_list = [
                res for res in
                content.xpath('//table/tr/td[@headers="din"]/form')
                if res.xpath('div/input[@type="submit"]')[0].value == din
            ]

        params_list = []
        for row in result_list:
            data = self._get_post_data(row)

            # special case for navigating away from a disambiguation page.
            if 'din' in params:
                data['M12_SEL_DINI'] = params['din']

            submit_name = row.xpath('./div/input[@type="submit"]/@name')[0]
            submit_value = row.xpath('./div/input[@type="submit"]/@value')[0]
            data[submit_name] = submit_value

            action = row.xpath('attribute::action')[0]
            result_params = {
                'post_data': data,
                'endpoint': self.get_region().base_url + action,
                'din': data['M13_SEL_DINI'],
                # Even though we are looking for data here, we might
                # get a disambiguation page, so we have to pretend we
                # get more tasks in case we do.
                'task_type': constants.GET_MORE_TASKS,
            }

            params_list.append(result_params)

        return params_list

    def _get_store_person_params(self, content):
        """Returns the parameters needed to store the person details.
        Args:
            content: (html tree) a webpage with the person details.
        Returns:
            A dict containing the params necessary to store the person info.
        """
        params = {
            'endpoint': None,
            'content': html.tostring(content),
            'task_type': constants.SCRAPE_DATA,
        }

        return params

    def populate_data(self, content, params, ingest_info):
        """Extracts data from the content passed into an ingest_info object.
        Args:
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

        if len(ingest_info.person) != 1:
            logging.error("Data extraction did not produce a single person, "
                          "as it should")
            return None

        if len(ingest_info.person[0].booking) != 1:
            logging.error("Data extraction did not produce a single booking, "
                          "as it should")
            return None

        # Get release date, if released
        if (ingest_info.person[0].booking[0].custody_status == 'RELEASED' or
                ingest_info.person[0].booking[0].custody_status ==
                'DISCHARGED'):
            release_date_string = content.xpath(
                '//*[@headers="t1l"]')[0].text.split()[0]
            ingest_info.person[0].booking[0].release_date = release_date_string

        # Parse charge information
        for row in content.xpath('//*[@id="ii"]/table[2]/tr')[1:]:
            charge_name = row.xpath('td[1]')[0].text.strip().rstrip()

            if not charge_name:
                break

            # Change 'class' into 'level'
            charge_class_str = row.xpath('td[2]')[0].text
            charge_level = None
            if charge_class_str.startswith('A'):
                charge_level = 'ONE'
            elif charge_class_str.startswith('B'):
                charge_level = 'TWO'
            elif charge_class_str.startswith('C'):
                charge_level = 'THREE'
            elif charge_class_str.startswith('D'):
                charge_level = 'FOUR'
            elif charge_class_str.startswith('E'):
                charge_level = 'FIVE'

            # Get the degree
            charge_degree = charge_name.split()[-1]
            try:
                _ = ChargeDegree.from_str(charge_degree)
                charge_name = ' '.join(charge_name.split()[:-1])
            except KeyError:
                charge_degree = None

            # Get whether the charge was an attempt
            attempted = 'False'
            if charge_name.lower().startswith('att'):
                attempted = 'True'

            ingest_info.person[0].booking[0].create_charge(
                attempted=attempted,
                charge_class=ChargeClass.FELONY,
                degree=charge_degree,
                level=charge_level,
                name=charge_name.strip(),
                status=ChargeStatus.SENTENCED,
            )

        return ingest_info
