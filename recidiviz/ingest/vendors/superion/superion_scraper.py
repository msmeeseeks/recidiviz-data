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


"""Scraper implementation for the Superion vendor. This handles all Superion
specific navigation and data extraction. All counties that use Superion should
have their county-specific scrapers inherit from this class.

Vendor-specific notes:
    - No historical data is kept, only data on current people
    - An initial request can be made to retrieve a page with all roster entries
    - The person page contains the current booking info

Background scraping procedure:
    1. A get without session variables to find the total number of people to
       be scraped.
    2. A home page GET to retrieve session variables.
    3. A request for each person.
"""

import copy
import json
import logging
import os
from typing import Optional

from recidiviz.ingest import constants
from recidiviz.ingest import scraper_utils
from recidiviz.ingest.base_scraper import BaseScraper
from recidiviz.ingest.extractor.html_data_extractor import HtmlDataExtractor
from recidiviz.ingest.models.ingest_info import IngestInfo


class SuperionScraper(BaseScraper):
    """Scraper for counties using Superion."""

    def __init__(self, region_name):
        key_mapping_file = 'superion.yaml'
        self.yaml_file = os.path.join(
            os.path.dirname(__file__), key_mapping_file)

        super(SuperionScraper, self).__init__(region_name)

        self._base_endpoint = self.get_region().base_url

        self._session_url = 'jailinmates.aspx'
        self._session_endpoint = '/'.join(
            [self._base_endpoint, self._session_url])

        self._search_url = 'jqHandler.ashx?op=s'
        self._search_endpoint = '/'.join(
            [self._base_endpoint, self._search_url])

        self._required_session_vars = ['__VIEWSTATE', '__VIEWSTATEGENERATOR',
                                       '__EVENTVALIDATION']

        self._person_index_param = (
            'ctl00$MasterPage$mainContent$CenterColumnContent$hfRecordIndex')
        self._detail_indicator_field = (
            'ctl00$MasterPage$mainContent$CenterColumnContent$btnInmateDetail')

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

        return data

    def _get_num_people_params(self, content, params):
        """Returns the params needed to start scraping people.

        Args:
            content: An lxml html tree.
            params: Parameters sent to the last task.

        Returns:
            a dictionary with data needed to track people scraping tasks.

        """
        json_content = json.loads(content.text)
        num_people = int(json_content['records'])

        params = {
            'endpoint': self._session_endpoint,
            'task_type': constants.GET_MORE_TASKS,
            'num_people': num_people,
        }

        return params

    def _get_people_params(self, content, params):
        """Returns the params needed to get all person data.

        Args:
            content: An lxml html tree.
            params: Parameters sent to the last task.

        Returns:
            a dictionary with data needed to get session info.

        """

        session_params = self._retrieve_session_vars(content)

        # add tasks for all people
        people_params = []
        for person_idx in range(params['num_people']):
            person_params = {
                'post_data': copy.deepcopy(session_params),
                'endpoint': self._session_endpoint,
                'task_type': constants.SCRAPE_DATA,
            }

            # Setup the search for the first person
            person_params['post_data'][self._person_index_param] = person_idx
            person_params['post_data'][self._detail_indicator_field] = ''

            people_params.append(person_params)

        return people_params

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
        params_list = []

        if self.is_initial_task(params['task_type']):
            # If it is our first task, grab the total number of people.
            params_list.append(self._get_num_people_params(content, params))
        else:
            # Add the next person, if there is one
            params_list.extend(self._get_people_params(content, params))
        return params_list

    def populate_data(self, content, params,
                      ingest_info: IngestInfo) -> Optional[IngestInfo]:
        """
        Populates the ingest info object from the content and params given

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.
            ingest_info: The IngestInfo object to populate
        """
        data_extractor = HtmlDataExtractor(self.yaml_file)
        ingest_info = data_extractor.extract_and_populate_data(content,
                                                               ingest_info)

        if len(ingest_info.people) != 1:
            logging.error("Data extractor didn't find exactly one person as "
                          "it should have")
            return ingest_info

        person = ingest_info.people[0]

        if person.age:
            person.age = person.age.strip().split()[0]

        # Separate bond type and amount. Superion overloads the
        # contents of the 'Bond Amount' field to contain both the bond
        # type and bond amount. When there is no bond, there is no '$'
        # character, and just the bond type.
        for booking in person.bookings:
            for charge in booking.charges:
                if charge.bond:
                    bond = charge.bond
                    if bond.amount:
                        # check for dollar amount present
                        if '$' in bond.amount:
                            type_and_amount = bond.amount
                            bond.bond_type = ' '.join(
                                type_and_amount.split('$')[:-1])
                            bond.amount = type_and_amount.split('$')[-1]
                        else:  # just a bond type, no amount
                            bond.bond_type = bond.amount
                            bond.amount = None

        # Test if the release date is a projected one
        for booking in person.bookings:
            if booking.release_date is not None and \
                   'ESTIMATED' in booking.release_date.upper():
                booking.projected_release_date = \
                    ' '.join(booking.release_date.split()[:-1])
                booking.release_date = None

        return ingest_info

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

    def get_initial_params(self):
        return {
            'endpoint': self._search_endpoint,
            'post_data': {
                't': 'ii',
                '_search': 'false',
                'rows': '1',
                'page': '1',
                'sidx': 'disp_name',
            },
        }
