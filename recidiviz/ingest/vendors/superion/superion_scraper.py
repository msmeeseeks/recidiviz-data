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

import json
import logging
import os
from typing import List, Optional

from recidiviz.ingest import constants, scraper_utils
from recidiviz.ingest.base_scraper import BaseScraper
from recidiviz.ingest.extractor.html_data_extractor import HtmlDataExtractor
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.task_params import ScrapedData, Task


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

    def _get_num_people_task(self, content) -> Task:
        """Returns the params needed to start scraping people.

        Args:
            content: An lxml html tree.
            params: Parameters sent to the last task.

        Returns:
            a dictionary with data needed to track people scraping tasks.

        """
        json_content = json.loads(content.text)
        num_people = int(json_content['records'])

        return Task(
            task_type=constants.TaskType.GET_MORE_TASKS,
            endpoint=self._session_endpoint,
            custom={
                'num_people': num_people,
            },
        )

    def _get_people_tasks(self, content, task) -> List[Task]:
        """Returns the params needed to get all person data.

        Args:
            content: An lxml html tree.
            params: Parameters sent to the last task.

        Returns:
            a dictionary with data needed to get session info.

        """

        session_params = self._retrieve_session_vars(content)

        # add tasks for all people
        tasks = []
        for person_idx in range(task.custom['num_people']):
            post_data = session_params.copy()
            # Setup the search for the first person
            post_data[self._person_index_param] = person_idx
            post_data[self._detail_indicator_field] = ''
            tasks.append(Task(
                task_type=constants.TaskType.SCRAPE_DATA,
                endpoint=self._session_endpoint,
                post_data=post_data,
            ))

        return tasks

    def get_more_tasks(self, content, task: Task) -> List[Task]:
        params_list = []

        if self.is_initial_task(task.task_type):
            # If it is our first task, grab the total number of people.
            params_list.append(self._get_num_people_task(content))
        else:
            # Add the next person, if there is one
            params_list.extend(self._get_people_tasks(content, task))
        return params_list

    def populate_data(self, content, task: Task,
                      ingest_info: IngestInfo) -> Optional[ScrapedData]:
        data_extractor = HtmlDataExtractor(self.yaml_file)
        ingest_info = data_extractor.extract_and_populate_data(content,
                                                               ingest_info)

        if len(ingest_info.people) != 1:
            logging.error("Data extractor didn't find exactly one person as "
                          "it should have")
            return ScrapedData(ingest_info=ingest_info, persist=True)

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

        return ScrapedData(ingest_info=ingest_info, persist=True)

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

    def get_initial_task(self) -> Task:
        return Task(
            task_type=constants.TaskType.INITIAL_AND_MORE,
            endpoint=self._search_endpoint,
            post_data={
                't': 'ii',
                '_search': 'false',
                'rows': '1',
                'page': '1',
                'sidx': 'disp_name',
            },
        )
