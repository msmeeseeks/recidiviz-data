# Recidiviz - a platform for tracking granular recidivism metrics in
# real time Copyright (C) 2019 Recidiviz, Inc.
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


"""Scraper implementation for the IML vendor. This handles all IML
specific navigation and data extraction. All counties that use IML should
have their county-specific scrapers inherit from this class.

Vendor-specific notes:
    - Historical data is kept, we still need to work out a method for
      retrieving it.
    - An initial request can be made to retrieve the first search page, which
      contains a cookie in the response headers.
    - Each search page has a list of people.

Background scraping procedure:
    1. A POST with static parameters to get the first search page results.
    2. A request for each person on the search page.
    3. A POST, incrementing the next first person index based on the contents
       of the 'nextSearch' form, until there are no more pages.
"""

import logging
import os
import re
from typing import List, Optional

from lxml import html
from lxml.etree import XMLSyntaxError  # pylint:disable=no-name-in-module

from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.mappable_enum import EnumParsingError
from recidiviz.ingest import constants
from recidiviz.ingest import scraper_utils
from recidiviz.ingest.base_scraper import BaseScraper
from recidiviz.ingest.extractor.html_data_extractor import HtmlDataExtractor
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.task_params import ScrapedData, Task


_NUM_PEOPLE_PER_SEARCH = 30


class ImlScraper(BaseScraper):
    """Scraper for counties using Iml."""

    def __init__(self, region_name, yaml_file=None, bond_yaml_file=None):
        # A yaml file can be passed in if the fields in the child scraper
        # instance are different or expanded.
        self.yaml_file = yaml_file or os.path.join(
            os.path.dirname(__file__), 'iml.yaml')

        # The bond yaml file can also be overridden
        self.bond_yaml_file = bond_yaml_file or os.path.join(
            os.path.dirname(__file__), 'iml_bond.yaml')

        super(ImlScraper, self).__init__(region_name)

        self._base_endpoint = self.get_region().base_url
        self._cookie = None

    def _get_next_search_page_task(self, content):
        """Returns the task needed to get the next search page.

        Args:
            content: An lxml html tree.

        Returns:
            a task with data needed to get the next page of search
            results.

        """

        def get_current_start(name):
            """Extract a current start index for a search form of the given
            name.

            Attr:
                name: (str) the name of the form to extract the current start
                    index from.

            Returns: the current start index.

            """

            form = content.xpath('//form[@name="{}"]'.format(name))
            if not form:
                logging.error("Did not find exactly one form, as expected")
                return None

            start_index = scraper_utils.get_value_from_html_tree(
                form[0], 'currentStart', attribute_name='name')

            return int(start_index)

        last_start_index = get_current_start('lastSearch')
        next_start_index = get_current_start('nextSearch')
        if next_start_index > last_start_index + _NUM_PEOPLE_PER_SEARCH:
            return None

        return Task(
            endpoint=self._base_endpoint,
            task_type=constants.TaskType.GET_MORE_TASKS,
            post_data={
                'flow_action': 'next',
                'currentStart': next_start_index,
            }
        )

    def _get_person_page_tasks(self, content) -> List[Task]:
        """Returns the tasks needed to get the details for all people listed
        on this page.

        Args:
            content: An lxml html tree.

        Returns:
            a list of tasks, each to get the detail page for one person.

        """

        person_tasks = []

        # Build a link from each row of the people table.
        cur_person = 1
        while True:
            row = content.cssselect('[id=row{}]'.format(cur_person))
            if not row:
                break

            if len(row) != 1:
                logging.error('Found more than one matching row when looking '
                              'for just one')
                return None

            row = row[0]

            # Get the person IDs for requests.
            ids = re.findall("'([0-9]+?)'", row.attrib['onclick'])
            if len(ids) != 3:
                logging.error('Did not find exactly 3 numbers in the row '
                              'click function call, as we should have.')
                return None

            sysID = int(ids[1])
            imgSysID = int(ids[2])

            # Get the person IDs for the DB.
            td_as = row.xpath('td/a')

            if len(td_as) != 4:
                logging.error('Did not find exactly 4 numbers in the row '
                              'links, as we should have.')
                return None

            booking_id = td_as[1].text.strip()
            person_id = td_as[2].text.strip()

            person_tasks.append(Task(
                endpoint=self._base_endpoint,
                task_type=constants.TaskType.SCRAPE_DATA,
                post_data={
                    'flow_action': 'edit',
                    'sysID': sysID,
                    'imgSysID': imgSysID,
                },
                custom={
                    'booking_id': booking_id,
                    'person_id': person_id,
                }
            ))

            cur_person += 1

        return person_tasks

    def get_more_tasks(self, content, task: Task) -> List[Task]:
        task_list = []

        # Extract the cookie if we need to.
        if self.is_initial_task(task.task_type):
            cookie = self._cookie
            self._cookie = None
        else:
            cookie = task.headers['Cookie']

        # Add the tasks for each person on this page.
        task_list.extend(self._get_person_page_tasks(content))

        # Add the next search page, if there is one.
        next_page_task = self._get_next_search_page_task(content)
        if next_page_task:
            task_list.append(next_page_task)

        # Add the cookie and the total number of people to each task.
        task_list_with_cookie = []
        for new_task in task_list:
            task_list_with_cookie.append(Task.evolve(
                new_task,
                headers={'Cookie': cookie},
            ))

        return task_list_with_cookie

    def populate_data(self, content, task: Task,
                      ingest_info: IngestInfo) -> Optional[ScrapedData]:
        data_extractor = HtmlDataExtractor(self.yaml_file)
        ingest_info = data_extractor.extract_and_populate_data(content,
                                                               ingest_info)
        if len(ingest_info.people) != 1:
            logging.error("Data extractor didn't find exactly one person as "
                          "it should have")
            return None
        person = ingest_info.people[0]

        if len(person.bookings) != 1:
            logging.error("Data extractor didn't find exactly one booking as "
                          "it should have")
            return None
        booking = person.bookings[0]

        person.person_id = task.custom['person_id']
        booking.booking_id = task.custom['booking_id']

        if person.race.startswith('HISPANIC'):
            person.ethnicity = 'HISPANIC'
            person.race = ' '.join(person.race.split()[1:])

        # Find the bonds and take their data
        bond_tab = next(t for t in content.xpath('//table')
                        if 'Bond Information' in t.text_content())
        bond_extractor = HtmlDataExtractor(self.bond_yaml_file)
        bond_ingest_info = bond_extractor.extract_and_populate_data(bond_tab,
                                                                    None)

        if bond_ingest_info.people:
            # Connect bonds and charges, and fill in an ICE hold when we
            # see it in the charge name.
            for charge in booking.charges:
                if charge.name == 'IMMIGRATION DETAINEE':
                    booking.create_hold(jurisdiction_name='ICE')

                this_charge = [c for c in
                               bond_ingest_info.people[0].bookings[0].charges
                               if c.case_number and
                               c.case_number == charge.case_number]

                if not this_charge:
                    continue

                if len(this_charge) > 1:
                    logging.error("Found two bonds for a single charge.")
                    return None

                this_charge = this_charge[0]

                charge.bond = this_charge.bond
                charge.judge_name = this_charge.judge_name

                # Transfer the bond type to charge status, if we
                # detect that the bond type field contains a charge
                # status enum value.
                try:
                    _ = ChargeStatus.from_str(charge.bond.bond_type)
                    charge.status = charge.bond.bond_type
                    charge.bond.bond_type = None
                except (AttributeError, EnumParsingError):
                    pass

        return ScrapedData(ingest_info=ingest_info, persist=True)

    def get_initial_endpoint(self):
        """Returns the initial endpoint to hit on the first call
        Returns:
            A string representing the initial endpoint to hit
        """
        return self._base_endpoint

    def get_initial_task(self):
        task = super(ImlScraper, self).get_initial_task()

        return Task.evolve(
            task,
            post_data={
                'flow_action': 'searchbyname',
                'quantity': _NUM_PEOPLE_PER_SEARCH,
                'systemUser_includereleasedinmate': 'N',
                'systemUser_includereleasedinmate2': 'N',
            }
        )

    def _fetch_content(self, endpoint, response_type, headers=None,
                       post_data=None, json_data=None):
        """Returns the page content.

        Args:
            endpoint: the endpoint to make a request to.
            headers: dict of headers to send in the request.
            post_data: dict of parameters to pass into the html request.

        Returns:
            Returns the content of the page or -1.
        """
        logging.info('Fetching content with endpoint: %s', endpoint)

        page = self.fetch_page(endpoint, headers=headers, post_data=post_data,
                               json_data=json_data)
        if page == -1:
            return -1
        try:
            html_tree = html.fromstring(page.content)
        except XMLSyntaxError as e:
            logging.error("Error parsing initial page. Error: %s\nPage:\n\n%s",
                          e, page.content)
            return -1

        # Set the cookie from the response headers if we need it.
        if 'Set-Cookie' in page.headers:
            self._cookie = page.headers['Set-Cookie'].split(';')[0]

        return html_tree
