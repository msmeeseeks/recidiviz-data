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
from typing import List, Optional

from lxml import html

from recidiviz.common.constants.charge import (ChargeClass, ChargeDegree,
                                               ChargeStatus)
from recidiviz.common.constants.mappable_enum import EnumParsingError
from recidiviz.common.constants.person import Ethnicity, Race
from recidiviz.ingest import constants, scraper_utils
from recidiviz.ingest.base_scraper import BaseScraper
from recidiviz.ingest.extractor.html_data_extractor import HtmlDataExtractor
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.task_params import ScrapedData, Task


class UsNyScraper(BaseScraper):
    """Class to scrape info from the NY state DOCCS searcher.
    """

    TOKEN_NAME = 'DFH_STATE_TOKEN'

    def __init__(self):
        self.booking_mapping_filepath = os.path.join(os.path.dirname(__file__),
                                                     'us_ny_booking.yaml')
        self.sentence_mapping_filepath = os.path.join(os.path.dirname(__file__),
                                                      'us_ny_sentence.yaml')

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

    def get_more_tasks(self, content, task: Task) -> List[Task]:
        task_list = []

        if self.is_initial_task(task.task_type):
            task_list.append(self._get_first_search_page_task(content))
        else:
            # Search and disambiguation pages have lists of people to
            # be scraped, person pages do not. Decide here if we're
            # handling a person detail page based on whether a list of
            # people pages was found.
            person_tasks = self._get_person_tasks(content, task)
            if person_tasks:
                task_list.extend(person_tasks)
            else:
                task_list.append(
                    self._get_store_person_task(content))
            task_list.extend(self._get_next_page_tasks(content))

        # Add session variables to the post data of each params dict.
        session_vars = self._get_session_vars(content)

        for next_task in task_list:
            if next_task.post_data:
                next_task.post_data.update(session_vars)

        return task_list

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

    def _get_first_search_page_task(self, content) -> Task:
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

        return Task(
            task_type=constants.TaskType.GET_MORE_TASKS,
            endpoint=self.get_region().base_url + action,
            post_data=post_data,
        )

    def _get_next_page_tasks(self, content) -> List[Task]:
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
        return [Task(
            task_type=constants.TaskType.GET_MORE_TASKS,
            endpoint=self.get_region().base_url + action,
            post_data=self._get_post_data(next_button),
        )]

    def _get_person_tasks(self, content, task) -> List[Task]:
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
        if not result_list and 'din' in task.custom:
            din = task.custom['din']

            # Find the particular DIN we were after on this click.
            result_list = [
                res for res in
                content.xpath('//table/tr/td[@headers="din"]/form')
                if res.xpath('div/input[@type="submit"]')[0].value == din
            ]

        task_list = []
        for row in result_list:
            data = self._get_post_data(row)

            # special case for navigating away from a disambiguation page.
            if 'din' in task.custom:
                data['M12_SEL_DINI'] = task.custom['din']

            submit_name = row.xpath('./div/input[@type="submit"]/@name')[0]
            submit_value = row.xpath('./div/input[@type="submit"]/@value')[0]
            data[submit_name] = submit_value

            action = row.xpath('attribute::action')[0]
            result_task = Task(
                # Even though we are looking for data here, we might
                # get a disambiguation page, so we have to pretend we
                # get more tasks in case we do.
                task_type=constants.TaskType.GET_MORE_TASKS,
                endpoint=self.get_region().base_url + action,
                post_data=data,
                custom={'din': data['M13_SEL_DINI']},
            )

            task_list.append(result_task)

        return task_list

    def _get_store_person_task(self, content) -> Task:
        """Returns the parameters needed to store the person details.
        Args:
            content: (html tree) a webpage with the person details.
        Returns:
            A dict containing the params necessary to store the person info.
        """
        return Task(
            endpoint=None,
            content=html.tostring(content, encoding='unicode'),
            task_type=constants.TaskType.SCRAPE_DATA,
        )

    def populate_data(self, content, task: Task,
                      ingest_info: IngestInfo) -> Optional[ScrapedData]:
        booking_extractor = HtmlDataExtractor(self.booking_mapping_filepath)
        ingest_info = booking_extractor.extract_and_populate_data(content,
                                                                  ingest_info)

        if len(ingest_info.people) != 1:
            logging.error("Data extraction did not produce a single person, "
                          "as it should.")
            # TODO This is for debugging issue #483. Remove when that
            # issue is solved.
            logging.error("task: %r", task)
            return None

        if len(ingest_info.people[0].bookings) != 1:
            logging.error("Data extraction did not produce a single booking, "
                          "as it should")
            return None

        # Handle this special case for the race.
        if ingest_info.people[0].race == 'WHITE/HISPANIC':
            ingest_info.people[0].race = Race.WHITE.value
            ingest_info.people[0].ethnicity = Ethnicity.HISPANIC.value
        elif ingest_info.people[0].race == 'BLACK/HISPANIC':
            ingest_info.people[0].race = Race.BLACK.value
            ingest_info.people[0].ethnicity = Ethnicity.HISPANIC.value

        # Get release date, if released
        release_date_fields = content.xpath('//*[@headers="t1l"]')
        if (ingest_info.people[0].bookings[0].custody_status == 'RELEASED' or
                ingest_info.people[0].bookings[0].custody_status ==
                'DISCHARGED') and release_date_fields:
            release_date_string = release_date_fields[0].text.split()[0]
            ingest_info.people[0].bookings[0].release_date = release_date_string

        sentence_extractor = HtmlDataExtractor(self.sentence_mapping_filepath)
        sentence_info = sentence_extractor.extract_and_populate_data(content)
        if len(sentence_info.people) != 1 or \
                len(sentence_info.people[0].bookings) != 1 or \
                len(sentence_info.people[0].bookings[0].charges) != 1 or \
                sentence_info.people[0].bookings[0].charges[0].sentence is None:
            logging.error("Data extraction did not produce a single "
                          "sentence, as it should")
        sentence = sentence_info.people[0].bookings[0].charges[0].sentence

        # Handle empty sentence lengths.
        empty_length = 'Years,Months,Days'
        if ''.join(sentence.min_length.split()) == empty_length:
            sentence.min_length = None
        if ''.join(sentence.max_length.split()) == empty_length:
            sentence.max_length = None
        elif sentence.max_length.upper().startswith('LIFE'):
            # Handle special case of life sentences.
            sentence.max_length = None
            sentence.is_life = "True"

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
            except EnumParsingError:
                charge_degree = None

            # Get whether the charge was an attempt
            attempted = 'False'
            if charge_name.lower().startswith('att'):
                attempted = 'True'

            ingest_info.people[0].bookings[0].create_charge(
                attempted=attempted,
                charge_class=ChargeClass.FELONY.value,
                degree=charge_degree,
                level=charge_level,
                name=charge_name.strip(),
                status=ChargeStatus.SENTENCED.value,
                sentence=sentence,
            )

        return ScrapedData(ingest_info=ingest_info, persist=True)
