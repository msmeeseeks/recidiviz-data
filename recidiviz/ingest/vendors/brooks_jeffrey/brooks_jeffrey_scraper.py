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

"""Scraper implementation for all websites using Brooks Jeffery marketing.
"""

import logging
import os
from typing import List, Optional

from recidiviz.ingest import constants
from recidiviz.ingest.base_scraper import BaseScraper
from recidiviz.ingest.extractor.html_data_extractor import HtmlDataExtractor
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.task_params import ScrapedData, Task


class BrooksJeffreyScraper(BaseScraper):
    """Scraper for counties using Brooks Jeffrey."""

    def __init__(self, region_name, mapping_filepath=None):
        if not mapping_filepath:
            mapping_filepath = os.path.join(
                os.path.dirname(__file__), 'brooks_jeffrey.yaml')
        self.mapping_filepath = mapping_filepath
        super(BrooksJeffreyScraper, self).__init__(region_name)

    def get_more_tasks(self, content, task: Task) -> List[Task]:
        content.make_links_absolute(self.get_region().base_url)

        params_list = []
        params_list.extend(self._get_person_tasks(content))
        params_list.extend(self._get_next_page_if_exists_tasks(content))
        return params_list

    def _get_next_page_if_exists_tasks(self, content) -> List[Task]:
        links = content.xpath('//a[@class="tbold"]')
        next_page_links = [link.xpath('./@href')[0] for link in links if
                           link.text_content() == ">>"]
        # There are multiple next page links on each roster page; however, they
        # are all equivalent, so choose the first one arbitrarily
        task_list = []
        if next_page_links:
            task_list.append(Task(
                task_type=constants.TaskType.GET_MORE_TASKS,
                endpoint=next_page_links[0],
            ))
        return task_list

    def _get_person_tasks(self, content) -> List[Task]:
        links = content.xpath('//a[@class="text2"]')
        person_links = [link.xpath('./@href')[0] for link in links if
                        link.text_content() == "View Profile >>>"]
        task_list = []
        for person_link in person_links:
            task_list.append(Task(
                task_type=constants.TaskType.SCRAPE_DATA,
                endpoint=person_link,
            ))
        return task_list

    def populate_data(self, content, task: Task,
                      ingest_info: IngestInfo) -> Optional[ScrapedData]:
        data_extractor = HtmlDataExtractor(self.mapping_filepath)
        ingest_info = data_extractor.extract_and_populate_data(content,
                                                               ingest_info)

        for person in ingest_info.people:
            if len(person.bookings) != 1 or len(person.bookings[0].charges) > 1:
                logging.error("Data extraction did not produce a single "
                              "booking with at most one charge, as it should")

            if person.bookings[0].charges:
                charge_names_raw = person.bookings[0].charges[0].name
                if charge_names_raw:
                    charge_names = charge_names_raw.split('\n')
                    person.bookings[0].charges = []
                    for charge_name in charge_names:
                        person.bookings[0].create_charge(name=charge_name)

        return ScrapedData(ingest_info=ingest_info, persist=True)
