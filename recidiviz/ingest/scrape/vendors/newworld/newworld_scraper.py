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

"""Scraper implementation for NewWorld vendor."""
import os
from typing import List, Optional

from recidiviz.ingest.scrape import constants, scraper_utils
from recidiviz.ingest.scrape.base_scraper import BaseScraper
from recidiviz.ingest.extractor.html_data_extractor import HtmlDataExtractor
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.scrape.task_params import ScrapedData, Task


class NewWorldScraper(BaseScraper):
    """ NewWorld Vendor scraper """
    def __init__(self, region_name, mapping_filepath=None):
        if not mapping_filepath:
            mapping_filepath = os.path.join(
                os.path.dirname(__file__), 'newworld.yaml')
        self.mapping_filepath = mapping_filepath
        super(NewWorldScraper, self).__init__(region_name)

    def populate_data(self, content, task: Task,
                      ingest_info: IngestInfo) -> Optional[ScrapedData]:
        # Bonds and charges are split in two tables. Merging them together
        self._merge_charge_and_bonds(content)

        data_extractor = HtmlDataExtractor(self.mapping_filepath)
        ingest_info = data_extractor.extract_and_populate_data(content,
                                                               ingest_info)
        scraper_utils.one('person', ingest_info)

        for charge in ingest_info.get_all_charges():
            if charge.offense_date == "No data":
                charge.offense_date = None
            if charge.bond and charge.bond.bond_id == "No data":
                charge.bond = None

        return ScrapedData(ingest_info=ingest_info, persist=True)

    def get_initial_task(self) -> Task:
        return Task(
            task_type=constants.TaskType.GET_MORE_TASKS,
            endpoint=self.get_region().base_url
            + "/NewWorld.InmateInquiry/nassau?Page=1",
            post_data={
                'page': '1'
            }
        )

    def get_more_tasks(self, content, task: Task) -> List[Task]:
        tasks = []
        post_data = task.post_data or {}
        if post_data['page'] == '1':
            tasks.extend(self._get_remaining_pages(content))
        tasks.extend(self._get_all_detail_pages(content))
        return tasks

    def _merge_charge_and_bonds(self, content):
        booking_elements = content.cssselect(".Booking")
        for booking_element in booking_elements:
            charge_table = booking_element.cssselect(".BookingCharges table")[0]
            bonds_table = booking_element.cssselect(".BookingBonds table")[0]

            # Merge header
            for e in bonds_table.find('thead/tr'):
                charge_table.find('thead/tr').append(e)

            # Merge rows
            charge_rows = charge_table.findall('tbody/tr')
            bonds_rows = bonds_table.findall('tbody/tr')
            if len(charge_rows) < len(bonds_rows):
                raise Exception("Expected number of charges >= number of bonds"
                                "but got %i charges and %i bonds" % \
                                (len(charge_rows), len(bonds_rows)))

            for c, b in zip(charge_rows, bonds_rows):
                for e in b:
                    c.append(e)


    def _get_remaining_pages(self, content) -> List[Task]:
        tasks = []
        options = content.cssselect('select[name="Page"] > option')
        for page in options[1:]:
            page_num = page.text_content()
            tasks.append(Task(
                task_type=constants.TaskType.GET_MORE_TASKS,
                endpoint=self.get_region().base_url
                +"/NewWorld.InmateInquiry/nassau?Page="+page_num,
                post_data={
                    'page': page_num
                }
            ))
        return tasks

    def _get_all_detail_pages(self, content) -> List[Task]:
        tasks = []
        links = content.cssselect('td[class="Name"] > a')

        for link in links:
            tasks.append(Task(
                task_type=constants.TaskType.SCRAPE_DATA,
                endpoint=self.get_region().base_url+link.get('href'),
            ))

        return tasks
