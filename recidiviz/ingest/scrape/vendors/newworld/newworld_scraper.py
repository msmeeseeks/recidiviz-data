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
from abc import abstractmethod
from typing import List, Optional
from urllib.parse import urljoin

from recidiviz.ingest.scrape import constants, scraper_utils
from recidiviz.ingest.scrape.base_scraper import BaseScraper
from recidiviz.ingest.extractor.html_data_extractor import HtmlDataExtractor
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.scrape.task_params import ScrapedData, Task

# URL path component shared across regions.
_NEWWORLD_INMATEINQUIRY = 'NewWorld.InmateInquiry'


class NewWorldScraper(BaseScraper):
    """ NewWorld Vendor scraper """

    def __init__(self, region_name):
        super(NewWorldScraper, self).__init__(region_name)
        self.roster_page_url = '/'.join((self.get_region().base_url,
                                         _NEWWORLD_INMATEINQUIRY,
                                         self.get_region_code()))

    @abstractmethod
    def get_region_code(self) -> str:
        """The last component of the path of the search page, which looks like
        http://<base_url>/NewWorld.InmateInquiry/<region_code>. For example,
        Nassau, FL's roster is located at
        https://dssinmate.nassauso.com/NewWorld.InmateInquiry/nassau, so the
        UsFlNassauScraper should return `nassau` here."""

    def populate_data(self, content, task: Task,
                      ingest_info: IngestInfo) -> Optional[ScrapedData]:
        yaml_template = os.path.join(os.path.dirname(__file__),
                                     'newworld_{}.yaml')

        person_extractor = HtmlDataExtractor(yaml_template.format('person'))
        booking_extractor = HtmlDataExtractor(yaml_template.format('booking'))
        bond_extractor = HtmlDataExtractor(yaml_template.format('bond'))

        ingest_info = person_extractor.extract_and_populate_data(content,
                                                                 ingest_info)
        person = scraper_utils.one('person', ingest_info)
        for booking_element in content.cssselect('.Booking'):
            booking_info = booking_extractor.extract_and_populate_data(
                booking_element)
            bond_info = bond_extractor.extract_and_populate_data(
                booking_element)

            booking = scraper_utils.one('booking', booking_info)
            bonds_with_dummy_charges = scraper_utils.one('booking',
                                                         bond_info).charges

            if bonds_with_dummy_charges and \
                    bonds_with_dummy_charges[0].bond.bond_id != 'No data':
                booking.charges.extend(bonds_with_dummy_charges)

            person.bookings.append(booking)

        return ScrapedData(ingest_info=ingest_info, persist=True)

    def get_initial_task(self) -> Task:
        return Task(task_type=constants.TaskType.GET_MORE_TASKS,
                    endpoint=self.roster_page_url,
                    params={'Page': '1'})

    def get_more_tasks(self, content, task: Task) -> List[Task]:
        tasks = []
        params = task.params or {}
        if params['Page'] == '1':
            tasks.extend(self._get_remaining_pages(content))
        tasks.extend(self._get_all_detail_pages(content))
        return tasks

    def _get_remaining_pages(self, content) -> List[Task]:
        tasks = []
        options = content.cssselect('select[name="Page"] > option')
        for page in options[1:]:
            page_num = page.text_content()
            tasks.append(Task(
                task_type=constants.TaskType.GET_MORE_TASKS,
                endpoint=self.roster_page_url,
                params={'Page': page_num}))
        return tasks

    def _get_all_detail_pages(self, content) -> List[Task]:
        tasks = []
        links = content.cssselect('td[class="Name"] > a')

        for link in links:
            tasks.append(Task(
                task_type=constants.TaskType.SCRAPE_DATA,
                endpoint=urljoin(self.get_region().base_url,
                                 link.get('href')),
            ))

        return tasks
