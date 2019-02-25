# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2019 Recidiviz, Inc.
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

"""Scraper implementation for us_fl_orange."""

import os
from string import ascii_lowercase
from typing import List, Optional

from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.scrape.base_scraper import BaseScraper
from recidiviz.ingest.scrape.task_params import ScrapedData, Task
from recidiviz.ingest.scrape import constants, scraper_utils
from recidiviz.ingest.extractor.html_data_extractor import HtmlDataExtractor


class UsFlOrangeScraper(BaseScraper):
    """Scraper implementation for us_fl_orange."""
    def __init__(self, _=None):
        super(UsFlOrangeScraper, self).__init__('us_fl_orange')
        self.yaml_file = os.path.join(os.path.dirname(__file__),
                                      'us_fl_orange.yaml')

    def populate_data(self, content, task: Task,
                      ingest_info: IngestInfo) -> Optional[ScrapedData]:
        # Automatically extract data with the extractor
        data_extractor = HtmlDataExtractor(self.yaml_file)
        data_extractor.extract_and_populate_data(content, ingest_info)

        # Add person.full_name from link to this page
        # and split out the case number from each charge
        person = scraper_utils.one('person', ingest_info)
        person.full_name = task.custom['name']
        for charge in ingest_info.get_all_charges():
            if charge.name:
                charge_name, case_number = charge.name.splitlines()
                charge.name = charge_name
                charge.case_number = case_number

        # Persist data
        return ScrapedData(ingest_info=ingest_info, persist=True)

    def get_more_tasks(self, content, task: Task) -> List[Task]:
        if self.is_initial_task(task.task_type):
            # The search requires non-empty input so we search for each letter
            # of the alphabet and the resulting pages will contain every person
            # whose last name starts with that letter
            result_pages = []
            for char in ascii_lowercase:
                result_pages.append(Task(
                    task_type=constants.TaskType.GET_MORE_TASKS,
                    endpoint=self.region.base_url,
                    post_data={'SEARCHTEXT': char}
                ))
            return result_pages

        # Parse out links to individual people
        people_pages = []
        links = content.xpath('//td[@class="ten"]/a')
        for person_link in links:
            people_pages.append(Task(
                task_type=constants.TaskType.SCRAPE_DATA,
                endpoint=self.link_to_endpoint(person_link),
                custom={'name': person_link.text_content().strip()}
            ))
        return people_pages

    def link_to_endpoint(self, link):
        return 'http://apps.ocfl.net' + link.xpath('./@href')[0]
