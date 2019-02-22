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

"""Scraper implementation for us_tn_bledsoe."""

from typing import List, Optional

from recidiviz.ingest.scrape.base_scraper import BaseScraper
from recidiviz.ingest.scrape import constants
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.scrape.task_params import ScrapedData, Task


class UsTnBledsoeScraper(BaseScraper):
    """Scraper implementation for us_tn_bledsoe."""
    def __init__(self):
        super(UsTnBledsoeScraper, self).__init__('us_tn_bledsoe')

    def populate_data(self, content, task: Task,
                      ingest_info: IngestInfo) -> Optional[ScrapedData]:

        names = content.xpath('//*[@class="inmate_name"]/text()')
        ext_ids = content.xpath(
            '//*[@class="inmate_name"]/following-sibling::*/text()')

        for ext_id, name in zip(ext_ids, names):
            name = name.replace('\t', '').replace('\n', '')
            ext_id = ext_id.split(' ')[2].replace('\t', '').replace(
                '\n', '')
            ingest_info.create_person(
                full_name=name,
                person_id=ext_id)

        return ScrapedData(ingest_info=ingest_info, persist=True)

    def get_more_tasks(self, content, task: Task) -> List[Task]:
        pass

    def get_initial_task(self) -> Task:
        return Task(task_type=constants.TaskType.SCRAPE_DATA,
                    endpoint=self.get_region().base_url)
