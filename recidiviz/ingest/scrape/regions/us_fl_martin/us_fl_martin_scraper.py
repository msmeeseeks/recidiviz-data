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

"""Scraper implementation for us_fl_martin."""
import os
from typing import Optional
from typing import List

from recidiviz.ingest.scrape.base_scraper import BaseScraper
from recidiviz.ingest.scrape import constants
from recidiviz.ingest.extractor.html_data_extractor import HtmlDataExtractor
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.scrape.task_params import ScrapedData, Task

class UsFlMartinScraper(BaseScraper):
    """Scraper implementation for us_fl_martin."""

    def __init__(self, mapping_filepath=None):
        if not mapping_filepath:
            mapping_filepath = os.path.join(
                os.path.dirname(__file__), 'us_fl_martin.yaml')
        self.mapping_filepath = mapping_filepath

        super(UsFlMartinScraper, self).__init__('us_fl_martin')

    def populate_data(self, content, task: Task,
                      ingest_info: IngestInfo) -> Optional[ScrapedData]:
        data_extractor = HtmlDataExtractor(self.mapping_filepath)
        ingest_info = data_extractor.extract_and_populate_data(content, \
            ingest_info)
        return ScrapedData(ingest_info, persist=True)

    def get_more_tasks(self, content, task: Task) -> List[Task]:
        return [Task(
            endpoint=self.get_region().base_url,
            task_type=constants.TaskType.SCRAPE_DATA,
            params={
                'RunReport': 'Run Report',
            }
        )]
