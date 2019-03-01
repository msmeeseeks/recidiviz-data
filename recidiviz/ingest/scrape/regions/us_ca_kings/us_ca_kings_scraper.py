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

"""Scraper implementation for us_ca_kings."""
import os
from typing import List, Optional
from urllib.parse import urljoin

from recidiviz.ingest.extractor.html_data_extractor import HtmlDataExtractor
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.scrape.base_scraper import BaseScraper
from recidiviz.ingest.scrape.constants import TaskType
from recidiviz.ingest.scrape.task_params import ScrapedData, Task


class UsCaKingsScraper(BaseScraper):
    """Scraper implementation for us_ca_kings."""

    def __init__(self):
        super(UsCaKingsScraper, self).__init__('us_ca_kings')
        self.mapping_filepath = os.path.join(
            os.path.dirname(__file__), 'us_ca_kings.yaml')

    def populate_data(self, content, task: Task,
                      ingest_info: IngestInfo) -> Optional[ScrapedData]:
        extractor = HtmlDataExtractor(self.mapping_filepath)
        ingest_info = extractor.extract_and_populate_data(content)
        return ScrapedData(ingest_info)

    def get_more_tasks(self, content, task: Task) -> List[Task]:
        return [
            Task(endpoint=urljoin(self.region.base_url, href),
                 task_type=TaskType.SCRAPE_DATA)
            for href in content.xpath('//table/tbody/tr/td/a/@href')
        ]
