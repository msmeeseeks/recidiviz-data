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

"""Vendor scraper implementation for Eagle Advantage."""
import logging
import math
import os
import re
from typing import Optional, Any, List
from urllib.parse import urlencode

from recidiviz.ingest import constants
from recidiviz.ingest.base_scraper import BaseScraper
from recidiviz.ingest.extractor.json_data_extractor import JsonDataExtractor
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.task_params import Task, ScrapedData

_ENDPOINT = 'http://offendermiddleservice.offenderindex.com/api/Values'
_PAGE_SIZE = 10


class EagleAdvantageScraper(BaseScraper):
    """Vendor scraper implementation for Eagle Advantage."""

    def __init__(self, region_name: str, agency_service_ip: str,
                 agency_service_port: int):
        """Child scrapers must provide agency_service_ip and
        agency_service_port"""
        super(EagleAdvantageScraper, self).__init__(region_name)
        self.yaml_file = os.path.join(
            os.path.dirname(__file__), 'eagle_advantage.yaml')
        self.agency_service_ip = agency_service_ip
        self.agency_service_port = agency_service_port

    def populate_data(self, content, task: Task,
                      ingest_info: IngestInfo) -> Optional[ScrapedData]:
        """NOTE: Child scrapers may need write their own populate_data
        function that calls this one and then processes charge.charge_notes.
        The field may have information about charges, bonds, or sentences;
        the child scraper should attempt to process the more common values."""
        extractor = JsonDataExtractor(key_mapping_file=self.yaml_file)
        ingest_info = extractor.extract_and_populate_data(content, ingest_info)

        for person in ingest_info.people:
            if len(person.bookings) != 1:
                logging.error("Person did not have exactly 1 booking as "
                              "expected")
                continue
            for charge in person.bookings[0].charges:
                regex: Any = re.compile(r'Id:(.*?),code:(.*?),off1:(.*?),'
                                        r'off2:(.*?),type:(.*?),sent:(.*?),'
                                        r'court:(.*?),counts:(\d?)', re.DOTALL)

                matches = re.match(regex, charge.name)
                if not matches:
                    logging.error("Charge information did not match expected "
                                  "pattern: %s", charge.name)
                    continue
                c_id, code, off1, off2, c_class, sent, court, counts = \
                    matches.groups()
                charge.charge_id = c_id
                charge.statute = code
                charge.name = off1 + off2
                charge.charge_class = c_class
                charge.charge_notes = sent
                charge.court_type = court
                charge.number_of_counts = counts

        return ScrapedData(ingest_info)

    def get_more_tasks(self, content, task: Task) -> List[Task]:
        total_count = int(content['TotalCount'])
        total_pages = math.ceil(total_count / _PAGE_SIZE)
        return [
            Task(endpoint=self._get_next_page_url(page_number),
                 task_type=constants.TaskType.SCRAPE_DATA,
                 response_type=constants.ResponseType.JSON)
            for page_number in range(1, total_pages + 1)
        ]

    def _get_next_page_url(self, page_number: int) -> str:
        post_data = {
            'three': 1,
            'fnmeLetters': '',
            'lnmeLetters': '',
            'needDate': 'false',
            'startDate': '',
            'stopDate': '',
            'getImg': 'false',
            'agencyServiceIP': self.agency_service_ip,
            'agencyServicePort': self.agency_service_port,
            'take': _PAGE_SIZE,
            'skip': _PAGE_SIZE * (page_number - 1),
            'page': page_number,
            'pageSize': _PAGE_SIZE
        }
        # Weirdly, the website doesn't accept POST requests, so encode manually.
        endpoint = '{}?{}'.format(_ENDPOINT, urlencode(post_data))
        return endpoint

    def get_initial_task(self) -> Task:
        endpoint = self._get_next_page_url(1)
        return Task(endpoint=endpoint,
                    task_type=constants.TaskType.INITIAL_AND_MORE,
                    response_type=constants.ResponseType.JSON)
