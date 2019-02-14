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


"""Scraper implementation for the BluHorse vendor. This handles all BluHorsee
specific navigation and data extraction. All counties that use BluHorse should
have their county-specific scrapers inherit from this class.

Vendor-specific notes:
    - The scraper is fully json based

Background scraping procedure:
    1. TODO
"""

import abc
import os
import enum
from typing import List, Optional, Set, Tuple

from recidiviz.ingest.scrape import constants
from recidiviz.ingest.scrape.base_scraper import BaseScraper
from recidiviz.ingest.extractor.json_data_extractor import JsonDataExtractor
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.scrape.task_params import ScrapedData, Task

class BluHorseScraper(BaseScraper):
    """Scraper for counties using BluHorse."""

    def __init__(self, region_name):
        super(BluHorseScraper, self).__init__(region_name)

    @staticmethod
    @abc.abstractmethod
    def get_jail_id() -> str:
        """Returns the id of the jail, as used in requests to BluHorse.

        Example:
            'LCRJ' for Letcher County, where requests need '?Jail=LCRJ'
        """

    class Page(enum.Enum):
        PASSKEY = 'GeneratePassKey'
        INMATES_LIST = 'GetInmates2'
        INMATE = 'GetInmate'
        CHARGES = 'GetInmateCharges'
        HOLDS = 'GetInmateHolds'
        BONDS = 'GetInmateBonds'
        ARREST = 'GetInmate_Arrest_Info'
        COURT_HISTORY = 'GetInmateCourtHistory'

    # NEXT_PAGE_MAP = {
    #     Page.PASSKEY: Page.INMATES_LIST,
    #     Page.IN
    # }

    def get_initial_task(self) -> Task:
        page = self.Page.PASSKEY
        return Task(
            task_type=constants.TaskType.INITIAL_AND_MORE,
            endpoint='/'.join([self.get_region().base_url, page.value]),
            response_type=constants.ResponseType.JSON,
            custom={
                'page': page.value
            },
        )

    def get_more_tasks(self, content, task: Task) -> List[Task]:
        print(str(content)[:100] + '...')
        page = self.Page(task.custom['page'])
        if page is self.Page.PASSKEY:
            task.custom['key'], task.custom['answer'] = \
                content['GeneratePassKeyResult'].split('|||')

            return [Task(
                task_type=constants.TaskType.SCRAPE_DATA_AND_MORE,
                endpoint='/'.join([self.get_region().base_url, 'GetInmates2']),
                params={
                    'Jail': self.get_jail_id(),
                    'key': task.custom['key'],
                    'ans': task.custom['answer'],
                },
                custom={**task.custom, 'page': self.Page.INMATES_LIST.value},
                response_type=constants.ResponseType.JSON,
            )]
        if page is self.Page.INMATES_LIST:
            return [Task(
                task_type=constants.TaskType.SCRAPE_DATA_AND_MORE,
                endpoint='/'.join([self.get_region().base_url, 'GetInmate']),
                params={
                    'Jail': self.get_jail_id(),
                    'bookno': person['BookNo'],
                    'key': task.custom['key'],
                    'answer': task.custom['answer'],
                    'Fields': 'ACEFGHIJKLMNO',
                    'isLogin': 'false',
                },
                custom={**task.custom, 'page': self.Page.INMATES_LIST.value},
                response_type=constants.ResponseType.JSON,
            ) for person in content['GetInmates2Result']]

        return []

    def populate_data(self, content, task: Task,
                      ingest_info: IngestInfo) -> Optional[ScrapedData]:
        page = self.Page(task.custom['page'])
        data_extractor = JsonDataExtractor(
            os.path.join(os.path.dirname(__file__), 'mappings',
                         '{}.yaml'.format(page.name.lower())))
        ingest_info = data_extractor.extract_and_populate_data(
            content, ingest_info)
        return ScrapedData(ingest_info, page is not self.Page.COURT_HISTORY)
