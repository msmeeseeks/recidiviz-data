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
import html
import os
import re
from datetime import datetime
from typing import List, Optional, Set, Tuple

import pytz

from recidiviz.common.constants.booking import AdmissionReason
from recidiviz.common.constants.bond import BondType
from recidiviz.common.constants.charge import ChargeClass, ChargeStatus
from recidiviz.ingest.scrape import constants
from recidiviz.ingest.scrape.base_scraper import BaseScraper
from recidiviz.ingest.extractor.json_data_extractor import JsonDataExtractor
from recidiviz.ingest.models.ingest_info import (Bond, Booking, Charge,
                                                 IngestInfo)
from recidiviz.ingest.scrape.task_params import ScrapedData, Task
from recidiviz.persistence.converter import converter_utils

class BluHorseScraper(BaseScraper):
    """Scraper for counties using BluHorse."""

    def __init__(self, region_name):
        super(BluHorseScraper, self).__init__(region_name)

    def get_initial_task(self) -> Task:
        return Task(
            task_type=constants.TaskType.INITIAL_AND_MORE,
            endpoint='/'.join([
                'http://inmates.bluhorse.com/InmateService.svc',
                'GeneratePassKey',
            ]),
            response_type=constants.ResponseType.JSON,
        )

    def populate_data(self, content, task: Task,
                      ingest_info: IngestInfo) -> Optional[ScrapedData]:
        return ScrapedData(ingest_info, False)