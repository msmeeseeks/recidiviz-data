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
"""Scraper implementation for us_pa_dauphin."""

from recidiviz.ingest import constants
from recidiviz.ingest.base_scraper import BaseScraper
from recidiviz.ingest.models.ingest_info import IngestInfo


class UsPaDauphinScraper(BaseScraper):
    """Scraper implementation for us_pa_dauphin."""
    def __init__(self):
        super(UsPaDauphinScraper, self).__init__('us_pa_dauphin')

    def populate_data(self, content, _, __):
        names = content.xpath('//table')[1].xpath('.//font')

        ingest_info = IngestInfo()
        for name in names:
            ingest_info.create_person(full_name=name.text)

        return ingest_info

    def get_more_tasks(self, content, params):
        pass

    def get_initial_params(self):
        return {'task_type': constants.SCRAPE_DATA}
