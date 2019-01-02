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
from recidiviz.ingest.base_scraper import BaseScraper
from recidiviz.ingest import constants
from recidiviz.ingest.extractor.data_extractor import DataExtractor

class UsFlMartinScraper(BaseScraper):
    """Scraper implementation for us_fl_martin."""

    def __init__(self, mapping_filepath=None):
        if not mapping_filepath:
            mapping_filepath = os.path.join(
                os.path.dirname(__file__), 'us_fl_martin.yaml')
        self.mapping_filepath = mapping_filepath

        super(UsFlMartinScraper, self).__init__('us_fl_martin')

    def set_initial_vars(self, content, params):
        pass

    def populate_data(self, content, params, ingest_info):
        data_extractor = DataExtractor(self.mapping_filepath)
        ingest_info = data_extractor.extract_and_populate_data(content, \
            ingest_info)
        return ingest_info

    def get_more_tasks(self, content, params):
        return [{
            'endpoint': self.get_region().base_url+"?RunReport=Run+Report",
            'task_type': constants.SCRAPE_DATA
        }]
