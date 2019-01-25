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

"""Scraper tests for us_ky_bullitt_county."""
import unittest
from lxml import html

from recidiviz.ingest import constants
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.us_ky_bullitt_county.us_ky_bullitt_county_scraper import UsKyBullittCountyScraper
from recidiviz.tests.utils.base_scraper_test import BaseScraperTest


class TestUsKyBullittCountyScraper(BaseScraperTest, unittest.TestCase):

    def _init_scraper_and_yaml(self):
        # The scraper to be tested. Required.
        self.scraper = UsKyBullittCountyScraper()
        # The path to the yaml mapping. Optional.
        self.yaml = None

    def test_get_more_tasks(self):
        # Tests navigation. Fill in |content| and |params| with the state of the
        # page to navigate from, and |expected_result| with the expected state
        # after navigation. Chain multiple calls to
        # |validate_and_return_get_more_tasks| together if necessary.
        content = html.fromstring('navigation page')
        params = {
            'endpoint': None,
            'task_type': constants.GET_MORE_TASKS,
        }
        expected_result = [
            {
                'endpoint': None,
                'task_type': None
            }
        ]

        self.validate_and_return_get_more_tasks(content, params, expected_result)


    def test_populate_data(self):
        # Tests scraping data. Fill in |content| and |params| with the state of
        # the page containing person data, and |expected_result| with the
        # IngestInfo objects that should be scraped from the page.
        content = html.fromstring('person data page')
        params = {
            'endpoint': None,
            'task_type': constants.SCRAPE_DATA,
        }
        expected_result = IngestInfo()

        self.validate_and_return_populate_data(content, params, expected_result)
