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

"""Scraper tests for us_tn_bledsoe."""
import unittest
from lxml import html

from recidiviz.ingest.scrape import constants
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.scrape.task_params import Task
from recidiviz.tests.ingest import fixtures
from recidiviz.ingest.scrape.regions.us_tn_bledsoe.us_tn_bledsoe_scraper \
    import UsTnBledsoeScraper
from recidiviz.tests.utils.base_scraper_test import BaseScraperTest


class TestUsTnBledsoeScraper(BaseScraperTest, unittest.TestCase):

    def _init_scraper_and_yaml(self):
        # The scraper to be tested. Required.
        self.scraper = UsTnBledsoeScraper()
        # The path to the yaml mapping. Optional.
        self.yaml = None

    def test_populate_data(self):
        # Tests scraping data. Fill in |content| with the state of the page
        # containing person data, and |expected_result| with the IngestInfo
        # objects that should be scraped from the page. A default Task will be
        # passed to the |populate_data| method, but this can be overriden by
        # supplying a |task| argument
        content = html.fromstring(
            fixtures.as_string('us_tn_bledsoe', 'report.html'))
        expected_info = IngestInfo()

        expected_info.create_person(
            full_name='SIMPSON,BART',
            person_id = '19243'
        )

        expected_info.create_person(
            full_name='SIMPSON,LISA',
            person_id = '52468'
        )   

        self.validate_and_return_populate_data(content, expected_info)
