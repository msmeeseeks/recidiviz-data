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

"""Scraper tests for us_pa_westmoreland."""
import unittest
from lxml import html

from recidiviz.tests.ingest import fixtures
from recidiviz.ingest.scrape import constants
from recidiviz.ingest.scrape.task_params import Task
# pylint: disable=fixme, line-too-long
from recidiviz.ingest.scrape.regions.us_pa_westmoreland.us_pa_westmoreland_scraper \
    import UsPaWestmorelandScraper
from recidiviz.tests.utils.base_scraper_test import BaseScraperTest
from recidiviz.ingest.models.ingest_info import IngestInfo

_INMATE_HTML = html.fromstring(
    fixtures.as_string('us_pa_westmoreland', 'inmates.html'))

# pylint: disable=fixme, missing-docstring
class TestUsPaWestmorelandScraper(BaseScraperTest, unittest.TestCase):

    def _init_scraper_and_yaml(self):
        # The scraper to be tested. Required.
        self.scraper = UsPaWestmorelandScraper()
        # The path to the yaml mapping. Optional.
        self.yaml = None

    def test_get_more_tasks(self):
        # Tests navigation. Fill in |content| and |params| with the state of the
        # page to navigate from, and |expected_result| with the expected state
        # after navigation. Chain multiple calls to
        # |validate_and_return_get_more_tasks| together if necessary.
        content = html.fromstring('navigation page')
        task = Task(
            task_type=constants.TaskType.INITIAL_AND_MORE,
            endpoint=self.scraper.region.base_url,
        )
        expected_result = [
            Task(
                endpoint=self.scraper.region.base_url,
                task_type=constants.TaskType.SCRAPE_DATA,
            ),
        ]

        self.validate_and_return_get_more_tasks(content, task, expected_result)


    def test_populate_data(self):
        # Tests scraping data. Fill in |content| with the state of the page
        # containing person data, and |expected_result| with the IngestInfo
        # objects that should be scraped from the page. A default Task will be
        # passed to the |populate_data| method, but this can be overriden by
        # supplying a |task| argument
        content = _INMATE_HTML
        expected_info = IngestInfo()

        p1 = expected_info.create_person(full_name='Doe, John',
                                         birthdate='01/01/1980')

        p1.create_booking(booking_id='111-1111',
                          admission_date='02/02/1981')

        p2 = expected_info.create_person(full_name='Smith, Jane',
                                         birthdate='02/02/1981')

        p2.create_booking(booking_id='2222-2222',
                          admission_date='03/03/1982',
                          custody_status='Held Elsewhere')

        self.validate_and_return_populate_data(content, expected_info)
