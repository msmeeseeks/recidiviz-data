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

"""Scraper tests for us_ms_desoto."""
import unittest
from lxml import html

from recidiviz.ingest import constants
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.task_params import Task
from recidiviz.ingest.scrape.regions.us_ms_desoto.us_ms_desoto_scraper import \
    UsMsDesotoScraper
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils.base_scraper_test import BaseScraperTest

_PERSON_LIST_PAGE = fixtures.as_string('us_ms_desoto', 'person_list_page.html')
_PERSON_PAGE = html.fromstring(
    fixtures.as_string('us_ms_desoto', 'person_page.html'))


class TestUsMsDesotoScraper(BaseScraperTest, unittest.TestCase):
    """Tests for UsMsDesotoScraper"""

    def _init_scraper_and_yaml(self):
        # The scraper to be tested. Required.
        self.scraper = UsMsDesotoScraper()
        # The path to the yaml mapping. Optional.
        self.yaml = None

    def test_get_more_tasks(self):
        task = Task(
            task_type=constants.TaskType.GET_MORE_TASKS,
            endpoint=self.scraper.get_base_endpoint_details(),
        )
        expected_result = [
            Task(
                task_type=constants.TaskType.SCRAPE_DATA,
                endpoint=self.scraper.get_base_endpoint_details() + '?id=1'
            ),
            Task(
                task_type=constants.TaskType.SCRAPE_DATA,
                endpoint=self.scraper.get_base_endpoint_details() + '?id=2'
            ),
        ]

        self.validate_and_return_get_more_tasks(
            _PERSON_LIST_PAGE, task, expected_result)

    def test_populate_data(self):
        # Tests scraping data. Fill in |content| and |params| with the state of
        # the page containing person data, and |expected_result| with the
        # IngestInfo objects that should be scraped from the page.
        task = Task(
            endpoint=self.scraper.get_base_endpoint_details(),
            task_type=constants.TaskType.SCRAPE_DATA,
        )
        expected_info = IngestInfo()

        person = expected_info.create_person()
        person.person_id = '1'
        person.birthdate = '03-05-1985'
        person.age = '33'
        person.gender = 'M'
        person.race = 'B'

        booking = person.create_booking()
        booking.admission_date = '08-14-2018'

        charge = booking.create_charge()
        charge.name = 'BATTERY - TOUCH OR STRIKE'
        charge.offense_date = '08-14-2018'
        charge.next_court_date = '12-11-2018'
        bond = charge.create_bond()
        bond.amount = '$5,000.00'
        bond.bond_type = 'Declined'

        charge = booking.create_charge()
        charge.name = 'SEX OFFENDER VIOLATION 2'
        charge.offense_date = '08-14-2018'
        charge.next_court_date = '12-11-2018'
        bond = charge.create_bond()
        bond.amount = '$10,000.00'
        bond.bond_type = 'Cash or Surety Bond'

        charge = booking.create_charge()
        charge.name = 'SEXUAL OFFENDER VIOLATION 1'
        charge.offense_date = '08-14-2018'
        charge.next_court_date = '12-11-2018'
        bond = charge.create_bond()
        bond.amount = '$10,000.00'
        bond.bond_type = 'Cash or Surety Bond'

        self.validate_and_return_populate_data(
            _PERSON_PAGE, expected_info, task=task)
