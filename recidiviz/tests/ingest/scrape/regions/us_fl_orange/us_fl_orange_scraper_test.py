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

"""Scraper tests for us_fl_orange."""
import unittest
from lxml import html

from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.scrape import constants
from recidiviz.ingest.scrape.task_params import Task
from recidiviz.ingest.scrape.regions.us_fl_orange.us_fl_orange_scraper import UsFlOrangeScraper
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils.base_scraper_test import BaseScraperTest


class TestUsFlOrangeScraper(BaseScraperTest, unittest.TestCase):

    def _init_scraper_and_yaml(self):
        # The scraper to be tested. Required.
        self.scraper = UsFlOrangeScraper()
        # The path to the yaml mapping. Optional.
        self.yaml = None

    def test_get_more_tasks(self):
        content = html.fromstring(
            fixtures.as_string('us_fl_orange', 'list.html'))
        task = Task(
            task_type=constants.TaskType.GET_MORE_TASKS,
            endpoint=None,
        )
        expected_result = [
            Task(
                endpoint='http://apps.ocfl.net/bailbond/default.asp?BookNumber=123&ID=456',
                task_type=constants.TaskType.SCRAPE_DATA,
                custom={ 'name': 'JONES, ALICE' }
            ),
        ]

        self.validate_and_return_get_more_tasks(content, task, expected_result)

    def test_populate_data(self):
        content = html.fromstring(
            fixtures.as_string('us_fl_orange', 'person.html'))
        expected_info = IngestInfo()
        person = expected_info.create_person(
            full_name='JONES, ALICE',
            race='Black',
            gender='Female',
            age='25'
        )
        booking = person.create_booking(
            booking_id='123',
            admission_date='09/01/2015'
        )
        booking.create_arrest(
            agency='Orange County Sheriff Office'
        )
        charge1 = booking.create_charge(
            status='Presentenced',
            name='SOME CHARGE',
            case_number='456'
        )
        charge1.create_bond(
            amount='456.00'
        )
        booking.create_arrest(
            agency='Orange County Sheriff Office'
        )
        charge2 = booking.create_charge(
            status='Presentenced',
            name='SOME OTHER CHARGE',
            case_number='789'
        )
        charge2.create_bond(
            amount='789.00'
        )
        task = Task(
            task_type=constants.TaskType.SCRAPE_DATA,
            endpoint='',
            custom={'name': 'JONES, ALICE'}
        )

        self.validate_and_return_populate_data(content, expected_info, task=task)
