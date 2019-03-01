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

"""Scraper tests for us_ca_kings."""
import unittest
from lxml import html

from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.scrape import constants
from recidiviz.ingest.scrape.constants import TaskType
from recidiviz.ingest.scrape.task_params import Task
from recidiviz.ingest.scrape.regions.us_ca_kings.us_ca_kings_scraper import \
    UsCaKingsScraper
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils.base_scraper_test import BaseScraperTest

_ROSTER_HTML = html.fromstring(fixtures.as_string('us_ca_kings', 'roster.html'))
_PERSON_HTML = html.fromstring(fixtures.as_string('us_ca_kings', 'person.html'))


class TestUsCaKingsScraper(BaseScraperTest, unittest.TestCase):

    def _init_scraper_and_yaml(self):
        self.scraper = UsCaKingsScraper()

    def test_get_more_tasks(self):
        task = Task(
            task_type=constants.TaskType.INITIAL_AND_MORE,
            endpoint=self.scraper.region.base_url,
        )
        expected_result = [
            Task(task_type=TaskType.SCRAPE_DATA,
                 endpoint='http://inmatelocator.countyofkings.com/'
                          'Inmate/Details/11111'),
            Task(task_type=TaskType.SCRAPE_DATA,
                 endpoint='http://inmatelocator.countyofkings.com/'
                          'Inmate/Details/22222'),
        ]
        self.validate_and_return_get_more_tasks(_ROSTER_HTML, task,
                                                expected_result)

    def test_populate_data(self):
        expected_info = IngestInfo()
        p = expected_info.create_person(person_id='11111',
                                        full_name='PERSON ONE',
                                        birthdate='01/01/1111',
                                        gender='M',
                                        race='White')
        b = p.create_booking(booking_id='18-01',
                             admission_date='2/2/2222 10:10:10 AM')
        b.create_arrest(arrest_date='2/2/2222 1:01:01 AM',
                        agency='AVENAL POLICE DEPARTMENT')
        b.create_charge(statute='STATUTE ONE',
                        name='CHARGE ONE',
                        charge_class='Felony',
                        case_number='1811111').create_bond(amount='$1')
        b.create_charge(statute='STATUTE TWO',
                        name='CHARGE TWO',
                        charge_class='Misdemeanor').create_bond(amount='$0')

        self.validate_and_return_populate_data(_PERSON_HTML, expected_info)
