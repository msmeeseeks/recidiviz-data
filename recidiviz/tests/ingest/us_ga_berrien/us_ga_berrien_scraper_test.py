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

"""Scraper tests for us_ga_berrien."""
import unittest

from recidiviz.ingest import constants
from recidiviz.ingest.constants import TaskType, ResponseType
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.task_params import Task
from recidiviz.ingest.us_ga_berrien.us_ga_berrien_scraper import \
    UsGaBerrienScraper
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils.base_scraper_test import BaseScraperTest

_PAGE = fixtures.as_dict('us_ga_berrien', 'page.json')


class TestUsGaBerrienScraper(BaseScraperTest, unittest.TestCase):
    def _init_scraper_and_yaml(self):
        self.scraper = UsGaBerrienScraper()

    def test_get_more_tasks(self):
        content = _PAGE
        params = {
            'task_type': constants.TaskType.GET_MORE_TASKS,
        }
        expected_result = [
            Task(task_type=TaskType.SCRAPE_DATA,
                 endpoint='http://offendermiddleservice.offenderindex.com/api'
                          '/Values?three=1&fnmeLetters=&lnmeLetters=&needDate'
                          '=false&startDate=&stopDate=&getImg=false'
                          '&agencyServiceIP=108.161.95.38&agencyServicePort='
                          '9000&take=10&skip=0&page=1&pageSize=10',
                 response_type=ResponseType.JSON),
            Task(task_type=TaskType.SCRAPE_DATA,
                 endpoint='http://offendermiddleservice.offenderindex.com/api'
                          '/Values?three=1&fnmeLetters=&lnmeLetters=&needDate'
                          '=false&startDate=&stopDate=&getImg=false'
                          '&agencyServiceIP=108.161.95.38&agencyServicePort='
                          '9000&take=10&skip=10&page=2&pageSize=10',
                 response_type=ResponseType.JSON),
        ]

        self.validate_and_return_get_more_tasks(content, params,
                                                expected_result)

    def test_populate_data(self):
        content = _PAGE
        expected_result = IngestInfo()

        p1 = expected_result.create_person(person_id='30', surname='SIMPSON',
                                           given_names='BART',
                                           birthdate='1981', gender='M',
                                           place_of_residence='SPRINGFIELD')
        b1 = p1.create_booking(custody_status='CURRENTLY BOOKED')
        b1.create_arrest(date='1/1/1111')
        b1.create_charge(charge_id='CHARGEID', statute='STATUTE',
                         name='PROBATION VIOLATION',
                         charge_class='Misdemeanor', number_of_counts='1',
                         court_type='MAGISTRATE',
                         charge_notes='BLAH').create_bond(amount='0')

        p2 = expected_result.create_person(person_id='35',
                                           surname='SPIMSTON',
                                           given_names='BARF',
                                           birthdate='1996',
                                           gender='M',
                                           place_of_residence='ADDRESS')
        b2 = p2.create_booking(custody_status='CURRENTLY BOOKED')
        b2.create_arrest(date='1/1/1111')
        b2.create_charge(charge_id='ID2',
                         statute='CODE',
                         name='POSSESSION',
                         charge_class='Felony',
                         court_type='SUPERIOR').create_bond(amount='15000')

        self.validate_and_return_populate_data(content, expected_result)
