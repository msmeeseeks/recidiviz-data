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

"""Scraper tests for us_tn_mcminn."""
import unittest

from recidiviz.ingest.scrape import constants
from recidiviz.ingest.scrape.constants import TaskType, ResponseType
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.scrape.task_params import Task
from recidiviz.ingest.scrape.regions.us_tn_mcminn.us_tn_mcminn_scraper import \
    UsTnMcminnScraper
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils.base_scraper_test import BaseScraperTest

_PAGE = fixtures.as_dict('us_tn_mcminn', 'page.json')
_ENDPOINT = 'http://offendermiddleservice.offenderindex.com/api/Values'


class TestUsTnMcminnScraper(BaseScraperTest, unittest.TestCase):
    """Tests for UsTnMcminnScraper"""

    def _init_scraper_and_yaml(self):
        self.scraper = UsTnMcminnScraper()

    def test_get_more_tasks(self):
        content = _PAGE
        params = {
            'task_type': constants.TaskType.GET_MORE_TASKS,
        }
        expected_result = [
            Task(task_type=TaskType.SCRAPE_DATA,
                 endpoint=_ENDPOINT,
                 response_type=ResponseType.JSON,
                 params={
                     'three': 1,
                     'fnmeLetters': '',
                     'lnmeLetters': '',
                     'needDate': 'false',
                     'startDate': '',
                     'stopDate': '',
                     'getImg': 'false',
                     'agencyServiceIP': 'mcminnsheriff.serveftp.net',
                     'agencyServicePort': 9000,
                     'take': 10,
                     'skip': 0,
                     'page': 1,
                     'pageSize': 10}),
            Task(task_type=TaskType.SCRAPE_DATA,
                 endpoint=_ENDPOINT,
                 response_type=ResponseType.JSON,
                 params={
                     'three': 1,
                     'fnmeLetters': '',
                     'lnmeLetters': '',
                     'needDate': 'false',
                     'startDate': '',
                     'stopDate': '',
                     'getImg': 'false',
                     'agencyServiceIP': 'mcminnsheriff.serveftp.net',
                     'agencyServicePort': 9000,
                     'take': 10,
                     'skip': 10,
                     'page': 2,
                     'pageSize': 10}),
            Task(task_type=TaskType.SCRAPE_DATA,
                 endpoint=_ENDPOINT,
                 response_type=ResponseType.JSON,
                 params={
                     'three': 1,
                     'fnmeLetters': '',
                     'lnmeLetters': '',
                     'needDate': 'false',
                     'startDate': '',
                     'stopDate': '',
                     'getImg': 'false',
                     'agencyServiceIP': 'mcminnsheriff.serveftp.net',
                     'agencyServicePort': 9000,
                     'take': 10,
                     'skip': 20,
                     'page': 3,
                     'pageSize': 10})
        ]

        self.validate_and_return_get_more_tasks(content, params,
                                                expected_result)

    def test_populate_data(self):
        content = _PAGE
        expected_result = IngestInfo()

        p1 = expected_result.create_person(person_id='5', surname='LAST',
                                           given_names='FIRST',
                                           birthdate='1989', gender='M',
                                           place_of_residence='123 PLACE PLACE')
        b1 = p1.create_booking(custody_status='CURRENTLY BOOKED')
        b1.create_arrest(date='1/1/1111')
        b1.create_charge(charge_id='16',
                         statute='TITLE',
                         name='VIOLATION',
                         charge_class='Felony',
                         number_of_counts='1',
                         court_type='CRIMINAL',
                         charge_notes='REVOKED').create_bond(amount='0')

        p2 = expected_result.create_person(person_id='18',
                                           surname='SIMPSON',
                                           given_names='BART',
                                           birthdate='1989',
                                           gender='M',
                                           place_of_residence='SPRINGFIELD')
        b2 = p2.create_booking(custody_status='CURRENTLY BOOKED')
        b2.create_arrest(date='1/1/1111')
        charge2 = b2.create_charge(charge_id='17',
                                   statute='TITLE',
                                   name='VIOLATIONOF PROBATION',
                                   charge_class='Misdemeanor',
                                   number_of_counts='1',
                                   court_type='GENERALSESSIONS',
                                   charge_notes='')
        charge2.create_bond(amount='1234.56')

        self.validate_and_return_populate_data(content, expected_result)
