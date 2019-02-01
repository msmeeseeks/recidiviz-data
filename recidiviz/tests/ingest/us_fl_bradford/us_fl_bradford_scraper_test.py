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

"""Scraper tests for us_fl_bradford."""
import unittest
from lxml import html

from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.us_fl_bradford.us_fl_bradford_scraper import \
    UsFlBradfordScraper
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils.base_scraper_test import BaseScraperTest

_PRINT_PAGE_HTML = html.fromstring(
    fixtures.as_string('us_fl_bradford', 'print.html'))


class TestUsFlBradfordScraper(BaseScraperTest, unittest.TestCase):
    """Only populate_data needs to be tested, since the initial task is a
    single SCRAPE_DATA task."""

    def _init_scraper_and_yaml(self):
        self.scraper = UsFlBradfordScraper()

    def test_populate_data(self):
        expected_info = IngestInfo()
        p1 = expected_info.create_person(person_id='MNINUMBER',
                                         full_name='LAST, FIRST',
                                         birthdate='1/1/1111',
                                         gender='\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t'
                                                '        MALE',
                                         race='\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t'
                                              '        \xa0 W',
                                         age='21',
                                         place_of_residence='123 AAAA')
        b1 = p1.create_booking(booking_id='BKGNO',
                               admission_date='1/1/1111 01:01 PM',
                               custody_status='In Jail')
        b1.create_charge(statute='784.021.1a',
                         name='CHARGE NAME 1',
                         degree='T', level='F',
                         case_number='23456 (BRADFORD'
                                     '\n\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t'
                                     'COUNTY SHERIFFS OFFICE)').create_bond(
            amount='$10000.00')
        b1.create_charge(statute='893.13.6a',
                         name='CHARGE-2',
                         degree='T',
                         level='F',
                         case_number='12345 (BRADFORD'
                                     '\n\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t'
                                     'COUNTY SHERIFFS OFFICE)').create_bond(
            amount='$5000.00')

        p2 = expected_info.create_person(person_id='PID2',
                                         full_name='LAST2,FIRST2',
                                         birthdate='1/1/1111',
                                         gender='\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t'
                                                '        MALE',
                                         race='\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t'
                                              '        \xa0 B',
                                         age='24',
                                         place_of_residence='3245')
        p2.create_booking(booking_id='BID2',
                          admission_date='2/2/2018 03:21 PM',
                          custody_status='In Jail').create_hold(
            jurisdiction_name='ALACHUA')

        with self.assertWarns(UserWarning):
            self.validate_and_return_populate_data(_PRINT_PAGE_HTML,
                                                   expected_info)
