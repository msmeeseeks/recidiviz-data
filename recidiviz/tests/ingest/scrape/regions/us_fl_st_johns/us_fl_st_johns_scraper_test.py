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

"""Scraper tests for us_fl_st_johns."""
import unittest
from lxml import html

from recidiviz.ingest.models.ingest_info import IngestInfo, Bond
from recidiviz.ingest.scrape.regions.us_fl_st_johns.us_fl_st_johns_scraper \
    import UsFlStJohnsScraper
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils.base_scraper_test import BaseScraperTest

_PRINT_PAGE_HTML = html.fromstring(
    fixtures.as_string('us_fl_st_johns', 'print.html'))


class TestUsFlStJohnsScraper(BaseScraperTest, unittest.TestCase):
    """Only populate_data needs to be tested, since the initial task is a
    single SCRAPE_DATA task."""

    def _init_scraper_and_yaml(self):
        self.scraper = UsFlStJohnsScraper()

    def test_populate_data(self):
        expected_info = IngestInfo()

        p1 = expected_info.create_person(person_id='YYYYYYYYYYYYYY1',
                                         full_name='LASTNAME1, FIRSTNAME1',
                                         gender='\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t'
                                                '        MALE',
                                         race='\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t'
                                              '        \xa0 W',
                                         age='30',
                                         place_of_residence='9999 N OAK, FL 32033')

        b1 = p1.create_booking(booking_id='XXXXXXXXXXXXXX1',
                               admission_date='01/01/2010 12:00 PM',
                               custody_status='In Jail')
        
        b1.create_charge(statute='100.000',
                         name='CONTEMPT OF COURT',
                         degree='N', level='N',
                         case_number="99-9999DR (ST. JOHNS COUNTY SHERIFF'S OFFICE)",
                         bond=Bond(amount='$99.00'))


        p2 = expected_info.create_person(person_id='YYYYYYYYYYYYYY2',                        
                                         full_name='LASTNAME2, FIRSTNAME2',
                                         gender='\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t'
                                                '        MALE',
                                         race='\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t'
                                              '        \xa0 W',
                                         age='30',
                                         place_of_residence='99 N OAK, FL 32086')

        b2 = p2.create_booking(booking_id='XXXXXXXXXXXXXX2',
                               admission_date='01/10/2010 12:00 PM',
                               custody_status='In Jail')
        
        b2.create_charge(statute='999.99.1B1',
                         name='BATTERY',
                         degree='F', level='M',
                         case_number="000-0000 (ST. JOHNS COUNTY SHERIFF'S OFFICE)",
                         bond=Bond(amount='NO BOND'))
        
        self.validate_and_return_populate_data(_PRINT_PAGE_HTML, expected_info)
