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

"""Scraper tests for us_fl_columbia."""
import unittest
from lxml import html

from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.scrape.regions.us_fl_columbia.us_fl_columbia_scraper \
    import UsFlColumbiaScraper
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils.base_scraper_test import BaseScraperTest

_PRINT_PAGE_HTML = html.fromstring(
    fixtures.as_string('us_fl_columbia', 'print.html'))


class TestUsFlColumbiaScraper(BaseScraperTest, unittest.TestCase):
    """Only populate_data needs to be tested, since the initial task is a
    single SCRAPE_DATA task."""

    def _init_scraper_and_yaml(self):
        self.scraper = UsFlColumbiaScraper()

    def test_populate_data(self):
        expected_info = IngestInfo()
        p1 = expected_info.create_person(person_id='dsfjgivcovdf',
                                         full_name='AAAAAAAAA',
                                         birthdate='1/1/1111',
                                         gender='\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t'
                                                '        MALE',
                                         race='\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t'
                                              '        \xa0 W',
                                         age='22',
                                         place_of_residence='asdhsfhishf')
        b1 = p1.create_booking(booking_id='ASDFjgih',
                               admission_date='01/01/2000 04:48 AM',
                               custody_status='In Jail')
        b1.create_charge(statute='123456',
                         name='sasfdzghderdsf',
                         degree='T',
                         level='F',
                         case_number='PC (LAKE CITY POLICE DEPARTMENT)'
                         ).create_bond(amount='$5000.00')

        p2 = expected_info.create_person(person_id='asdfggdfgdf',
                                         full_name='sdfhjgkhfdsjk',
                                         birthdate='1/1/1111',
                                         gender='\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t'
                                                '        MALE',
                                         race='\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t'
                                              '        \xa0 W',
                                         age='30',
                                         place_of_residence='asdfghjasdfhjfkd')
        p2.create_booking(booking_id='sadfghjk',
                          admission_date='01/01/2011 02:01 PM',
                          custody_status='In Jail')

        with self.assertWarns(UserWarning):
            self.validate_and_return_populate_data(_PRINT_PAGE_HTML,
                                                   expected_info)
