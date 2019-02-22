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

"""Scraper tests for us_fl_okeechobee."""
import unittest
from lxml import html

from recidiviz.ingest.scrape import constants
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.scrape.task_params import Task
from recidiviz.ingest.scrape.regions.us_fl_okeechobee.us_fl_okeechobee_scraper import UsFlOkeechobeeScraper
from recidiviz.tests.utils.base_scraper_test import BaseScraperTest


class TestUsFlOkeechobeeScraper(BaseScraperTest, unittest.TestCase):
    """Only populate_data needs to be tested, since the initial task is a
    single SCRAPE_DATA task."""

    def _init_scraper_and_yaml(self):
        self.scraper = UsFlOkeechobeeScraper()

    def test_populate_data(self):
        content = '''
Name #,Last,First,Middle,D.O.B.,Age,Last Intake,Race,Days in Custody,Booking Type,Scheduled Release
123,LLL,FFF,MMM,1/23/1945,99,02/02/2019 11:18:15,"HISPANIC",3,VIOLATION OF PROBATION,
456,L2,F2,M2,08/07/1986,17,10/20/2018 2:22:22,"N-BLACK, NON-HISP",119,WARRANT,05/28/2019 7:15:01
888,NO,MIDDLENAME,,3/03/1933,33,08/15/2018 7:15:12,"N-WHITE, NON HISPANIC",101,BOND REVOCATION,
3131,ABC,DEF,GHI,11/11/1911,11,11/12/1911 11:11:11,"N-INDIAN/ALASKAN NAT, NON-HISP",11,EXTRADITION,
        '''.strip()
        expected_info = IngestInfo()
        p1 = expected_info.create_person(person_id='123',
                                         given_names='FFF',
                                         middle_names='MMM',
                                         surname='LLL',
                                         birthdate='1/23/1945',
                                         race='HISPANIC',
                                         age='99')
        p1.create_booking(admission_date='02/02/2019 11:18:15',
                          admission_reason='VIOLATION OF PROBATION')

        p2 = expected_info.create_person(person_id='456',
                                         given_names='F2',
                                         middle_names='M2',
                                         surname='L2',
                                         birthdate='08/07/1986',
                                         race='N-BLACK, NON-HISP',
                                         ethnicity='NOT_HISPANIC',
                                         age='17')
        p2_booking = p2.create_booking(
            admission_date='10/20/2018 2:22:22',
            projected_release_date='05/28/2019 7:15:01')
        p2_booking.create_charge(status='WARRANT')

        p3 = expected_info.create_person(person_id='888',
            surname='NO',
            given_names='MIDDLENAME',
            birthdate='3/03/1933',
            age='33',
            race='N-WHITE, NON HISPANIC',
            ethnicity='NOT_HISPANIC')
        p3_booking = p3.create_booking(admission_date='08/15/2018 7:15:12')
        p3_charge = p3_booking.create_charge()
        p3_charge.create_bond(status='BOND REVOCATION')

        p4 = expected_info.create_person(
            person_id='3131',
            surname='ABC',
            given_names='DEF',
            middle_names='GHI',
            birthdate='11/11/1911',
            age='11',
            race='N-INDIAN/ALASKAN NAT, NON-HISP',
            ethnicity='NOT_HISPANIC')
        p4_booking = p4.create_booking(admission_date='11/12/1911 11:11:11')
        p4_hold = p4_booking.create_hold(jurisdiction_name='EXTRADITION',
            status='EXTRADITION')

        self.validate_and_return_populate_data(content, expected_info)
