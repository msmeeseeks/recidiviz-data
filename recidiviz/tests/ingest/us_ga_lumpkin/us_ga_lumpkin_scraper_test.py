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

"""Scraper tests for us_ga_lumpkin."""
import unittest

from recidiviz.ingest.models.ingest_info import (Bond, Booking, Charge,
                                                 IngestInfo)
from recidiviz.ingest.us_ga_lumpkin.us_ga_lumpkin_scraper import \
    UsGaLumpkinScraper
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.ingest.vendors.zuercher.zuercher_scraper_test import \
    ZuercherScraperTest

_LOAD_JSON = fixtures.as_dict('us_ga_lumpkin', 'load.json')


class TestUsGaLumpkinScraper(ZuercherScraperTest, unittest.TestCase):

    def _init_scraper_and_yaml(self):
        self.scraper = UsGaLumpkinScraper()

    def test_populate_data(self):
        expected_info = IngestInfo()

        expected_info.create_person(
            full_name='CHARGES, BASIC',
            gender='Male',
            age='21',
            race='White',
            bookings=[Booking(
                admission_date='2019-01-19',
                charges=[Charge(
                    offense_date='01/19/2019',
                    statute='42-8-38',
                    name='Probation Violation',
                    charge_notes='Arrest warrant 18-526A,B,C issued by ' +
                    'Lumpkin County, GA',
                ), ],
            ), ],
        )
        expected_info.create_person(
            full_name='CHARGES, UNSPECIFIED KEY',
            gender='Male',
            age='62',
            race='White',
            bookings=[Booking(
                admission_date='2019-01-18',
                charges=[Charge(
                    offense_date='12/29/1999',
                    name='19-13-6 -',
                    bond=Bond(
                        amount='$1050.00',
                        bond_type='Other',
                    ),
                ), ],
            ), ],
        )
        expected_info.create_person(
            full_name='HOLD, NEW TYPE',
            gender='Male',
            age='32',
            race='White',
            bookings=[Booking(
                admission_date='2019-01-08',
                charges=[Charge( 
                    offense_date='02/01/2019',
                    name='DRC Sanction for Lumpkin County Sheriff\'s Office',
                ), ],
            ), ],
        )

        self.validate_and_return_populate_data(_LOAD_JSON, expected_info)
