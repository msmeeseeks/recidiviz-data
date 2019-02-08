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

"""Scraper tests for us_ga_toombs."""
import unittest

from recidiviz.ingest.models.ingest_info import (Bond, Booking, Charge, Hold,
                                                 IngestInfo)
from recidiviz.ingest.scrape.regions.us_ga_toombs.us_ga_toombs_scraper import \
    UsGaToombsScraper
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.ingest.scrape.vendors.zuercher.zuercher_scraper_test \
    import ZuercherScraperTest

_LOAD_JSON = fixtures.as_dict('us_ga_toombs', 'load.json')


class TestUsGaToombsScraper(ZuercherScraperTest, unittest.TestCase):

    def _init_scraper_and_yaml(self):
        self.scraper = UsGaToombsScraper()

    def test_populate_data(self):
        expected_info = IngestInfo()

        expected_info.create_person(
            full_name='ADAMS, ONE',
            gender='Male',
            age='30',
            race='White',
            bookings=[Booking(
                admission_date='2019-01-24',
                admission_reason='PAROLE_VIOLATION',
                charges=[Charge(
                    name='Parole Hold: Parole Warrant warrant 677956',
                    charge_class='PAROLE_VIOLATION',
                    status='SENTENCED',
                ), ],
            ), ],
        )
        expected_info.create_person(
            full_name='ADAMS, TWO',
            gender='Male',
            age='18',
            race='Black',
            bookings=[Booking(
                admission_date='2018-12-21',
                holds=[Hold(
                    jurisdiction_name='Treutlen County Sheriff\'s Office',
                ), ],
            ), ],
        )
        expected_info.create_person(
            full_name='ADAMS, THREE',
            gender='Male',
            age='27',
            race='White',
            bookings=[Booking(
                admission_date='2019-01-12',
                holds=[Hold(
                    jurisdiction_name='Orange County, Virginia',
                ), ],
            ), ],
        )
        expected_info.create_person(
            full_name='ADAMS, FOUR',
            gender='Male',
            age='44',
            race='White',
            bookings=[Booking(
                admission_date='2019-01-03',
                admission_reason='PROBATION_VIOLATION',
                charges=[Charge(
                    offense_date='01/16/2019',
                    name='Mi Probation: Probation- Misdemeanor warrant 18SR203',
                    charge_class='PROBATION_VIOLATION',
                    status='SENTENCED',
                    judge_name='Probation',
                    bond=Bond(
                        amount='$300.00',
                        bond_type='Cash',
                    ),
                ), ],
            ), ],
        )
        expected_info.create_person(
            full_name='ADAMS, FIVE',
            gender='Male',
            age='21',
            race='White',
            bookings=[Booking(
                admission_date='2019-01-23',
                admission_reason='PROBATION_VIOLATION',
                charges=[Charge(
                    offense_date='10/16/2018',
                    name='Fe Probation: Probation- Felony warrant 15CR158',
                    charge_class='PROBATION_VIOLATION',
                    status='SENTENCED',
                ), ],
            ), ],
        )
        expected_info.create_person(
            full_name='ADAMS, SIX',
            gender='Male',
            age='32',
            race='Black',
            bookings=[Booking(
                admission_date='2018-12-21',
                charges=[Charge(
                    offense_date='01/30/2019',
                    name='Sentenced for Vidalia City Court',
                    status='SENTENCED',
                ), ],
            ), ],
        )
        expected_info.create_person(
            full_name='ADAMS, SEVEN',
            gender='Male',
            age='24',
            race='Black',
            bookings=[Booking(
                admission_date='2018-06-19',
                charges=[Charge(
                    offense_date='01/03/2019',
                    name='Child Support Pickup Order warrant 18CV00253',
                    judge_name='Child Support',
                    bond=Bond(
                        amount='$1600.00',
                        bond_type='Cash Bond',
                    ),
                ), ],
            ), ],
        )
        expected_info.create_person(
            full_name='ADAMS, EIGHT',
            gender='Female',
            age='19',
            race='White',
            bookings=[Booking(
                admission_date='2018-10-17',
                charges=[Charge(
                    offense_date='11/28/2018',
                    name='Revoked Bond for Toombs County Superior Court',
                    status='PRETRIAL',
                    judge_name='RReeves- Superior Court',
                    bond=Bond(
                        bond_type='Revoked Bond',
                    ),
                ), ],
            ), ],
        )
        expected_info.create_person(
            full_name='ADAMS, NINE',
            gender='Male',
            age='21',
            race='Black',
            bookings=[Booking(
                admission_date='2019-01-28',
                charges=[Charge(
                    offense_date='01/22/2019',
                    name='No Bond-Previous Case for Toombs County Superior '
                    'Court',
                    status='PRETRIAL',
                    bond=Bond(
                        bond_type='No Bond',
                    ),
                ), ],
            ), ],
        )
        expected_info.create_person(
            full_name='ADAMS, TEN',
            gender='Male',
            age='48',
            race='Black',
            bookings=[Booking(
                admission_date='2018-08-20',
                charges=[Charge(
                    offense_date='01/25/2019',
                    name='OFF BOND for Toombs County Superior Court',
                    status='PRETRIAL',
                    judge_name='KPalmer- Superior Court',
                    bond=Bond(
                        amount='$5000.00',
                        bond_type='Property Bond',
                    ),
                ), ],
            ), ],
        )
        expected_info.create_person(
            full_name='ADAMS, ELEVEN',
            gender='Male',
            age='22',
            race='Black',
            bookings=[Booking(
                admission_date='2019-01-28',
                charges=[Charge(
                    offense_date='01/08/2019',
                    name='Bench- FTA- Superior Court warrant 18CR80',
                ), ],
            ), ],
        )
        expected_info.create_person(
            full_name='ADAMS, TWELVE',
            gender='Male',
            age='42',
            race='Black',
            bookings=[Booking(
                admission_date='2018-11-28',
                holds=[Hold(
                    jurisdiction_name='Tattnall County Sheriff\'s Office',
                ), ],
            ), ],
        )
        expected_info.create_person(
            full_name='ADAMS, THIRTEEN',
            gender='Male',
            age='23',
            race='White',
            bookings=[Booking(
                admission_date='2018-08-06',
                holds=[Hold(
                    jurisdiction_name='Toombs County Superior Court',
                ), ],
            ), ],
        )
        expected_info.create_person(
            full_name='ADAMS, FOURTEEN',
            gender='Male',
            age='39',
            race='White',
            bookings=[Booking(
                admission_date='2019-01-17',
                charges=[Charge(
                    offense_date='06/19/2018',
                    statute='16-7-1',
                    name='Burglary 2nd Degree',
                    judge_name='RO- First Appearance',
                    charge_notes='Felony Arrest warrant 06-19-18-1R',
                    bond=Bond(
                        bond_type='No Bond',
                    ),
                ), Charge(
                    offense_date='07/10/2018',
                    statute='16-7-1',
                    name='Burglary',
                    degree='1st',
                    judge_name='RO- First Appearance',
                    charge_notes='Felony Arrest warrant 07-10-18-11R',
                    bond=Bond(
                        bond_type='No Bond',
                    ),
                ), ],
                holds=[Hold(
                    jurisdiction_name='Bulloch County Sheriff\'s Office',
                ), ],
            ), ],
        )

        self.validate_and_return_populate_data(_LOAD_JSON, expected_info)
