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

"""Scraper tests for us_ga_floyd."""
import itertools
import unittest

from recidiviz.ingest.models.ingest_info import (Bond, Booking, Charge, Hold,
                                                 IngestInfo, Sentence)
from recidiviz.ingest.us_ga_floyd.us_ga_floyd_scraper import UsGaFloydScraper
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.ingest.vendors.zuercher.zuercher_scraper_test import \
    ZuercherScraperTest

_LOAD_JSON = fixtures.as_dict('us_ga_floyd', 'load.json')

class TestUsGaFloydScraper(ZuercherScraperTest, unittest.TestCase):

    def _init_scraper_and_yaml(self):
        self.scraper = UsGaFloydScraper()

    def test_populate_data(self):
        expected_info = IngestInfo()

        expected_info.create_person(
            full_name='ADAMS, ONE',
            gender='Male',
            age='27',
            race='Black',
            bookings=[Booking(
                admission_date='2019-01-23',
                holds=[Hold(
                    jurisdiction_name='Habersham County Sheriff\'s Office',
                ),],
            ),],
        )

        expected_info.create_person(
            full_name='ADAMS, TWO',
            gender='Male',
            age='58',
            race='White',
            bookings=[Booking(
                admission_date='2018-04-04',
                charges=[Charge(
                    statute='42-8-38',
                    name='PROBATION VIOLATION (WHEN PROBATION TERMS ARE ' +
                    'ALTERED)',
                    charge_class='FELONY',
                    charge_notes='Probation warrant 16CR02594R issued by ' +
                    'Floyd County, GA',
                ),],
            ),],
        )

        expected_info.create_person(
            full_name='ADAMS, THREE',
            gender='Male',
            age='36',
            race='Black',
            bookings=[Booking(
                admission_date='2017-12-11',
                # Here we lose the arrest date of 09/27/2018
                holds=[Hold(
                    jurisdiction_name='U.S. Immigration & Customs Enforcment',
                ),],
            ),],
        )

        expected_info.create_person(
            full_name='ADAMS, FOUR',
            gender='Male',
            age='20',
            race='Black',
            bookings=[Booking(
                admission_date='2019-01-28',
                holds=[Hold(
                    jurisdiction_name='Chattooga County Sheriff\'s Office',
                ),],
            ),],
        )

        expected_info.create_person(
            full_name='ADAMS, FIVE',
            gender='Male',
            age='21',
            race='White',
            bookings=[Booking(
                admission_date='2018-10-26',
                charges=[Charge(
                    offense_date='12/25/2017',
                    statute='16-5-60(B)',
                    name='RECKLESS CONDUCT (Cleared by Arrest)',
                    number_of_counts='4',
                    bond=Bond(
                        bond_type='Property',
                    ),
                ),],
            ),],
        )

        expected_info.create_person(
            full_name='ADAMS, SIX',
            gender='Male',
            age='26',
            race='White',
            bookings=[Booking(
                admission_date='2019-01-14',
                charges=[Charge(
                    offense_date='10/25/2018',
                    statute='42-8-38',
                    name='PROBATION VIOLATION (WHEN PROBATION TERMS ARE ' +
                    'ALTERED)',
                    charge_class='FELONY',
                    judge_name='Robert Couey, Floyd Magistrate Court',
                    charge_notes='Probation warrant 12CR01193AR5 issued by ' +
                    'Floyd County, GA',
                    bond=Bond(
                        bond_type='No Bond',
                    ),
                ),],
                holds=[Hold(
                    jurisdiction_name='Gordon County Sheriff\'s Office',
                ),],
            ),],
        )

        expected_info.create_person(
            full_name='ADAMS, SEVEN',
            gender='Female',
            age='31',
            race='White',
            bookings=[Booking(
                admission_date='2018-12-05',
                charges=[Charge(
                    offense_date='10/16/2018',
                    name='RE-BOOK for Floyd County Sheriff\'s Office',
                    bond=Bond(
                        bond_type='No Bond',
                    ),
                ),],
                # Here we lose the arrest date of 10/16/2018.
                holds=[Hold(
                    jurisdiction_name='Georgia Department of Corrections',
                ),],
            ),],
        )

        expected_info.create_person(
            full_name='ADAMS, EIGHT',
            gender='Female',
            age='41',
            race='White',
            bookings=[Booking(
                admission_date='2019-01-25',
                # Probation revocation
                charges=[Charge(
                    offense_date='01/22/2019',
                    name='Court Order',
                    charge_notes='Probation warrant 18TR03261 issued by ' +
                    'Floyd County, GA',
                    bond=Bond(
                        bond_type='No Bond',
                    ),
                ),],
            ),],
        )

        expected_info.create_person(
            full_name='ADAMS, NINE',
            gender='Male',
            age='46',
            race='White',
            bookings=[Booking(
                admission_date='2019-01-31',
                charges=[Charge(
                    offense_date='07/26/2018',
                    statute='16-5-1',
                    name='Murder; felony murder - Gun - NonFamily',
                    status='PRETRIAL',
                    judge_name='Robert Couey, Floyd Magistrate Court',
                    charge_notes='Felony warrant 18CW23370 issued by Floyd ' +
                    'County, GA',
                ),],
            ),],
        )

        expected_info.create_person(
            full_name='ADAMS, TEN',
            gender='Male',
            age='32',
            race='White',
            bookings=[Booking(
                admission_date='2019-01-20',
                charges=[Charge(
                    statute='16-5-23.1(F)(1)',
                    name='BATTERY - FAMILY VIOLENCE (1ST OFFENSE) (Cleared ' +
                    'by Arrest)',
                    charge_class='MISD',
                ),],
            ),],
        )

        expected_info.create_person(
            full_name='ADAMS, ELEVEN',
            gender='Male',
            age='28',
            race='White',
            bookings=[Booking(
                admission_date='2018-06-20',
                charges=[Charge(
                    offense_date='01/04/2019',
                    bond=Bond(
                        bond_type='No Bond',
                    ),
                    sentence=Sentence(
                        min_length='15 days',
                        max_length='15 days',
                    ),
                ),],
            ),],
        )

        expected_info.create_person(
            full_name='ADAMS, TWELVE',
            gender='Male',
            age='24',
            race='White',
            bookings=[Booking(
                admission_date='2018-09-06',
                charges=[*itertools.repeat(Charge(
                    offense_date='11/02/2018',
                    statute='42-8-38',
                    name='PROBATION VIOLATION (WHEN PROBATION TERMS ARE ' +
                    'ALTERED)',
                    charge_class='FELONY',
                    status='Guilty (CCH-310)',
                    judge_name='John E. Niedrach, Floyd Superior Court',
                    bond=Bond(
                        bond_type='No Bond',
                    ),
                ), 2), Charge(
                    offense_date='11/21/2018',
                    name='Court Order',
                    status='SENTENCED',
                    bond=Bond(
                        bond_type='No Bond',
                    ),
                ),],
                holds=[Hold(
                    jurisdiction_name='Polk County Sheriff\'s Office',
                ),],
            ),],
        )

        expected_info.create_person(
            full_name='ADAMS, THIRTEEN',
            gender='Male',
            age='27',
            race='Black',
            bookings=[Booking(
                admission_date='2017-12-25',
                # Note: Should be same bond object for all three bonds
                charges=[Charge(
                    offense_date='12/25/2017',
                    statute='16-7-60',
                    name='Arson in the first degree (Residence) (Cleared by ' +
                    'Arrest)',
                    judge_name='Crystal Burkhalter, Floyd Magistrate Court',
                    bond=Bond(
                        amount='$7900.00',
                        bond_type='Property',
                    ),
                ), Charge(
                    offense_date='12/25/2017',
                    statute='16-5-60(B)',
                    name='RECKLESS CONDUCT (Cleared by Arrest)',
                    number_of_counts='4',
                    judge_name='Crystal Burkhalter, Floyd Magistrate Court',
                    bond=Bond(
                        amount='$7900.00',
                        bond_type='Property',
                    ),
                ), Charge(
                    offense_date='12/25/2017',
                    statute='16-5-102(a)',
                    name='Exploitation and Intimidation of disabled adults, ' +
                    'elder persons, and residents. (Cleared by Arrest)',
                    charge_class='Felony',
                    judge_name='Crystal Burkhalter, Floyd Magistrate Court',
                    bond=Bond(
                        amount='$7900.00',
                        bond_type='Property',
                    ),
                ),],
            ),],
        )

        expected_info.create_person(
            full_name='ADAMS, FOURTEEN',
            gender='Male',
            age='28',
            race='Black',
            bookings=[Booking(
                admission_date='2019-01-25',
                charges=[Charge(
                    offense_date='01/25/2019',
                    statute='17-6-12',
                    name='FAILURE TO APPEAR',
                    charge_class='MISDEMEANOR',
                    judge_name='John E. Niedrach, Floyd Superior Court',
                    charge_notes='Bench warrant 18CR02781 issued by Floyd ' +
                    'County, GA',
                    bond=Bond(
                        amount='$2600.00',
                        bond_type='Property',
                    ),
                ),],
            ),],
        )

        expected_info.create_person(
            full_name='ADAMS, FIFTEEN',
            gender='Female',
            age='34',
            race='Black',
            bookings=[Booking(
                admission_date='2019-01-14',
                charges=[Charge(
                    offense_date='12/13/2018',
                    statute='16-5-41',
                    name='FALSE IMPRISONMENT (Cleared by Arrest)',
                    judge_name='WilliamSparks, Floyd Superior Court',
                    bond=Bond(
                        amount='$15000.00',
                    ),
                ), Charge(
                    offense_date='12/13/2018',
                    statute='16-8-40',
                    name='Robbery - Strongarm - Business (Cleared by Arrest)',
                    judge_name='WilliamSparks, Floyd Superior Court',
                    bond=Bond(
                        amount='$15000.00',
                    ),
                ), Charge(
                    offense_date='12/13/2018',
                    statute='16-11-41',
                    name='PUBLIC DRUNK (Cleared by Arrest)',
                    judge_name='WilliamSparks, Floyd Superior Court',
                    bond=Bond(
                        amount='$15000.00',
                    ),
                ),],
            ),],
        )

        self.validate_and_return_populate_data(_LOAD_JSON, expected_info)
