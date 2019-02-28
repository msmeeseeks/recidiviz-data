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

"""Scraper tests for us_wa_king."""
import unittest

from recidiviz.ingest.models.ingest_info import (Bond, Booking, Charge,
                                                 IngestInfo)
from recidiviz.ingest.scrape.constants import ResponseType, TaskType
from recidiviz.ingest.scrape.regions.us_wa_king.us_wa_king_scraper import \
    UsWaKingScraper
from recidiviz.ingest.scrape.task_params import Task
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils.base_scraper_test import BaseScraperTest

_SEARCH_JSON = fixtures.as_dict('us_wa_king', 'SearchByCCN.json')
_NO_RESULTS_JSON = {
    'd': '{"People": null, "RecordCount": 0, '
         '"NoResultsMessage": "No results were found matching your query."}'
}


class TestUsWaKingScraper(BaseScraperTest, unittest.TestCase):
    """Scraper tests for us_wa_king."""
    def _init_scraper_and_yaml(self):
        self.scraper = UsWaKingScraper()

    def test_get_more_tasks(self):
        task = self.scraper.get_initial_task()
        content = {
            'd': '{"People": ['
                 '  {'
                 '      "CCN": "0001",'
                 '      "CurrentCustodyFacility": "Seattle",'
                 '      "LatestBooking": {"BA": "90001"}'
                 '  }, {'
                 '      "CCN": "0002",'
                 '      "CurrentCustodyFacility": "Bellevue",'
                 '      "LatestBooking": {"BA": "90002"}'
                 '  }'
                 ']}'
        }
        expected_tasks = [Task(
            task_type=TaskType.SCRAPE_DATA,
            endpoint=self.scraper.get_region().base_url + '/SearchByCCN',
            response_type=ResponseType.JSON,
            headers={'Content-Type': 'application/json; charset=utf-8'},
            params={'ccn': '"0001"', 'isSearch': 'false'},
            custom={'person': {
                'CCN': '0001',
                'CurrentCustodyFacility': 'Seattle',
                'LatestBooking': {'BA': '90001',},
            }, }
        ), Task(
            task_type=TaskType.SCRAPE_DATA,
            endpoint=self.scraper.get_region().base_url + '/SearchByCCN',
            response_type=ResponseType.JSON,
            headers={'Content-Type': 'application/json; charset=utf-8'},
            params={'ccn': '"0002"', 'isSearch': 'false'},
            custom={'person': {
                'CCN': '0002',
                'CurrentCustodyFacility': 'Bellevue',
                'LatestBooking': {'BA': '90002',},
            }, }
        ),]
        self.validate_and_return_get_more_tasks(content, task, expected_tasks)

    def test_populate_data(self):
        expected = IngestInfo()

        expected.create_person(
            person_id='12345678',
            surname='PERSON',
            given_names='FIRST',
            middle_names='LISTED',
            bookings=[Booking(
                booking_id='00000002',
                admission_date='12/16/2018 05:13 AM',
                custody_status='True',
                facility='King County Correctional Facility - Seattle (Seattle '
                'Correctional Facility)',
                charges=[Charge(
                    statute='1399',
                    name='ASSAULT INV (NCIC Code: 1399)',
                    status='INVESTIGATED AND CHARGED',
                    bond=Bond(
                        amount='BAIL DENIED',
                    ),
                ), Charge(
                    statute='2899',
                    name='STOLEN PROPERTY INV (NCIC Code: 2899)',
                    status='INVESTIGATED AND CHARGED',
                    bond=Bond(
                        amount='BAIL DENIED',
                    ),
                ), Charge(
                    statute='3599',
                    name='VUCSA/ILL DRUGS INV (NCIC Code: 3599)',
                    status='INVESTIGATED AND CHARGED',
                    bond=Bond(
                        amount='BAIL DENIED',
                    ),
                ), Charge(
                    statute='9A.56.070',
                    name='POSS STOLEN VEHICLE (NCIC Code: 2499)',
                    case_number='181067452',
                    court_type='K C Superior Court ',
                    bond=Bond(
                        amount='$100,000.00',
                    ),
                ), ],
            ), Booking(
                booking_id='00000001',
                admission_date='06/24/2018 06:50 AM',
                release_date='06/25/2018 08:55 PM',
                release_reason='CONDITIONAL RELEASE',
                charges=[Charge(
                    statute='2299',
                    name='BURGLARY INV (NCIC Code: 2299)',
                    status='CONDITIONAL RELEASE',
                    bond=Bond(
                        amount='BAIL DENIED',
                    ),
                ), Charge(
                    statute='3599',
                    name='VUCSA/ILL DRUGS INV (NCIC Code: 3599)',
                    status='CONDITIONAL RELEASE',
                    bond=Bond(
                        amount='BAIL DENIED',
                    ),
                ), ],
            ), ],
        )

        self.validate_and_return_populate_data(
            _SEARCH_JSON, expected, task=Task(
                task_type=TaskType.SCRAPE_DATA, endpoint='blank',
                params={'CCN': '12345678'},
                custom={'person': {
                    'CCN': '12345678',
                    'CurrentCustodyFacility': 'Seattle Correctional Facility',
                    'LatestBooking': {'BA': 'don\'t use',},
                }, }))

    def test_populate_data_empty_response(self):
        expected = IngestInfo()

        expected.create_person(
            person_id='12345678',
            surname='PERSON',
            given_names='FIRST',
            middle_names='LISTED',
            bookings=[Booking(
                booking_id='00000001',
                admission_date='02/25/2019 10:54 PM',
                facility='Seattle Correctional Facility',
                charges=[Charge(
                    name='ASSAULT 4 (DV)',
                ), ],
            ),  ],
        )

        self.validate_and_return_populate_data(
            _NO_RESULTS_JSON, expected, task=Task(
                task_type=TaskType.SCRAPE_DATA, endpoint='blank',
                params={'CCN': '12345678'},
                custom={'person': {
                    'LastName': 'PERSON',
                    'FirstName': 'FIRST',
                    'MiddleName': 'LISTED',
                    'CCN': '12345678',
                    'CurrentCustodyFacility': 'Seattle Correctional Facility',
                    'LatestBooking': {
                        'BA': '00000001',
                        'BookingDate': '02/25/2019 10:54 PM',
                        'Charges': [],
                        'ChargesString': 'ASSAULT 4 (DV)'
                    },
                }, }))
