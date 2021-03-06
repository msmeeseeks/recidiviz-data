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

"""Scraper tests for regions that use a jailtracker_scraper.

Any region scraper test class that inherits from JailTrackerScraperTest must
implement the following:

    def _get_scraper(self):
        return RegionScraper()
"""

import abc
from lxml import html

from recidiviz.ingest.scrape import constants
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.scrape.task_params import Task
from recidiviz.ingest.scrape.vendors.jailtracker.jailtracker_scraper import \
    JailTrackerRequestRateExceededError
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils.base_scraper_test import BaseScraperTest

_LANDING_HTML = html.fromstring(
    fixtures.as_string('vendors/jailtracker', 'landing_page.html'))
_ROSTER_JSON = fixtures.as_dict('vendors/jailtracker', 'roster.json')
_PERSON_PROBATION_JSON = fixtures.as_dict('vendors/jailtracker', 'person.json')
_PERSON_AGENCIES_JSON = fixtures.as_dict('vendors/jailtracker',
                                         'person_agencies.json')
_MAX_EXCEEDED_JSON = fixtures.as_dict('vendors/jailtracker',
                                      'max_exceeded.json')
_CASES_JSON = fixtures.as_dict('vendors/jailtracker', 'cases.json')
_CHARGES_JSON = fixtures.as_dict('vendors/jailtracker', 'charges.json')
_CHARGES_WITH_BOND_TYPE_JSON = fixtures.as_dict('vendors/jailtracker',
                                                'charges_sentenced.json')
_CHARGES_WITH_CASES_JSON = fixtures.as_dict('vendors/jailtracker',
                                            'charges_with_cases.json')
# Key in param dict for JSON object for charges.
_CHARGES = "charge"

# pylint:disable=protected-access
class JailTrackerScraperTest(BaseScraperTest):
    """Tests for JailTrackerScraper."""

    SESSION_TOKEN = 'y1n3ohdxid0gx02sgu3o1vsx'

    @abc.abstractmethod
    def _get_scraper(self):
        """Gets the child vendor scraper type.
        """

    def _init_scraper_and_yaml(self):
        # The scraper to be tested. Required.
        self.scraper = self._get_scraper()
        # The path to the yaml mapping. Optional.
        self.yaml = None

        # Set up some things
        suffix_landing = self.scraper._ROSTER_REQUEST_SUFFIX_TEMPLATE.format(
            session=self.SESSION_TOKEN,
            start=0,
            limit=self.scraper._ROSTER_PAGE_SIZE)
        self.roster_endpoint = "/".join(
            [self.scraper._URL_BASE, suffix_landing])

        suffix_cases = self.scraper._CASES_REQUEST_SUFFIX_TEMPLATE.format(
            session=self.SESSION_TOKEN,
            arrest=1)
        self.cases_endpoint = "/".join(
            [self.scraper._URL_BASE, suffix_cases])

    def test_get_more_tasks_landing(self):
        expected_result = [Task(
            task_type=constants.TaskType.GET_MORE_TASKS,
            endpoint=self.roster_endpoint,
            response_type=constants.ResponseType.JSON,
            custom={
                self.scraper._REQUEST_TARGET: self.scraper._ROSTER_REQUEST,
                self.scraper._SESSION_TOKEN: self.SESSION_TOKEN,
            },
        )]

        self.validate_and_return_get_more_tasks(
            _LANDING_HTML, self.scraper.get_initial_task(), expected_result)

    def test_get_more_tasks_rate_error(self):
        task = Task(
            task_type=constants.TaskType.GET_MORE_TASKS,
            endpoint=self.roster_endpoint,
            response_type=constants.ResponseType.JSON,
            custom={
                self.scraper._REQUEST_TARGET: self.scraper._ROSTER_REQUEST,
                self.scraper._SESSION_TOKEN: self.SESSION_TOKEN,
            },
        )

        rate_error_content = {
            'data': '',
            'error': self.scraper._MAX_REQUESTS_EXCEEDED_ERROR_MSG,
            'success': False,
        }

        expected_result = []

        with self.assertRaises(JailTrackerRequestRateExceededError):
            self.validate_and_return_get_more_tasks(
                rate_error_content, task, expected_result)

    def test_get_more_tasks_roster(self):
        task = Task(
            task_type=constants.TaskType.GET_MORE_TASKS,
            endpoint=self.roster_endpoint,
            response_type=constants.ResponseType.JSON,
            custom={
                self.scraper._REQUEST_TARGET: self.scraper._ROSTER_REQUEST,
                self.scraper._SESSION_TOKEN: self.SESSION_TOKEN,
            },
        )

        person_suffix = self.scraper._PERSON_REQUEST_SUFFIX_TEMPLATE.format(
            session=self.SESSION_TOKEN,
            arrest=1)
        person_endpoint1 = "/".join(
            [self.scraper._URL_BASE, person_suffix])

        person_suffix = self.scraper._PERSON_REQUEST_SUFFIX_TEMPLATE.format(
            session=self.SESSION_TOKEN,
            arrest=2)
        person_endpoint2 = "/".join(
            [self.scraper._URL_BASE, person_suffix])

        expected_result = [Task(
            task_type=constants.TaskType.GET_MORE_TASKS,
            endpoint=person_endpoint1,
            response_type=constants.ResponseType.JSON,
            custom={
                self.scraper._ARREST_NUMBER: 1,
                self.scraper._REQUEST_TARGET: self.scraper._PERSON_REQUEST,
                self.scraper._SESSION_TOKEN: self.SESSION_TOKEN,
            },
        ), Task(
            task_type=constants.TaskType.GET_MORE_TASKS,
            endpoint=person_endpoint2,
            response_type=constants.ResponseType.JSON,
            custom={
                self.scraper._ARREST_NUMBER: 2,
                self.scraper._REQUEST_TARGET: self.scraper._PERSON_REQUEST,
                self.scraper._SESSION_TOKEN: self.SESSION_TOKEN,
            },
        )]

        self.validate_and_return_get_more_tasks(
            _ROSTER_JSON, task, expected_result)

    def test_populate_data_probation(self):
        task = Task(
            task_type=constants.TaskType.SCRAPE_DATA,
            endpoint='test',
            response_type=constants.ResponseType.JSON,
            custom={
                self.scraper._ARREST_NUMBER: 1,
                self.scraper._REQUEST_TARGET: self.scraper._CHARGES_REQUEST,
                self.scraper._SESSION_TOKEN: self.SESSION_TOKEN,
                self.scraper._PERSON: _PERSON_PROBATION_JSON,
                self.scraper._CASES: {},
            }
        )

        expected_info = IngestInfo()

        person = expected_info.create_person()
        person.surname = 'Bart'
        person.given_names = 'Simpson'
        person.gender = 'M'
        person.age = '20'
        person.race = 'White'

        booking = person.create_booking()
        booking.booking_id = '1'
        booking.admission_date = '12/5/2018 10:21:00 AM'
        booking.release_reason = 'PROBATION'
        booking.custody_status = 'RELEASED'

        charge = booking.create_charge()
        charge.charge_id = '1'
        charge.name = 'DISORDERLY CONDUCT-NOISE'
        charge.offense_date = '2017-03-10'
        charge.status = 'Probation'
        charge.number_of_counts = '2'
        charge.charge_class = 'M'
        charge.case_number = '11'
        charge.statute = '123'
        bond = charge.create_bond()
        bond.amount = '500'
        bond.bond_type = 'CASH'

        charge = booking.create_charge()
        charge.charge_id = '2'
        charge.name = 'DEPRESSANT/STIMULANT/NARCOTIC-POSSESSION < 100X DOSE'
        charge.offense_date = '2017-03-16'
        charge.status = 'Probation'
        charge.charge_class = 'M'
        charge.number_of_counts = '1'
        charge.case_number = '12'
        charge.statute = '123'
        bond = charge.create_bond()
        bond.amount = '500'
        bond.bond_type = 'CASH'

        self.validate_and_return_populate_data(
            _CHARGES_JSON, expected_info, task=task)

    def test_populate_data_cases(self):
        task = Task(
            task_type=constants.TaskType.SCRAPE_DATA,
            endpoint='test',
            response_type=constants.ResponseType.JSON,
            custom={
                self.scraper._ARREST_NUMBER: 1,
                self.scraper._REQUEST_TARGET: self.scraper._CHARGES_REQUEST,
                self.scraper._SESSION_TOKEN: self.SESSION_TOKEN,
                self.scraper._PERSON: _PERSON_PROBATION_JSON,
                self.scraper._CASES: _CASES_JSON,
            }
        )

        expected_info = IngestInfo()
        person = expected_info.create_person(
            surname='Bart', given_names='Simpson', gender='M', age='20',
            race='White')
        booking = person.create_booking(
            booking_id='1', admission_date='12/5/2018 10:21:00 AM',
            release_reason='PROBATION', custody_status='RELEASED')

        booking.create_charge(
            charge_id='1', name='Charge1', offense_date='2017-03-10')

        charge = booking.create_charge(
            charge_id='2', name='Charge2', offense_date='2017-03-10',
            status='PENDING', case_number='case_id_1')
        charge.create_bond(amount='1', bond_type='CASH')
        charge.create_sentence(min_length='1y 0m 0d', max_length='1y 0m 0d')

        charge = booking.create_charge(
            charge_id='3', name='Charge3', offense_date='2017-03-10',
            status='OPEN', case_number='case_id_2')
        charge.create_bond(amount='0')
        charge.create_sentence(min_length='2y 0m 0d', max_length='2y 0m 0d')

        charge = booking.create_charge(
            charge_id='4', name='Charge4', offense_date='2017-03-10',
            status='OPEN', case_number='case_id_2')
        charge.create_bond(amount='500', bond_type='CASH')
        charge.create_sentence(min_length='2y 0m 0d', max_length='2y 0m 0d')

        self.validate_and_return_populate_data(
            _CHARGES_WITH_CASES_JSON, expected_info, task=task)

    def test_populate_data_charge_type_from_bond_type(self):
        task = Task(
            task_type=constants.TaskType.SCRAPE_DATA,
            endpoint='test',
            response_type=constants.ResponseType.JSON,
            custom={
                self.scraper._ARREST_NUMBER: 1,
                self.scraper._REQUEST_TARGET: self.scraper._CHARGES_REQUEST,
                self.scraper._SESSION_TOKEN: self.SESSION_TOKEN,
                self.scraper._PERSON: _PERSON_PROBATION_JSON,
                self.scraper._CASES: {},
            }
        )

        expected_info = IngestInfo()
        person = expected_info.create_person(
            surname='Bart', given_names='Simpson', gender='M', age='20',
            race='White')
        booking = person.create_booking(
            booking_id='1', admission_date='12/5/2018 10:21:00 AM',
            release_reason='PROBATION', custody_status='RELEASED')
        booking.create_charge(charge_id='1', name='Charge1',
                              offense_date='2017-03-10', status='SENTENCED')

        self.validate_and_return_populate_data(
            _CHARGES_WITH_BOND_TYPE_JSON, expected_info, task=task)

    def test_populate_data_facility(self):
        task = Task(
            task_type=constants.TaskType.SCRAPE_DATA,
            endpoint='test',
            response_type=constants.ResponseType.JSON,
            custom={
                self.scraper._ARREST_NUMBER: 1,
                self.scraper._REQUEST_TARGET: self.scraper._CHARGES_REQUEST,
                self.scraper._SESSION_TOKEN: self.SESSION_TOKEN,
                self.scraper._PERSON: _PERSON_AGENCIES_JSON,
                self.scraper._CASES: {},
            },
        )

        expected_info = IngestInfo()

        person = expected_info.create_person()
        person.surname = 'Bart'
        person.given_names = 'Simpson'
        person.gender = 'M'
        person.age = '20'
        person.race = 'White'

        booking = person.create_booking()
        booking.booking_id = '1'
        booking.admission_date = '12/5/2018 10:21:00 AM'
        booking.facility = 'TEST FACILITY'

        charge = booking.create_charge()
        charge.charge_id = '1'
        charge.name = 'DISORDERLY CONDUCT-NOISE'
        charge.offense_date = '2017-03-10'
        charge.status = 'Probation'
        charge.number_of_counts = '2'
        charge.charge_class = 'M'
        charge.case_number = '11'
        charge.statute = '123'
        bond = charge.create_bond()
        bond.amount = '500'
        bond.bond_type = 'CASH'

        charge = booking.create_charge()
        charge.charge_id = '2'
        charge.name = 'DEPRESSANT/STIMULANT/NARCOTIC-POSSESSION < 100X DOSE'
        charge.offense_date = '2017-03-16'
        charge.status = 'Probation'
        charge.charge_class = 'M'
        charge.number_of_counts = '1'
        charge.case_number = '12'
        charge.statute = '123'
        bond = charge.create_bond()
        bond.amount = '500'
        bond.bond_type = 'CASH'

        self.validate_and_return_populate_data(
            _CHARGES_JSON, expected_info, task=task)

    def test_populate_data_person_max_requests_exceeded(self):
        task = Task(
            task_type=constants.TaskType.SCRAPE_DATA,
            endpoint='test',
            response_type=constants.ResponseType.JSON,
            custom={
                self.scraper._ARREST_NUMBER: 1,
                self.scraper._REQUEST_TARGET: self.scraper._PERSON_REQUEST,
                self.scraper._SESSION_TOKEN: self.SESSION_TOKEN,
                self.scraper._PERSON: _MAX_EXCEEDED_JSON,
                self.scraper._CASES: {},
            },
        )

        with self.assertRaises(JailTrackerRequestRateExceededError):
            self.validate_and_return_populate_data(
                _MAX_EXCEEDED_JSON, {}, task=task)

    def test_populate_data_charges_max_requests_exceeded(self):
        task = Task(
            task_type=constants.TaskType.SCRAPE_DATA,
            endpoint='test',
            response_type=constants.ResponseType.JSON,
            custom={
                self.scraper._ARREST_NUMBER: 1,
                self.scraper._REQUEST_TARGET: self.scraper._CHARGES_REQUEST,
                self.scraper._SESSION_TOKEN: self.SESSION_TOKEN,
                self.scraper._PERSON: _PERSON_AGENCIES_JSON,
                _CHARGES: _MAX_EXCEEDED_JSON,
                self.scraper._CASES: {},
            },
        )

        with self.assertRaises(JailTrackerRequestRateExceededError):
            self.validate_and_return_populate_data(
                _MAX_EXCEEDED_JSON, {}, task=task)
