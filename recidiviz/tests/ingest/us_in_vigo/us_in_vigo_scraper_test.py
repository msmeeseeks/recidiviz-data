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

"""Scraper tests for us_in_vigo."""
import unittest
from lxml import html

from recidiviz.ingest import constants
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.task_params import Task
from recidiviz.ingest.us_in_vigo.us_in_vigo_scraper import UsInVigoScraper
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils.base_scraper_test import BaseScraperTest

_LANDING_HTML = html.fromstring(
    fixtures.as_string('us_in_vigo', 'landing_page.html'))
_ROSTER_JSON = fixtures.as_dict('us_in_vigo', 'roster.json')
_PERSON_JSON = fixtures.as_dict('us_in_vigo', 'person.json')
_CHARGES_JSON = fixtures.as_dict('us_in_vigo', 'charges.json')


class TestUsInVigoScraper(BaseScraperTest, unittest.TestCase):

    SESSION_TOKEN = 'pfmzr1n5f1fepin44puf5lok'

    def _init_scraper_and_yaml(self):
        # The scraper to be tested. Required.
        self.scraper = UsInVigoScraper()
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

    def test_populate_data(self):
        task = Task(
            task_type=constants.TaskType.SCRAPE_DATA,
            endpoint='test',
            response_type=constants.ResponseType.JSON,
            custom={
                self.scraper._ARREST_NUMBER: 1,
                self.scraper._REQUEST_TARGET: self.scraper._CHARGES_REQUEST,
                self.scraper._SESSION_TOKEN: self.SESSION_TOKEN,
                self.scraper._PERSON: _PERSON_JSON,
                self.scraper._CASES: {},
            },
        )

        expected_info = IngestInfo()

        person = expected_info.create_person()
        person.surname='Simpson'
        person.given_names='Bart'
        person.gender='M'
        person.age='32'
        person.race='U'
        booking = person.create_booking(
            booking_id='1',
            admission_date='4/18/2018 2:02:56 PM')

        booking.create_charge(
            charge_id='135247',
            name=('Escape from lawful detention, violating a home detention '
                  'order or removing electronic monitoring or GPS device'),
            charge_class='F',
            status='BOND SET',
            number_of_counts='1',
            court_type='SUPERIOR COURT',
            case_number='84D06-1712-F6-004096').create_bond(
                amount='25000.0',
                bond_type='CASH ONLY -- NO 10% -- NO PROFESSIONAL BONDSMAN',)
        booking.create_charge(
            charge_id='135248',
            name='Dealing in methamphetamine',
            charge_class='F',
            status='BOND SET',
            number_of_counts='1',
            court_type='COUNTY COURT',
            case_number='84D06-1804-F4-001092').create_bond(
                amount='25000.0',
                bond_type='CASH ONLY -- NO 10% -- NO PROFESSIONAL BONDSMAN',)
        booking.create_charge(
            charge_id='135249',
            name='Maintaining a common nuisance',
            charge_class='F',
            status='BOND SET',
            number_of_counts='1',
            court_type='COURT TYPE',
            case_number='84D06-1804-F4-001092').create_bond(
                amount='0.0',
                bond_type='WITH 10% ALLOWED',)
        booking.create_charge(
            charge_id='135250',
            name='Possession of meth',
            charge_class='F',
            status='BOND SET',
            number_of_counts='1',
            court_type='TERRE HAUTE CITY COURT',
            case_number='84D06-1804-F4-001092').create_bond(
                amount='0.0',
                bond_type='BAIL CONSOLIDATED TO ONE CHARGE',)
        booking.create_charge(
            charge_id='135251',
            name=('Dealing in methamphetamine/enhancing circumstances in '
                  'IC 35-48-1-16.5 for prior'),
            charge_class='F',
            status='BOND SET',
            number_of_counts='1',
            court_type='OTHER (NOT CLASSIFIED)',
            case_number='84D06-1804-F4-001092').create_bond(
                amount='0.0',
                bond_type='GENERAL',)
        booking.create_charge(
            charge_id='135252',
            name='Habitual Offender -',
            status='BOND SET',
            number_of_counts='1',
            court_type='PAROLE VIOLATION (STATE ONLY)',
            case_number='84D06-1804-F4-001092').create_bond(
                amount='0.0',
                bond_type='BAIL CONSOLIDATED TO ONE CHARGE',)
        booking.create_charge(
            charge_id='140599',
            name='Battery -',
            charge_class='M',
            status='RELEASED BY COURT',
            number_of_counts='1',
            court_type='TERRE HAUTE CITY COURT',
            case_number='84H01-1803-CM-1888').create_bond(
                amount='0.0',
                bond_type='RELEASE ON RECOGNIZANCE',)

        self.validate_and_return_populate_data(
            _CHARGES_JSON, expected_info, expected_persist=True, task=task)
