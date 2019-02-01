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

"""Scraper tests for us_vt."""
import unittest
from lxml import html

from recidiviz.ingest import constants
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.task_params import Task
from recidiviz.ingest.us_vt.us_vt_scraper import UsVtScraper
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils.base_scraper_test import BaseScraperTest

_LANDING_HTML = html.fromstring(
    fixtures.as_string('us_vt', 'landing_page.html'))
_ROSTER_JSON = fixtures.as_dict('us_vt', 'roster.json')
_PERSON_PROBATION_JSON = fixtures.as_dict('us_vt', 'person.json')
_PERSON_AGENCIES_JSON = fixtures.as_dict('us_vt', 'person_agencies.json')
_CHARGES_JSON = fixtures.as_dict('us_vt', 'charges.json')


class TestUsVtScraper(BaseScraperTest, unittest.TestCase):

    SESSION_TOKEN = 'yznvp4bq2qt42qmqwswr3wqw'

    def _init_scraper_and_yaml(self):
        # The scraper to be tested. Required.
        self.scraper = UsVtScraper()
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
        bond = charge.create_bond()
        bond.amount = '500'
        bond.bond_type = 'CASH'

        charge = booking.create_charge()
        charge.charge_id = '2'
        charge.name = (
            'DEPRESSANT/STIMULANT/NARCOTIC-POSSESSION \u003c 100X DOSE')
        charge.offense_date = '2017-03-16'
        charge.status = 'Probation'
        charge.charge_class = 'M'
        charge.number_of_counts = '1'
        charge.case_number = '12'
        bond = charge.create_bond()
        bond.amount = '500'
        bond.bond_type = 'CASH'

        self.validate_and_return_populate_data(
            _CHARGES_JSON, expected_info, task=task)

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
        bond = charge.create_bond()
        bond.amount = '500'
        bond.bond_type = 'CASH'

        charge = booking.create_charge()
        charge.charge_id = '2'
        charge.name = (
            'DEPRESSANT/STIMULANT/NARCOTIC-POSSESSION \u003c 100X DOSE')
        charge.offense_date = '2017-03-16'
        charge.status = 'Probation'
        charge.charge_class = 'M'
        charge.number_of_counts = '1'
        charge.case_number = '12'
        bond = charge.create_bond()
        bond.amount = '500'
        bond.bond_type = 'CASH'

        self.validate_and_return_populate_data(
            _CHARGES_JSON, expected_info, task=task)
