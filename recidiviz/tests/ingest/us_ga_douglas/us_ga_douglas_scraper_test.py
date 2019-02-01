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

"""Scraper tests for us_ga_douglas."""
import unittest
from datetime import datetime

import pytz
from mock import patch

from recidiviz.ingest import constants
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.task_params import Task
from recidiviz.ingest.us_ga_douglas.us_ga_douglas_scraper import \
    UsGaDouglasScraper
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils.base_scraper_test import BaseScraperTest

_INIT_JSON = fixtures.as_dict('us_ga_douglas', 'init.json')
_LOAD_JSON = fixtures.as_dict('us_ga_douglas', 'load.json')
_LOAD_JSON_INVALID_SUFFIX = fixtures.as_dict('us_ga_douglas',
                                             'load.invalid_suffix.json')

_DATETIME = datetime(2019, 1, 1, 12, 0, 0, 0, tzinfo=pytz.utc)
_DATETIME_STR = '2019-01-01T12:00:00+00:00'

class TestUsGaDouglasScraper(BaseScraperTest, unittest.TestCase):

    def _init_scraper_and_yaml(self):
        # The scraper to be tested. Required.
        self.scraper = UsGaDouglasScraper()
        # The path to the yaml mapping. Optional.
        self.yaml = None

    @staticmethod
    def _make_task(task_type, count, start):
        return Task(
            task_type=task_type,
            endpoint='https://douglas-so-ga.zuercherportal.com/api/portal/'
            'inmates/load',
            json={
                'name': '',
                'race': 'all',
                'sex': 'all',
                'cell_block': 'all',
                'held_for_agency': 'any',
                'in_custody': _DATETIME_STR,
                'paging': {
                    'count': count,
                    'start': start,
                },
                'sorting': {
                    'sort_by_column_tag': 'name',
                    'sort_descending': False,
                },
            },
            custom={
                'search_time': _DATETIME_STR,
            },
            response_type=constants.ResponseType.JSON,
        )

    @patch('recidiviz.ingest.vendors.zuercher.zuercher_scraper.datetime')
    def test_get_more_tasks_initial(self, mock_datetime):
        mock_datetime.now.return_value = _DATETIME

        task = self.scraper.get_initial_task()
        expected_result = [self._make_task(
            task_type=constants.TaskType.SCRAPE_DATA_AND_MORE,
            count=100, start=0)]

        self.validate_and_return_get_more_tasks(
            _INIT_JSON, task, expected_result)


    @patch('recidiviz.ingest.vendors.zuercher.zuercher_scraper.datetime')
    def test_get_more_tasks_middle(self, mock_datetime):
        mock_datetime.now.return_value = _DATETIME

        content = {'total_record_count': 450}
        task = self._make_task(
            task_type=constants.TaskType.SCRAPE_DATA_AND_MORE,
            count=100, start=100)
        expected_result = [self._make_task(
            task_type=constants.TaskType.SCRAPE_DATA_AND_MORE,
            count=100, start=200)]

        self.validate_and_return_get_more_tasks(
            content, task, expected_result)


    @patch('recidiviz.ingest.vendors.zuercher.zuercher_scraper.datetime')
    def test_get_more_tasks_end(self, mock_datetime):
        mock_datetime.now.return_value = _DATETIME

        content = {'total_record_count': 450}
        task = self._make_task(
            task_type=constants.TaskType.SCRAPE_DATA_AND_MORE,
            count=100, start=300)
        expected_result = [self._make_task(
            task_type=constants.TaskType.SCRAPE_DATA,
            count=100, start=400)]

        self.validate_and_return_get_more_tasks(
            content, task, expected_result)


    def test_populate_data(self):
        expected_info = IngestInfo()

        # first
        person = expected_info.create_person()
        person.full_name = 'WARRANTS, SOME'
        person.age = '29'
        person.gender = 'Male'
        person.race = 'Black'

        booking = person.create_booking()
        booking.admission_date = '2018-11-20'

        charge = booking.create_charge()
        charge.name = 'Probation Violation'
        charge.statute = '42-8-38'
        charge.charge_notes = 'Violation of Probation warrant 1809203071'
        charge.offense_date = '11/20/2018'

        charge = booking.create_charge()
        charge.name = 'Probation Violation'
        charge.statute = '42-8-38'
        charge.charge_notes = 'Violation of Probation warrant 17CR00459R2'
        charge.offense_date = '11/20/2018'

        # second
        person = expected_info.create_person()
        person.full_name = 'CHARGES, WARRANTS AND'
        person.age = '21'
        person.gender = 'Male'
        person.race = 'Black'

        booking = person.create_booking()
        booking.admission_date = '2018-08-01'

        charge = booking.create_charge()
        charge.name = 'Murder'
        charge.statute = '16-5-1'
        charge.charge_notes = ('Pending Warrant warrant 18MSC00985 issued by '
                               'Douglas County, GA')
        charge.offense_date = '08/01/2018'
        charge.create_bond(amount='$250000.00')

        charge = booking.create_charge()
        charge.name = 'Aggravated Assault Deadly Weapon'
        charge.statute = '16-5-21(a)(2)'
        charge.charge_notes = ('Pending Warrant warrant 18MSC00986 issued by '
                               'Douglas County, GA')
        charge.offense_date = '08/01/2018'
        charge.create_bond(amount='$250000.00')

        # third
        person = expected_info.create_person()
        person.full_name = 'DPD, BOARDED'
        person.age = '33'
        person.gender = 'Male'
        person.race = 'Black'

        booking = person.create_booking()
        booking.admission_date = '2019-01-21'

        charge = booking.create_charge()
        charge.name = 'Boarded for DPD'
        charge.statute = '999.999'
        charge.status = 'Pending'
        charge.offense_date = '01/21/2019'

        # fourth
        person = expected_info.create_person()
        person.full_name = 'CHARGES, NO'
        person.age = '40'
        person.gender = 'Male'
        person.race = 'White'

        # fifth
        person = expected_info.create_person()
        person.full_name = 'CHARGES, STRANGE'
        person.age = '42'
        person.gender = 'Male'
        person.race = 'White'

        booking = person.create_booking()
        booking.admission_date = '2018-12-31'

        charge = booking.create_charge()
        charge.name = 'Off Bond'
        charge.statute = '999.999'
        charge.offense_date = '12/18/2018'
        charge.create_bond(bond_type='No Bond')

        charge = booking.create_charge()
        charge.name = 'Terroristic Threats and Acts'
        charge.statute = '16-11-37'
        charge.charge_class = 'Felony'
        charge.status = 'Pending'

        charge = booking.create_charge()
        charge.name = 'Contempt of Superior Court'
        charge.statute = '15-6-8'
        charge.charge_class = 'F'
        charge.charge_notes = 'Failure to Comply warrant 13CR00998'

        charge = booking.create_charge()
        charge.name = 'Probation Violation'
        charge.statute = '42-8-38'
        charge.charge_notes = ('Violation of Probation warrant 16CR00064R1 '
                               'issued by Fayette County, GA')

        booking.create_hold(jurisdiction_name='Fayette County, GA')

        charge = booking.create_charge()
        charge.name = 'Cruelty to Children 3 rd Degree'
        charge.statute = '16-5-70'
        charge.charge_class = 'Misd'
        charge.number_of_counts = '3'
        charge.charge_notes = 'Pending Warrant warrant 18MJD01824'

        charge = booking.create_charge()
        charge.name = 'Extraditon for extradition'

        charge = booking.create_charge()
        charge.name = 'Probation-F: Unspecified warrant 18-12-14-3416'
        charge.offense_date = '12/14/2018'

        charge = booking.create_charge()
        charge.name = 'Parole for Douglas County Sheriff\'s Office'
        charge.offense_date = '12/20/2018'

        # sixth
        person = expected_info.create_person()
        person.full_name = 'SUFFIX, WITH'
        person.age = '78'
        person.gender = 'Female'
        person.race = 'White'

        booking = person.create_booking()
        booking.admission_date = '2019-01-17'

        charge = booking.create_charge()
        charge.name = ('Determination of habitual violators; revocation of '
                       'license; probationary license')
        charge.statute = '40-5-58(c)(1)'
        charge.offense_date = '11/14/2018'
        charge.create_bond(amount='$4000.00')

        self.validate_and_return_populate_data(_LOAD_JSON, expected_info)

    def test_populate_data_invalid_suffix(self):
        with self.assertRaises(ValueError):
            self.validate_and_return_populate_data(_LOAD_JSON_INVALID_SUFFIX,
                                                   IngestInfo())
