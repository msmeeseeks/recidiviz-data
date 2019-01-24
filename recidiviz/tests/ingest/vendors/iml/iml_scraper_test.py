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
"""Scraper tests for us_pa_greene."""

import unittest

from lxml import html
from recidiviz.ingest import constants

from recidiviz.ingest.models.ingest_info import IngestInfo, _Person, \
    _Booking, _Charge, _Bond, _Hold
from recidiviz.ingest.task_params import Task
from recidiviz.ingest.vendors.iml.iml_scraper import ImlScraper
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils.base_scraper_test import BaseScraperTest

_ROSTER_HTML = html.fromstring(
    fixtures.as_string('vendors/iml', 'roster_page.html'))
_PERSON_HTML = html.fromstring(
    fixtures.as_string('vendors/iml', 'person_page.html'))

_SEARCH_RESULT_IDS = [
    {'imgSysID': 329631750999041,
     'sysID': 329631750999041},
    {'imgSysID': 0,
     'sysID': 329627767273473},
    {'imgSysID': 0,
     'sysID': 329627828045825},
    {'imgSysID': 19603894628769,
     'sysID': 19603894628769},
    {'imgSysID': 19781046137889,
     'sysID': 19781046137889},
    {'imgSysID': 343792968334337,
     'sysID': 19734825916321},
    {'imgSysID': 309239600838657,
     'sysID': 19781118755873},
    {'imgSysID': 19607750604065,
     'sysID': 19607750604065},
    {'imgSysID': 19502194755617,
     'sysID': 19502194755617},
    {'imgSysID': 19781252811809,
     'sysID': 19781252811809},
    {'imgSysID': 19386108541601,
     'sysID': 19386108541601},
    {'imgSysID': 19607076736161,
     'sysID': 19607076736161},
    {'imgSysID': 343721435902977,
     'sysID': 19607650629281},
    {'imgSysID': 19603640650017,
     'sysID': 19603640650017},
    {'imgSysID': 19502045242273,
     'sysID': 19502045242273},
    {'imgSysID': 19605646366625,
     'sysID': 19605646366625},
    {'imgSysID': 19781258512033,
     'sysID': 19781258512033},
    {'imgSysID': 19604434937889,
     'sysID': 19604434937889},
    {'imgSysID': 19537838090273,
     'sysID': 19537838090273},
    {'imgSysID': 19501957170849,
     'sysID': 19781258759457},
    {'imgSysID': 19734821075489,
     'sysID': 19734821075489},
    {'imgSysID': 19254259178529,
     'sysID': 19254259178529},
    {'imgSysID': 19734543648545,
     'sysID': 19734543648545},
    {'imgSysID': 19502368865697,
     'sysID': 19502368865697},
    {'imgSysID': 19454520001057,
     'sysID': 19606715891489},
    {'imgSysID': 19734119108769,
     'sysID': 19734119108769},
    {'imgSysID': 18882901744545,
     'sysID': 19537758056865},
    {'imgSysID': 19780902678433,
     'sysID': 19780902678433},
    {'imgSysID': 19502372851105,
     'sysID': 19502372851105},
    {'imgSysID': 19603644613409,
     'sysID': 19603644613409},
]

[
{'endpoint': 'http://168.229.183.33:8084/IML',
     'headers': {'Cookie': 'yum!'},
     'post_data': {'currentStart': 31,
                   'flow_action': 'next'},
     'task_type': 4},
]


class ImlScraperTest(BaseScraperTest, unittest.TestCase):
    """Scraper tests for the Iml vendor scraper."""

    def _init_scraper_and_yaml(self):
        self.scraper = ImlScraper('us_nj_bergen')
        self.yaml = None

    def test_get_session_vars_and_num_people(self):
        expected_result = [
            Task(endpoint='http://168.229.183.33:8084/IML',
                 headers={'Cookie': None},
                 custom={'booking_id': 'NAME, NO',
                         'person_id': 'NAME, NO'},
                 post_data={'flow_action': 'edit',
                            'imgSysID': ids['imgSysID'],
                            'sysID': ids['sysID']},
                 task_type=constants.TaskType.SCRAPE_DATA)
            for ids in _SEARCH_RESULT_IDS]

        expected_result.append(
            Task(endpoint='http://168.229.183.33:8084/IML',
                 headers={'Cookie': None},
                 post_data={'flow_action': 'next',
                            'currentStart': 31},
                 task_type=constants.TaskType.GET_MORE_TASKS))

        task = self.scraper.get_initial_task()

        self.validate_and_return_get_more_tasks(_ROSTER_HTML, task,
                                                expected_result)

    def test_get_next_search_page_and_people_tasks(self):
        expected_result = [
            Task(endpoint='http://168.229.183.33:8084/IML',
                 headers={'Cookie': 'yum!'},
                 custom={'booking_id': 'NAME, NO',
                         'person_id': 'NAME, NO'},
                 post_data={'flow_action': 'edit',
                            'imgSysID': ids['imgSysID'],
                            'sysID': ids['sysID']},
                 task_type=constants.TaskType.SCRAPE_DATA)
            for ids in _SEARCH_RESULT_IDS]

        expected_result.append(
            Task(endpoint='http://168.229.183.33:8084/IML',
                 headers={'Cookie': 'yum!'},
                 post_data={'flow_action': 'next',
                            'currentStart': 31},
                 task_type=constants.TaskType.GET_MORE_TASKS))

        task = Task(
            endpoint='no.way.url',
            task_type=constants.TaskType.GET_MORE_TASKS,
            headers={'Cookie': 'yum!'},
        )

        self.validate_and_return_get_more_tasks(_ROSTER_HTML, task,
                                                expected_result)

    def test_populate_data(self):
        expected_result = IngestInfo(
            people=[_Person(
                person_id='12345',
                full_name='BART SIMPSON',
                birthdate='01/27/1986',
                race='BLACK',
                bookings=[_Booking(
                    booking_id='67890',
                    admission_date='07/25/2018',
                    facility='MAIN',
                    charges=[_Charge(
                        offense_date='07/25/2018',
                        statute='2C:21-31C',
                        name='IMMIGRATION DETAINEE')
                    ],
                    holds=[_Hold(jurisdiction_name='ICE')
                    ],
                )])
            ])

        task = Task(
            endpoint='no.way.url',
            task_type=constants.TaskType.SCRAPE_DATA,
            headers={'Cookie': 'yum!'},
            custom={'person_id': '12345',
                    'booking_id': '67890'}
        )

        self.validate_and_return_populate_data(
            _PERSON_HTML, expected_result, task=task)
