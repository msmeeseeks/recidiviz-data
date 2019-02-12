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

"""Scraper tests for us_fl_alachua."""
import unittest
from recidiviz.ingest.models.ingest_info import IngestInfo, Person, Booking, Charge, Bond
from recidiviz.ingest.scrape.constants import TaskType, ResponseType
from recidiviz.ingest.scrape.regions.us_fl_alachua.us_fl_alachua_scraper import UsFlAlachuaScraper
from recidiviz.ingest.scrape.task_params import Task
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils.base_scraper_test import BaseScraperTest

_DETAILS_PERSON_HTML = fixtures.as_html5('us_fl_alachua', 'details_person.html')

_DETAILS_ROSTER_HTML = fixtures.as_html5('us_fl_alachua', 'details_roster.html')


class TestUsFlAlachuaScraper(BaseScraperTest, unittest.TestCase):

    def _init_scraper_and_yaml(self):
        # The scraper to be tested. Required.
        self.scraper = UsFlAlachuaScraper()
        # The path to the yaml mapping. Optional.
        self.yaml = None

    def test_get_more_tasks(self):
        # Tests navigation. Fill in |content| and |params| with the state of the
        # page to navigate from, and |expected_result| with the expected state
        # after navigation. Chain multiple calls to
        # |validate_and_return_get_more_tasks| together if necessary.
        params = Task(
            endpoint=None,
            task_type=TaskType.GET_MORE_TASKS
        )
        expected_result = [
            Task(
                task_type=TaskType.SCRAPE_DATA,
                endpoint='http://oldweb.circuit8.org/cgi-bin/jaildetail.cgi?bookno=ASO18JBN007414',
                response_type=ResponseType.HTML,
                headers=None,
                cookies={},
                post_data=None,
                json=None,
                custom={'Booking Date': '11/28/2018\n            '},
                content=None),
            Task(
                task_type=TaskType.SCRAPE_DATA,
                endpoint='http://oldweb.circuit8.org/cgi-bin/jaildetail.cgi?bookno=ASO18JBN007216',
                response_type=ResponseType.HTML,
                headers=None,
                cookies={},
                post_data=None,
                json=None,
                custom={'Booking Date': '11/18/2018\n            '},
                content=None),
            Task(
                task_type=TaskType.SCRAPE_DATA,
                endpoint='http://oldweb.circuit8.org/cgi-bin/jaildetail.cgi?bookno=ASO18JBN007385',
                response_type=ResponseType.HTML,
                headers=None,
                cookies={},
                post_data=None,
                json=None,
                custom={'Booking Date': '11/27/2018\n            '},
                content=None),
            Task(
                task_type=TaskType.SCRAPE_DATA,
                endpoint='http://oldweb.circuit8.org/cgi-bin/jaildetail.cgi?bookno=ASO18JBN007991',
                response_type=ResponseType.HTML,
                headers=None,
                cookies={},
                post_data=None,
                json=None,
                custom={'Booking Date': '12/28/2018\n            '},
                content=None),
        ]
        self.validate_and_return_get_more_tasks(_DETAILS_ROSTER_HTML, params, expected_result)

    def test_populate_data(self):
        # Tests scraping data. Fill in |content| and |params| with the state of
        # the page containing person data, and |expected_result| with the
        # IngestInfo objects that should be scraped from the page.
        task = Task(
            endpoint=None,
            task_type=TaskType.SCRAPE_DATA,
            custom={'Booking Date': '08/14/2018'}
        )
        expected_result = IngestInfo(people=[Person(
            full_name='LAST NAME, FIRST NAME',
                bookings=[
                    Booking(
                        admission_date='08/14/2018',
                        charges=[
                            Charge(
                                 statute='316.1935(3)(a)',
                                 name=': FLEE/ELUDE OFFICER/DISREGARD SAFETY OF OTHERS (REVOKED BY COURT)',
                                 charging_entity='ASO', status='UNSENTENCED',
                                 case_number='012017CF003604A',
                                 bond=Bond(
                                     amount='$ 0.00')
                            ), Charge(
                                 statute='322.34(5)',
                                 name=': OPERATING WHILE DL REVOKED FOR HABITUAL TRAFFIC OFFENDER',
                                 charging_entity='ASO', status='UNSENTENCED',
                                 case_number='012017CF003604A',
                                 bond=Bond(
                                     amount='$ 0.00')
                            ), Charge(
                                 statute='843.02',
                                 name='RESIST OFFICER: OBSTRUCT WO VIOLENCE',
                                 degree='1st',
                                 charge_class='M',
                                 charging_entity='ASO',
                                 status='UNSENTENCED',
                                 case_number='012017CF003604A',
                                 bond=Bond(
                                     amount='$ 0.00')
                            ), Charge(
                                 statute='790.01.2',
                                 name='CARRYING CONCEALED WEAPON: UNLICENSED FIREARM',
                                 degree='3rd', charge_class='F', charging_entity='ASO',
                                 status='UNSENTENCED', case_number='012018CF001194A',
                                 bond=Bond(
                                     amount='$ 0.00')
                            ), Charge(
                                 statute='790.23.1a',
                                 name='POSSESSION OF WEAPON: OR AMMO BY CONVICTED FLA FELON',
                                 degree='2nd', charge_class='F', charging_entity='ASO',
                                 status='UNSENTENCED', case_number='012018CF001194A',
                                 bond=Bond(
                                     amount='$ 0.00')
                            ), Charge(
                                 statute='893.13.6a',
                                 name='DRUGS-POSSESS: CNTRL SUB WO PRESCRIPTION',
                                 degree='3rd', charge_class='F', charging_entity='ASO',
                                 status='UNSENTENCED', case_number='012018CF001194A',
                                 bond=Bond(
                                     amount='$ 0.00')
                            ), Charge(
                                 statute='322.34(5)',
                                 name=': OPERATING WHILE DL REVOKED FOR HABITUAL TRAFFIC OFFENDER',
                                 charging_entity='ASO', status='UNSENTENCED',
                                 case_number='012018CF001194A',
                                 bond=Bond(
                                     amount='$ 0.00')
                            ), Charge(
                                 statute='316.072.3',
                                 name='PUBLIC ORDER CRIMES: FAIL TO OBEY POLICE OR FIRE DEPARTMENT',
                                 degree='2nd', charge_class='M', charging_entity='ASO',
                                 status='UNSENTENCED', case_number='012018CF001194A',
                                 bond=Bond(
                                     amount='$ 0.00'
                                 ),
                            ),
                        ]
                    )
                ]
            ), ])
        with self.assertWarns(UserWarning):
            self.validate_and_return_populate_data(_DETAILS_PERSON_HTML, expected_result, task=task)
