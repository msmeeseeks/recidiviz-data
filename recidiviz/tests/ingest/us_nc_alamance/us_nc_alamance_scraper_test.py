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
"""Scraper tests for us_nc_alamance."""

import unittest

from lxml import html
from recidiviz.ingest import constants

from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.task_params import Task
from recidiviz.ingest.us_nc_alamance.us_nc_alamance_scraper import \
    UsNcAlamanceScraper
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils.base_scraper_test import BaseScraperTest

_NUM_PEOPLE_JSON = html.fromstring(
    fixtures.as_string('us_nc_alamance', 'num_people.json'))
_FRONT_HTML = html.fromstring(
    fixtures.as_string('us_nc_alamance', 'front_page.html'))
_PERSON_HTML = html.fromstring(
    fixtures.as_string('us_nc_alamance', 'person_page.html'))


class SuperionScraperTest(BaseScraperTest, unittest.TestCase):
    """Scraper tests for the Superion vendor scraper."""

    def _init_scraper_and_yaml(self):
        self.scraper = UsNcAlamanceScraper()
        self.yaml = None

    def test_get_num_people(self):
        expected_result = [Task(
            endpoint='https://apps.alamance-nc.com/p2c/jailinmates.aspx',
            task_type=constants.TaskType.GET_MORE_TASKS,
            custom={
                'num_people': 374,
            }
        )]

        task = Task(
            task_type=constants.TaskType.INITIAL_AND_MORE,
            endpoint='',
        )

        self.validate_and_return_get_more_tasks(_NUM_PEOPLE_JSON, task,
                                                expected_result)

    def test_get_session_vars(self):

        # pylint:disable=line-too-long
        expected_result = [Task(
            task_type=constants.TaskType.SCRAPE_DATA,
            endpoint='https://apps.alamance-nc.com/p2c/jailinmates.aspx',
            post_data={
                '__EVENTVALIDATION': None,
                '__VIEWSTATE': '/wEPDwUKMTMyNTg1OTUwMQ9kFgJmD2QWAmYPZBYGZg9kFgQCBQ9kFgRmDxYCHgRUZXh0BUU8c2NyaXB0IHNyYz0ianMvanF1ZXJ5LTEuOC4zLm1pbi5qcyIgdHlwZT0idGV4dC9qYXZhc2NyaXB0Ij48L3NjcmlwdD5kAgEPFgIfAAX7AjxsaW5rIHR5cGU9InRleHQvY3NzIiBocmVmPSJjc3Mvc3VwZXJmaXNoLmNzcyIgbWVkaWE9InNjcmVlbiIgcmVsPSJzdHlsZXNoZWV0IiAvPjxzY3JpcHQgdHlwZT0idGV4dC9qYXZhc2NyaXB0IiBzcmM9J2pzL2hvdmVySW50ZW50LmpzJz48L3NjcmlwdD4gPHNjcmlwdCB0eXBlPSJ0ZXh0L2phdmFzY3JpcHQiIHNyYz0nanMvc3VwZXJmaXNoLmpzJz48L3NjcmlwdD4gPHNjcmlwdCBzcmM9ImpxdWkvMS4xMS40L2pxdWVyeS11aS0xLjExLjQuY3VzdG9tLm1pbi5qcyIgdHlwZT0idGV4dC9qYXZhc2NyaXB0Ij48L3NjcmlwdD48bGluayBocmVmPSJqcXVpLzEuMTEuNC9zdGFydC9qcXVlcnktdWkuY3NzIiByZWw9IlN0eWxlc2hlZXQiIGNsYXNzPSJ1aS10aGVtZSIgLz5kAgkPFgIfAAXxATxzY3JpcHQgbGFuZ3VhZ2U9ImphdmFzY3JpcHQiIHR5cGU9InRleHQvamF2YXNjcmlwdCI+JChkb2N1bWVudCkucmVhZHkoZnVuY3Rpb24oKSB7JCgndWwuc2YtbWVudScpLnN1cGVyZmlzaCh7ZGVsYXk6ICAgICAgIDEwMDAsYW5pbWF0aW9uOiAgIHtvcGFjaXR5OidzaG93JyxoZWlnaHQ6J3Nob3cnfSxzcGVlZDogICdmYXN0JyxhdXRvQXJyb3dzOiAgZmFsc2UsIGRyb3BTaGFkb3dzOiBmYWxzZSB9KTt9KTs8L3NjcmlwdD5kAgEPZBYEAgMPFgIeBWFsaWduBQZjZW50ZXIWCAIBDxYCHgNzcmMFF34vaW1hZ2VzL0FnZW5jeU5hbWUuc3ZnZAIDDxYEHghkaXNhYmxlZAUIZGlzYWJsZWQeB1Zpc2libGVoFgJmD2QWAgIBD2QWAgIBD2QWAmYPEA8WBh4NRGF0YVRleHRGaWVsZAUEdGV4dB4ORGF0YVZhbHVlRmllbGQFBGxpbmseC18hRGF0YUJvdW5kZ2QQFQQESG9tZRAtIFF1aWNrIExpbmtzIC0gA0ZBUQ5Jbm1hdGUgSW5xdWlyeRUEC34vbWFpbi5hc3B4A34vMAp+L2ZhcS5hc3B4En4vamFpbGlubWF0ZXMuYXNweBQrAwRnZ2dnFgFmZAIFD2QWAmYPZBYCZg9kFgICAQ8WAh8ABewHPHVsIGNsYXNzPSJzZi1tZW51IHVpLXdpZGdldCIgc3R5bGU9InotaW5kZXg6OTk5Ij48bGkgY2xhc3M9IiIgc3R5bGU9IiI+PGEgc3R5bGU9IiIgY2xhc3M9InVpLWNvcm5lci1hbGwgdWktc3RhdGUtZGVmYXVsdCIgaHJlZj0iLi9tYWluLmFzcHgiPkhvbWU8L2E+PC9saT48bGkgY2xhc3M9IiIgc3R5bGU9IiI+PGEgc3R5bGU9IiIgY2xhc3M9InVpLWNvcm5lci1hbGwgdWktc3RhdGUtZGVmYXVsdCIgaHJlZj0iLi9mYXEuYXNweCI+RmFxPC9hPjwvbGk+PGxpIGNsYXNzPSIiIHN0eWxlPSIiPjxhIHN0eWxlPSIiIGNsYXNzPSJ1aS1jb3JuZXItYWxsIHVpLXN0YXRlLWRlZmF1bHQiIGhyZWY9Ii4vamFpbGlubWF0ZXMuYXNweCI+SW5tYXRlIElucXVpcnk8L2E+PC9saT48bGkgY2xhc3M9IiIgc3R5bGU9InotaW5kZXg6OTk7IHBvc2l0aW9uOnJlbGF0aXZlOyI+PGEgY2xhc3M9InVpLWNvcm5lci1hbGwgdWktc3RhdGUtZGVmYXVsdCIgaHJlZj0iIyI+UXVpY2sgTGlua3M8L2E+PHVsIHN0eWxlPSJ0ZXh0LWFsaWduOmxlZnQ7d2lkdGg6MjAwcHg7dG9wOiAyNXB4OyBkaXNwbGF5Om5vbmU7Ij48bGkgc3R5bGU9IndpZHRoOjIwMHB4Ij48YSBzdHlsZT0iZm9udC1zaXplOi43ZW07IiBjbGFzcz0idWktY29ybmVyLWFsbCB1aS1zdGF0ZS1ob3ZlciB1aS1zdGF0ZS1kZWZhdWx0IiBocmVmPSIuL21haW4uYXNweCI+SG9tZTwvYT48L2xpPjxsaSBzdHlsZT0id2lkdGg6MjAwcHgi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',
                '__VIEWSTATEGENERATOR': None,
                'ctl00$MasterPage$mainContent$CenterColumnContent$btnInmateDetail': '',
                'ctl00$MasterPage$mainContent$CenterColumnContent$hfRecordIndex': 0
            },
        )]

        task = Task(
            task_type=constants.TaskType.GET_MORE_TASKS,
            endpoint='',
            custom={
                'num_people': 1,
            }
        )

        self.validate_and_return_get_more_tasks(_FRONT_HTML, task,
                                                expected_result)

    def test_populate_data(self):
        expected_info = IngestInfo()

        person = expected_info.create_person(
            full_name='SIMPSON, BART',
            gender='MALE',
            age='24',
            race='BLACK')

        booking = person.create_booking(admission_date='8/2/2018')

        booking.create_charge(
            name='BREAKING AND ENTERING-BUILDING  (FELONY)',
            charge_class='FELONY',
            status='AWAITING TRIAL',
            case_number='18CR54172').create_bond(
                amount='0.00',
                bond_type='SECURE BOND ')
        booking.create_charge(
            name='PAROLE VIOLATION',
            status='SENTENCED',).create_bond(
                bond_type='N/A')
        booking.create_charge(
            name='BREAKING AND ENTERING-BUILDING  (FELONY)',
            charge_class='FELONY',
            status='AWAITING TRIAL',
            case_number='18CR54173').create_bond(
                amount='0.00',
                bond_type='SECURE BOND ')

        self.validate_and_return_populate_data(_PERSON_HTML, expected_info)
