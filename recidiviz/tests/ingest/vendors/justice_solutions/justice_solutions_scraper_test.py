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
"""
Scraper tests for regions that use unaltered justice_solutions_scraper.
Any region scraper test class that inherits from JusticeSolutionsScraperTest
must implement the following:
    _init_scraper_and_yaml(self):
        self.scraper = RegionScraperCls()
"""

import cattr
from lxml import html
from mock import patch

from recidiviz.ingest.constants import TaskType, ResponseType
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.task_params import Task
from recidiviz.ingest.vendors.justice_solutions.justice_solutions_scraper \
    import _Redirect
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils.base_scraper_test import BaseScraperTest

_REDIRECT_CONTENT = fixtures.as_string('vendors/justice_solutions',
                                       'roster_redirect.html')

_ROSTER_PAGE_HTML = html.fromstring(
    fixtures.as_string('vendors/justice_solutions', 'roster.html'))

_PERSON_PAGE_HTML = html.fromstring(
    fixtures.as_string('vendors/justice_solutions', 'person_page.html'))

_TIME_AND_DATE = 'TIME AND DATE'


class JusticeSolutionsScraperTest(BaseScraperTest):
    """Test class for JusticeSolutionsScraper."""

    def test_roster_page_redirect(self):
        expected_result = [
            Task(task_type=TaskType.GET_MORE_TASKS,
                 endpoint='http://findtheinmate.com/redirect.html')
        ]
        initial_task = self.scraper.get_initial_task()
        initial_task = cattr.structure(cattr.unstructure(initial_task), Task)

        self.validate_and_return_get_more_tasks(_REDIRECT_CONTENT,
                                                initial_task,
                                                expected_result)

    @patch('recidiviz.ingest.vendors.justice_solutions'
           '.justice_solutions_scraper._get_time_and_date')
    def test_roster_page_navigation(self, mock_time_and_date):
        mock_time_and_date.return_value = _TIME_AND_DATE
        expected_result = [
            Task(task_type=TaskType.GET_MORE_TASKS,
                 endpoint='http://findtheinmate.com/cgi-bin/webshell.asp',
                 response_type=ResponseType.RAW,
                 post_data={
                     'GATEWAY': 'GATEWAY',
                     'P_100': '18-0123',
                     'XGATEWAY': 'GET.INMATE.DATA',
                     'CGISCRIPT': 'webshell.asp',
                     'XEVENT': 'VERIFY',
                     'WEBIOHANDLE': _TIME_AND_DATE,
                     'MYPARENT': 'px',
                     'APPID': 'jsinq',
                     'WEBWORDSKEY': 'SAMPLE',
                     'DEVPATH': self.scraper.get_devpath(),
                     'OPERCODE': 'dummy',
                     'PASSWD': 'dummy'},
                 custom={'redirect': _Redirect.PERSON,
                         'gender': '\n                M\n            '}),
            Task(
                task_type=TaskType.GET_MORE_TASKS,
                endpoint='http://findtheinmate.com/cgi-bin/webshell.asp',
                response_type=ResponseType.RAW,
                post_data={
                    'GATEWAY': 'GATEWAY',
                    'P_100': '18-0456',
                    'XGATEWAY': 'GET.INMATE.DATA',
                    'CGISCRIPT': 'webshell.asp',
                    'XEVENT': 'VERIFY',
                    'WEBIOHANDLE': _TIME_AND_DATE,
                    'MYPARENT': 'px',
                    'APPID': 'jsinq',
                    'WEBWORDSKEY': 'SAMPLE',
                    'DEVPATH': self.scraper.get_devpath(),
                    'OPERCODE': 'dummy',
                    'PASSWD': 'dummy'},
                custom={'redirect': _Redirect.PERSON,
                        'gender': '\n                M\n            '})
        ]
        initial_task = Task(task_type=TaskType.GET_MORE_TASKS, endpoint='')

        self.validate_and_return_get_more_tasks(_ROSTER_PAGE_HTML,
                                                initial_task,
                                                expected_result)

    def test_person_page_redirect(self):
        expected_result = [
            Task(task_type=TaskType.SCRAPE_DATA,
                 endpoint='http://findtheinmate.com/redirect.html',
                 custom={'gender': 'gender'})
        ]
        initial_task = Task(task_type=TaskType.GET_MORE_TASKS, endpoint='',
                            custom={'redirect': _Redirect.PERSON,
                                    'gender': 'gender'})
        initial_task = cattr.structure(cattr.unstructure(initial_task), Task)

        self.validate_and_return_get_more_tasks(_REDIRECT_CONTENT,
                                                initial_task,
                                                expected_result)

    def test_populate_data(self):
        expected_info = IngestInfo()
        person = expected_info.create_person(person_id='12345',
                                             full_name='LAST,FIRST',
                                             gender='M',
                                             age='48',
                                             race='WHITE',
                                             place_of_residence=' IL\xa0 60060')
        booking = person.create_booking(admission_date='01/01/1111')
        booking.create_charge(name='DISORDERLY CONDUCT',
                              charging_entity='AGENCY',
                              next_court_date='1/1/1111')
        booking.create_charge(name='OBSTRUCTING GOVERNMENTAL OPERATIONS',
                              court_type='DISTRICT').create_bond(amount='100')

        task = Task(task_type=TaskType.SCRAPE_DATA, endpoint='',
                    custom={'gender': 'M'})

        self.validate_and_return_populate_data(
            _PERSON_PAGE_HTML, expected_info, task=task)
