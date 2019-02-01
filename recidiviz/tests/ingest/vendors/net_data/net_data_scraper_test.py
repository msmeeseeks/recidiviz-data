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
"""Scraper tests for regions that use unaltered net_data_scraper.

Any region scraper test class that inherits from NetDataScraperTest must
implement the following:

    _init_scraper_and_yaml(self):
       self.scraper = RegionScraperCls()
"""

from lxml import html

from recidiviz.ingest.constants import TaskType, ResponseType
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.task_params import Task
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils.base_scraper_test import BaseScraperTest

_MAIN_PAGE = fixtures.as_string('vendors/net_data', 'main_page.html')
_PERSON_XML = fixtures.as_string('vendors/net_data', 'person.xml')
_PERSON_XSL = fixtures.as_string('vendors/net_data', 'person_xsl')
_ROSTER_XML = fixtures.as_string('vendors/net_data', 'roster.xml')
_ROSTER_XSL = fixtures.as_string('vendors/net_data', 'roster_xsl')


class NetDataScraperTest(BaseScraperTest):
    """Base test class for all regions implemented with the NetDataScraper."""

    def _get_base_url(self):
        return self.scraper.region.base_url.rpartition('/')[0] + '/'

    def test_get_more_tasks_initial_task(self):
        content = html.fromstring(_MAIN_PAGE)
        task = Task(endpoint=self.scraper.region.base_url,
                    task_type=TaskType.INITIAL)
        expected_task = Task(
            endpoint=self._get_base_url() + 'CGIBOK108.ws',
            task_type=TaskType.GET_MORE_TASKS,
            response_type=ResponseType.TEXT,
            post_data={'S108LIB': 'QS36F', 'S108PFX': 'LE',
                       'S108CNTY': 'county'},
            custom={'page_type': 'roster'})
        self.validate_and_return_get_more_tasks(content, task, [expected_task])

    def test_get_more_tasks_roster(self):
        content = _ROSTER_XML
        task = Task(
            endpoint=self._get_base_url() + 'CGIBOK108.ws',
            task_type=TaskType.GET_MORE_TASKS,
            response_type=ResponseType.TEXT,
            post_data={'S108LIB': 'QS36F', 'S108PFX': 'LE',
                       'S108CNTY': 'county'},
            custom={'page_type': 'roster'})
        expected_task = Task(
            endpoint=self._get_base_url() + 'CGIBOK108.xsl',
            task_type=TaskType.GET_MORE_TASKS,
            response_type=ResponseType.RAW,
            custom={'roster_content': content, 'page_type': 'roster_xsl'})
        self.validate_and_return_get_more_tasks(content, task, [expected_task])

    def test_get_more_tasks_roster_xsl(self):
        content = _ROSTER_XSL
        task = Task(
            endpoint=self._get_base_url() + 'CGIBOK108.xsl',
            task_type=TaskType.GET_MORE_TASKS,
            response_type=ResponseType.RAW,
            custom={'roster_content': _ROSTER_XML, 'page_type': 'roster_xsl'})
        expected_tasks = [
            Task(
                endpoint=self._get_base_url() + 'CGIBOK100.ws',
                task_type=TaskType.GET_MORE_TASKS,
                response_type=ResponseType.TEXT,
                post_data={'S100KEY': '%0A1%0A', 'S100LIB': 'QS36F',
                           'S100PFX': 'LE', 'S100CNTY': 'county',
                           'S100COCOD': ''},
                custom={'page_type': 'person'}),
            Task(
                endpoint=self._get_base_url() + 'CGIBOK100.ws',
                task_type=TaskType.GET_MORE_TASKS,
                response_type=ResponseType.TEXT,
                post_data={'S100KEY': '%0A2%0A', 'S100LIB': 'QS36F',
                           'S100PFX': 'LE', 'S100CNTY': 'county',
                           'S100COCOD': ''},
                custom={'page_type': 'person'}),

        ]
        self.validate_and_return_get_more_tasks(content, task, expected_tasks)

    def test_get_more_tasks_person(self):
        content = _PERSON_XML
        task = Task(
            endpoint=self._get_base_url() + 'CGIBOK100.ws',
            task_type=TaskType.GET_MORE_TASKS,
            response_type=ResponseType.TEXT,
            post_data={'S100KEY': '%0A1%0A', 'S100LIB': 'QS36F',
                       'S100PFX': 'LE', 'S100CNTY': 'county',
                       'S100COCOD': ''},
            custom={'page_type': 'person'})

        expected_task = Task(
            endpoint=self._get_base_url() + 'CGIBOK100.xsl',
            task_type=TaskType.SCRAPE_DATA,
            response_type=ResponseType.RAW,
            custom={'person_page_content': content})
        self.validate_and_return_get_more_tasks(content, task, [expected_task])

    def test_populate_data(self):
        content = _PERSON_XSL
        task = Task(
            endpoint=self._get_base_url() + 'CGIBOK100.xsl',
            task_type=TaskType.SCRAPE_DATA,
            response_type=ResponseType.RAW,
            custom={'person_page_content': _PERSON_XML})
        expected_info = IngestInfo()
        person = expected_info.create_person()
        person.person_id = 'PersonId'
        person.full_name = 'FullName'
        person.race = 'W'
        person.gender = 'M'
        person.ethnicity = 'N'
        person.age = '99'

        closed_booking = person.create_booking()
        closed_booking.admission_date = '01/01/2011'
        closed_booking.release_date = '02/02/2012'
        charge_1 = closed_booking.create_charge()
        charge_1.name = 'Charge1'
        bond_1 = charge_1.create_bond()
        bond_1.amount = '$1.00'

        open_booking = person.create_booking()
        open_booking.booking_id = 'BookingId'
        open_booking.admission_date = '03/03/2013'

        charge_2 = open_booking.create_charge()
        charge_2.name = 'Charge2'
        bond_2 = charge_2.create_bond()
        bond_2.amount = '$2.00'

        charge_3 = open_booking.create_charge()
        charge_3.name = 'Charge3'
        bond_3 = charge_3.create_bond()
        bond_3.amount = '$3.00'

        charge_4 = open_booking.create_charge()
        charge_4.name = 'Charge4'
        bond_4 = charge_4.create_bond()
        bond_4.amount = '$4.00'

        self.validate_and_return_populate_data(content, expected_info,
                                               task=task)
