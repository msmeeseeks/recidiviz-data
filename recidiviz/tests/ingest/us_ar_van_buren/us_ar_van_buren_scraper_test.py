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

"""Scraper tests for us_ar_van_buren."""
import unittest
from lxml import html

from recidiviz.ingest import constants
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.task_params import Task
from recidiviz.ingest.us_ar_van_buren.us_ar_van_buren_scraper import \
    UsArVanBurenScraper
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils.base_scraper_test import BaseScraperTest

_ROSTER_HTML = html.fromstring(fixtures.as_string('us_ar_van_buren',
                                                  'roster.html'))
_DETAILS_HTML = html.fromstring(fixtures.as_string('us_ar_van_buren',
                                                   'detail.html'))


class TestUsArVanBurenScraper(BaseScraperTest, unittest.TestCase):

    def _init_scraper_and_yaml(self):
        self.scraper = UsArVanBurenScraper()

    def test_get_more_tasks(self):
        content = _ROSTER_HTML
        task = Task(
            task_type=constants.TaskType.GET_MORE_TASKS,
            endpoint=None,
        )
        expected_result = [
            Task(
                task_type=constants.TaskType.GET_MORE_TASKS,
                endpoint='{}?grp=40'.format(self.scraper.region.base_url),
            ),
        ]

        self.validate_and_return_get_more_tasks(content, task, expected_result)

    def test_populate_data(self):
        content = _DETAILS_HTML
        expected_info = IngestInfo()
        p = expected_info.create_person(full_name='FULL NAME', gender='M',
                                          age='100', race='W')
        b = p.create_booking(booking_id='123456789',
                             admission_date='11-11-1111 11:11 am',
                             total_bond_amount='$10,000.00')
        b.create_arrest(agency='VBSO')
        b.create_charge(name='Charge 1')
        b.create_charge(name='Charge 2')
        b.create_charge(name='Charge 3')
        b.create_charge(name='Charge 4')

        self.validate_and_return_populate_data(content, expected_info)
