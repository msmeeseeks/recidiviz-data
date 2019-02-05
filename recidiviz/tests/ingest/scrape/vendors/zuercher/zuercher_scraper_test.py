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
Scraper tests for regions that use zuercher_scraper.

Any region scraper test class that inherits from ZuercherScraperTest must
implement the following:
     _init_scraper_and_yaml(self):
       self.scraper = RegionScraperCls()
"""
from datetime import datetime

import pytz
from mock import patch

from recidiviz.ingest.scrape import constants
from recidiviz.ingest.scrape.task_params import Task
from recidiviz.tests.utils.base_scraper_test import BaseScraperTest


class ZuercherScraperTest(BaseScraperTest):
    """Common tests for Zuercher scrapers"""

    _DATETIME = datetime(2019, 1, 1, 12, 0, 0, 0, tzinfo=pytz.utc)
    _DATETIME_STR = '2019-01-01T12:00:00+00:00'

    def _make_task(self, task_type, count, start):
        return Task(
            task_type=task_type,
            endpoint=self.scraper.get_region().base_url +
            '/api/portal/inmates/load',
            json={
                'name': '',
                'race': 'all',
                'sex': 'all',
                'cell_block': 'all',
                'held_for_agency': 'any',
                'in_custody': self._DATETIME_STR,
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
                'search_time': self._DATETIME_STR,
            },
            response_type=constants.ResponseType.JSON,
        )

    @patch('recidiviz.ingest.scrape.vendors.zuercher.zuercher_scraper.datetime')
    def test_get_more_tasks_initial(self, mock_datetime):
        mock_datetime.now.return_value = self._DATETIME

        task = self.scraper.get_initial_task()
        expected_result = [self._make_task(
            task_type=constants.TaskType.SCRAPE_DATA_AND_MORE,
            count=100, start=0)]

        self.validate_and_return_get_more_tasks(
            {}, task, expected_result)


    @patch('recidiviz.ingest.scrape.vendors.zuercher.zuercher_scraper.datetime')
    def test_get_more_tasks_middle(self, mock_datetime):
        mock_datetime.now.return_value = self._DATETIME

        content = {'total_record_count': 450}
        task = self._make_task(
            task_type=constants.TaskType.SCRAPE_DATA_AND_MORE,
            count=100, start=100)
        expected_result = [self._make_task(
            task_type=constants.TaskType.SCRAPE_DATA_AND_MORE,
            count=100, start=200)]

        self.validate_and_return_get_more_tasks(
            content, task, expected_result)


    @patch('recidiviz.ingest.scrape.vendors.zuercher.zuercher_scraper.datetime')
    def test_get_more_tasks_end(self, mock_datetime):
        mock_datetime.now.return_value = self._DATETIME

        content = {'total_record_count': 450}
        task = self._make_task(
            task_type=constants.TaskType.SCRAPE_DATA_AND_MORE,
            count=100, start=300)
        expected_result = [self._make_task(
            task_type=constants.TaskType.SCRAPE_DATA,
            count=100, start=400)]

        self.validate_and_return_get_more_tasks(
            content, task, expected_result)
