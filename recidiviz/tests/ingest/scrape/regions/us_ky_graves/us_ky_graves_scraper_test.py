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

"""Scraper tests for us_ky_graves."""
import unittest
from lxml import html

from recidiviz.ingest import constants
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.task_params import Task
from recidiviz.ingest.us_ky_letcher.us_ky_graves_scraper import UsKyGravesScraper
from recidiviz.tests.utils.base_scraper_test import BaseScraperTest


class TestUsKyLetcherScraper(BaseScraperTest, unittest.TestCase):

    def _init_scraper_and_yaml(self):
        # The scraper to be tested. Required.
        self.scraper = UsKyLetcherScraper()
        # The path to the yaml mapping. Optional.
        self.yaml = None

    def test_get_more_tasks(self):
        # Tests navigation. Fill in |content| and |params| with the state of the
        # page to navigate from, and |expected_result| with the expected state
        # after navigation. Chain multiple calls to
        # |validate_and_return_get_more_tasks| together if necessary.
        content = html.fromstring('navigation page')
        task = Task(
            task_type=constants.TaskType.GET_MORE_TASKS,
            endpoint=None,
        )
        expected_result = [
            Task(
                endpoint=None,
                task_type=None,
            ),
        ]

        self.validate_and_return_get_more_tasks(content, task, expected_result)


    def test_populate_data(self):
        # Tests scraping data. Fill in |content| with the state of the page
        # containing person data, and |expected_result| with the IngestInfo
        # objects that should be scraped from the page. A default Task will be
        # passed to the |populate_data| method, but this can be overriden by
        # supplying a |task| argument
        content = html.fromstring('person data page')
        expected_info = IngestInfo()

        self.validate_and_return_populate_data(content, expected_info)


"""
      charges[7]: Charge:
         offense_date: 03/10/2018
         statute: 52196
         name: POSSESSION OF FIREARM BY CONVICTED FELON
         charge_class: F
         level: D
         fee_dollars: 0.00
         case_number: 18-F-136
         bond: Bond:
            amount: 5000.00
            bond_type: FINE AND COSTS
            status: JAIL CREDIT
      holds[0]: Hold:
         jurisdiction_name: KY PROBATION AND PAROLE
"""
