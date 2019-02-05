# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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

"""Scraper tests for us_fl_osceola."""
import unittest
from lxml import html

from recidiviz.ingest.scrape import constants
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.scrape.task_params import Task
from recidiviz.ingest.scrape.regions.us_fl_osceola.us_fl_osceola_scraper \
    import UsFlOsceolaScraper
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils.base_scraper_test import BaseScraperTest

_SEARCH_PAGE_HTML = html.fromstring(
    fixtures.as_string('us_fl_osceola', 'search.html'))
_DETAIL_PAGE_HTML = html.fromstring(
    fixtures.as_string('us_fl_osceola', 'details.html'))


class TestUsFlOsceolaCountyScraper(BaseScraperTest, unittest.TestCase):
    """ Osceola County Scraper tests"""

    def _init_scraper_and_yaml(self):
        # The scraper to be tested. Required.
        self.scraper = UsFlOsceolaScraper()
        # The path to the yaml mapping. Optional.
        self.yaml = None

    def test_initial_task(self):
        task = Task(
            task_type=constants.TaskType.INITIAL,
            endpoint=None,
        )
        expected_result = [
            Task(
                task_type=constants.TaskType.GET_MORE_TASKS,
                endpoint=(self.scraper.get_region().base_url +
                          '/Apps/CorrectionsReports/Report/Search'),
            )
        ]

        self.validate_and_return_get_more_tasks("", task, expected_result)

    def test_get_more_tasks(self):
        task = Task(
            task_type=constants.TaskType.GET_MORE_TASKS,
            endpoint=None,
        )
        expected_result = [
            Task(
                endpoint=(self.scraper.get_region().base_url +
                          '/Apps/CorrectionsReports/Report/Details/1'),
                task_type=constants.TaskType.SCRAPE_DATA
            ),
            Task(
                endpoint=(self.scraper.get_region().base_url +
                          '/Apps/CorrectionsReports/Report/Details/2'),
                task_type=constants.TaskType.SCRAPE_DATA
            ),
            Task(
                endpoint=(self.scraper.get_region().base_url +
                          '/Apps/CorrectionsReports/Report/Details/3'),
                task_type=constants.TaskType.SCRAPE_DATA
            )
        ]

        self.validate_and_return_get_more_tasks(_SEARCH_PAGE_HTML,
                                                task, expected_result)

    def test_populate_data(self):
        expected_info = IngestInfo()
        booking = expected_info.create_person(
            gender="M",
            race="B",
            birthdate="01/01/1967",
            full_name="NAME           NAME           NAME",
            person_id="1"
        ).create_booking(
            booking_id="B1"
        )

        booking.create_charge(
            statute="843.02",
            name="AAA-AAA",
            charge_id="C1",
            offense_date="07/29/2018",
            case_number='AAAA'
        ).create_bond(amount="$0")

        booking.create_charge(
            statute="893.13-1C1    `",
            name="AAAA",
            charge_id="C2",
            offense_date="07/30/2018",
            case_number='AAAA'
        ).create_bond(amount="$10,000")

        booking.create_charge(
            statute="901.31",
            name="AAA",
            charge_id="C3",
            offense_date="07/30/2017",
            case_number='VVVVV'
        ).create_bond(amount="$0")

        self.validate_and_return_populate_data(_DETAIL_PAGE_HTML,
                                               expected_info)
