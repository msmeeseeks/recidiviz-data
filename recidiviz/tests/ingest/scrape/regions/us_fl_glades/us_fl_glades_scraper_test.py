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

"""Scraper tests for us_fl_glades."""
import unittest
from lxml import html

from recidiviz.ingest.scrape import constants
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.scrape.task_params import Task
from recidiviz.ingest.scrape.regions.us_fl_glades.us_fl_glades_scraper \
    import UsFlGladesScraper
from recidiviz.tests.utils.base_scraper_test import BaseScraperTest
from recidiviz.tests.ingest import fixtures

_SEARCH_PAGE_HTML = html.fromstring(
    fixtures.as_string('us_fl_glades', 'search_results.html'))
_LAST_PAGE_HTML = html.fromstring(
    fixtures.as_string('us_fl_glades', 'last_page.html'))
_DETAIL_HTML = html.fromstring(
    fixtures.as_string('us_fl_glades', 'detail.html'))


class TestUsFlGladesScraper(BaseScraperTest, unittest.TestCase):
    """ Test us_fl_glades scraper """

    def _init_scraper_and_yaml(self):
        # The scraper to be tested. Required.
        self.scraper = UsFlGladesScraper()
        # The path to the yaml mapping. Optional.
        self.yaml = None

    def test_initial_task(self):
        content = None
        task = Task(
            task_type=constants.TaskType.INITIAL,
            endpoint=None,
        )

        endpoint = self.scraper.get_region().base_url + \
                    '/INMATE_Results.php'

        expected_result = [
            Task(
                task_type=constants.TaskType.GET_MORE_TASKS,
                endpoint=endpoint,
            )
        ]

        self.validate_and_return_get_more_tasks(
            content, task, expected_result)

    def test_get_more_tasks(self):
        content = _SEARCH_PAGE_HTML
        task = Task(
            task_type=constants.TaskType.GET_MORE_TASKS,
            endpoint=self.scraper.get_region().base_url
                        + '/NMATE_Results.php',
        )

        endpoint = self.scraper.get_region().base_url + \
            '/INMATE_Results.php' + \
            '?pageNum_WADAINMATE=1&totalRows_WADAINMATE=9834'

        expected_result = [
            Task(
                task_type=constants.TaskType.GET_MORE_TASKS,
                endpoint=endpoint,
            ),
            Task(
                task_type=constants.TaskType.SCRAPE_DATA,
                endpoint=self.scraper.get_region().base_url + \
                         '/INMATE_Detail.php' + \
                         '?INMATE_ID=1&pageNum_WADAINMATE=0',
            ),
            Task(
                task_type=constants.TaskType.SCRAPE_DATA,
                endpoint=self.scraper.get_region().base_url + \
                         '/INMATE_Detail.php?' + \
                         'INMATE_ID=2&pageNum_WADAINMATE=0',
            )
        ]

        self.validate_and_return_get_more_tasks(
            content, task, expected_result)

    def test_get_more_tasks_last_page(self):
        content = _LAST_PAGE_HTML
        task = Task(
            task_type=constants.TaskType.GET_MORE_TASKS,
            endpoint=self.scraper.get_region().base_url + \
                     '/INMATE_Results.php'+ \
                     '?pageNum_WADAINMATE=546&totalRows_WADAINMATE=9834',
        )

        expected_result = []
        self.validate_and_return_get_more_tasks(
            content, task, expected_result)

    def test_populate_data(self):
        content = _DETAIL_HTML

        expected_info = IngestInfo()

        person = expected_info.create_person(
            full_name="NAME NAME",
            gender="F",
            race="WHITE",
            birthdate="01-01-1993",
            place_of_residence="1111  STREET FL 30000",
            person_id="HCSO00"
        )

        person.create_booking(
            release_date="--",
            admission_date="12-17-2018",
            booking_id="HCSO00",
            custody_status="IN CUSTODY"
        ).create_charge(
            statute="948.06 / PROB VIOLATION"
        ).create_bond(
            amount="$"
        )

        person.create_booking(
            release_date="12-12-2018",
            admission_date="12-12-2018",
            booking_id="HCSO00",
        ).create_charge(
            statute="817.565.1a / FRAUD",
            name="WLLFLLY DEFRAUD/ATTMPT DEFRAUD URINE DRUG TEST",
            number_of_counts="1"
        ).create_bond(
            amount="$0"
        )

        b = person.create_booking(
            release_date="02-22-2018",
            admission_date="02-21-2018",
            booking_id="HCS01",
        )
        b.create_charge(
            statute="316.193.3c1 / DUI-UNLAW BLD ALCH",
            name="DUI AND DAMAGE PROPERTY",
            number_of_counts="1"
        ).create_bond(
            amount="$1000"
        )
        b.create_charge(
            statute="893.13.6a / DRUGS-POSSESS",
            name="CNTRL SUB WO PRESCRIPTION 3 GRAMS OR MORE",
            number_of_counts="1"
        ).create_bond(
            amount="$3500"
        )
        b.create_charge(
            statute="316.192.3a1 / MOVING TRAFFIC VIOL",
            name="RECKLESS DRIVE DAMAGE PERSON OR PROPERTY",
            number_of_counts="1"
        ).create_bond(
            amount="$1000"
        )

        b.create_charge(
            statute="316.063.1 / HIT AND RUN",
            name="ACC-UNATTENDED VEH OR PROP WO LEAVING ID",
            number_of_counts="1"
        ).create_bond(
            amount="$500"
        )

        self.validate_and_return_populate_data(content, expected_info)
