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

"""Scraper tests for us_fl_hendry."""
import unittest
from lxml import html

from recidiviz.ingest import constants
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.us_fl_hendry.us_fl_hendry_scraper \
    import UsFlHendryScraper
from recidiviz.tests.utils.base_scraper_test import BaseScraperTest
from recidiviz.tests.ingest import fixtures

_SEARCH_PAGE_HTML = html.fromstring(
    fixtures.as_string('us_fl_hendry', 'search_results.html'))
_LAST_PAGE_HTML = html.fromstring(
    fixtures.as_string('us_fl_hendry', 'last_page.html'))
_DETAIL_HTML = html.fromstring(
    fixtures.as_string('us_fl_hendry', 'detail.html'))


class TestUsFlHendryScraper(BaseScraperTest, unittest.TestCase):
    """ Test us_fl_hendry scraper """

    def _init_scraper_and_yaml(self):
        # The scraper to be tested. Required.
        self.scraper = UsFlHendryScraper()
        # The path to the yaml mapping. Optional.
        self.yaml = None

    def test_initial_task(self):
        content = None
        params = {
            'endpoint': None,
            'task_type': constants.INITIAL_TASK,
        }

        endpoint = self.scraper.get_region().base_url + \
                    '/inmate_search/INMATE_Results.php'

        expected_result = [
            {
                'endpoint': endpoint,
                'task_type': constants.GET_MORE_TASKS
            }
        ]

        self.validate_and_return_get_more_tasks(
            content, params, expected_result)

    def test_get_more_tasks(self):
        content = _SEARCH_PAGE_HTML
        params = {
            'endpoint': self.scraper.get_region().base_url
                        + '/inmate_search/NMATE_Results.php',
            'task_type': constants.GET_MORE_TASKS,
        }

        endpoint = self.scraper.get_region().base_url + \
            '/inmate_search/INMATE_Results.php' + \
            '?pageNum_WADAINMATE=1&totalRows_WADAINMATE=9834'

        expected_result = [
            {
                'endpoint': endpoint,
                'task_type': constants.GET_MORE_TASKS
            },
            {
                'endpoint': self.scraper.get_region().base_url+
                            '/inmate_search/INMATE_Detail.php'+
                            '?INMATE_ID=1&pageNum_WADAINMATE=0',
                'task_type': constants.SCRAPE_DATA
            },
            {
                'endpoint': self.scraper.get_region().base_url+
                            '/inmate_search/INMATE_Detail.php?'+
                            'INMATE_ID=2&pageNum_WADAINMATE=0',
                'task_type': constants.SCRAPE_DATA
            }
        ]

        self.validate_and_return_get_more_tasks(
            content, params, expected_result)

    def test_get_more_tasks_last_page(self):
        content = _LAST_PAGE_HTML
        params = {
            'endpoint': self.scraper.get_region().base_url +
                        '/inmate_search/INMATE_Results.php'+
                        '?pageNum_WADAINMATE=546&totalRows_WADAINMATE=9834',
            'task_type': constants.GET_MORE_TASKS,
        }

        expected_result = []
        self.validate_and_return_get_more_tasks(
            content, params, expected_result)

    def test_populate_data(self):
        content = _DETAIL_HTML
        params = {
            'endpoint': None,
            'task_type': constants.SCRAPE_DATA,
        }

        expected_result = IngestInfo()

        person = expected_result.create_person(
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

        self.validate_and_return_populate_data(
            content, params, expected_result)
