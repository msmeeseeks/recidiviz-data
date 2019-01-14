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

"""Scraper tests for us_fl_martin."""
import unittest
from lxml import html

from recidiviz.ingest import constants
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.tests.utils.base_scraper_test import BaseScraperTest
from recidiviz.tests.ingest import fixtures
from recidiviz.ingest.us_fl_martin.us_fl_martin_scraper \
    import UsFlMartinScraper

_REPORT_PAGE_HTML = html.fromstring(
    fixtures.as_string('us_fl_martin', 'report.html'))


class TestUsFlMartinScraper(BaseScraperTest, unittest.TestCase):

    def _init_scraper_and_yaml(self):
        # The scraper to be tested. Required.
        self.scraper = UsFlMartinScraper()
        # The path to the yaml mapping. Optional.
        self.yaml = None

    def test_get_more_tasks(self):
        # Tests navigation. Fill in |content| and |params| with the state of the
        # page to navigate from, and |expected_result| with the expected state
        # after navigation. Chain multiple calls to
        # |validate_and_return_get_more_tasks| together if necessary.
        content = ""
        params = {
            'endpoint': None,
            'task_type': constants.INITIAL_TASK,
        }

        endpoint = self.scraper.get_region().base_url + '?RunReport=Run+Report'
        expected_result = [
            {
                'endpoint': endpoint,
                'task_type': constants.SCRAPE_DATA
            }
        ]

        self.validate_and_return_get_more_tasks(
            content, params, expected_result)

    def test_populate_data(self):
        # Tests scraping data. Fill in |content| and |params| with the state of
        # the page containing person data, and |expected_result| with the
        # IngestInfo objects that should be scraped from the page.
        content = _REPORT_PAGE_HTML
        params = {
            'endpoint': None,
            'task_type': constants.SCRAPE_DATA,
        }
        expected_result = IngestInfo()

        expected_result.create_person(
            person_id="111111",
            surname="AARDVARK",
            given_names="ARTHUR",
            # given_names="ARTHUR A",
            gender="M",
            birthdate="01/01/1994",
            race="B")

        expected_result.people[0].create_booking().create_arrest(
            agency="AAA",
            date="01/01/2018")
        # date="01/01/2018 10:00:00")

        expected_result.people[0].bookings[0].create_charge(
            statute="FS*893.13(6b)",
            name="CHARGE 1",
            charge_class="Misdemeanor",
        ).create_bond(amount="$0.00")

        expected_result.people[0].bookings[0].create_charge(
            statute="FS*893.13(6A)",
            name="CHARGE 2",
            charge_class="Unknown"
        ).create_bond(amount="$0.00")

        expected_result.create_person(
            person_id="2222",
            surname="AARDVARK",
            given_names="BART",
            # given_names="BART B",
            gender="F",
            birthdate="01/01/1975",
            race="W")

        expected_result.people[1].create_booking().create_arrest(
            agency="CCC",
            date="01/01/2018")
        # date="01/01/2018 08:00:00")

        expected_result.people[1].bookings[0].create_charge(
            statute="FS*893.147",
            name="CHARGE 1",
            charge_class="Felony",
        ).create_bond(amount="$500.00")

        self.validate_and_return_populate_data(
            content, params, expected_result)
