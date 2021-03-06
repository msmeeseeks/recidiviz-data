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

from recidiviz.ingest.scrape import constants
from recidiviz.ingest.models.ingest_info import IngestInfo, Bond
from recidiviz.ingest.scrape.task_params import Task
from recidiviz.tests.utils.base_scraper_test import BaseScraperTest
from recidiviz.tests.ingest import fixtures
from recidiviz.ingest.scrape.regions.us_fl_martin.us_fl_martin_scraper \
    import UsFlMartinScraper

_REPORT_PAGE_HTML = html.fromstring(
    fixtures.as_string('us_fl_martin', 'report.html'))


class TestUsFlMartinScraper(BaseScraperTest, unittest.TestCase):
    """Tests for UsFlMartinScraper"""

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
        task = Task(
            task_type=constants.TaskType.INITIAL,
            endpoint=None,
        )

        expected_result = [
            Task(
                task_type=constants.TaskType.SCRAPE_DATA,
                endpoint=self.scraper.get_region().base_url,
                params={'RunReport': 'Run Report'}
            )
        ]

        self.validate_and_return_get_more_tasks(
            content, task, expected_result)

    def test_populate_data(self):
        # Tests scraping data. Fill in |content| and |params| with the state of
        # the page containing person data, and |expected_result| with the
        # IngestInfo objects that should be scraped from the page.
        content = _REPORT_PAGE_HTML
        expected_info = IngestInfo()

        expected_info.create_person(
            person_id="111111",
            surname="AARDVARK",
            given_names="ARTHUR",
            # given_names="ARTHUR A",
            gender="M",
            birthdate="01/01/1994",
            race="B")

        expected_info.people[0].create_booking().create_arrest(
            agency="AAA",
            arrest_date="01/01/2018")
        # date="01/01/2018 10:00:00")

        expected_info.people[0].bookings[0].create_charge(
            statute="FS*893.13(6b)",
            name="CHARGE 1",
            charge_class="Misdemeanor",
        ).create_bond(amount="$0.00")

        expected_info.people[0].bookings[0].create_charge(
            statute="FS*893.13(6A)",
            name="CHARGE 2",
            charge_class="Unknown"
        ).create_bond(amount="$0.00")

        expected_info.create_person(
            person_id="2222",
            surname="AARDVARK",
            given_names="BART",
            # given_names="BART B",
            gender="F",
            birthdate="01/01/1975",
            race="W")

        expected_info.people[1].create_booking().create_arrest(
            agency="CCC",
            arrest_date="01/01/2018")
        # date="01/01/2018 08:00:00")

        expected_info.people[1].bookings[0].create_charge(statute="FS*893.147",
                                                          name="CHARGE 1",
                                                          charge_class="Felony",
                                                          bond=Bond(
                                                              amount="$500.00")
                                                          )

        self.validate_and_return_populate_data(content, expected_info)
