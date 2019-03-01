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

"""Scraper tests for us_fl_nassau."""
import unittest

from lxml import html

from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.scrape import constants
from recidiviz.ingest.scrape.regions.us_fl_nassau.us_fl_nassau_scraper import \
    UsFlNassauScraper
from recidiviz.ingest.scrape.task_params import Task
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils.base_scraper_test import BaseScraperTest

_SEARCH_RESULTS_HTML = html.fromstring(
    fixtures.as_string('us_fl_nassau', 'search_results.html'))

_DETAILS_HTML = html.fromstring(
    fixtures.as_string('us_fl_nassau', 'details.html'))


class TestUsFlNassauScraper(BaseScraperTest, unittest.TestCase):
    """ Test FL Nassau scraper """

    def _init_scraper_and_yaml(self):
        self.scraper = UsFlNassauScraper()

    def test_get_more_tasks(self):
        content = _SEARCH_RESULTS_HTML
        task = Task(
            task_type=constants.TaskType.GET_MORE_TASKS,
            endpoint=self.scraper.roster_page_url,
            params={'Page': '1'}
        )
        expected = [
            Task(task_type=constants.TaskType.GET_MORE_TASKS,
                 endpoint=self.scraper.roster_page_url,
                 params={'Page': '2'}),
            Task(task_type=constants.TaskType.GET_MORE_TASKS,
                 endpoint=self.scraper.roster_page_url,
                 params={'Page': '3'}),
            Task(task_type=constants.TaskType.SCRAPE_DATA,
                 endpoint='/'.join((self.scraper.roster_page_url,
                                    'Inmate/Detail/-1'))),
            Task(task_type=constants.TaskType.SCRAPE_DATA,
                 endpoint='/'.join((self.scraper.roster_page_url,
                                    'Inmate/Detail/-2'))),
        ]

        self.validate_and_return_get_more_tasks(content, task, expected)

    def test_populate_data(self):
        content = _DETAILS_HTML
        expected_info = IngestInfo()

        person = expected_info.create_person(
            full_name="AARDVARK, ARTHUR",
            person_id="10000",
            gender="Male",
            race="White",
            place_of_residence="FERNANDINA BEACH, FLORIDA 32034",
        )

        # Booking 1
        booking = person.create_booking(
            booking_id="2017-00000000",
            admission_date="1/1/2017 11:41 AM",
            release_date="3/1/2017 12:13 PM"
        )
        booking.create_charge(
            name="CRIMINAL REGISTRATION (NOT AN ARREST)",
            offense_date="3/15/2017 11:44 AM",
            charging_entity="AGENCY"
        ).create_sentence(
            min_length='120 days',
            max_length='120 days')

        # Booking 2
        booking = person.create_booking(
            booking_id="2015-00000001",
            admission_date="12/9/2015 9:57 PM",
            release_date="6/9/2016 5:30 AM"
        )
        booking.create_charge(
            name="FELONY BATTERY",
            offense_date="5/19/2016 2:39 PM",
            charge_class="Felony"
        )
        booking.create_charge(
            name="AGG ASSAULT",
            offense_date="5/19/2016 2:38 PM",
            charge_class="Felony"
        )
        booking.create_charge(
            name="VIOLATE DOM VIOL COND",
            offense_date="12/9/2015 4:50 PM",
            charge_class="Misdemeanor"
        )
        booking.create_charge().create_bond(
            bond_id="2015-00000001",
            bond_type="No Bond",
            amount="$0.00"
        )

        # Booking 3
        booking = person.create_booking(
            booking_id="2015-00000002",
            admission_date="11/15/2015 10:08 PM",
            release_date="12/9/2015 4:19 PM"
        )
        booking.create_charge(
            name="FEL BATT DOM W STRANGULATION",
            offense_date="11/15/2015 8:34 PM",
            charge_class="Felony"
        )

        booking.create_charge(
            name="AGG ASSAULT W INT COMT FEL",
            offense_date="11/15/2015 8:34 PM",
            charge_class="Felony"
        )

        booking.create_charge().create_bond(
            bond_id="2015-00000001",
            bond_type="Cash Bond",
            amount="$3,002.00"
        )
        booking.create_charge().create_bond(
            bond_id="2015-00000002",
            bond_type="Cash Bond",
            amount="$3,002.00"
        )

        self.validate_and_return_populate_data(content, expected_info)
