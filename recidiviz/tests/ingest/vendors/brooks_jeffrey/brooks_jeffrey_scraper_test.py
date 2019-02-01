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
"""
Scraper tests for regions that use unaltered brooks_jeffrey_scraper.
Any region scraper test class that inherits from BrooksJeffreyScraperTest must

implement the following:
     _init_scraper_and_yaml(self):
       self.scraper = RegionScraperCls()
"""
from copy import copy

from lxml import html

from recidiviz.common.constants.bond import BondStatus
from recidiviz.ingest import constants
from recidiviz.ingest.models import ingest_info
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.task_params import Task
from recidiviz.ingest.vendors.brooks_jeffrey.brooks_jeffrey_scraper import \
    _parse_total_bond_if_necessary
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils.base_scraper_test import BaseScraperTest

_FRONT_PAGE_HTML = html.fromstring(
    fixtures.as_string('vendors/brooks_jeffrey', 'home_page.html'))

_PERSON_PAGE_HTML = html.fromstring(
    fixtures.as_string('vendors/brooks_jeffrey', 'person_page.html'))

_PERSON_PAGE_MULTIPLE_BONDS_HTML = html.fromstring(
    fixtures.as_string('vendors/brooks_jeffrey',
                       'person_page_multiple_bonds.html'))

_PERSON_PAGE_NO_BOND_AVAILABLE_HTML = html.fromstring(
    fixtures.as_string('vendors/brooks_jeffrey',
                       'person_page_no_bond_available.html'))


class BrooksJeffreyScraperTest(BaseScraperTest):
    """Test class for TestBrooksJeffreyScraper."""

    def _get_base_url(self):
        return self.scraper.region.base_url.rpartition('/')[0] + '/'

    def test_home_page_navigation(self):
        expected_result = [
            Task(
                task_type=constants.TaskType.SCRAPE_DATA,
                endpoint=
                self._get_base_url() + 'roster_view.php?booking_num=123'
            ),
            Task(
                task_type=constants.TaskType.SCRAPE_DATA,
                endpoint=
                self._get_base_url() + 'roster_view.php?booking_num=456'
            ),
            Task(
                task_type=constants.TaskType.SCRAPE_DATA,
                endpoint=
                self._get_base_url() + 'roster_view.php?booking_num=789'
            ),
            Task(
                task_type=constants.TaskType.GET_MORE_TASKS,
                endpoint=self._get_base_url() + 'roster.php?grp=40'
            ),
        ]
        self.validate_and_return_get_more_tasks(
            _get_front_page(),
            Task(task_type=constants.TaskType.INITIAL, endpoint=''),
            expected_result)

    def test_populate_data(self):
        expected_info = IngestInfo()

        person = expected_info.create_person()
        person.full_name = "First Middle Last"
        person.gender = "M"
        person.age = "100"
        person.race = "W"

        booking = person.create_booking()
        booking.booking_id = "123"
        booking.admission_date = "1-1-2048- 12:30 am"
        booking.total_bond_amount = "$695.00"

        booking.create_charge(name="Charge 1")
        booking.create_charge(name="Charge 2")
        booking.create_charge(name="Charge 3")

        arrest = booking.create_arrest()
        arrest.agency = "Agency"

        self.validate_and_return_populate_data(
            _get_person_page(), expected_info)

    def test_populate_data_no_bond_available(self):
        expected_info = IngestInfo()

        person = expected_info.create_person()
        person.full_name = "First Middle Last"
        person.gender = "M"
        person.age = "100"
        person.race = "W"

        booking = person.create_booking()
        booking.booking_id = "123"
        booking.admission_date = "1-1-2048- 12:30 am"

        charge_1 = booking.create_charge(name="Charge 1")
        charge_1.create_bond(status="DENIED")
        charge_2 = booking.create_charge(name="Charge 2")
        charge_2.create_bond(status="DENIED")
        charge_3 = booking.create_charge(name="Charge 3")
        charge_3.create_bond(status="DENIED")

        arrest = booking.create_arrest()
        arrest.agency = "Agency"

        self.validate_and_return_populate_data(
            _get_person_page_no_bond_available(), expected_info)

    def test_populate_data_multiple_bonds(self):
        expected_info = IngestInfo()

        person = expected_info.create_person()
        person.full_name = "First Middle Last"
        person.gender = "M"
        person.age = "100"
        person.race = "W"

        booking = person.create_booking()
        booking.booking_id = "123"
        booking.admission_date = "1-1-2048- 12:30 am"

        charge_1 = booking.create_charge(name="Charge 1")
        charge_1.create_bond(amount="$1")
        charge_2 = booking.create_charge(name="Charge 2")
        charge_2.create_bond(amount="$2")
        charge_3 = booking.create_charge(name="Charge 3")
        charge_3.create_bond(amount="$3")

        arrest = booking.create_arrest()
        arrest.agency = "Agency"

        self.validate_and_return_populate_data(
            _get_person_page_multiple_bonds(), expected_info)

    def test_parse_total_bond_denied(self):
        booking = ingest_info.Booking(total_bond_amount='DENIED.')
        assert _parse_total_bond_if_necessary(booking) \
               == (None, BondStatus.DENIED)

    def test_parse_total_bond_no_bond(self):
        booking = ingest_info.Booking(total_bond_amount='No Bond.')
        assert _parse_total_bond_if_necessary(booking) \
               == (None, BondStatus.DENIED)

    def test_parse_total_bond_must_see_judge(self):
        booking = ingest_info.Booking(total_bond_amount='Must See Judge')
        assert _parse_total_bond_if_necessary(booking) \
               == (None, BondStatus.PENDING)

    def test_parse_total_bond_multiple_bonds(self):
        booking = ingest_info.Booking(total_bond_amount='500 \n 600')
        assert _parse_total_bond_if_necessary(booking) == (['500', '600'], None)


def _get_person_page_no_bond_available():
    # Make defensive copy since content is mutable
    return copy(_PERSON_PAGE_NO_BOND_AVAILABLE_HTML)


def _get_person_page_multiple_bonds():
    # Make defensive copy since content is mutable
    return copy(_PERSON_PAGE_MULTIPLE_BONDS_HTML)


def _get_person_page():
    # Make defensive copy since content is mutable
    return copy(_PERSON_PAGE_HTML)


def _get_front_page():
    # Make defensive copy since content is mutable
    return copy(_FRONT_PAGE_HTML)
