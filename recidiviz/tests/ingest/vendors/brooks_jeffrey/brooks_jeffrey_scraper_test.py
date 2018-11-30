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

"""Tests for Brooks Jeffrey scraper:
ingest/vendors/brooks_jeffrey/brooks_jeffrey_scraper.py."""

from lxml import html
from recidiviz.ingest import constants
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.vendors.brooks_jeffrey.brooks_jeffrey_scraper import \
    BrooksJeffreyScraper
from recidiviz.tests.ingest import fixtures

_FRONT_PAGE_HTML = html.fromstring(
    fixtures.as_string('vendors/brooks_jeffrey', 'home_page.html'))

_PERSON_PAGE_HTML = html.fromstring(
    fixtures.as_string('vendors/brooks_jeffrey', 'person_page.html'))


class TestBrooksJeffreyScraper(object):
    """Test class for TestBrooksJeffreyScraper."""

    def setup_method(self, _test_method):
        # 'us_ar_van_buren' region chose arbitrarily from regions using Brooks
        # Jeffrey vendor. Region must be specified.
        self.subject = BrooksJeffreyScraper('us_ar_van_buren')

    def test_home_page_navigation(self):
        result = self.subject.get_more_tasks(_FRONT_PAGE_HTML, {
            'task_type': constants.INITIAL_TASK})
        expected_result = [
            {
                'endpoint': 'https://www.vbcso.com/roster_view'
                            '.php?booking_num=123',
                'task_type': constants.SCRAPE_DATA
            },
            {
                'endpoint': 'https://www.vbcso.com/roster_view'
                            '.php?booking_num=456',
                'task_type': constants.SCRAPE_DATA
            },
            {
                'endpoint': 'https://www.vbcso.com/roster_view'
                            '.php?booking_num=789',
                'task_type': constants.SCRAPE_DATA
            },
            {
                'endpoint': 'https://www.vbcso.com/roster.php?grp=40',
                'task_type': constants.SCRAPE_DATA_AND_MORE
            }
        ]
        assert result == expected_result

    def test_populate_data(self):
        result = self.subject.populate_data(_PERSON_PAGE_HTML, {}, None)
        expected_info = IngestInfo()

        person = expected_info.create_person()
        person.given_names = "First Middle"
        person.surname = "Last"
        person.sex = "M"
        person.age = "100"
        person.race = "W"

        booking = person.create_booking()
        booking.booking_id = "123"
        booking.admission_date = "1-1-2048- 12:30 am"
        booking.total_bond_amount = "$695.00"

        charge = booking.create_charge()
        charge.name = "Charge 1"

        arrest = booking.create_arrest()
        arrest.agency = "Agency"

        assert expected_info == result