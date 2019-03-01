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

"""Scraper tests for us_ga_forsyth."""
import unittest
from lxml import html

from recidiviz.ingest.models.ingest_info import IngestInfo, Person, Booking, \
    Charge, Bond, Hold
from recidiviz.ingest.scrape.regions.us_ga_forsyth.us_ga_forsyth_scraper \
    import UsGaForsythScraper
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.ingest.scrape.vendors.superion.superion_scraper_test \
    import SuperionScraperTest


_DETAIL_HTML = html.fromstring(
    fixtures.as_string('us_ga_forsyth', 'detail_page.html'))


class TestUsGaForsythScraper(SuperionScraperTest, unittest.TestCase):
    def _get_scraper(self):
        return UsGaForsythScraper()

    def test_enum_overrides_and_custom_populate_data(self):
        expected = IngestInfo(people=[
            Person(full_name='SIMPSON, BART Q',
                   gender='MALE',
                   age='29',
                   race='BLACK',
                   bookings=[
                       Booking(
                           admission_date='1/4/2019',
                           charges=[
                               Charge(name='HOLD FOR ANOTHER JURISDICTION',
                                      bond=Bond(bond_type='NO BOND'),
                                      next_court_date='3/5/2019'),
                               Charge(name='HOLD FOR ANOTHER JURISDICTION',
                                      status='SANCTION',
                                      bond=Bond(bond_type='ANY BOND')),
                               Charge(name='JAIL BOND FEE',
                                      status='JAIL FEE',
                                      fee_dollars='0.00',
                                      bond=Bond())],
                           holds=[
                               Hold(
                                   jurisdiction_name='HOLD FOR OTHER'
                                   ' AGENCY')])])])

        self.validate_and_return_populate_data(_DETAIL_HTML, expected)
