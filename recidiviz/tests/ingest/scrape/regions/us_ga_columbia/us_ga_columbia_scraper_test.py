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

"""Scraper tests for us_ga_columbia."""
import unittest
from lxml import html

from recidiviz.ingest.models.ingest_info import IngestInfo, Person, Booking, \
    Charge, Bond
from recidiviz.ingest.scrape.regions.us_ga_columbia.us_ga_columbia_scraper \
    import UsGaColumbiaScraper
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.ingest.scrape.vendors.superion.superion_scraper_test \
    import SuperionScraperTest


_DETAIL_HTML = html.fromstring(
    fixtures.as_string('us_ga_columbia', 'detail_page.html'))


class TestUsGaColumbiaScraper(SuperionScraperTest, unittest.TestCase):
    def _get_scraper(self):
        return UsGaColumbiaScraper()

    def test_enum_overrides_and_custom_populate_data(self):
        expected = IngestInfo(
            people=[
                Person(full_name='SIMPSON, BART Q',
                       gender='MALE',
                       age='22',
                       race='WHITE',
                       bookings=[
                           Booking(admission_date='12/20/2018',
                                   charges=[
                                       Charge(
                                           name=
                                           'MARIJUANA-POSSESS LESS THAN 1 OZ',
                                           status='PRETRIAL',
                                           bond=Bond(
                                               bond_type='BONDING COMAPNY',
                                               status='REVOKED')),
                                       Charge(
                                           name=
                                           'MARIJUANA-POSSESS LESS THAN 1 OZ',
                                           status='AWAITING TRIAL',
                                           bond=Bond(
                                               bond_type='PAYMENT'))])])])
        self.validate_and_return_populate_data(_DETAIL_HTML, expected)
