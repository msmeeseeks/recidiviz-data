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

"""Scraper tests for us_nc_cabarrus."""
import unittest
from lxml import html

from recidiviz.ingest.scrape.regions.us_nc_cabarrus.us_nc_cabarrus_scraper \
    import UsNcCabarrusScraper
from recidiviz.ingest.models.ingest_info import IngestInfo, Person, Booking, \
    Charge
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.ingest.scrape.vendors.superion.superion_scraper_test \
    import SuperionScraperTest

_ENUM_OVERRIDE_HTML = html.fromstring(
    fixtures.as_string('us_nc_cabarrus', 'enum_override.html'))


class TestUsNcCabarrusScraper(SuperionScraperTest, unittest.TestCase):

    def _get_scraper(self):
        return UsNcCabarrusScraper()

    def test_enum_overrides(self):
        expected = IngestInfo(people=[
            Person(full_name='SIMPSON, BART Q',
                   gender='MALE',
                   age='46',
                   race='WHITE',
                   bookings=[Booking(charges=[
                       Charge(name='FAIL TO APPEAR-MISDEMEAN',
                              status='WAITING TRANSPORT TO DAC',
                              case_number='DUMMY')])])])

        self.validate_and_return_populate_data(
            _ENUM_OVERRIDE_HTML, expected)
