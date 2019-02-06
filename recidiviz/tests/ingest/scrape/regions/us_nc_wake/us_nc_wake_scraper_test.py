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

"""Scraper tests for us_nc_wake."""
import unittest
from lxml import html

from recidiviz.ingest.scrape.regions.us_nc_wake.us_nc_wake_scraper import \
    UsNcWakeScraper
from recidiviz.ingest.models.ingest_info import IngestInfo, Person, Booking, \
    Charge, Bond
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.ingest.scrape.vendors.superion.superion_scraper_test \
    import SuperionScraperTest

_ENUM_OVERRIDE_HTML = html.fromstring(
    fixtures.as_string('us_nc_wake', 'enum_override.html'))


class TestUsNcWakeScraper(SuperionScraperTest, unittest.TestCase):
    """Scraper tests for us_nc_wake."""
    def _get_scraper(self):
        return UsNcWakeScraper()

    def test_enum_overrides_and_custom_populate_data(self):
        expected = IngestInfo(people=[
            Person(full_name='SIMPSON, BART Q',
                   gender='MALE',
                   age='29',
                   race='WHITE',
                   bookings=[Booking(charges=[
                       Charge(name='FIRST DEGREE KIDNAPPING',
                              status='AWAITING TRIAL',
                              case_number='DUMMY',
                              bond=Bond(amount='0.00',
                                        bond_type='ELECTRONIC HOUSE ARREST'
                                        ' - SECURED BOND ')),
                       Charge(name='FIRST DEGREE KIDNAPPING',
                              status='SENTENCED',
                              case_number='DUMMY'),
                       Charge(name='FIRST DEGREE KIDNAPPING',
                              status='AWAITING TRIAL',
                              case_number='DUMMY',
                              bond=Bond(status='PENDING')),
                       Charge(name='FIRST DEGREE KIDNAPPING',
                              status='AWAITING TRIAL',
                              case_number='DUMMY',
                              bond=Bond(bond_type='DISMISSED 234')),
                       Charge(name='FIRST DEGREE KIDNAPPING',
                              status='AWAITING TRIAL',
                              case_number='DUMMY',
                              bond=Bond(bond_type='SECURE BOND - 2ND OR '
                                        'SUBSEQUENT FTA ON THIS CASE')),
                   ],
                                     admission_date='7/1/2018')])])

        self.validate_and_return_populate_data(
            _ENUM_OVERRIDE_HTML, expected)
