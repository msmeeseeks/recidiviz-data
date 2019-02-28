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

"""Scraper tests for us_ga_coweta."""
import unittest
from lxml import html

from recidiviz.ingest.models.ingest_info import IngestInfo, Person, Booking, \
    Charge, Bond
from recidiviz.ingest.scrape.regions.us_ga_coweta.us_ga_coweta_scraper \
    import UsGaCowetaScraper
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.ingest.scrape.vendors.superion.superion_scraper_test \
    import SuperionScraperTest


_DETAIL_HTML = html.fromstring(
    fixtures.as_string('us_ga_coweta', 'detail_page.html'))


class TestUsGaCowetaScraper(SuperionScraperTest, unittest.TestCase):
    def _get_scraper(self):
        return UsGaCowetaScraper()

    def test_enum_overrides(self):
        expected = IngestInfo(
            people=[
                Person(
                    full_name='SIMPSON, BART Q',
                    gender='MALE',
                    age='41',
                    race='WHITE OR HISPANIC',
                    bookings=[
                        Booking(
                            admission_date='2/20/2019',
                            charges=[
                                Charge(
                                    name='VGCSA SCH 1/2 NARCOTIC POSSESSION',
                                    status='NOL PROC D',
                                    case_number='WA26139',
                                    next_court_date='12/25/2019',
                                    bond=Bond(
                                        bond_type='PROFFESSIONAL BONDING CO')),
                                Charge(
                                    name='TRAFFICKING PERSON FOR LABOR/SEXUAL'
                                    ' SERVITUDE',
                                    status='REBOOK',
                                    case_number='19EW008011',
                                    bond=Bond(bond_type='NO BOND')),
                                Charge(
                                    name='VGCSA MARIJUANA POSSESSION (MISD)',
                                    status='RELEASE PER PROBATION',
                                    case_number='WA26139',
                                    bond=Bond(bond_type='N/A'))])])])

        self.validate_and_return_populate_data(_DETAIL_HTML, expected)
