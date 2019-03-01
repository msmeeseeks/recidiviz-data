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

"""Scraper tests for us_al_morgan."""
import unittest
from lxml import html

from recidiviz.ingest.models.ingest_info import IngestInfo, Person, Booking, \
    Charge, Bond
from recidiviz.ingest.scrape.regions.us_al_morgan.us_al_morgan_scraper import \
    UsAlMorganScraper
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.ingest.scrape.vendors.superion.superion_scraper_test import \
    SuperionScraperTest


_DETAIL_HTML = html.fromstring(
    fixtures.as_string('us_al_morgan', 'detail_page.html'))


class TestUsAlMorganScraper(SuperionScraperTest, unittest.TestCase):
    def _get_scraper(self):
        return UsAlMorganScraper()

    def test_enum_overrides_and_custom_populate_data(self):
        expected = IngestInfo(people=[
            Person(full_name='SIMPSON, BART Q',
                   gender='MALE',
                   age='43',
                   race='WHITE',
                   bookings=[
                       Booking(admission_date='12/13/2018',
                               charges=[
                                   Charge(name=
                                          'UNLAWFUL POSS CONTROLLED SUBSTANCE',
                                          status='CHARGE COMBINED WITH OTHERS',
                                          case_number='DC18-2906',
                                          bond=Bond(
                                              amount='2,500.00',
                                              bond_type=
                                              'APPROVE TO SIGN ON BOND ')),
                                   Charge(name=
                                          'UNLAWFUL POSS CONTROLLED SUBSTANCE',
                                          status='ENTRY ERROR',
                                          case_number='DC18-2906',
                                          bond=Bond(
                                              bond_type=
                                              'COMBINED BONDS TO 1BOND')),
                                   Charge(name=
                                          'POSSESSION OF DRUG PARAPHERNALIA',
                                          status='MUNICIPAL COURT',
                                          case_number='DC18-2907',
                                          bond=Bond(
                                              amount='300.00',
                                              bond_type='MUNICIPAL BOND ')),
                                   Charge(name=
                                          'POSSESSION OF DRUG PARAPHERNALIA',
                                          status='SETTLED',
                                          case_number='DC18-2907',
                                          bond=Bond(bond_type='NO BILLED')),
                                   Charge(name=
                                          'POSSESSION OF DRUG PARAPHERNALIA',
                                          status='TRANSIT',
                                          case_number='DC18-2907',
                                          bond=Bond(bond_type='NO BILLED')),
                                   Charge(name=
                                          'POSSESSION OF DRUG PARAPHERNALIA',
                                          status='WARRANT RECALLED',
                                          case_number='DC18-2907',
                                          bond=Bond(bond_type='NO BILLED'))])])
        ])

        self.validate_and_return_populate_data(_DETAIL_HTML, expected)
