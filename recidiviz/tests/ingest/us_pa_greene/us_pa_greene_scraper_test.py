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
"""Scraper tests for us_pa_greene."""

import unittest

import pytest
from lxml import html

from recidiviz.ingest.models.ingest_info import IngestInfo, _Person, \
    _Booking, _Charge, _Bond, _Hold
from recidiviz.ingest.us_pa_greene.us_pa_greene_scraper import UsPaGreeneScraper
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils.base_scraper_test import BaseScraperTest

_DETAILS_HTML = html.fromstring(
    fixtures.as_string('us_pa_greene', 'details.html'))
_NO_CHARGES_HTML = html.fromstring(
    fixtures.as_string('us_pa_greene', 'no_charges.html'))


class UsPaGreeneScraperTest(BaseScraperTest, unittest.TestCase):
    """Scraper tests for us_pa_greene."""

    def _init_scraper_and_yaml(self):
        self.scraper = UsPaGreeneScraper()
        self.yaml = None

    def test_populate_data(self):
        expected_info = IngestInfo(people=[
            _Person(person_id='18-00000', full_name='LAST, FIRST',
                    birthdate='01/01/2001', gender='Male', age='1111',
                    race='White/Eurp/ N.Afr/Mid Eas',
                    bookings=[
                        _Booking(booking_id='18-00000',
                                 admission_date='01/01/2001 00:00',
                                 holds=[_Hold(jurisdiction_name=
                                              'District Court 11-1-11')],
                                 charges=[
                                     _Charge(statute='0000(a)(1)',
                                             name='charge 1',
                                             case_number='MJ-10000-CR0000-2000',
                                             bond=_Bond(amount='$50,000.00')),
                                     _Charge(statute='000000(A)(16)',
                                             name='charge 2',
                                             case_number='CR-100-2000',
                                             bond=_Bond(
                                                 amount='$2,000.00'))])])])

        self.validate_and_return_populate_data(
            _DETAILS_HTML, expected_info)

    def test_populate_data_no_charges(self):
        expected_info = IngestInfo(people=[_Person(full_name='LAST, FIRST')])

        with pytest.warns(UserWarning):
            self.validate_and_return_populate_data(_NO_CHARGES_HTML,
                                                   expected_info)
