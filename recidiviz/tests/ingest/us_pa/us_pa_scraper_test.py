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
"""Scraper tests for us_pa."""

import unittest
from recidiviz.ingest.models.ingest_info import IngestInfo, _Person, _Booking, \
    _Charge, _Sentence
from recidiviz.ingest.us_pa.us_pa_scraper import UsPaScraper
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils.base_scraper_test import BaseScraperTest

_DETAILS_PAGE_HTML = fixtures.as_dict('us_pa', 'AA0000.json')


class TestScraperDetailsPage(BaseScraperTest, unittest.TestCase):

    def _init_scraper_and_yaml(self):
        self.scraper = UsPaScraper()
        self.yaml = None

    def test_parse(self):

        expected_info = IngestInfo(people=[_Person(
            person_id="AA0000",
            birthdate="01/01/1000",
            race="WHITE",
            gender="MALE",
            bookings=[
                _Booking(
                    facility="CHESTER",
                    charges=[_Charge(sentence=_Sentence(
                        sentencing_region="WASHINGTON"
                    ))]
                )
            ]
        ), ])

        self.validate_and_return_populate_data(_DETAILS_PAGE_HTML,
                                               expected_info)
