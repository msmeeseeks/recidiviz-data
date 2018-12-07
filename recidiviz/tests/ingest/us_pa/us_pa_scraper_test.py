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

from lxml import html
from recidiviz.ingest.models.ingest_info import IngestInfo, _Person, _Booking, \
    _Charge, _Sentence
from recidiviz.ingest.us_pa.us_pa_scraper import UsPaScraper
from recidiviz.tests.ingest import fixtures

_DETAILS_PAGE_HTML = html.fromstring(
    fixtures.as_string('us_pa', 'AA0000.json'))


class TestScraperDetailsPage(object):
    def test_parse(self):
        print _DETAILS_PAGE_HTML
        print _DETAILS_PAGE_HTML.text
        actual = UsPaScraper().populate_data(_DETAILS_PAGE_HTML, {},
                                             IngestInfo())

        expected = IngestInfo(people=[_Person(
            person_id="AA0000",
            given_names="FIRST",
            surname="LAST",
            birthdate="01/01/1000",
            race="WHITE",
            gender="MALE",
            bookings=[
                _Booking(
                    facility="CHESTER",
                    charges=[_Charge(sentence=_Sentence(
                        county_of_commitment="WASHINGTON"
                    ))]
                )
            ]
        ), ])

        assert actual == expected
