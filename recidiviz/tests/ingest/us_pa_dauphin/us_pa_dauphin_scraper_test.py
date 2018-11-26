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
"""Scraper tests for us_pa_dauphin."""

from lxml import html
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.us_pa_dauphin.us_pa_dauphin_scraper import \
    UsPaDauphinScraper
from recidiviz.tests.ingest import fixtures

_FRONT_PAGE_HTML = html.fromstring(
    fixtures.as_string('us_pa_dauphin', 'website.html'))


class TestScraperFrontPage(object):
    """Scraper tests for us_pa_dauphin."""

    def setup_method(self, _test_method):
        self.subject = UsPaDauphinScraper()

    def test_website(self):
        result = self.subject.populate_data(_FRONT_PAGE_HTML, None, None)

        expected_result = IngestInfo()
        expected_result.create_person(
            surname="FIRST_A           ,LAST_A     MIDDLE_A      ")
        expected_result.create_person(
            surname="FIRST_B                ,LAST_B     MIDDLE_B      ")
        expected_result.create_person(
            surname="FIRST_C            ,LAST_C     MIDDLE_C         ")

        assert result == expected_result
