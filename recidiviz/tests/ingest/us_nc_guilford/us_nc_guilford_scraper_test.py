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

"""Scraper tests for us_nc_guilford."""
import unittest

from recidiviz.ingest.us_nc_guilford.us_nc_guilford_scraper import \
    UsNcGuilfordScraper
from recidiviz.tests.ingest.vendors.superion.superion_scraper_test import \
    SuperionScraperTest

class TestUsNcGuilfordScraper(SuperionScraperTest, unittest.TestCase):
    def _get_scraper(self):
        return UsNcGuilfordScraper()
