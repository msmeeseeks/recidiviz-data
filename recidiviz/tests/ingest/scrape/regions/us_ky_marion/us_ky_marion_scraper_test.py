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

"""Scraper tests for us_ky_marion."""
import unittest

from recidiviz.ingest.scrape.regions.us_ky_marion.us_ky_marion_scraper import \
    UsKyMarionScraper
from recidiviz.tests.ingest.scrape.vendors.jailtracker.jailtracker_scraper_test \
    import JailTrackerScraperTest


class TestUsKyMarionScraper(JailTrackerScraperTest, unittest.TestCase):
    def _get_scraper(self):
        return UsKyMarionScraper()
