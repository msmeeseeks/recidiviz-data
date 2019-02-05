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

"""Scraper tests for us_tx_ochiltree."""
import unittest
from recidiviz.ingest.scrape.regions.us_tx_ochiltree.us_tx_ochiltree_scraper \
    import UsTxOchiltreeScraper
from recidiviz.tests.ingest.scrape.vendors.net_data import \
    NetDataScraperTest


class TestUsTxOchiltreeScraper(NetDataScraperTest, unittest.TestCase):

    def _init_scraper_and_yaml(self):
        # The scraper to be tested. Required.
        self.scraper = UsTxOchiltreeScraper()
