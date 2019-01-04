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

"""Tests for ingest/models/scrape_key.py."""

import unittest

from recidiviz.ingest import constants
from recidiviz.ingest.models.scrape_key import ScrapeKey


class TestScrapeKey(unittest.TestCase):
    """Tests for ingest/models/scrape_key.py."""

    def test_eq_different_regions(self):
        left = ScrapeKey("us_ny", constants.BACKGROUND_SCRAPE)
        right = ScrapeKey("us_fl", constants.BACKGROUND_SCRAPE)

        assert left != right


    def test_eq_different_types(self):
        left = ScrapeKey("us_ny", constants.BACKGROUND_SCRAPE)
        right = ScrapeKey("us_ny", constants.SNAPSHOT_SCRAPE)

        assert left != right


    def test_eq_different_everything(self):
        left = ScrapeKey("us_ny", constants.SNAPSHOT_SCRAPE)
        right = ScrapeKey("us_fl", constants.BACKGROUND_SCRAPE)

        assert left != right


    def test_eq_same(self):
        left = ScrapeKey("us_ny", constants.BACKGROUND_SCRAPE)
        right = ScrapeKey("us_ny", constants.BACKGROUND_SCRAPE)

        assert left == right


    def test_eq_different_objects(self):
        left = ScrapeKey("us_ny", constants.BACKGROUND_SCRAPE)
        right = "We don't read the papers, we don't read the news"

        assert not left.__eq__(right)


    def test_repr(self):
        scrape_key = ScrapeKey("us_ut", constants.SNAPSHOT_SCRAPE)

        representation = scrape_key.__repr__()

        assert representation == "<ScrapeKey region_code: us_ut, " \
                                 "scrape_type: snapshot>"


    def test_no_region(self):
        expected_error_message = 'A scrape key must include both a region ' \
                                       'code and a scrape type'
        with self.assertRaisesRegex(ValueError, expected_error_message):
            ScrapeKey(None, constants.SNAPSHOT_SCRAPE)


    def test_no_scrape_type(self):
        expected_error_message = 'A scrape key must include both a region ' \
                                       'code and a scrape type'
        with self.assertRaisesRegex(ValueError, expected_error_message):
            ScrapeKey("us_ut", None)
