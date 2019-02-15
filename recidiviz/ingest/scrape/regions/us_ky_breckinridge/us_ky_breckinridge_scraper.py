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

"""Scraper implementation for us_ky_breckinridge."""

from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.ingest.scrape.vendors.jailtracker.jailtracker_scraper import \
    JailTrackerScraper


class UsKyBreckinridgeScraper(JailTrackerScraper):
    """Scraper implementation for us_ky_breckinridge."""
    def __init__(self):
        super(UsKyBreckinridgeScraper, self).__init__('us_ky_breckinridge')

    def get_jailtracker_index(self):
        """Returns the index used in the JailTracker URL to request a specific
        region's landing page.

        A JailTracker landing page URL ends with: "/jailtracker/index/<INDEX>".
        This value can either be text or a number. In either case, this method
        should return the value as a string.
        """
        return 'BRECKINRIDGE_COUNTY_KY'

    def get_enum_overrides(self) -> EnumOverrides:
        overrides_builder = super(UsKyBreckinridgeScraper,
                                  self).get_enum_overrides().to_builder()
        return overrides_builder.build()
