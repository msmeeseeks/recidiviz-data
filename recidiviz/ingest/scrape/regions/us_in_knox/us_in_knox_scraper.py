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

"""Scraper implementation for us_in_knox."""
from recidiviz.common.constants.charge import CourtType
from recidiviz.ingest.scrape.vendors.jailtracker.jailtracker_scraper import \
    JailTrackerScraper



class UsInKnoxScraper(JailTrackerScraper):
    """Scraper implementation for us_in_knox."""
    def __init__(self):
        super(UsInKnoxScraper, self).__init__('us_in_knox')


    def get_jailtracker_index(self):
        """Returns the index used in the JailTracker URL to request a specific
        region's landing page.

        A JailTracker landing page URL ends with: "/jailtracker/index/<INDEX>".
        This value can either be text or a number. In either case, this method
        should return the value as a string.
        """
        return 'Knox_County_IN'


    def get_enum_overrides(self):
        """Returns region-specific enumeration overrides."""
        overrides_builder = super(UsInKnoxScraper,
                                  self).get_enum_overrides().to_builder()

        overrides_builder.add(lambda s: s.startswith('SUPERIOR'),
                              CourtType.SUPERIOR)
        return overrides_builder.build()
