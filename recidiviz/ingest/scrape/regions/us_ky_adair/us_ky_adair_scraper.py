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

"""Scraper implementation for us_ky_adair."""
from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.common.constants.charge import ChargeClass, ChargeStatus
from recidiviz.ingest.scrape.vendors.jailtracker.jailtracker_scraper import \
    JailTrackerScraper


class UsKyAdairScraper(JailTrackerScraper):
    """Scraper implementation for us_ky_adair."""
    def __init__(self):
        super(UsKyAdairScraper, self).__init__('us_ky_adair')

    def get_jailtracker_index(self):
        """Returns the index used in the JailTracker URL to request a specific
        region's landing page.

        A JailTracker landing page URL ends with: "/jailtracker/index/<INDEX>".
        This value can either be text or a number. In either case, this method
        should return the value as a string.
        """
        return 'Adair_County_RJ_KY'

    def get_enum_overrides(self) -> EnumOverrides:
        """Returns region-specific enumeration overrides."""
        overrides_builder = super(
            UsKyAdairScraper, self).get_enum_overrides().to_builder()

        # I believe this is a typo for what should be Class 'B'
        overrides_builder.add('V', ChargeClass.MISDEMEANOR)

        # This stands for Technical Violation eg. either Parole or Probation
        overrides_builder.add('T', ChargeClass.OTHER)

        # Charge Status
        overrides_builder.add('SERVE SENTENCE', ChargeStatus.SENTENCED)
        return overrides_builder.build()
        