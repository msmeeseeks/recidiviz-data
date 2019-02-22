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

"""Scraper implementation for us_tn_carter."""

from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.ingest.scrape.vendors.jailtracker.jailtracker_scraper import \
    JailTrackerScraper
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.bond import BondType


class UsTnCarterScraper(JailTrackerScraper):
    """Scraper implementation for us_tn_carter."""
    def __init__(self):
        super(UsTnCarterScraper, self).__init__('us_tn_carter')


    def get_jailtracker_index(self):
        return 'Carter_County_TN'

    def get_enum_overrides(self) -> EnumOverrides:
        overrides_builder = super(UsTnCarterScraper,
                                  self).get_enum_overrides().to_builder()
        # Add mappings here by calling overrides_builder.add() or .ignore()
        overrides_builder.add('FINAL SENTENCED', ChargeStatus.SENTENCED)
        overrides_builder.add('STRAIGHT BOND', BondType.CASH)

        return overrides_builder.build()
