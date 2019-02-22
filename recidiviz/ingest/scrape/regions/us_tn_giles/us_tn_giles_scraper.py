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

"""Scraper implementation for us_tn_giles."""

from typing import Optional

from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.ingest.scrape.vendors.jailtracker.jailtracker_scraper import \
    JailTrackerScraper
from recidiviz.common.constants.bond import BondType
from recidiviz.ingest.scrape.task_params import ScrapedData, Task
from recidiviz.ingest.models.ingest_info import IngestInfo, Person
from recidiviz.ingest.scrape import scraper_utils


class UsTnGilesScraper(JailTrackerScraper):
    """Scraper implementation for us_tn_giles."""
    def __init__(self):
        super(UsTnGilesScraper, self).__init__('us_tn_giles')

    def get_jailtracker_index(self):
        """Returns the index used in the JailTracker URL to request a specific
        region's landing page.

        A JailTracker landing page URL ends with: "/jailtracker/index/<INDEX>".
        This value can either be text or a number. In either case, this method
        should return the value as a string.
        """
        return 'GILES_COUNTY_TN'

    def populate_data(self, content, task: Task,
                      ingest_info: IngestInfo) -> Optional[ScrapedData]:
        scraped_data: Optional[ScrapedData] = super(
            UsTnGilesScraper, self).populate_data(content, task, ingest_info)

        person = scraper_utils.one('person', scraped_data.ingest_info) \
        if scraped_data else Person()

        for booking in person.bookings:
            for charge in booking.charges:
                if not charge.bond:
                    continue
                if charge.bond.bond_type == '0':
                    if charge.bond.amount == '0.0':
                        charge.bond.bond_type = BondType.NO_BOND.value
        return scraped_data

    def get_enum_overrides(self) -> EnumOverrides:
        overrides_builder = super(UsTnGilesScraper,
                                  self).get_enum_overrides().to_builder()
        # Add mappings here by calling overrides_builder.add() or .ignore()
        return overrides_builder.build()
