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

"""Scraper implementation for us_ky_mason."""

from typing import Optional

from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.ingest.scrape.vendors.jailtracker.jailtracker_scraper import \
    JailTrackerScraper
from recidiviz.common.constants.charge import ChargeClass
from recidiviz.common.constants.bond import BondType
from recidiviz.ingest.scrape.task_params import ScrapedData, Task
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.scrape import scraper_utils


class UsKyMasonScraper(JailTrackerScraper):
    """Scraper implementation for us_ky_mason."""
    def __init__(self):
        super(UsKyMasonScraper, self).__init__('us_ky_mason')

    def get_jailtracker_index(self):
        """Returns the index used in the JailTracker URL to request a specific
        region's landing page.

        A JailTracker landing page URL ends with: "/jailtracker/index/<INDEX>".
        This value can either be text or a number. In either case, this method
        should return the value as a string.
        """
        return 'MASON_COUNTY_KY'

    def populate_data(self, content, task: Task,
                      ingest_info: IngestInfo) -> Optional[ScrapedData]:
        scraped_data: Optional[ScrapedData] = super(
            UsKyMasonScraper, self).populate_data(content, task,
                                                  ingest_info)
        ingest_info = scraped_data.ingest_info if scraped_data else ingest_info
        person = scraper_utils.one('person', ingest_info)

        for booking in person.bookings:
            for charge in booking.charges:
                if charge.status and charge.status == 'WAIVED_EXTRADITION':
                    booking.create_hold()
                    charge.status = None

        return ScrapedData(ingest_info=ingest_info, persist=True)

    def get_enum_overrides(self) -> EnumOverrides:
        overrides_builder = super(UsKyMasonScraper,
                                  self).get_enum_overrides().to_builder()
        overrides_builder.add('V', ChargeClass.INFRACTION)
        # TODO(#806) Figure out what this stands for.
        overrides_builder.ignore('T')
        overrides_builder.add('10 CASH', BondType.PARTIAL_CASH)
        return overrides_builder.build()
