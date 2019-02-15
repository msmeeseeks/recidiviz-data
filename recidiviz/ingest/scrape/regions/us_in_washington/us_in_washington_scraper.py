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

"""Scraper implementation for us_in_washington."""
from typing import Optional

from recidiviz.common.constants.bond import BondType
from recidiviz.common.constants.charge import ChargeClass
from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.ingest.models.ingest_info import IngestInfo, Person
from recidiviz.ingest.scrape import scraper_utils
from recidiviz.ingest.scrape.task_params import ScrapedData, Task
from recidiviz.ingest.scrape.vendors.jailtracker.jailtracker_scraper import \
    JailTrackerScraper


class UsInWashingtonScraper(JailTrackerScraper):
    """Scraper implementation for us_in_washington."""

    def __init__(self):
        super(UsInWashingtonScraper, self).__init__('us_in_washington')

    def get_jailtracker_index(self):
        """Returns the index used in the JailTracker URL to request a specific
        region's landing page.

        A JailTracker landing page URL ends with: "/jailtracker/index/<INDEX>".
        This value can either be text or a number. In either case, this method
        should return the value as a string.
        """
        return 'Washington_County_IN'

    def populate_data(self, content, task: Task,
                      ingest_info: IngestInfo) -> Optional[ScrapedData]:
        scraped_data: Optional[ScrapedData] = super(
            UsInWashingtonScraper, self).populate_data(content, task,
                                                       ingest_info)

        person = scraper_utils.one('person', scraped_data.ingest_info) \
            if scraped_data else Person()

        for booking in person.bookings:
            for charge in booking.charges:
                if charge.charge_class and charge.charge_class == '6':
                    charge.charge_class = ChargeClass.FELONY.value
                    charge.level = '6'

        return scraped_data

    def get_enum_overrides(self) -> EnumOverrides:
        overrides_builder = super(UsInWashingtonScraper,
                                  self).get_enum_overrides().to_builder()
        overrides_builder.add('A', ChargeClass.MISDEMEANOR)
        overrides_builder.ignore('SERVE TIME', BondType)
        overrides_builder.ignore('X', ChargeClass)
        return overrides_builder.build()
