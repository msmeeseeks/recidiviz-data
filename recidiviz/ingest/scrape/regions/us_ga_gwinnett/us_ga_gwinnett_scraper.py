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

"""Scraper implementation for us_ga_gwinnett."""
from typing import Optional

from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.scrape.task_params import ScrapedData, Task
from recidiviz.ingest.scrape.vendors.smart_cop.smart_cop_scraper import \
    SmartCopScraper


class UsGaGwinnettScraper(SmartCopScraper):
    """Scraper implementation for us_ga_gwinnett."""

    def __init__(self):
        super(UsGaGwinnettScraper, self).__init__('us_ga_gwinnett')

    def populate_data(self, content, task: Task,
                      ingest_info: IngestInfo) -> Optional[ScrapedData]:
        scraped_data = super(UsGaGwinnettScraper,
                             self).populate_data(content, task, ingest_info)
        if not scraped_data:
            return None
        ingest_info = scraped_data.ingest_info

        # This website somewhat regularly puts arrest warrant codes in this
        # field (the columns just don't line up). We aren't scraping that
        # data, so scrub the field if it's filled incorrectly.
        for person in ingest_info.people:
            for booking in person.bookings:
                for charge in booking.charges:
                    bond = charge.bond
                    if charge.fee_dollars and \
                            not charge.fee_dollars.startswith('$'):
                        charge.fee_dollars = None
                        if bond and bond.bond_type:
                            booking.create_arrest(officer_name=bond.bond_type)
                            bond.bond_type = None
                    if bond and bond.amount == 'NO BOND':
                        bond.bond_type = bond.amount

        return ScrapedData(ingest_info)
