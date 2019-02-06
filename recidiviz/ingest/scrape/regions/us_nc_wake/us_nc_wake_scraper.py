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

"""Scraper implementation for us_nc_wake."""
from typing import Optional

from recidiviz.common.constants.bond import BondStatus
from recidiviz.common.constants.bond import BondType
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.ingest.scrape import scraper_utils
from recidiviz.ingest.models.ingest_info import IngestInfo, Person
from recidiviz.ingest.scrape.task_params import ScrapedData, Task
from recidiviz.ingest.scrape.vendors.superion.superion_scraper import \
    SuperionScraper


class UsNcWakeScraper(SuperionScraper):
    """Scraper implementation for us_nc_wake."""
    def __init__(self):
        super(UsNcWakeScraper, self).__init__('us_nc_wake')

    def populate_data(self, content, task: Task,
                      ingest_info: IngestInfo) -> Optional[ScrapedData]:
        scraped_data: Optional[ScrapedData] = super(
            UsNcWakeScraper, self).populate_data(content, task, ingest_info)

        person = scraper_utils.one('person', scraped_data.ingest_info) \
            if scraped_data else Person()

        for booking in person.bookings:
            for charge in booking.charges:
                if not charge.bond:
                    continue
                if charge.bond.bond_type.startswith('SENTENCE'):
                    charge.bond.bond_type = BondType.NO_BOND.value
                    charge.status = ChargeStatus.SENTENCED.value
                if charge.bond.bond_type.startswith('PENDING'):
                    charge.bond.bond_type = None
                    charge.bond.status = BondStatus.PENDING.value

        return scraped_data

    def get_enum_overrides(self):
        return {
            'DISMISSED 234': BondType.NO_BOND,
            'ELECTRONIC HOUSE ARREST - SECURED BOND': BondType.SECURED,
            'SECURE BOND - 2ND OR SUBSEQUENT FTA ON THIS CASE':
                BondType.SECURED,
        }
