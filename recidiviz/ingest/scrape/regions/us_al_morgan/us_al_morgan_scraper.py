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

"""Scraper implementation for us_al_morgan."""
from typing import Optional

from recidiviz.common.constants.bond import BondType
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.ingest.scrape import scraper_utils
from recidiviz.ingest.models.ingest_info import IngestInfo, Person
from recidiviz.ingest.scrape.task_params import ScrapedData, Task
from recidiviz.ingest.scrape.vendors.superion.superion_scraper import \
    SuperionScraper


class UsAlMorganScraper(SuperionScraper):
    """Scraper implementation for us_al_morgan."""
    def __init__(self):
        super(UsAlMorganScraper, self).__init__('us_al_morgan')

    def populate_data(self, content, task: Task,
                      ingest_info: IngestInfo) -> Optional[ScrapedData]:
        scraped_data: Optional[ScrapedData] = super(
            UsAlMorganScraper, self).populate_data(content, task, ingest_info)

        person = scraper_utils.one('person', scraped_data.ingest_info) \
            if scraped_data else Person()

        for booking in person.bookings:
            for charge in booking.charges:
                if not charge.bond:
                    continue
                # TODO (#816) use enum overrides to set the status without code
                if charge.bond.bond_type and \
                   charge.bond.bond_type.strip() == 'BOND REVOKED':
                    charge.bond.status = charge.bond.bond_type
                    charge.bond.bond_type = None

        return scraped_data

    def get_enum_overrides(self):
        overrides_builder = super(
            UsAlMorganScraper, self).get_enum_overrides().to_builder()

        overrides_builder.add('APPROVE TO SIGN ON BOND', BondType.SECURED)
        overrides_builder.add('CHARGE COMBINED WITH OTHERS',
                              ChargeStatus.DROPPED)
        overrides_builder.add('COMBINED BONDS TO 1BOND', BondType.NO_BOND)
        overrides_builder.ignore('CONDITIONAL CHECK JUDGES ORDER', BondType)
        overrides_builder.ignore('ENTRY ERROR', ChargeStatus)
        overrides_builder.add('MUNICIPAL BOND', BondType.CASH)
        overrides_builder.add('MUNICIPAL COURT', ChargeStatus.PRETRIAL)
        overrides_builder.add('NO BILLED', BondType.UNSECURED)
        overrides_builder.add('SETTLED', ChargeStatus.DROPPED)
        overrides_builder.add('TRANSIT', ChargeStatus.SENTENCED)
        overrides_builder.add('WARRANT RECALLED', ChargeStatus.PRETRIAL)

        return overrides_builder.build()
