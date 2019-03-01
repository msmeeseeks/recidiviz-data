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

"""Scraper implementation for us_ga_columbia."""
from typing import Optional

from recidiviz.common.constants.bond import BondStatus
from recidiviz.common.constants.bond import BondType
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.scrape.task_params import ScrapedData, Task
from recidiviz.ingest.scrape.vendors.superion.superion_scraper \
    import SuperionScraper


class UsGaColumbiaScraper(SuperionScraper):
    """Scraper implementation for us_ga_columbia."""
    def __init__(self):
        super(UsGaColumbiaScraper, self).__init__('us_ga_columbia')

    def populate_data(self, content, task: Task,
                      ingest_info: IngestInfo) -> Optional[ScrapedData]:
        scraped_data: Optional[ScrapedData] = super(
            UsGaColumbiaScraper, self).populate_data(content, task,
                                                     ingest_info)

        for charge in ingest_info.get_all_charges():
            if charge.status and 'BOND REVOKED' in charge.status:
                bond = charge.bond or charge.create_bond()
                charge.status = ChargeStatus.PRETRIAL.value
                bond.status = BondStatus.REVOKED.value

        return scraped_data

    def get_enum_overrides(self):
        overrides_builder = super(
            UsGaColumbiaScraper, self).get_enum_overrides().to_builder()

        overrides_builder.add('BONDING COMAPNY', BondType.SECURED)
        overrides_builder.add('PAYMENT', BondType.CASH)

        return overrides_builder.build()
