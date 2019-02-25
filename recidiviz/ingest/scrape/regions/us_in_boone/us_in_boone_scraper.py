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

"""Scraper implementation for us_in_boone."""
from typing import Optional

from recidiviz.common.constants.bond import BondType
from recidiviz.common.constants.charge import ChargeClass
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.charge import CourtType
from recidiviz.ingest.scrape import scraper_utils
from recidiviz.ingest.models.ingest_info import IngestInfo, Person
from recidiviz.ingest.scrape.task_params import ScrapedData, Task
from recidiviz.ingest.scrape.vendors.jailtracker import JailTrackerScraper



class UsInBooneScraper(JailTrackerScraper):
    """Scraper implementation for us_in_boone."""
    def __init__(self):
        super(UsInBooneScraper, self).__init__('us_in_boone')

    def get_jailtracker_index(self):
        """Returns the index used in the JailTracker URL to request a specific
        region's landing page.

        A JailTracker landing page URL ends with: "/jailtracker/index/<INDEX>".
        This value can either be text or a number. In either case, this method
        should return the value as a string.
        """
        return 'Boone_County_IN'

    def populate_data(self, content, task: Task,
                      ingest_info: IngestInfo) -> Optional[ScrapedData]:
        scraped_data: Optional[ScrapedData] = super(
            UsInBooneScraper, self).populate_data(content, task, ingest_info)

        person = scraper_utils.one('person', scraped_data.ingest_info) \
            if scraped_data else Person()

        for booking in person.bookings:
            for charge in booking.charges:
                if charge.status:
                    if charge.status.upper().startswith('HOLD FOR OTHER'):
                        booking.create_hold(jurisdiction_name=charge.name)
                        charge.status = None
                    elif charge.status.upper() == \
                         'RELEASED ON OWN RECOGNIZANCE':
                        charge.status = None
                        if not charge.bond:
                            charge.create_bond()
                        charge.bond.bond_type = BondType.NO_BOND.value

                if charge.charge_class and charge.charge_class.isnumeric():
                    charge.level = charge.charge_class
                    charge.charge_class = ChargeClass.FELONY.value

        return scraped_data

    def get_enum_overrides(self):
        """Returns region-specific enumeration overrides."""
        overrides_builder = super(
            UsInBooneScraper, self).get_enum_overrides().to_builder()

        # Court types
        overrides_builder.add('BOONE CIRCUIT COURT', CourtType.CIRCUIT)
        overrides_builder.add(lambda s: s.startswith('BOONE SUPERIOR'),
                              CourtType.SUPERIOR)
        overrides_builder.ignore('COURT TYPE', CourtType)

        # Charge statuses
        overrides_builder.add('DOC SENTENCED', ChargeStatus.SENTENCED)
        overrides_builder.add('INITIAL HEARING HELD', ChargeStatus.PRETRIAL)
        overrides_builder.add('PROBABLE CAUSE VERIFIED', ChargeStatus.PENDING)
        overrides_builder.ignore('COMMITMENT', ChargeStatus)
        overrides_builder.ignore('NEW CASE', ChargeStatus)

        # Charge classes
        overrides_builder.add('X', ChargeClass.FELONY)
        overrides_builder.ignore('A', ChargeClass)
        overrides_builder.ignore('C', ChargeClass)

        return overrides_builder.build()
