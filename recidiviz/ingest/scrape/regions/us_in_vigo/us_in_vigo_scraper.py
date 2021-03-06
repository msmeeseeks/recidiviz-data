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

"""Scraper implementation for us_in_vigo."""
from typing import Optional

from recidiviz.common.constants.bond import BondType
from recidiviz.common.constants.charge import ChargeClass
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.charge import CourtType
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.scrape.task_params import ScrapedData, Task
from recidiviz.ingest.scrape.vendors.jailtracker import \
    JailTrackerScraper


class UsInVigoScraper(JailTrackerScraper):
    """Scraper implementation for us_in_vigo."""

    def __init__(self, ):
        super(UsInVigoScraper, self).__init__('us_in_vigo')

    def get_jailtracker_index(self):
        return 'Vigo_County_IN'

    # pylint:disable=arguments-differ
    def populate_data(self, content, task: Task,
                      existing_ingest_info:
                      IngestInfo) -> Optional[ScrapedData]:
        scraped_data: Optional[ScrapedData] = super(
            UsInVigoScraper, self).populate_data(content, task,
                                                 existing_ingest_info)

        if not scraped_data:
            raise ValueError('No scraped data populated as we expected '
                             'it would be.')

        if not scraped_data.ingest_info:
            raise ValueError('No ingest info populated as we expected '
                             'it would be')

        # Manual charge info manipulations.
        for booking in scraped_data.ingest_info.get_all_bookings():
            for charge in booking.charges:
                if charge.court_type:
                    # Chop 'DIVISION <N> ' off the court type string
                    if charge.court_type.upper().startswith('DIVISION'):
                        charge.court_type = ' '.join(
                            charge.court_type.split()[2:])

                    # Look for holds in court type
                    court_hold_keys = [
                        'DEPARTMENT OF CORRECTIONS',
                        'INDIANA OUT OF COUNTY',
                        'OUT OF STATE COURTS',
                    ]
                    if charge.court_type in court_hold_keys:
                        booking.create_hold(jurisdiction_name=charge.court_type)
                        charge.court_type = None

                # Look for level of charges in charge_class
                if charge.charge_class and charge.charge_class.isnumeric():
                    charge.level = charge.charge_class
                    charge.charge_class = ChargeClass.FELONY.value
                elif charge.charge_class in ['A', 'B', 'C']:
                    charge.level = charge.charge_class
                    charge.charge_class = ChargeClass.MISDEMEANOR.value

        return ScrapedData(ingest_info=scraped_data.ingest_info, persist=True)

    def get_enum_overrides(self):
        overrides_builder = super(
            UsInVigoScraper, self).get_enum_overrides().to_builder()

        overrides_builder.add('*', ChargeStatus)
        overrides_builder.add('.', ChargeStatus)
        overrides_builder.ignore('BAIL CONSOLIDATED TO ONE CHARGE',
                                 BondType)
        overrides_builder.add('BOND SET', ChargeStatus.PRETRIAL)
        overrides_builder.add('CASH ONLY NO 10 NO PROFESSIONAL BONDSMAN',
                              BondType.CASH)
        overrides_builder.add('COUNTY COURT', CourtType.DISTRICT)
        overrides_builder.add('COURT TYPE', CourtType.DISTRICT)
        overrides_builder.add('GENERAL', ChargeStatus)
        overrides_builder.add('IV D SUPPORT COURT', CourtType.CIVIL)
        overrides_builder.add('OTHER NOT CLASSIFIED',
                              CourtType.EXTERNAL_UNKNOWN)
        overrides_builder.add('PAROLE VIOLATION STATE ONLY', ChargeStatus)
        overrides_builder.add('RELEASED BY COURT', ChargeStatus.PRETRIAL)
        overrides_builder.add('SC', ChargeClass.CIVIL)
        overrides_builder.add('TERRE HAUTE CITY COURT', CourtType.DISTRICT)
        overrides_builder.add('WITH 10 ALLOWED', BondType.CASH)

        return overrides_builder.build()
