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

"""Scraper implementation for us_nc_buncombe."""

from recidiviz.common.constants.bond import BondType
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.ingest.scrape.vendors.superion.superion_scraper import \
    SuperionScraper


class UsNcBuncombeScraper(SuperionScraper):
    """Scraper implementation for us_nc_buncombe."""
    def __init__(self):
        super(UsNcBuncombeScraper, self).__init__('us_nc_buncombe')

    def get_enum_overrides(self):
        overrides_builder = super(
            UsNcBuncombeScraper, self).get_enum_overrides().to_builder()

        overrides_builder.add('DOM VIO', BondType.NO_BOND)
        overrides_builder.add('INCLUDED W OTHER', BondType.EXTERNAL_UNKNOWN)
        # Publicly drunk and held without court order could mean they have a
        # potentially pending charge.  Could possibly be no charge status?
        overrides_builder.add(
            'INEBRIATE HELD WITHOUT COURT ORDER', ChargeStatus.PENDING)
        overrides_builder.add('SECURED FTA PRIOR', BondType.NO_BOND)
        overrides_builder.ignore('OTHER', BondType)

        return overrides_builder.build()
