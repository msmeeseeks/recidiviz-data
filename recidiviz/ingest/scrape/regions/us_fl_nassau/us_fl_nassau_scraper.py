# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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

"""Scraper implementation for us_fl_nassau."""
from recidiviz.common.constants.bond import BondType
from recidiviz.common.constants.charge import ChargeClass
from recidiviz.ingest.scrape.vendors import NewWorldScraper


class UsFlNassauScraper(NewWorldScraper):
    """Scraper implementation for us_fl_nassau."""
    def __init__(self):
        super(UsFlNassauScraper, self).__init__('us_fl_nassau')

    def get_region_code(self) -> str:
        return 'nassau'

    def get_enum_overrides(self):
        overrides_builder = super(
            UsFlNassauScraper, self).get_enum_overrides().to_builder()

        overrides_builder.add('DELINQUENCY', ChargeClass.MISDEMEANOR)
        overrides_builder.add('MOVING VIOLATION', ChargeClass.MISDEMEANOR)
        overrides_builder.add('NO PC', BondType.CASH)
        overrides_builder.ignore('DROP NOTICE', BondType)

        return overrides_builder.build()
