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


"""Scraper implementation for NC Guilford County (Superion)
"""

from recidiviz.common.constants.bond import BondType
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.ingest.vendors.superion.superion_scraper import SuperionScraper


class UsNcGuilfordScraper(SuperionScraper):
    """Scraper for people in Guilford County (NC) facilities."""
    def __init__(self):
        super(UsNcGuilfordScraper, self).__init__('us_nc_guilford')

    def get_enum_overrides(self):
        return {
            '': BondType.NO_BOND,
            'CUSTODY RELEASE': BondType.NO_BOND,
            'FEDERAL INMATE': None,  # TODO remove after #542 is addressed.
            'IMMIGRATION': None,  # TODO remove after #542 is addressed.
            'PAROLE HEARING PENDING': ChargeStatus.PENDING,
            'PRE-SENTENCED QUICK DIPS': ChargeStatus.SENTENCED,
            'RELEASE PER JUDGE': ChargeStatus.DROPPED,
            'SENTENCED WEEKENDER': ChargeStatus.SENTENCED,
            'WRITTEN PROMISE TO APPEAR': BondType.NO_BOND,
        }
