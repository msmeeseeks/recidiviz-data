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

"""Scraper implementation for us_ga_douglas."""
from recidiviz.common.constants.bond import BondType
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.ingest.scrape.vendors.zuercher import ZuercherScraper


class UsGaDouglasScraper(ZuercherScraper):
    """Scraper implementation for us_ga_douglas."""

    WARRANT_KEY = 'Warrant Charge:'
    UNSPECIFIED_WARRANT_KEY = 'Warrant:'

    def __init__(self):
        super(UsGaDouglasScraper, self).__init__(region_name='us_ga_douglas')

    @staticmethod
    def get_jurisdiction_name():
        return 'Douglas County, GA'

    def get_enum_overrides(self):
        return {
            **super(UsGaDouglasScraper, self).get_enum_overrides(),
            # Charge Statuses
            'NEG PLEA': ChargeStatus.PRETRIAL,

            # Bond Types
            'BONDING COMPANY': BondType.UNSECURED,
        }
