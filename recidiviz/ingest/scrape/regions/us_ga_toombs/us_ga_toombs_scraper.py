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

"""Scraper implementation for us_ga_toombs."""
from recidiviz.common.constants.bond import BondType
from recidiviz.ingest.scrape.vendors.zuercher import ZuercherScraper


class UsGaToombsScraper(ZuercherScraper):
    """Scraper implementation for us_ga_toombs."""

    WARRANT_CHARGE_KEY = 'New Charge:'
    PLAIN_WARRANT_KEY = 'Bench Warrant:'
    PAROLE_KEY = 'Parole Hold:'
    HOLD_KEY = 'Outside Agency'
    ADDITIONAL_HOLD_KEY = 'Out of State'
    FELONY_PROBATION_KEY = 'Fe Probation:'
    MISDEMEANOR_PROBATION_KEY = 'Mi Probation:'

    BONDSMAN_OFF_BOND_KEY = 'Off Bond'

    def __init__(self):
        super(UsGaToombsScraper, self).__init__(region_name='us_ga_toombs')

    @staticmethod
    def get_jurisdiction_name():
        return 'Toombs County, GA'

    def get_enum_overrides(self):
        return {
            **super(UsGaToombsScraper, self).get_enum_overrides(),

            # Bond Types
            'PROPERTY Bond': BondType.SECURED,
            'REVOKED BOND': BondType.NO_BOND,
            'CHILD SUPPORT RELEASE PAYMENT': BondType.CASH,
            'OR PER JUDGE': BondType.NO_BOND,
            'NA': None,
        }
