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

"""Scraper implementation for us_ga_floyd."""
from recidiviz.common.constants.bond import BondType
from recidiviz.common.constants.charge import ChargeClass, ChargeStatus
from recidiviz.ingest.scrape.vendors.zuercher import ZuercherScraper


class UsGaFloydScraper(ZuercherScraper):
    """Scraper implementation for us_ga_floyd."""

    def __init__(self):
        super(UsGaFloydScraper, self).__init__(region_name='us_ga_floyd')

    @staticmethod
    def get_jurisdiction_name():
        return 'Floyd County, GA'

    def get_enum_overrides(self):
        return {
            **super(UsGaFloydScraper, self).get_enum_overrides(),
            # Charge Status
            'NOLLE PROSSED (CCH-300)': ChargeStatus.DROPPED,
            'DISMISSED (CCH-305)': ChargeStatus.DROPPED,
            'GUILTY (CCH-310)': ChargeStatus.CONVICTED,

            # Charge Classes
            'FEL': ChargeClass.FELONY,
            'FEL.': ChargeClass.FELONY,
            'MISD.': ChargeClass.MISDEMEANOR,

            # Bond Types
            'PROPERTY': BondType.SECURED,
        }
