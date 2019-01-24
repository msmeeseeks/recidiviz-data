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

"""Scraper implementation for us_pa_greene."""
from recidiviz.common.constants.bond import BondType
from recidiviz.common.constants.charge import ChargeClass
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.ingest.vendors.jailtracker.jailtracker_scraper import\
    JailTrackerScraper

class UsPaGreeneScraper(JailTrackerScraper):
    """Scraper for people in Greene County (PA) facilities."""
    def __init__(self):
        super(UsPaGreeneScraper, self).__init__('us_pa_greene')


    def get_jailtracker_index(self):
        """Returns the index used in the JailTracker URL to request a specific
        region's landing page.

        A JailTracker landing page URL ends with: "/jailtracker/index/<INDEX>".
        This value can either be text or a number. In either case, this method
        should return the value as a string.
        """
        return 'Greene_County_PA'

    def get_enum_overrides(self):
        return {
            # Charge Classes
            'TO BE DETERMINED': None,
            'SUMMARY': ChargeClass.INFRACTION,

            # Charge Statuses
            'SENTENCED BY COMMONWEALTH': ChargeStatus.SENTENCED,

            # Bond Types
            'CASH/SURETY': BondType.UNSECURED,
            'STRAIGHT': BondType.CASH,
            'NO BOND PERMITTED (UNBONDABLE CHARGE)': BondType.NO_BOND,
            'PERCENTAGE': BondType.CASH,
        }
