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
from recidiviz.ingest.scrape.vendors.jailtracker import\
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
        overrides_builder = super(
            UsPaGreeneScraper, self).get_enum_overrides().to_builder()

        overrides_builder.add('CASH SURETY', BondType.UNSECURED)
        overrides_builder.add(lambda s: s.startswith('NO BOND PERMITTED'),
                              BondType.NO_BOND)
        # If sentenced appears in bond type, we ignore it here but note
        # that we do not need to map it to a booking status because this region
        # will have booking status set in addition to the overloaded bond type
        # so we need only ignore it in the bond type.
        overrides_builder.add('NO BOND (SENTENCED)', BondType.NO_BOND)
        overrides_builder.add('PERCENTAGE', BondType.CASH)
        overrides_builder.add('COLLATERAL', BondType.CASH)
        overrides_builder.add('SENTENCED BY COMMONWEALTH',
                              ChargeStatus.SENTENCED)
        overrides_builder.add('STRAIGHT', BondType.CASH)
        overrides_builder.add('SUMMARY', ChargeClass.INFRACTION)
        overrides_builder.ignore('TO BE DETERMINED', ChargeClass)

        return overrides_builder.build()
