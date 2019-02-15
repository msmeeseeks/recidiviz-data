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

    WARRANT_CHARGE_KEY = 'Warrant:'
    PLAIN_WARRANT_KEY = 'Plain Warrant:'

    def __init__(self):
        super(UsGaFloydScraper, self).__init__(region_name='us_ga_floyd')

    @staticmethod
    def get_jurisdiction_name():
        return 'Floyd County, GA'

    def get_enum_overrides(self):
        overrides_builder = super(
            UsGaFloydScraper, self).get_enum_overrides().to_builder()

        overrides_builder.add('DISMISSED CCH 305', ChargeStatus.DROPPED)
        overrides_builder.add('FEL', ChargeClass.FELONY)
        overrides_builder.add('GUILTY CCH 310', ChargeStatus.CONVICTED)
        overrides_builder.add('MISD', ChargeClass.MISDEMEANOR)
        overrides_builder.add('NOLLE PROSSED CCH 300', ChargeStatus.DROPPED)
        overrides_builder.add('PROPERTY', BondType.SECURED)

        return overrides_builder.build()
