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

"""Scraper implementation for us_ga_coweta."""
from recidiviz.common.constants.bond import BondType
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.person import Race
from recidiviz.ingest.scrape.vendors.superion.superion_scraper \
    import SuperionScraper


class UsGaCowetaScraper(SuperionScraper):
    """Scraper implementation for us_ga_coweta."""
    def __init__(self):
        super(UsGaCowetaScraper, self).__init__('us_ga_coweta')

    def get_enum_overrides(self):
        overrides_builder = super(
            UsGaCowetaScraper, self).get_enum_overrides().to_builder()

        overrides_builder.add('WHITE OR HISPANIC', Race.WHITE)
        overrides_builder.add('NOL PROC D', ChargeStatus.DROPPED)
        overrides_builder.add('PROFFESSIONAL BONDING CO', BondType.SECURED)
        overrides_builder.add('REBOOK', ChargeStatus.PRETRIAL)
        overrides_builder.ignore('RELEASE PER PROBATION', ChargeStatus)

        return overrides_builder.build()
