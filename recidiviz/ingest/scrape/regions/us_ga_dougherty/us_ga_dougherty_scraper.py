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

"""Scraper implementation for us_ga_dougherty."""
from recidiviz.common.constants.bond import BondType
from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.ingest.scrape.vendors.superion.superion_scraper \
    import SuperionScraper


class UsGaDoughertyScraper(SuperionScraper):
    """Scraper implementation for us_ga_dougherty."""
    def __init__(self):
        super(UsGaDoughertyScraper, self).__init__('us_ga_dougherty')

    def get_enum_overrides(self):
        overrides = super(UsGaDoughertyScraper, self).get_enum_overrides()
        overrides_builder = overrides.to_builder()

        overrides_builder.add(
            lambda x: not BondType.can_parse(
                x, EnumOverrides.empty()) and x.strip().endswith('BOND'),
            BondType.SECURED)

        return overrides_builder.build()
