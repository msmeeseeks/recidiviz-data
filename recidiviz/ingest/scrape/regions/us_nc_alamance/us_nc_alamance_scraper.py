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

"""Scraper implementation for us_nc_alamance."""
from recidiviz.common.constants.bond import BondType
from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.ingest.scrape.vendors.superion.superion_scraper import \
    SuperionScraper


class UsNcAlamanceScraper(SuperionScraper):
    """Scraper implementation for us_nc_alamance."""
    def __init__(self):
        super(UsNcAlamanceScraper, self).__init__('us_nc_alamance')

    def get_enum_overrides(self) -> EnumOverrides:
        overrides_builder = super(UsNcAlamanceScraper,
                                  self).get_enum_overrides().to_builder()
        # Is this correct?
        overrides_builder.ignore('OTHER CUST ORDER', BondType)
        return overrides_builder.build()
