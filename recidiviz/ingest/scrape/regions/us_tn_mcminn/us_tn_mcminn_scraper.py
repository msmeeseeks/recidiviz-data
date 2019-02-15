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

"""Scraper implementation for us_tn_mcminn."""
from recidiviz.common.constants.charge import CourtType
from recidiviz.ingest.scrape.vendors.eagle_advantage import \
    EagleAdvantageScraper


class UsTnMcminnScraper(EagleAdvantageScraper):
    """Scraper implementation for us_tn_mcminn."""

    def __init__(self):
        super(UsTnMcminnScraper,
              self).__init__('us_tn_mcminn',
                             agency_service_ip='mcminnsheriff.serveftp.net',
                             agency_service_port=9000)

    def get_enum_overrides(self):
        overrides_builder = super(
            UsTnMcminnScraper, self).get_enum_overrides().to_builder()

        overrides_builder.add('GENERALSESSIONS', CourtType.DISTRICT)

        # JUVENILE seems to mostly refer to child support charges
        overrides_builder.add('JUVENILE', CourtType.DISTRICT)

        return overrides_builder.build()
