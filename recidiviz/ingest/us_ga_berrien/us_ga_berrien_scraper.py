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

"""Scraper implementation for us_ga_berrien."""
from recidiviz.common.constants.charge import ChargeClass
from recidiviz.ingest.vendors.eagle_advantage.eagle_advantage_scraper import \
    EagleAdvantageScraper


class UsGaBerrienScraper(EagleAdvantageScraper):
    """Scraper implementation for us_ga_berrien."""

    def __init__(self):
        super(UsGaBerrienScraper,
              self).__init__('us_ga_berrien',
                             agency_service_ip='108.161.95.38',
                             agency_service_port=9000)

    def get_enum_overrides(self):
        return {'O': ChargeClass.OTHER}
