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

"""Scraper implementation for us_mo_cape_girardeau."""

from recidiviz.ingest.vendors.brooks_jeffrey.brooks_jeffrey_scraper import \
    BrooksJeffreyScraper


class UsMoCapeGirardeauScraper(BrooksJeffreyScraper):
    """Scraper implementation for us_mo_cape_girardeau."""
    def __init__(self, mapping_filepath=None):
        super(UsMoCapeGirardeauScraper, self).__init__('us_mo_cape_girardeau')
