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

"""Scraper implementation for us_tx_wichita."""
from recidiviz.ingest.vendors.justice_solutions.justice_solutions_scraper \
    import JusticeSolutionsScraper


class UsTxWichitaScraper(JusticeSolutionsScraper):
    """Scraper implementation for us_tx_wichita."""
    def __init__(self):
        super(UsTxWichitaScraper, self).__init__('us_tx_wichita')

    def get_devpath(self) -> str:
        return '/GSASYS/CLIENTS/WICHITA.WEB.CRIME'
