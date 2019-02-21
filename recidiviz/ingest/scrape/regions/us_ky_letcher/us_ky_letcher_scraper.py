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

"""Scraper implementation for us_ky_letcher."""
from recidiviz.ingest.scrape.vendors.bluhorse.bluhorse_scraper import \
    BluHorseScraper

class UsKyLetcherScraper(BluHorseScraper):
    """Scraper implementation for us_ky_letcher."""
    def __init__(self):
        super(UsKyLetcherScraper, self).__init__('us_ky_letcher')

    @staticmethod
    def get_jail_id() -> str:
        return 'LCRJ'

    @staticmethod
    def get_request_fields() -> str:
        return 'ACEFGHIJKLMNO'
