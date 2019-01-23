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

"""Scraper implementation for us_ms_desoto."""
from recidiviz.ingest.vendors.dcn.dcn_scraper import DcnScraper


class UsMsDesotoScraper(DcnScraper):
    """Scraper for people in Desoto county MS."""

    def __init__(self):
        super(UsMsDesotoScraper, self).__init__('us_ms_desoto')

    def get_base_endpoint_details(self):
        """Returns the base endpoint for the details page of a person"""
        return 'https://jail.desotosheriff.org/DCN/inmate-details'
