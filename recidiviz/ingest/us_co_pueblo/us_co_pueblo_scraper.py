# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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


"""Scraper implementation for CO Mesa County

Region-specific notes:

Background scraping procedure:
    1. A starting home page GET
    2. A details page for a person's current booking
"""

import lxml.html as html
from recidiviz.ingest.generic_scraper import GenericScraper
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest import constants


class UsCoPuebloScraper(GenericScraper):
    """Scraper Pueblo County, Colorado jails."""
    def __init__(self):
        super(UsCoPuebloScraper, self).__init__('us_co_pueblo')

    def set_initial_vars(self, content, params):
        pass

    def get_more_tasks(self, content, params):
        task_type = params.get('task_type', self.get_initial_task_type())
        params_list = []

        if self.is_initial_task(task_type):
            params_list.extend(UsCoPuebloScraper._get_person_params(content))
        if self.should_scrape_person(task_type):
            ingest_info = UsCoPuebloScraper._scrape_content(content)
        return params_list

    @staticmethod
    def _get_person_params(content):
        doc = html.fromstring(content)
        params_list = []
        for link in doc.xpath('//a[text()=" (Click to View Details) "]/@href'):
            params_list.extend({
               'endpoint': link,
               'task_type': constants.SCRAPE_PERSON_AND_RECORD_AND_MORE,
             })
        return params_list

    @staticmethod
    def _scrape_content(content):
        return IngestInfo()

    def person_id_is_fuzzy(self):
        return True
