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

"""Scraper implementation for us_fl_osceola."""
import os
from recidiviz.ingest import constants
from recidiviz.ingest.base_scraper import BaseScraper
from recidiviz.ingest.extractor.html_data_extractor import HtmlDataExtractor

class UsFlOsceolaScraper(BaseScraper):
    """Scraper implementation for us_fl_osceola."""

    def __init__(self, mapping_filepath=None):
        if not mapping_filepath:
            mapping_filepath = os.path.join(
                os.path.dirname(__file__), 'us_fl_osceola.yaml')
        self.mapping_filepath = mapping_filepath

        super(UsFlOsceolaScraper, self).__init__('us_fl_osceola')

    def populate_data(self, content, params, ingest_info):

        # Modify table column title
        headers = content.xpath("//th[text()=\"Booking\"]")
        for header in headers:
            header.text = "Booking Column"

        data_extractor = HtmlDataExtractor(self.mapping_filepath)
        ingest_info = data_extractor.extract_and_populate_data(content,
                                                               ingest_info)

        if len(ingest_info.person) != 1:
            raise Exception("Expected exactly one person per page, "
                            "but got %i" % len(ingest_info.person))

        # Split statute and charge name
        for booking in ingest_info.person[0].booking:
            for charge in booking.charge:
                s = charge.statute.split(' - ')
                if len(s) == 2:
                    charge.statute = s[0]
                    charge.name = s[1]

        return ingest_info

    def get_more_tasks(self, content, params):
        task_type = params.get('task_type', self.get_initial_task_type())

        if self.is_initial_task(task_type):
            return [{
                'endpoint': self.get_region().base_url +
                            '/Apps/CorrectionsReports/Report/Search',
                'task_type': constants.GET_MORE_TASKS
            }]

        return self._get_detail_tasks(content)

    def _get_detail_tasks(self, content):
        tasks = []
        table = content.cssselect('table.tablesorter tbody')[0]
        for row in table:
            if len(row) > 1:
                tasks.append({
                    'endpoint': self.get_region().base_url +
                                row[1].find('a').get('href'),
                    'task_type': constants.SCRAPE_DATA
                })
        return tasks
