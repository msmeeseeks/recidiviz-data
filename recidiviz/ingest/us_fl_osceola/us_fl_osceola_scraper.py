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
from typing import List, Optional

from recidiviz.ingest import constants
from recidiviz.ingest.base_scraper import BaseScraper
from recidiviz.ingest.extractor.html_data_extractor import HtmlDataExtractor
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.task_params import Task


class UsFlOsceolaScraper(BaseScraper):
    """Scraper implementation for us_fl_osceola."""

    def __init__(self, mapping_filepath=None):
        if not mapping_filepath:
            mapping_filepath = os.path.join(
                os.path.dirname(__file__), 'us_fl_osceola.yaml')
        self.mapping_filepath = mapping_filepath

        super(UsFlOsceolaScraper, self).__init__('us_fl_osceola')

    def populate_data(self, content, task: Task,
                      ingest_info: IngestInfo) -> Optional[IngestInfo]:

        # Modify table column title
        headers = content.xpath("//th[text()=\"Booking\"]")
        for header in headers:
            header.text = "Booking Column"

        data_extractor = HtmlDataExtractor(self.mapping_filepath)
        ingest_info = data_extractor.extract_and_populate_data(content,
                                                               ingest_info)

        if len(ingest_info.people) != 1:
            raise Exception("Expected exactly one person per page, "
                            "but got %i" % len(ingest_info.people))

        # Split statute and charge name
        for booking in ingest_info.people[0].bookings:
            for charge in booking.charges:
                if charge.statute:
                    s = charge.statute.split(' - ')
                    if len(s) == 2:
                        charge.statute = s[0]
                        charge.name = s[1]

        return ingest_info

    def get_more_tasks(self, content, task: Task) -> List[Task]:
        if self.is_initial_task(task.task_type):
            return [Task(
                endpoint=self.get_region().base_url +
                '/Apps/CorrectionsReports/Report/Search',
                task_type=constants.TaskType.GET_MORE_TASKS
            )]

        return self._get_detail_tasks(content)

    def _get_detail_tasks(self, content) -> List[Task]:
        tasks = []
        table = content.cssselect('table.tablesorter tbody')[0]
        for row in table:
            if len(row) > 1:
                tasks.append(Task(
                    endpoint=self.get_region().base_url +
                    row[1].find('a').get('href'),
                    task_type=constants.TaskType.SCRAPE_DATA
                ))
        return tasks
