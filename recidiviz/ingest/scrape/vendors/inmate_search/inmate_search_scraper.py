
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

"""Scraper implementation for inmate search."""
import os
import re
from typing import List, Optional, Set

from recidiviz.ingest.scrape import constants
from recidiviz.ingest.scrape.base_scraper import BaseScraper
from recidiviz.ingest.scrape.errors import ScraperError
from recidiviz.ingest.extractor.html_data_extractor import HtmlDataExtractor
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.scrape.task_params import ScrapedData, Task


class InmateSearchScraper(BaseScraper):
    """Scraper implementation for inmate search."""

    def __init__(self,
                 region_name,
                 search_page_url,
                 profile_url_divider,
                 mapping_filepath=None):
        if not mapping_filepath:
            mapping_filepath = os.path.join(
                os.path.dirname(__file__), 'inmate_search.yaml')
        self.mapping_filepath = mapping_filepath
        self.search_page_url = search_page_url
        self.profile_url_divider = profile_url_divider

        super(InmateSearchScraper, self).__init__(region_name)

    def populate_data(self, content, task: Task,
                      ingest_info: IngestInfo) -> Optional[ScrapedData]:
        # "No record found."
        if content.cssselect('.WADANoResultsMessage'):
            return None

        # Modify duplicate fields so dataextractor can differentiate
        headers = content.xpath("//th/div[text()=\"Release Date:\"]")
        for header in headers:
            header.text = "unused:"

        data_extractor = HtmlDataExtractor(self.mapping_filepath)
        ingest_info = data_extractor.extract_and_populate_data(content,
                                                               ingest_info)

        if len(ingest_info.people) != 1:
            raise ScraperError("Expected only 1 person on page, but found %i" %
                               len(ingest_info.people))

        person = ingest_info.people[0]

        tables = content.cssselect('table')
        for table in tables:
            title = table.cssselect('.WADAPageTitle')
            if title and title[0].text_content() == "Booking History":

                i = 0
                for tr in table[1:]:
                    if len(person.bookings) <= i:
                        raise ScraperError("DataExtractor did not create enough"
                                           " bookings. %i expected, %i found" %
                                           (len(table[1:]),
                                            len(person.bookings)))

                    booking = person.bookings[i]

                    charge_table = tr.cssselect('table')
                    for charge_td in charge_table[0]:
                        self._add_charge(charge_td, booking)
                    i += 1

        return ScrapedData(ingest_info=ingest_info, persist=True)

    def _add_charge(self, charge_td, booking):
        charge_search = re.search(
            "Charge:\\W(\\S+\\W*/\\W*.+)\\n" +
            "\\W+(.+)\\W+Counts:\\W*" +
            "Bond\\WAmount:\\W*(\\d*)\\W*(\\$\\d*)",
            charge_td.text_content(), re.I)

        if charge_search:
            charge = booking.create_charge()
            charge.statute = charge_search.group(1)

            charge.name = charge_search.group(2)\
                .replace('\n', '')\
                .replace('\t', '')\
                .replace(u'\xa0', u'')\
                .strip()

            if charge_search.group(3):
                charge.number_of_counts = charge_search.group(3)

            charge.create_bond(amount=charge_search.group(4))

    def get_more_tasks(self, content, task: Task) -> List[Task]:
        if self.is_initial_task(task.task_type):
            return [self._get_search_page()]

        tasks = []
        tasks.extend(self._get_next_page(content, task.endpoint))
        tasks.extend(self._get_profiles(content))
        return tasks

    def _get_search_page(self) -> Task:
        return Task(
            task_type=constants.TaskType.GET_MORE_TASKS,
            endpoint=self.get_region().base_url +
            self.search_page_url,
        )

    def _get_next_page(self, content, endpoint) -> List[Task]:
        next_button = content.cssselect('[title="Next"]')
        if next_button:
            next_endpoint = self.get_region().base_url + \
                next_button[0].get('href')
            if next_endpoint == endpoint:
                # When endpoint is equal to current enpoint we've reached
                # the last page
                return []

            return [Task(
                task_type=constants.TaskType.GET_MORE_TASKS,
                endpoint=next_endpoint,
            )]

        return []

    def _get_profiles(self, content) -> List[Task]:
        link_elms = content.cssselect('td.WADAResultsTableCell > a')

        tasks = []
        links: Set[str] = set()
        for elm in link_elms:
            if elm.get('href') not in links:
                links.add(elm.get('href'))
                tasks.append(Task(
                    task_type=constants.TaskType.SCRAPE_DATA,
                    endpoint=self.get_region().base_url +
                    self.profile_url_divider + elm.get('href'),
                ))

        return tasks
