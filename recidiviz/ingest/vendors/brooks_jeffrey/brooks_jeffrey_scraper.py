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

"""Scraper implementation for all websites using Brooks Jeffery marketing.
"""

import logging
import os

from recidiviz.ingest import constants
from recidiviz.ingest.base_scraper import BaseScraper
from recidiviz.ingest.extractor.data_extractor import DataExtractor


class BrooksJeffreyScraper(BaseScraper):
    """Scraper for counties using Brooks Jeffrey."""

    def __init__(self, region_name, mapping_filepath=None):
        if not mapping_filepath:
            mapping_filepath = os.path.join(
                os.path.dirname(__file__), 'brooks_jeffrey.yaml')
        self.mapping_filepath = mapping_filepath
        super(BrooksJeffreyScraper, self).__init__(region_name)

    def set_initial_vars(self, content, params):
        pass

    def get_more_tasks(self, content, params):
        task_type = params.get('task_type', self.get_initial_task_type())
        params_list = []
        if self.is_initial_task(task_type) or self.should_get_more_tasks(
                task_type):
            content.make_links_absolute(self.get_initial_endpoint())
            params_list.extend(self._get_person_params(content))
            params_list.extend(self._get_next_page_if_exists(content))
        return params_list

    def _get_next_page_if_exists(self, content):
        links = content.xpath('//a[@class="tbold"]')
        next_page_links = [link.xpath('./@href')[0] for link in links if
                           link.text_content() == ">>"]
        # There are multiple next page links on each roster page; however, they
        # are all equivalent, so choose the first one arbitrarily
        params_list = []
        if next_page_links:
            params_list.append({
                'endpoint': next_page_links[0],
                'task_type': constants.GET_MORE_TASKS
            })
        return params_list

    def _get_person_params(self, content):
        links = content.xpath('//a[@class="text2"]')
        person_links = [link.xpath('./@href')[0] for link in links if
                        link.text_content() == "View Profile >>>"]
        params_list = []
        for person_link in person_links:
            params_list.append({
                'endpoint': person_link,
                'task_type': constants.SCRAPE_DATA
            })
        return params_list

    def populate_data(self, content, params, ingest_info):
        data_extractor = DataExtractor(self.mapping_filepath)
        ingest_info = data_extractor.extract_and_populate_data(content,
                                                               ingest_info)

        for person in ingest_info.person:
            if len(person.booking) != 1 or len(person.booking[0].charge) > 1:
                logging.error("Data extraction did not produce a single "
                              "booking with at most one charge, as it should")

            if person.booking[0].charge:
                charge_names = person.booking[0].charge[0].name.split('\n')
                person.booking[0].charge = []
                for charge_name in charge_names:
                    person.booking[0].create_charge(name=charge_name)

        return ingest_info
