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

"""Scraper implementation for us_fl_alachua."""
from typing import Optional, List

from recidiviz.ingest.html_5_base_scraper import Html5BaseScraper
from recidiviz.ingest.extractor.html_data_extractor import HtmlDataExtractor
from recidiviz.ingest import constants
import os
import logging

from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.task_params import Task, ScrapedData


class UsFlAlachuaScraper(Html5BaseScraper):
    """Scraper implementation for us_fl_alachua."""

    def __init__(self, mapping_filepath=None):
        if not mapping_filepath:
            mapping_filepath = os.path.join(
                os.path.dirname(__file__), 'us_fl_alachua.yaml')
        self.mapping_filepath = mapping_filepath
        super(UsFlAlachuaScraper, self).__init__('us_fl_alachua')

    def set_initial_vars(self, content, params):
        pass

    # def populate_data(self, content, params, ingest_info):
    #     data_extractor = HtmlDataExtractor(self.mapping_filepath)
    #     ingest_info = data_extractor.extract_and_populate_data(content,
    #                                                            ingest_info)
    #
    #     for person in ingest_info.people:
    #         if len(person.bookings) != 1 or len(person.bookings[0].charges) > 1:
    #             logging.error("Data extraction did not produce a single "
    #                           "booking with at most one charge, as it should")
    #         if person.bookings[0].charges:
    #             print("person.bookings")
    #             print(person.bookings)
    #             # sometime there isn't a charge name if the case table doesn't exist on the page
    #             for i in range(0, len(person.bookings[0].charges)):
    #                 print(i)
    #                 print(person.bookings[0].charges[i])
    #                 if person.bookings[0].charges[i].name:
    #                     charge_names = person.bookings[0].charges[i].name.split('\n')
    #                     # person.bookings[0].charges = []
    #                     for charge_name in charge_names:
    #                         person.bookings[0].create_charge(name=charge_name)
    #         else:
    #             logging.error("there is no bookings table in %s, the following is the ingest info: %s",
    #                           content, ingest_info)
    #     return ScrapedData(ingest_info = ingest_info)

    def populate_data(self, content, task: Task,
                      ingest_info: IngestInfo) -> Optional[ScrapedData]:

        data_extractor = HtmlDataExtractor(self.mapping_filepath)
        ingest_info = data_extractor.extract_and_populate_data(content,
                                                               ingest_info)

        print("ingest_info")
        print(ingest_info)




        # for person in ingest_info.people:
        #     if len(person.bookings) != 1 or len(person.bookings[0].charges) > 1:
        #         logging.error("Data extraction did not produce a single "
        #                       "booking with at most one charge, as it should")
        #     # for i in range(0, len(person.bookings)):
        #     #     if person.bookings[i].charges:
        #     #         for j in range(0, len(person.bookings[i].charges)):
        #     #             charge_name = person.bookings[i].charges[j].name
        #     #             if charge_name:
        #     #                 person.bookings[i].crea
        #     #             if charge_names_raw:
        #     #                 charge_names = charge_names_raw.split('\n')
        #     #                 person.bookings[i].charges = []
        #     #                 for charge_name in charge_names:
        #     #                     person.bookings[i].create_charge(name=charge_name)

        return ScrapedData(ingest_info=ingest_info, persist=True)

    # def get_more_tasks(self, content, task: Task):
    #     """
    #     Gets more tasks based on the content and params passed in.  This
    #     function should determine which task params, if any, should be
    #     added to the queue
    #
    #     Args:
    #         content: An lxml html tree.
    #         taks: object of parameters passed from the last scrape session.
    #
    #     Returns:
    #         A list of params containing endpoint and task_type at minimum.
    #     """
    #     task_type = task.task_type
    #     params_list = []
    #     if self.is_initial_task(task_type) or self.should_get_more_tasks(
    #             task_type):
    #         content.make_links_absolute(self.region.base_url)
    #         params_list.extend(self._get_person_params(content))
    #     return params_list

    def get_more_tasks(self, content, task: Task) -> List[Task]:
        content.make_links_absolute(self.get_region().base_url)

        params_list = []
        params_list.extend(self._get_person_tasks(content))
        params_list.extend(self._get_next_page_if_exists_tasks(content))
        return params_list

    def _get_next_page_if_exists_tasks(self, content) -> List[Task]:
        links = content.xpath('//a[@class="tbold"]')
        next_page_links = [link.xpath('./@href')[0] for link in links if
                           link.text_content() == ">>"]
        # There are multiple next page links on each roster page; however, they
        # are all equivalent, so choose the first one arbitrarily
        task_list = []
        if next_page_links:
            task_list.append(Task(
                task_type=constants.TaskType.GET_MORE_TASKS,
                endpoint=next_page_links[0],
            ))
        return task_list

    # def _get_person_params(self, content):
    #     links = content.xpath('//a')
    #     person_links = [link.xpath('./@href')[0] for link in links if link.text_content() != "Image"]
    #     params_list = []
    #     for person_link in person_links:
    #         params_list.append(
    #             Task(endpoint = person_link, task_type=constants.TaskType.SCRAPE_DATA)
    #         )
    #     return params_list

    def _get_person_tasks(self, content) -> List[Task]:
        links = content.xpath('//a')
        person_links = [link.xpath('./@href')[0] for link in links if link.text_content() != "Image"]
        task_list = []
        for person_link in person_links:
            task_list.append(Task(
                task_type=constants.TaskType.SCRAPE_DATA,
                endpoint=person_link,
            ))
        return task_list


    def get_enum_overrides(self):
        return {'C': None, 'L': None}
