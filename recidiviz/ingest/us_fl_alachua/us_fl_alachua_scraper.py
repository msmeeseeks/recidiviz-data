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

from recidiviz.common.constants.charge import ChargeDegree
from recidiviz.ingest.html_5_base_scraper import Html5BaseScraper
from recidiviz.ingest.extractor.html_data_extractor import HtmlDataExtractor
from recidiviz.ingest import constants
import os
import logging

from recidiviz.ingest.models.ingest_info import IngestInfo, Bond, Hold
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

    def populate_data(self, content, task: Task,
                      ingest_info: IngestInfo) -> Optional[ScrapedData]:
        tables = content.xpath('//table')
        person_table, charge_tables = tables[0], tables[1:]
        extractor = HtmlDataExtractor(self.mapping_filepath)

        person_info = extractor.extract_and_populate_data(person_table,
                                                          ingest_info)
        person_info.people[0].create_booking()
        hold_federal = extractor.get_value('Federal')
        hold_other = extractor.get_value('Other County')
        hold_bool = extractor.get_value('Hold')
        hold = 'No'
        if hold_federal=='Y':
            hold = 'Federal'
        elif hold_other=='Y':
            hold = 'Other County'
        elif hold_bool=='Y':
            hold = 'Unknown'
        if hold != 'No':
            hold = Hold(jurisdiction_name = hold)
            person_info.people[0].bookings[0].holds.append(hold)
        person_info.people[0].bookings[0].admission_date = task.custom['Booking Date']
        for charge_table in charge_tables:
            charges_info = extractor.extract_and_populate_data(charge_table)
            bond_amount = extractor.get_value('Bond Amount')
            bond = Bond(amount= bond_amount)
            agency = extractor.get_value('Agency')
            status = extractor.get_value('Status')
            case_no = extractor.get_value('Case #')
            if not charges_info:
                continue
            for charge in charges_info.people[0].bookings[0].charges:
                if charge.charge_class == 'C':
                    charge.charge_class = None
                if charge.degree and not ChargeDegree.can_parse(charge.degree, self.get_enum_overrides()):
                    charge.level = charge.degree
                    charge.degree = None
                charge.bond = bond
                charge.charging_entity = agency
                charge.status = status
                charge.case_number = case_no
                person_info.people[0].bookings[0].charges.append(charge)
        return ScrapedData(ingest_info=ingest_info, persist=True)

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

    def _get_person_tasks(self, content) -> List[Task]:
        task_list = []
        for row in content.xpath('//tr')[1:]:
            person_link = row[0][0].xpath('./@href')[0]
            booking_date = [row[2].text][0]
            task_list.append(Task(
                task_type=constants.TaskType.SCRAPE_DATA,
                endpoint=person_link,
                custom = {'Booking Date': booking_date}
            ))
        return task_list

    def get_enum_overrides(self):
        return {'N': None, 'F': ChargeDegree.FIRST, 'S': ChargeDegree.SECOND, 'T': ChargeDegree.THIRD}
