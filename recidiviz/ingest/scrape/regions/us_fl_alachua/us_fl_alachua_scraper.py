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
import os
from typing import Optional, List
from recidiviz.common.constants.charge import ChargeDegree
from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.ingest.extractor.html_data_extractor import HtmlDataExtractor
from recidiviz.ingest.models.ingest_info import IngestInfo, Bond
from recidiviz.ingest.scrape import constants, scraper_utils
from recidiviz.ingest.scrape.html_5_base_scraper import Html5BaseScraper
from recidiviz.ingest.scrape.task_params import Task, ScrapedData


class UsFlAlachuaScraper(Html5BaseScraper):
    """Scraper implementation for us_fl_alachua."""

    def __init__(self, mapping_filepath=None):
        if not mapping_filepath:
            mapping_filepath = os.path.join(
                os.path.dirname(__file__), 'us_fl_alachua.yaml')
        self.mapping_filepath = mapping_filepath
        super(UsFlAlachuaScraper, self).__init__('us_fl_alachua')

    def populate_data(self, content, task: Task,
                      ingest_info: IngestInfo) -> Optional[ScrapedData]:
        tables = content.xpath('//table')
        person_table, charge_tables = tables[0], tables[1:]
        extractor = HtmlDataExtractor(self.mapping_filepath)

        person_info = extractor.extract_and_populate_data(person_table,
                                                          ingest_info)
        one_person = scraper_utils.one('person', person_info)
        created_booking = one_person.create_booking(
            admission_date=task.custom['Booking Date'])
        hold_federal = extractor.get_value('Federal')
        hold_other = extractor.get_value('Other County')
        hold_bool = extractor.get_value('Hold')
        hold = None
        if hold_federal == 'Y':
            hold = 'Federal'
        elif hold_other == 'Y':
            hold = 'Other County'
        elif hold_bool == 'Y':
            hold = 'Unknown'
        if hold:
            created_booking.create_hold(jurisdiction_name=hold)
        for charge_table in charge_tables:
            charges_info = extractor.extract_and_populate_data(charge_table)
            if not charges_info:
                continue
            bond_amount = extractor.get_value('Bond Amount')
            bond = Bond(amount=bond_amount)
            agency = extractor.get_value('Agency')
            status = extractor.get_value('Status')
            case_no = extractor.get_value('Case #')
            one_booking = scraper_utils.one('booking', charges_info)
            for charge in one_booking.charges:
                # TODO (#816) map this in the enum overrides when that's
                #  possible
                if charge.charge_class == 'C':
                    charge.charge_class = None
                if charge.degree and not ChargeDegree.can_parse(
                        charge.degree, self.get_enum_overrides()):
                    charge.level = charge.degree
                    charge.degree = None
                charge.bond = bond
                charge.charging_entity = agency
                charge.status = status
                charge.case_number = case_no
                created_booking.charges.append(charge)
        return ScrapedData(ingest_info=ingest_info, persist=True)

    def get_more_tasks(self, content, task: Task) -> List[Task]:
        content.make_links_absolute(self.get_region().base_url)
        params_list = self._get_person_tasks(content)
        return params_list

    def _get_person_tasks(self, content) -> List[Task]:
        task_list = []
        for row in content.xpath('//tr')[1:]:
            person_link = row[0][0].xpath('./@href')[0]
            booking_date = [row[2].text][0]
            task_list.append(Task(
                task_type=constants.TaskType.SCRAPE_DATA,
                endpoint=person_link,
                custom={'Booking Date': booking_date}
            ))
        return task_list

    def get_enum_overrides(self):
        overrides_builder = EnumOverrides.Builder()

        overrides_builder.add('F', ChargeDegree.FIRST)
        overrides_builder.ignore('N')
        overrides_builder.add('S', ChargeDegree.SECOND)
        overrides_builder.add('T', ChargeDegree.THIRD)

        return overrides_builder.build()
