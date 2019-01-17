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

"""Scraper implementation for NewWorld vendor."""
import os
from recidiviz.ingest.base_scraper import BaseScraper
from recidiviz.ingest import constants
from recidiviz.ingest.extractor.html_data_extractor import HtmlDataExtractor

class NewWorldScraper(BaseScraper):
    """ NewWorld Vendor scraper """
    def __init__(self, region_name, mapping_filepath=None):
        if not mapping_filepath:
            mapping_filepath = os.path.join(
                os.path.dirname(__file__), 'newworld.yaml')
        self.mapping_filepath = mapping_filepath
        super(NewWorldScraper, self).__init__(region_name)

    def populate_data(self, content, params, ingest_info):
        # Bonds and charges are split in two tables. Merging them together
        self._merge_charge_and_bonds(content)

        data_extractor = HtmlDataExtractor(self.mapping_filepath)
        ingest_info = data_extractor.extract_and_populate_data(content,
                                                               ingest_info)

        if len(ingest_info.people) != 1:
            raise Exception("Expected exactly one person per page, "
                            "but got %i" % len(ingest_info.person))

        for booking in ingest_info.people[0].bookings:
            for charge in booking.charges:
                if charge.bond and charge.bond.bond_id == "No data":
                    charge.bond = None

        return ingest_info

    def get_initial_params(self):
        params = {
            'endpoint': self.get_region().base_url
                        +"/NewWorld.InmateInquiry/nassau?Page=1",
            'task_type': constants.GET_MORE_TASKS,
            'data': {
                'page': 1
            }
        }
        return params

    def get_more_tasks(self, content, params):
        tasks = []
        if params['data']['page'] == 1:
            tasks.extend(self._get_remaining_pages(content))
        tasks.extend(self._get_all_detail_pages(content))
        return tasks

    def _merge_charge_and_bonds(self, content):
        booking_elements = content.cssselect(".Booking")
        for booking_element in booking_elements:
            charge_table = booking_element.cssselect(".BookingCharges table")[0]
            bonds_table = booking_element.cssselect(".BookingBonds table")[0]

            # Merge header
            for e in bonds_table.find('thead/tr'):
                charge_table.find('thead/tr').append(e)

            # Merge rows
            charge_rows = charge_table.findall('tbody/tr')
            bonds_rows = bonds_table.findall('tbody/tr')
            if len(charge_rows) < len(bonds_rows):
                raise Exception("Expected number of charges >= number of bonds"
                                "but got %i charges and %i bonds" % \
                                (len(charge_rows), len(bonds_rows)))

            for c, b in zip(charge_rows, bonds_rows):
                for e in b:
                    c.append(e)


    def _get_remaining_pages(self, content):
        tasks = []
        options = content.cssselect('select[name="Page"] > option')
        for page in options[1:]:
            page_num = page.text_content()
            tasks.append({
                'endpoint': self.get_region().base_url
                            +"/NewWorld.InmateInquiry/nassau?Page="+page_num,
                'task_type': constants.GET_MORE_TASKS,
                'data': {
                    'page': int(page_num)
                }
            })
        return tasks

    def _get_all_detail_pages(self, content):
        tasks = []
        links = content.cssselect('td[class="Name"] > a')

        for link in links:
            tasks.append({
                'endpoint': self.get_region().base_url+link.get('href'),
                'task_type': constants.SCRAPE_DATA
            })

        return tasks
