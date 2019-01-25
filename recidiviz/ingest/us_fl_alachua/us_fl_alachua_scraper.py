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
from recidiviz.ingest.html_5_base_scraper import Html5BaseScraper
from recidiviz.ingest.extractor.html_data_extractor import HtmlDataExtractor
from recidiviz.ingest import constants
import os
import logging

class UsFlAlachuaScraper(Html5BaseScraper):
    """Scraper implementation for us_fl_alachua."""
    def __init__(self, mapping_filepath=None):
        print("in init")
        if not mapping_filepath:
            mapping_filepath = os.path.join(
                os.path.dirname(__file__), 'us_fl_alachua.yaml')
        self.mapping_filepath = mapping_filepath
        super(UsFlAlachuaScraper, self).__init__('us_fl_alachua')

    def set_initial_vars(self, content, params):
        pass

    def populate_data(self, content, params, ingest_info):
        import ipdb; ipdb.set_trace()
        data_extractor = HtmlDataExtractor(self.mapping_filepath)
        ingest_info = data_extractor.extract_and_populate_data(content,
                                                               ingest_info)
        print('\n')
        print('ingest info')
        print(ingest_info)
        print('\n')

        for person in ingest_info.people:
            if len(person.bookings) != 1 or len(person.bookings[0].charges) > 1:
                logging.error("Data extraction did not produce a single "
                              "booking with at most one charge, as it should")
            if person.bookings[0].charges:
                charge_names = person.bookings[0].charges[0].name.split('\n')
                person.bookings[0].charges = []
                for charge_name in charge_names:
                    person.bookings[0].create_charge(name=charge_name)
        return ingest_info

    def get_more_tasks(self, content, params):
        """
        Gets more tasks based on the content and params passed in.  This
        function should determine which task params, if any, should be
        added to the queue

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A list of params containing endpoint and task_type at minimum.
        """
        task_type = params.get('task_type', self.get_initial_task_type())
        params_list = []
        if self.is_initial_task(task_type) or self.should_get_more_tasks(
                task_type):
            content.make_links_absolute(self.get_initial_endpoint())
            params_list.extend(self._get_person_params(content))
        return params_list

    def _get_person_params(self, content):
        links = content.xpath('//a')
        person_links = [link.xpath('./@href')[0] for link in links if link.text_content() != "Image"]
        params_list = []
        for person_link in person_links:
            params_list.append({
                'endpoint': person_link,
                'task_type': constants.SCRAPE_DATA
            })
        return params_list