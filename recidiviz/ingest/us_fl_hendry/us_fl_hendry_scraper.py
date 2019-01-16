
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

"""Scraper implementation for us_fl_hendry."""
import os
import re
from recidiviz.ingest.base_scraper import BaseScraper
from recidiviz.ingest import constants
from recidiviz.ingest.extractor.html_data_extractor import HtmlDataExtractor


class UsFlHendryScraper(BaseScraper):
    """Scraper implementation for us_fl_hendry."""

    def __init__(self, mapping_filepath=None):
        if not mapping_filepath:
            mapping_filepath = os.path.join(
                os.path.dirname(__file__), 'us_fl_hendry.yaml')
        self.mapping_filepath = mapping_filepath

        super(UsFlHendryScraper, self).__init__('us_fl_hendry')

    def populate_data(self, content, params, ingest_info):

        # Modify duplicate fields so dataextractor can differentiate
        headers = content.xpath("//th/div[text()=\"Release Date:\"]")
        for header in headers:
            header.text = "unused:"

        data_extractor = HtmlDataExtractor(self.mapping_filepath)
        ingest_info = data_extractor.extract_and_populate_data(content,
                                                               ingest_info)

        if len(ingest_info.people) != 1:
            raise Exception("Expected only 1 person on page, but found %i" %
                            len(ingest_info.person))

        person = ingest_info.people[0]

        tables = content.cssselect('table')
        for table in tables:
            title = table.cssselect('.WADAPageTitle')
            if title and title[0].text_content() == "Booking History":

                i = 0
                for tr in table[1:]:
                    if len(person.bookings) <= i:
                        raise Exception("DataExtractor did not create enough "
                                        "bookings. %i expected, %i found" %
                                        (len(table[1:]), len(person.booking)))

                    booking = person.bookings[i]

                    charge_table = tr.cssselect('table')
                    for charge_td in charge_table[0]:
                        charge_search = re.search(
                            "Charge:\\W(\\S+\\W*/\\W*.+)\\n" +
                            "\\W+(.+)\\W+Counts:\\W*" +
                            "Bond\\WAmount:\\W*(\\d*)\\W*(\\$\\d*)",
                            charge_td.text_content(), re.I)

                        charge = booking.create_charge()
                        charge.statute = charge_search.group(1)

                        charge.name = charge_search.group(2)\
                            .replace('\n', '')\
                            .replace('\t', '')\
                            .replace(u'\xa0', u'')\
                            .strip()

                        if charge.name == '':
                            charge.name = None

                        if charge_search.group(3):
                            charge.number_of_counts = charge_search.group(3)

                        charge.create_bond(amount=charge_search.group(4))
                    i += 1

        return ingest_info

    def get_more_tasks(self, content, params):
        if self.is_initial_task(params['task_type']):
            return [self._get_search_page()]

        tasks = []
        tasks.extend(self._get_next_page(content, params['endpoint']))
        tasks.extend(self._get_profiles(content))
        return tasks

    def _get_search_page(self):
        return {
            'endpoint': self.get_region().base_url +
                        "/inmate_search/INMATE_Results.php",

            'task_type': constants.GET_MORE_TASKS
        }

    def _get_next_page(self, content, endpoint):
        next_button = content.cssselect('[title="Next"]')
        if next_button:
            next_endpoint = self.get_region().base_url + \
                next_button[0].get('href')
            if next_endpoint == endpoint:
                # When endpoint is equal to current enpoint we've reached
                # the last page
                return []

            return [{
                'endpoint': next_endpoint,
                'task_type': constants.GET_MORE_TASKS
            }]

        return []

    def _get_profiles(self, content):
        link_elms = content.cssselect('td.WADAResultsTableCell > a')

        tasks = []
        links = []
        for elm in link_elms:
            if elm.get('href') not in links:
                links.append(elm.get('href'))
                tasks.append({
                    'endpoint': self.get_region().base_url +
                                '/inmate_search/' + elm.get('href'),
                    'task_type': constants.SCRAPE_DATA
                })

        return tasks
