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


"""Scraper implementation for the DCN vendor. This handles all DCN
specific navigation and data extraction. All counties that use DCN should
have their county-specific scrapers inherit from this class.

Background scraping procedure:
    1. A starting home page GET
      (to get VIEWSTATE and CALLBACKPARAMS which is needed)
    2. the next page contains links to every person currently in jail
    3. URL is extracted from every person and visited
    4. Person page has the relevant info extracted
"""
import abc
import os
import re
from typing import Optional, List

from recidiviz.common.constants.bond import BondType
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.ingest.scrape import scraper_utils, constants
from recidiviz.ingest.scrape.base_scraper import BaseScraper
from recidiviz.ingest.extractor.html_data_extractor import HtmlDataExtractor
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.scrape.task_params import ScrapedData, Task


class DcnScraper(BaseScraper):
    """Scraper for counties using DCN."""

    def __init__(self, region_name, yaml_file=None):
        super(DcnScraper, self).__init__(region_name)

        self.yaml = yaml_file or os.path.join(
            os.path.dirname(__file__), 'dcn_scraper.yaml')
        self._required_session_vars = ['__VIEWSTATE']

        self._keys_re = r"keys':\[(.*)\]},"
        self._people_re = r"DCN\/inmate-details(.*?)\\\'>"
        self._callback_params = "c0:KV|691;[{}];GB|22;12|PAGERONCLICK5|PSP-1;"

    @abc.abstractmethod
    def get_base_endpoint_details(self):
        """Returns the base endpoint for the details page of a person"""

    def _retrieve_session_vars(self, content):
        """Gets the variables necessary to complete requests.

        Args:
            content: An lxml html tree.

        Returns:
            A dict of session vars needed for the next scrape.
        """
        data = {
            session_var: scraper_utils.get_value_from_html_tree(content,
                                                                session_var)
            for session_var in self._required_session_vars
        }
        # Viewstate is much large, compress it before sending it to the queue.
        data['__VIEWSTATE'] = scraper_utils.compress_string(
            data['__VIEWSTATE'], level=9)
        data['__CALLBACKID'] = 'gvInmates'
        return data

    def _get_all_pages_params(self, content) -> Task:
        """Returns the task required to load all of the people at once.

        Args:
            content: An lxml html tree.

        Returns:
            A Task with all of the people in it.
        """
        post_data = self._retrieve_session_vars(content)
        all_scripts = content.xpath('.//script')
        for script in all_scripts:
            if 'keys' in script.text_content():
                keys = re.findall(self._keys_re, script.text_content())[0]
                post_data['__CALLBACKPARAM'] = self._callback_params. \
                    format(keys)
                break
        return Task(
            task_type=constants.TaskType.GET_MORE_TASKS,
            post_data=post_data,
            endpoint=self.get_region().base_url,
            response_type=constants.ResponseType.TEXT,
        )

    def _get_all_people_params(self, content) -> List[Task]:
        """Returns all of the tasks required for every person on the page.

        Args:
            content: An lxml html tree.

        Returns:
            A list of tasks each one a single persons booking page.
        """
        task_list = []
        urls = re.findall(self._people_re, content)
        for url in urls:
            person_url = self.get_base_endpoint_details() + url
            task_list.append(Task(
                task_type=constants.TaskType.SCRAPE_DATA,
                endpoint=person_url,
            ))
        return task_list

    def populate_data(self, content, task: Task,
                      ingest_info: IngestInfo) -> Optional[ScrapedData]:
        data_extractor = HtmlDataExtractor(self.yaml)
        ingest_info = data_extractor.extract_and_populate_data(content)
        # The charges table has a row at the bottom which is always bad data.
        del ingest_info.people[0].bookings[0].charges[-1]
        # The bond type is sometimes overloaded to included charge status.
        for charge in ingest_info.people[0].bookings[0].charges:
            if charge.bond:
                if charge.bond.bond_type and \
                        ChargeStatus.can_parse(charge.bond.bond_type,
                                               self.get_enum_overrides()):
                    charge.status = charge.bond.bond_type
                    charge.bond.bond_type = None

        return ScrapedData(ingest_info=ingest_info, persist=True)

    def get_more_tasks(self, content, task: Task):
        task_list = []
        task_type = task.task_type
        if self.is_initial_task(task_type):
            task_list.append(self._get_all_pages_params(content))
        else:
            task_list.extend(self._get_all_people_params(content))
        return task_list

    def transform_post_data(self, data):
        """If the child needs to transform the data in any way before it sends
        the request, it can override this function.

        Args:
            data: dict of parameters to send as data to the post request.
        """
        compression_key = '__VIEWSTATE'
        if data and compression_key in data:
            data[compression_key] = scraper_utils.decompress_string(
                data[compression_key])

    def get_enum_overrides(self) -> EnumOverrides:
        overrides_builder = EnumOverrides.Builder()
        overrides_builder.add('CASH OR SURETY BOND', BondType.UNSECURED)
        overrides_builder.add('DECLINED', BondType.NO_BOND)
        overrides_builder.add('PURGE', BondType.CASH)
        return overrides_builder.build()
