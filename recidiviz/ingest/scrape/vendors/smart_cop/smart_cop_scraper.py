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

"""Generic scraper implementation for regions using SmartCOP's (formerly CTS)
SmartWEB roster."""

import json
import os
from typing import Optional, List
from lxml import html

from recidiviz.common.constants.person import Race
from recidiviz.ingest.scrape.base_scraper import BaseScraper
from recidiviz.ingest.scrape.constants import ResponseType, TaskType
from recidiviz.ingest.extractor.html_data_extractor import HtmlDataExtractor
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.scrape.task_params import Task, ScrapedData

# TypeJailSearch options ("Search For" dropdown):
#   0: "Current Inmates Only"
#   1: "Released Inmates Only"
#   2: "Both Current and Released"
# SortOrder: 0: Ascending, 1: Descending
# SortOption: 0: Name, 1: Booking Date
# NOTE: when converting this to a string for print preview, order matters.
# Don't reorder the fields here.
_SEARCH_PARAMS = {
    'FirstName': '',
    'MiddleName': '',
    'LastName': '',
    'BeginBookDate': '',
    'EndBookDate': '',
    'BeginReleaseDate': '',
    'EndReleaseDate': '',
    'TypeJailSearch': 0,
    'RecordsLoaded': 0,
    'SortOrder': 1,
    'SortOption': 1,
    'IsDefault': False
}


class SmartCopScraper(BaseScraper):
    """
    SmartCOP websites provide two ways to query the current population. The
    bottom of the initial page has a 'Print' link; this will report the whole
    population on a single page.

     However, if the page is too large, the Print page won't load. In this
     case, we'll fall back on searching incrementally. We'll search for 'Current
     Inmates Only', sorted by booking date, and continue to 'Load More Results'
     (the other link at the bottom of the page) until no more results are
     loaded.
    """

    def __init__(self, region_name):
        super(SmartCopScraper, self).__init__(region_name)
        self.yaml_file = os.path.join(
            os.path.dirname(__file__), 'smart_cop.yaml')

    def populate_data(self, content, task: Task,
                      ingest_info: IngestInfo) -> Optional[ScrapedData]:
        html_content = content
        if isinstance(content, dict):
            html_text = content['d']['Data']['data']
            if not html_text.strip():
                return None
            html_content = html.fromstring(html_text)

        if self._is_error_page(html_content):
            return None

        extractor = HtmlDataExtractor(self.yaml_file)
        ingest_info = extractor.extract_and_populate_data(html_content,
                                                          ingest_info)
        for person in ingest_info.people:
            if not person.full_name:
                raise ValueError('Extraction did not produce a full_name field')
            name, race, gender, dob, _, __ = person.full_name.split('\n')
            person.full_name = name
            person.race = race.replace('(', '').replace('/', '')
            person.gender = gender
            person.birthdate = dob.lstrip('\t /DOB:')
        return ScrapedData(ingest_info, persist=True)

    def get_more_tasks(self, content, task: Task) -> List[Task]:
        if isinstance(content, html.HtmlElement):
            if self._is_error_page(content):
                return [self.first_add_more_results_task()]
            return []

        returned = content['d']['Data']['resultsReturned']
        if returned == 0:
            return []
        last_index = task.json['RecordsLoaded']
        task.json['RecordsLoaded'] = str(int(last_index) + returned)
        return [task]

    def get_initial_task(self) -> Task:
        return self.get_preview_task()

    def first_add_more_results_task(self) -> Task:
        endpoint = '{}/{}'.format(self.region.base_url, 'AddMoreResults')
        return Task(endpoint=endpoint, json=_SEARCH_PARAMS,
                    response_type=ResponseType.JSON,
                    task_type=TaskType.SCRAPE_DATA_AND_MORE)

    def get_preview_task(self) -> Task:
        """Some rosters have a small enough population that the main endpoint
        can be hit with preview=true. This is a print preview function that
        shows all the search results on a single page. All scrapers will
        attempt this first; if it fails we fall back on requesting 'more
        results' instead."""
        params = {'preview': True, 'searchvalues': json.dumps(_SEARCH_PARAMS)}
        return Task(endpoint=self.region.base_url,
                    task_type=TaskType.SCRAPE_DATA_AND_MORE,
                    params=params)

    def _is_error_page(self, content: html.HtmlElement) -> bool:
        # The initial task has an HTML response rather than a JSON response;
        # if the response is an error page we were unable to load the
        # print preview.
        title = content.find('.//title')
        return title is not None and title.text.strip() == 'Error'

    def get_enum_overrides(self):
        return {
            'A': Race.ASIAN,
            'I': Race.AMERICAN_INDIAN_ALASKAN_NATIVE,
            'U': Race.EXTERNAL_UNKNOWN,

            # Oliver- do you know what these charge degrees mean?
            'C': None,
            'F': None,
            'L': None,
            'M': None,
            'N': None,
            'S': None,
            'T': None,
        }
