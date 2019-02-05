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

"""Generic scraper implementation for regions using Justice Solutions.

All subclasses of JusticeSolutionsScraper must provide a |devpath| value
(usually something like '/GSASYS/CLIENTS/<REGION>.WEB.CRIME') by inspecting
the website's POST data sent to webshell.asp.

Sites list all names on a single page, and users click through to each
person. The initial URL (for the roster) and each subsequent URL (for each
person) resolve to a javascript redirect, so we double the number of tasks.
The flow is as follows:

    1. Request the initial roster (GET.INMATE.LIST)
    2. Find the redirect URL (in a <script> that sets self.location)
    3. Find the person ID from each person on the roster
    4. For each person, queue a task that requires a redirect, which then
       queues a task to scrape the person's data.
"""
import abc
import os
import re
import time
from datetime import date
from enum import Enum, auto
from typing import List, Optional, Dict, Iterable
from urllib.parse import urljoin

import more_itertools

from recidiviz.ingest.scrape.base_scraper import BaseScraper
from recidiviz.ingest.scrape.constants import TaskType, ResponseType
from recidiviz.ingest.errors import ScraperError
from recidiviz.ingest.extractor.html_data_extractor import HtmlDataExtractor
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.scrape.task_params import ScrapedData, Task

_BASE_URL = 'http://findtheinmate.com'
_WEBSHELL_URL = urljoin(_BASE_URL, 'cgi-bin/webshell.asp')
_GET_INMATE_LIST = 'GET.INMATE.LIST'
_GET_INMATE_DATA = 'GET.INMATE.DATA'
_INITIAL_P_100 = 'INFO_5'


class _Redirect(Enum):
    ROSTER = auto()
    PERSON = auto()


class JusticeSolutionsScraper(BaseScraper):
    """Generic scraper implementation for Justice Solutions."""

    def __init__(self, region_name: str):
        super(JusticeSolutionsScraper, self).__init__(region_name)
        self.mapping_filepath = os.path.join(
            os.path.dirname(__file__), 'justice_solutions.yaml')
        self.devpath = self.get_devpath()

    @abc.abstractmethod
    def get_devpath(self) -> str:
        """Returns the devpath argument found in the website's post data.

        This value is found by inspecting the website's POST data sent to
        webshell.asp. Its format is often '/GSASYS/CLIENTS/<REGION>.WEB.CRIME'.
        """

    def populate_data(self, content, task: Task,
                      ingest_info: IngestInfo) -> Optional[ScrapedData]:
        extractor = HtmlDataExtractor(self.mapping_filepath)
        ingest_info = extractor.extract_and_populate_data(content, ingest_info)
        place = '{} {}'.format(extractor.get_value('CITY'),
                               extractor.get_value('STATE / ZIP'))
        person = more_itertools.one(ingest_info.people)
        person.place_of_residence = place
        person.gender = task.custom['gender']
        return ScrapedData(ingest_info)

    def get_more_tasks(self, content, task: Task) -> List[Task]:
        if 'redirect' not in task.custom:
            # Called once on the actual HTML of the roster page
            return list(self._get_all_people_tasks(content))

        regex = r'self\.location=\"([^\"]+)'
        match = re.search(regex, str(content))
        if not match:
            raise ScraperError('Could not find self.location in redirect page '
                               '{}'.format(content))
        redirect_path = match.group(1)
        endpoint = urljoin(_BASE_URL, redirect_path)
        # Use Enum.value because the enum-ness is lost in serialization.
        if task.custom['redirect'] == _Redirect.ROSTER.value:
            return [Task(endpoint=endpoint, task_type=TaskType.GET_MORE_TASKS)]
        if task.custom['redirect'] == _Redirect.PERSON.value:
            return [Task(endpoint=endpoint, task_type=TaskType.SCRAPE_DATA,
                         # continue passing gender along to SCRAPE_DATA
                         custom={'gender': task.custom['gender']})]

        raise ScraperError("task.custom['redirect'] expected either ROSTER or "
                           "PERSON but got {}".format(task.custom['redirect']))

    def _get_all_people_tasks(self, content) -> Iterable[Task]:
        for row in content.xpath('//tr[starts-with('
                                 '@onclick, "LoadInmateData")]'):
            person_id = row.attrib['onclick'].split("'")[1]
            # gender is listed on the roster but not on individual pages.
            gender = row[4].text

            yield Task(endpoint=_WEBSHELL_URL,
                       post_data=self._create_payload(person_id),
                       task_type=TaskType.GET_MORE_TASKS,
                       custom={'redirect': _Redirect.PERSON, 'gender': gender},
                       response_type=ResponseType.RAW)

    def get_initial_task(self) -> Task:
        return Task(endpoint=_WEBSHELL_URL,
                    post_data=self._create_payload(),
                    task_type=TaskType.GET_MORE_TASKS,
                    response_type=ResponseType.RAW,
                    custom={'redirect': _Redirect.ROSTER})

    def _create_payload(self, person_id: str = None) -> Dict:
        """Creates post data. When called with no arguments, creates the post
        data for the roster page. Otherwise, creates the post data for a person
        page."""
        p_100 = person_id if person_id else _INITIAL_P_100
        xgateway = _GET_INMATE_DATA if person_id else _GET_INMATE_LIST
        payload = {
            'GATEWAY': 'GATEWAY',
            'P_100': p_100,
            'XGATEWAY': xgateway,
            'CGISCRIPT': 'webshell.asp',
            'XEVENT': 'VERIFY',
            'WEBIOHANDLE': _get_time_and_date(),
            'MYPARENT': 'px',
            'APPID': 'jsinq',
            'WEBWORDSKEY': 'SAMPLE',
            'DEVPATH': self.devpath,
            'OPERCODE': 'dummy',
            'PASSWD': 'dummy'
        }
        if xgateway == _GET_INMATE_LIST:
            return {
                **payload,
                'P_102': 'INFO_5',
                'P_103': 'DESC',
                'P_104': self.devpath,
            }
        return payload


def _get_time_and_date() -> str:
    """The post data field 'WEBIOHANDLE' concatenates javascript's
    Date.getTime() and Date.getDate(). This function approximates that value."""
    return '{}{}'.format(time.time() * 1000, date.today().day)
