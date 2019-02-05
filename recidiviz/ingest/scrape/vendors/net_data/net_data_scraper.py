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
"""Generic scraper implementation for all regions using NET Data vendor.

Scraper flow:
    1. With main html, scrapes required post data necessary to reach roster.
    2. With roster xml, requests roster xsl stylesheet.
    3. With roster xsl, merges roster xml and xsl into html. Scrapes necessary
       post data for individual people and scrapes action needed to go to
       person pages. Queues tasks for each person.
    4. With person xml, requests person xsl stylesheet.
    5. (In populate_data): With person xsl, merges person xml and xsl into html.
       Uses html data extractor to extract all key/value pairs for this person,
       then manually merges any found bookings that share the same admission and
       release date. Finally adds the found booking_id onto the scraped open
       booking (booking_id is provided only for the current booking).
"""

import logging
import os
import re
from copy import copy
from typing import Optional, List

import more_itertools
from lxml import etree, html

from recidiviz.ingest.scrape import constants
from recidiviz.ingest.scrape.base_scraper import BaseScraper
from recidiviz.ingest.errors import ScraperError
from recidiviz.ingest.extractor.html_data_extractor import HtmlDataExtractor
from recidiviz.ingest.models.ingest_info import IngestInfo, Booking
from recidiviz.ingest.scrape.task_params import Task, ScrapedData

_BOOKING_ID_KEY = 'ARREST NO'
_PAGE_TYPE_KEY = 'page_type'
_PAGE_TYPE_ROSTER = 'roster'
_PAGE_TYPE_ROSTER_XSL = 'roster_xsl'
_PAGE_TYPE_PERSON = 'person'
_PATH_PREFIX = 'bok/'
_PERSON_KEY_SUBSTR = 'Javascript:run'
_PERSON_KEY_REGEX = r'Javascript:.*\'(.*?)\''
_PERSON_PAGE_CONTENT_KEY = 'person_page_content'
_POST_DATA_XPATH = '//input[@type="hidden"]'
_ROSTER_CONTENT_KEY = 'roster_content'
_ROSTER_FORM_XPATH = '//form[@id="form1"]'
_STYLESHEET_REGEX = r'href=\"(.*?)\"'
_KEY_NUMBER_REGEX = r'S(10[0-9])KEY'


class NetDataScraper(BaseScraper):
    """Scraper for counties using NET Data Scraper."""

    def __init__(self, region_name, mapping_filepath=None):
        if not mapping_filepath:
            mapping_filepath = os.path.join(
                os.path.dirname(__file__), 'net_data.yaml')
        self.mapping_filepath = mapping_filepath
        super(NetDataScraper, self).__init__(region_name)

    def get_more_tasks(self, content, task: Task) -> List[Task]:
        if self.is_initial_task(task.task_type):
            return _get_roster_task(content, task)

        if self.should_get_more_tasks(task.task_type):
            if task.custom[_PAGE_TYPE_KEY] == _PAGE_TYPE_ROSTER:
                return _get_roster_stylesheet_task(content, task)
            if task.custom[_PAGE_TYPE_KEY] == _PAGE_TYPE_ROSTER_XSL:
                return _get_person_tasks(content, task)
            if task.custom[_PAGE_TYPE_KEY] == _PAGE_TYPE_PERSON:
                return _get_person_stylesheet_task(content, task)
        raise ScraperError('Unexpected task configuration %s' % task)

    def populate_data(
            self, content, task: Task,
            ingest_info: IngestInfo) -> Optional[ScrapedData]:
        html_content = _get_html_content(
            task.custom[_PERSON_PAGE_CONTENT_KEY], content)
        data_extractor = HtmlDataExtractor(self.mapping_filepath)
        ingest_info = data_extractor.extract_and_populate_data(
            html_content, ingest_info)

        open_booking_id = data_extractor.get_value(_BOOKING_ID_KEY)
        _merge_bookings(ingest_info)
        _put_booking_id_on_open_booking(
            ingest_info, open_booking_id, self.region.region_code)
        return ScrapedData(ingest_info=ingest_info)


def _get_roster_task(content, task: Task) -> List[Task]:
    endpoint = _generate_url(
        task.endpoint,
        more_itertools.one(content.xpath(_ROSTER_FORM_XPATH)).action)
    post_data = {elem.name: elem.value for elem in
                 content.xpath(_POST_DATA_XPATH)}
    task = Task(endpoint=endpoint, post_data=post_data,
                task_type=constants.TaskType.GET_MORE_TASKS,
                response_type=constants.ResponseType.TEXT,
                custom={_PAGE_TYPE_KEY: _PAGE_TYPE_ROSTER})
    return [task]


def _get_roster_stylesheet_task(content, task: Task) -> List[Task]:
    endpoint = _generate_url(task.endpoint, _get_stylesheet(content))
    task = Task(endpoint=endpoint,
                task_type=constants.TaskType.GET_MORE_TASKS,
                response_type=constants.ResponseType.RAW,
                custom={_ROSTER_CONTENT_KEY: content,
                        _PAGE_TYPE_KEY: _PAGE_TYPE_ROSTER_XSL})
    return [task]


def _get_person_tasks(content, task: Task) -> List[Task]:
    html_content = _get_html_content(task.custom[_ROSTER_CONTENT_KEY],
                                     content)
    post_data = {elem.name: elem.value for elem in
                 html_content.xpath(_POST_DATA_XPATH)}
    person_xpath = _get_person_form_xpath(html_content)
    new_url = _generate_url(task.endpoint, more_itertools.one(
        html_content.xpath(person_xpath)).action)

    task_list = []
    for key in _get_person_keys(html_content):
        post_data_for_key = copy(post_data)
        post_data_for_key[_get_form_key(html_content)] = key
        task_list.append(
            Task(endpoint=new_url,
                 post_data=post_data_for_key,
                 task_type=constants.TaskType.GET_MORE_TASKS,
                 response_type=constants.ResponseType.TEXT,
                 custom={_PAGE_TYPE_KEY: _PAGE_TYPE_PERSON}))
    return task_list


def _generate_url(endpoint: str, action: str) -> str:
    # URLs for NetData always assume the form 'http://...../bok/xxx.xx'. Here we
    # remove everything after the 'bok/' to create a new base for our url.
    base = endpoint.rsplit('/', 1)[0]

    # Actions can come in the form 'action', 'bok/action', or '/bok/action'. In
    # all cases, we want normalized_action = 'action'.
    if '/' in action:
        normalized_action = action.rsplit('/', 1)[1]
    else:
        normalized_action = action

    return base + '/' + normalized_action


def _get_stylesheet(raw_content) -> str:
    search_result = re.search(_STYLESHEET_REGEX, raw_content)
    if not search_result:
        raise ScraperError('No stylesheet found in raw_content')
    return search_result.group(1)


def _get_person_stylesheet_task(content, task: Task) -> List[Task]:
    endpoint = _generate_url(task.endpoint, _get_stylesheet(content))
    task = Task(endpoint=endpoint, task_type=constants.TaskType.SCRAPE_DATA,
                response_type=constants.ResponseType.RAW,
                custom={_PERSON_PAGE_CONTENT_KEY: content})
    return [task]


def _get_person_keys(content) -> List[str]:
    keys: List[str] = []
    for link in content.xpath('//a/@href'):
        if _PERSON_KEY_SUBSTR in link:
            keys.append(more_itertools.one(
                re.findall(_PERSON_KEY_REGEX, str(link))))
    return keys


def _get_html_content(xml_content, stylesheet_content):
    xslt_tree = etree.XML(stylesheet_content)
    xml_elems = etree.XML(xml_content).getroottree()
    return html.fromstring(str(xml_elems.xslt(xslt_tree)))


def _get_key_num(content) -> str:
    content_str = str(html.tostring(content))
    regex_match = re.search(_KEY_NUMBER_REGEX, content_str)
    if not regex_match:
        raise ScraperError(
            'No matches for regex %s in content' % _KEY_NUMBER_REGEX)
    return regex_match.group(1)


def _get_form_key(content) -> str:
    return 'S' + _get_key_num(content) + 'KEY'


def _get_person_form_xpath(content) -> str:
    return '//form[@id="S' + _get_key_num(content) + 'Form"]'


def _merge_bookings(ingest_info: IngestInfo) -> None:
    """By default, the Data Extractor will create a single booking for each
    scraped charge. This method compresses all bookings that share the same
    admission and release date."""

    for person in ingest_info.people:
        merged_bookings: List[Booking] = []
        for booking in person.bookings:
            merged_booking = _find_matching_booking(
                booking, merged_bookings)
            if merged_booking:
                merged_booking.charges.extend(booking.charges)
            else:
                merged_bookings.append(booking)
        person.bookings = merged_bookings


def _find_matching_booking(
        booking_to_match: Booking, bookings: List[Booking]) \
        -> Optional[Booking]:
    for booking in bookings:
        if booking.admission_date == booking_to_match.admission_date and \
                booking.release_date == booking_to_match.release_date:
            return booking
    return None


def _put_booking_id_on_open_booking(
        ingest_info: IngestInfo,
        open_booking_id: Optional[str], region_code: str) -> None:
    if not open_booking_id:
        return

    open_booking_exists = False
    for booking in more_itertools.one(ingest_info.people).bookings:
        if not booking.release_date:
            open_booking_exists = True
            booking.booking_id = open_booking_id

    if not open_booking_exists:
        logging.error(
            'Scraped open booking id: %s, but no open booking found on '
            'the scraped person for region %s', open_booking_id, region_code)
