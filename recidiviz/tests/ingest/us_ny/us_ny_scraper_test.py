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
"""Scraper tests for us_ny."""

import unittest
from lxml import html
from recidiviz.ingest import constants
from recidiviz.ingest.models.ingest_info import IngestInfo, _Person, _Booking, \
    _Charge, _Sentence
from recidiviz.ingest.us_ny.us_ny_scraper import UsNyScraper
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils.base_scraper_test import BaseScraperTest

_SEARCH_PAGE_HTML = html.fromstring(
    fixtures.as_string('us_ny', 'search_page.html'))
_SEARCH_RESULTS_PAGE_HTML = html.fromstring(
    fixtures.as_string('us_ny', 'search_results_page.html'))
_DETAILS_PAGE_HTML = html.fromstring(
    fixtures.as_string('us_ny', 'person_page.html'))
_DETAILS_PAGE_WHITE_HISPANIC_HTML = html.fromstring(
    fixtures.as_string('us_ny', 'person_page_white_hispanic.html'))
_DETAILS_PAGE_BLACK_HISPANIC_HTML = html.fromstring(
    fixtures.as_string('us_ny', 'person_page_black_hispanic.html'))


class TestScraperSearchPage(BaseScraperTest, unittest.TestCase):
    """Test parsing the UsNy search page
    """

    def _init_scraper_and_yaml(self):
        self.scraper = UsNyScraper()

    def test_parse(self):
        expected = [{
            'post_data': {
                'K01': 'WINQ000',
                'DFH_STATE_TOKEN': 'abcdefgh',
                'DFH_MAP_STATE_TOKEN': '',
                'M00_DIN_FLD1I': '',
                'M00_DIN_FLD2I': '',
                'M00_DIN_FLD3I': '',
                'M00_DOBCCYYI': '',
                'M00_FIRST_NAMEI': '',
                'M00_LAST_NAMEI': 'a',
                'M00_MID_NAMEI': '',
                'M00_NAME_SUFXI': '',
                'M00_NYSID_FLD1I': '',
                'M00_NYSID_FLD2I': ''},
            'endpoint':
                'http://nysdoccslookup.doccs.ny.gov/GCA00P00/WIQ1/WINQ000',
            'task_type': 4,
        }]

        self.validate_and_return_get_more_tasks(
            _SEARCH_PAGE_HTML, {}, expected)


class TestScraperSearchResultsPage(BaseScraperTest, unittest.TestCase):
    """Test parsing the UsNy search results page
    """

    def _init_scraper_and_yaml(self):
        self.scraper = UsNyScraper()

    def test_parse(self):
        expected = [
            {'din': '1111aaa',
             'endpoint':
                 'http://nysdoccslookup.doccs.ny.gov/GCA00P00/WIQ3/WINQ130',
             'post_data': {'DFH_MAP_STATE_TOKEN': '',
                           'DFH_STATE_TOKEN': 'abcdefgh',
                           'K01': 'WINQ130',
                           'K02': '1234567',
                           'K03': '',
                           'K04': '1',
                           'K05': '2',
                           'K06': '1',
                           'M13_PAGE_CLICKI': '',
                           'M13_SEL_DINI': '1111aaa',
                           'din1': '1111aaa'},
             'task_type': 4},
            {'din': '2222bbb',
             'endpoint':
                 'http://nysdoccslookup.doccs.ny.gov/GCA00P00/WIQ3/WINQ130',
             'post_data': {'DFH_MAP_STATE_TOKEN': '',
                           'DFH_STATE_TOKEN': 'abcdefgh',
                           'K01': 'WINQ130',
                           'K02': '1234567',
                           'K03': '',
                           'K04': '1',
                           'K05': '2',
                           'K06': '1',
                           'M13_PAGE_CLICKI': '',
                           'M13_SEL_DINI': '2222bbb',
                           'din2': '2222bbb'},
             'task_type': 4},
            {'din': '3333ccc',
             'endpoint':
                 'http://nysdoccslookup.doccs.ny.gov/GCA00P00/WIQ3/WINQ130',
             'post_data': {'DFH_MAP_STATE_TOKEN': '',
                           'DFH_STATE_TOKEN': 'abcdefgh',
                           'K01': 'WINQ130',
                           'K02': '1234567',
                           'K03': '',
                           'K04': '1',
                           'K05': '2',
                           'K06': '1',
                           'M13_PAGE_CLICKI': '',
                           'M13_SEL_DINI': '3333ccc',
                           'din3': '3333ccc'},
             'task_type': 4},
            {'din': '4444ddd',
             'endpoint':
                 'http://nysdoccslookup.doccs.ny.gov/GCA00P00/WIQ3/WINQ130',
             'post_data': {'DFH_MAP_STATE_TOKEN': '',
                           'DFH_STATE_TOKEN': 'abcdefgh',
                           'K01': 'WINQ130',
                           'K02': '1234567',
                           'K03': '',
                           'K04': '1',
                           'K05': '2',
                           'K06': '1',
                           'M13_PAGE_CLICKI': '',
                           'M13_SEL_DINI': '4444ddd',
                           'din4': '4444ddd'},
             'task_type': 4},
            {'endpoint':
                 'http://nysdoccslookup.doccs.ny.gov/GCA00P00/WIQ3/WINQ130',
             'post_data': {'DFH_MAP_STATE_TOKEN': '',
                           'DFH_STATE_TOKEN': 'abcdefgh',
                           'K01': 'WINQ130',
                           'K02': '1234567',
                           'K03': '',
                           'K04': '1',
                           'K05': '2',
                           'K06': '1',
                           'M13_PAGE_CLICKI': 'Y',
                           'M13_SEL_DINI': ''},
             'task_type': 4},
        ]

        params = {
            'task_type': constants.GET_MORE_TASKS,
        }

        self.validate_and_return_get_more_tasks(
            _SEARCH_RESULTS_PAGE_HTML, params, expected)


class TestScraperDetailsPage(BaseScraperTest, unittest.TestCase):
    """Test parsing the UsNy person details page
    """

    def _init_scraper_and_yaml(self):
        self.scraper = UsNyScraper()

    def test_parse(self):

        expected = [{
            'endpoint': None,
            'task_type': constants.SCRAPE_DATA,
            'content': html.tostring(_DETAILS_PAGE_HTML, encoding='unicode'),
        }]

        params = {
            'task_type': constants.GET_MORE_TASKS,
        }

        self.validate_and_return_get_more_tasks(
            _DETAILS_PAGE_HTML, params, expected)


class TestIngest(BaseScraperTest, unittest.TestCase):
    """Test ingesting the data from the UsNy person detail page.
    """

    def _init_scraper_and_yaml(self):
        self.scraper = UsNyScraper()

    def test_parse(self):

        expected_sentence = _Sentence(
            min_length='0008 Years, 04 Months,\n                00 Days',
            max_length='0025 Years, 00 Months,\n                00 Days')
        expected = IngestInfo(people=[_Person(
            birthdate='04/22/1972',
            bookings=[
                _Booking(
                    admission_date='05/10/2013',
                    charges=[
                        _Charge(
                            attempted='True',
                            charge_class='FELONY',
                            degree='1ST',
                            level='TWO',
                            name='ATT MANSLAUGHTER',
                            status='SENTENCED',
                            sentence=expected_sentence,
                            ),
                        _Charge(
                            attempted='False',
                            charge_class='FELONY',
                            level='TWO',
                            name='ARMED ROBBERY',
                            status='SENTENCED',
                            sentence=expected_sentence,
                        )
                    ],
                    custody_status='RELEASED',
                    facility='QUEENSBORO',
                    projected_release_date='07/04/1998',
                    release_date='04/07/14',
                )],
            gender='MALE',
            person_id='1234567',
            race='WHITE',
            surname='SIMPSON, BART',
        )])

        params = {
            'endpoint': None,
            'task_type': constants.SCRAPE_DATA,
            'content': html.tostring(_DETAILS_PAGE_HTML, encoding='unicode'),
        }

        self.validate_and_return_populate_data(
            _DETAILS_PAGE_HTML, params, expected, IngestInfo())

    def test_parse_white_hispanic(self):
        expected_sentence = _Sentence(
            min_length='0008 Years, 04 Months,\n                00 Days',
            max_length='0025 Years, 00 Months,\n                00 Days')
        expected = IngestInfo(people=[_Person(
            birthdate='04/22/1972',
            bookings=[
                _Booking(
                    admission_date='05/10/2013',
                    charges=[
                        _Charge(
                            attempted='True',
                            charge_class='FELONY',
                            degree='1ST',
                            level='TWO',
                            name='ATT MANSLAUGHTER',
                            status='SENTENCED',
                            sentence=expected_sentence,
                        ),
                        _Charge(
                            attempted='False',
                            charge_class='FELONY',
                            level='TWO',
                            name='ARMED ROBBERY',
                            status='SENTENCED',
                            sentence=expected_sentence,
                        )
                    ],
                    custody_status='RELEASED',
                    facility='QUEENSBORO',
                    projected_release_date='07/04/1998',
                    release_date='04/07/14',
                )],
            gender='MALE',
            person_id='1234567',
            race='WHITE',
            ethnicity='HISPANIC',
            surname='SIMPSON, BART',
        )])

        params = {
            'endpoint': None,
            'task_type': constants.SCRAPE_DATA,
            'content': html.tostring(
                _DETAILS_PAGE_WHITE_HISPANIC_HTML, encoding='unicode'),
        }

        self.validate_and_return_populate_data(
            _DETAILS_PAGE_WHITE_HISPANIC_HTML, params, expected, IngestInfo())

    def test_parse_black_hispanic(self):
        expected_sentence = _Sentence(
            min_length='0008 Years, 04 Months,\n                00 Days',
            max_length='0025 Years, 00 Months,\n                00 Days')
        expected = IngestInfo(people=[_Person(
            birthdate='04/22/1972',
            bookings=[
                _Booking(
                    admission_date='05/10/2013',
                    charges=[
                        _Charge(
                            attempted='True',
                            charge_class='FELONY',
                            degree='1ST',
                            level='TWO',
                            name='ATT MANSLAUGHTER',
                            status='SENTENCED',
                            sentence=expected_sentence,
                        ),
                        _Charge(
                            attempted='False',
                            charge_class='FELONY',
                            level='TWO',
                            name='ARMED ROBBERY',
                            status='SENTENCED',
                            sentence=expected_sentence,
                        )
                    ],
                    custody_status='RELEASED',
                    facility='QUEENSBORO',
                    projected_release_date='07/04/1998',
                    release_date='04/07/14',
                )],
            gender='MALE',
            person_id='1234567',
            race='BLACK',
            ethnicity='HISPANIC',
            surname='SIMPSON, BART',
        )])

        params = {
            'endpoint': None,
            'task_type': constants.SCRAPE_DATA,
            'content': html.tostring(
                _DETAILS_PAGE_BLACK_HISPANIC_HTML, encoding='unicode'),
        }

        self.validate_and_return_populate_data(
            _DETAILS_PAGE_BLACK_HISPANIC_HTML, params, expected, IngestInfo())
