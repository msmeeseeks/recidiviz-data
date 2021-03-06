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
from recidiviz.ingest.scrape import constants
from recidiviz.ingest.models.ingest_info import IngestInfo, Person, Booking, \
    Charge, Sentence
from recidiviz.ingest.scrape.task_params import Task
from recidiviz.ingest.scrape.regions.us_ny.us_ny_scraper import UsNyScraper
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
_DETAILS_PAGE_NO_RELEASE_HTML = html.fromstring(
    fixtures.as_string('us_ny', 'person_page_no_release_date.html'))
_DETAILS_PAGE_LIFE_SENTENCE_HTML = html.fromstring(
    fixtures.as_string('us_ny', 'person_page_life_sentence.html'))


class TestScraperSearchPage(BaseScraperTest, unittest.TestCase):
    """Test parsing the UsNy search page
    """

    def _init_scraper_and_yaml(self):
        self.scraper = UsNyScraper()

    def test_parse(self):
        expected = [Task(
            task_type=constants.TaskType.GET_MORE_TASKS,
            endpoint='http://nysdoccslookup.doccs.ny.gov/GCA00P00/WIQ1/WINQ000',
            post_data={
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
                'M00_NYSID_FLD2I': '',
            },
        )]

        task = Task(
            task_type=constants.TaskType.INITIAL_AND_MORE,
            endpoint='',
        )

        self.validate_and_return_get_more_tasks(
            _SEARCH_PAGE_HTML, task, expected)


class TestScraperSearchResultsPage(BaseScraperTest, unittest.TestCase):
    """Test parsing the UsNy search results page
    """

    def _init_scraper_and_yaml(self):
        self.scraper = UsNyScraper()

    def test_parse(self):
        expected = [
            Task(
                task_type=constants.TaskType.GET_MORE_TASKS,
                endpoint=
                'http://nysdoccslookup.doccs.ny.gov/GCA00P00/WIQ3/WINQ130',
                post_data={'DFH_MAP_STATE_TOKEN': '',
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
                custom={'din': '1111aaa'},
            ), Task(
                task_type=constants.TaskType.GET_MORE_TASKS,
                endpoint=
                'http://nysdoccslookup.doccs.ny.gov/GCA00P00/WIQ3/WINQ130',
                post_data={'DFH_MAP_STATE_TOKEN': '',
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
                custom={'din': '2222bbb'},
            ), Task(
                task_type=constants.TaskType.GET_MORE_TASKS,
                endpoint=
                'http://nysdoccslookup.doccs.ny.gov/GCA00P00/WIQ3/WINQ130',
                post_data={'DFH_MAP_STATE_TOKEN': '',
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
                custom={'din': '3333ccc'},
            ), Task(
                task_type=constants.TaskType.GET_MORE_TASKS,
                endpoint=
                'http://nysdoccslookup.doccs.ny.gov/GCA00P00/WIQ3/WINQ130',
                post_data={'DFH_MAP_STATE_TOKEN': '',
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
                custom={'din': '4444ddd'},
            ), Task(
                task_type=constants.TaskType.GET_MORE_TASKS,
                endpoint=
                'http://nysdoccslookup.doccs.ny.gov/GCA00P00/WIQ3/WINQ130',
                post_data={'DFH_MAP_STATE_TOKEN': '',
                           'DFH_STATE_TOKEN': 'abcdefgh',
                           'K01': 'WINQ130',
                           'K02': '1234567',
                           'K03': '',
                           'K04': '1',
                           'K05': '2',
                           'K06': '1',
                           'M13_PAGE_CLICKI': 'Y',
                           'M13_SEL_DINI': ''},
            )
        ]

        task = Task(
            task_type=constants.TaskType.GET_MORE_TASKS,
            endpoint='',
        )

        self.validate_and_return_get_more_tasks(
            _SEARCH_RESULTS_PAGE_HTML, task, expected)


class TestScraperDetailsPage(BaseScraperTest, unittest.TestCase):
    """Test parsing the UsNy person details page
    """

    def _init_scraper_and_yaml(self):
        self.scraper = UsNyScraper()

    def test_parse(self):

        expected = [Task(
            task_type=constants.TaskType.SCRAPE_DATA,
            endpoint='',
            content=html.tostring(_DETAILS_PAGE_HTML, encoding='unicode'),
        )]


        task = Task(
            task_type=constants.TaskType.GET_MORE_TASKS,
            endpoint='',
        )

        self.validate_and_return_get_more_tasks(
            _DETAILS_PAGE_HTML, task, expected)


class TestIngest(BaseScraperTest, unittest.TestCase):
    """Test ingesting the data from the UsNy person detail page.
    """

    def _init_scraper_and_yaml(self):
        self.scraper = UsNyScraper()

    def test_parse(self):

        expected_sentence = Sentence(
            min_length='0008 Years, 04 Months,\n                00 Days',
            max_length='0025 Years, 00 Months,\n                00 Days')
        expected_info = IngestInfo(people=[Person(
            birthdate='04/22/1972',
            bookings=[
                Booking(
                    admission_date='05/10/2013',
                    charges=[
                        Charge(
                            attempted='True',
                            charge_class='FELONY',
                            degree='1ST',
                            level='TWO',
                            name='ATT MANSLAUGHTER',
                            status='SENTENCED',
                            sentence=expected_sentence,
                            ),
                        Charge(
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
            full_name='SIMPSON, BART',
        )])

        task = Task(
            task_type=constants.TaskType.SCRAPE_DATA,
            endpoint=None,
            content=html.tostring(_DETAILS_PAGE_HTML, encoding='unicode'),
        )

        self.validate_and_return_populate_data(
            _DETAILS_PAGE_HTML, expected_info, task=task)

    def test_parse_white_hispanic(self):
        expected_sentence = Sentence(
            min_length='0008 Years, 04 Months,\n                00 Days',
            max_length='0025 Years, 00 Months,\n                00 Days')
        expected_info = IngestInfo(people=[Person(
            birthdate='04/22/1972',
            bookings=[
                Booking(
                    admission_date='05/10/2013',
                    charges=[
                        Charge(
                            attempted='True',
                            charge_class='FELONY',
                            degree='1ST',
                            level='TWO',
                            name='ATT MANSLAUGHTER',
                            status='SENTENCED',
                            sentence=expected_sentence,
                        ),
                        Charge(
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
            full_name='SIMPSON, BART',
        )])

        task = Task(
            task_type=constants.TaskType.SCRAPE_DATA,
            endpoint=None,
            content=html.tostring(
                _DETAILS_PAGE_WHITE_HISPANIC_HTML, encoding='unicode'),
        )

        self.validate_and_return_populate_data(
            _DETAILS_PAGE_WHITE_HISPANIC_HTML, expected_info, task=task)

    def test_parse_black_hispanic(self):
        expected_sentence = Sentence(
            min_length='0008 Years, 04 Months,\n                00 Days',
            max_length='0025 Years, 00 Months,\n                00 Days')
        expected_info = IngestInfo(people=[Person(
            birthdate='04/22/1972',
            bookings=[
                Booking(
                    admission_date='05/10/2013',
                    charges=[
                        Charge(
                            attempted='True',
                            charge_class='FELONY',
                            degree='1ST',
                            level='TWO',
                            name='ATT MANSLAUGHTER',
                            status='SENTENCED',
                            sentence=expected_sentence,
                        ),
                        Charge(
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
            full_name='SIMPSON, BART',
        )])

        task = Task(
            task_type=constants.TaskType.SCRAPE_DATA,
            endpoint=None,
            content=html.tostring(
                _DETAILS_PAGE_BLACK_HISPANIC_HTML, encoding='unicode'),
        )

        self.validate_and_return_populate_data(
            _DETAILS_PAGE_BLACK_HISPANIC_HTML, expected_info, task=task)

    def test_parse_life_sentence(self):
        expected_sentence = Sentence(
            min_length='0008 Years, 04 Months,\n                00 Days',
            is_life='True')
        expected_info = IngestInfo(people=[Person(
            birthdate='04/22/1972',
            bookings=[
                Booking(
                    admission_date='05/10/2013',
                    charges=[
                        Charge(
                            attempted='True',
                            charge_class='FELONY',
                            degree='1ST',
                            level='TWO',
                            name='ATT MANSLAUGHTER',
                            status='SENTENCED',
                            sentence=expected_sentence,
                        ),
                        Charge(
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
            full_name='SIMPSON, BART',
        )])

        task = Task(
            task_type=constants.TaskType.SCRAPE_DATA,
            endpoint=None,
            content=html.tostring(
                _DETAILS_PAGE_LIFE_SENTENCE_HTML, encoding='unicode'),
        )

        self.validate_and_return_populate_data(
            _DETAILS_PAGE_LIFE_SENTENCE_HTML, expected_info, task=task)

    def test_parse_no_release_date(self):
        expected_sentence = Sentence(
            min_length='0008 Years, 04 Months,\n                00 Days',
            max_length='0025 Years, 00 Months,\n                00 Days')
        expected_info = IngestInfo(people=[Person(
            birthdate='04/22/1972',
            bookings=[
                Booking(
                    admission_date='05/10/2013',
                    charges=[
                        Charge(
                            attempted='True',
                            charge_class='FELONY',
                            degree='1ST',
                            level='TWO',
                            name='ATT MANSLAUGHTER',
                            status='SENTENCED',
                            sentence=expected_sentence,
                        ),
                        Charge(
                            attempted='False',
                            charge_class='FELONY',
                            level='TWO',
                            name='ARMED ROBBERY',
                            status='SENTENCED',
                            sentence=expected_sentence,
                        )
                    ],
                    custody_status='DISCHARGED',
                    facility='QUEENSBORO',
                )],
            gender='MALE',
            person_id='1234567',
            race='BLACK',
            ethnicity='HISPANIC',
            full_name='SIMPSON, BART',
        )])

        task = Task(
            task_type=constants.TaskType.SCRAPE_DATA,
            endpoint=None,
            content=html.tostring(
                _DETAILS_PAGE_NO_RELEASE_HTML, encoding='unicode'),
        )

        self.validate_and_return_populate_data(
            _DETAILS_PAGE_NO_RELEASE_HTML, expected_info, task=task)
