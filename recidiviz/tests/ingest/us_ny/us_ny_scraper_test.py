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

"""Tests for the New York scraper: ingest/us_ny/us_ny_scraper.py."""

import json
from datetime import date
from datetime import datetime

from lxml import html
from mock import patch
import responses

from google.appengine.ext import ndb
from google.appengine.ext.db import InternalError
from google.appengine.ext import testbed

import requests

from recidiviz.ingest.models.scrape_key import ScrapeKey
from recidiviz.ingest.sessions import ScrapeSession
from recidiviz.ingest.us_ny.us_ny_person import UsNyPerson
from recidiviz.ingest.us_ny.us_ny_record import UsNyRecord
from recidiviz.ingest.us_ny.us_ny_scraper import UsNyScraper
from recidiviz.models.record import Offense, SentenceDuration
from recidiviz.models.snapshot import Snapshot
from recidiviz.tests.ingest.matchers import DeserializedJson
from recidiviz.tests.ingest import fixtures


SEARCH_PAGE = fixtures.as_string('us_ny', 'search_page.html')

SEARCH_RESULTS_PAGE = fixtures.as_string('us_ny', 'search_results_page.html')

SEARCH_RESULTS_PAGE_MISSING_NEXT = fixtures.as_string(
    'us_ny', 'search_results_page_missing_next.html')

PERSON_PAGE = fixtures.as_string('us_ny', 'person_page.html')


class TestScrapeSearchPage(object):
    """Tests for the scrape_search_page method."""

    @responses.activate
    @patch("google.appengine.api.taskqueue.add")
    @patch("recidiviz.ingest.scraper_utils.get_headers")
    @patch("recidiviz.ingest.scraper_utils.get_proxies")
    def test_expected_html_background(self,
                                      mock_proxies,
                                      mock_headers,
                                      mock_taskqueue):
        """Tests the happy path case for background scrapes."""
        scraper = UsNyScraper()
        scrape_type = 'background'

        setup_mocks(mock_proxies, mock_headers, mock_taskqueue)

        responses.add(responses.GET, 'http://nysdoccslookup.doccs.ny.gov',
                      body=SEARCH_PAGE, status=200)

        result = scraper.scrape_search_page({'scrape_type': scrape_type,
                                             'content': 'AAARDVARK'})
        assert result is None

        verify_mocks(mock_proxies, mock_headers, mock_taskqueue, 1)

        task_params = json.dumps({'first_page': True,
                                  'k01': 'WINQ000',
                                  'token': 'abcdefgh',
                                  'action': '/GCA00P00/WIQ1/WINQ000',
                                  'scrape_type': scrape_type,
                                  'content': 'AAARDVARK'})
        mock_taskqueue.assert_called_with(url=scraper.scraper_work_url,
                                          queue_name='us-ny-scraper',
                                          params={
                                              'region': 'us_ny',
                                              'task':
                                                  'scrape_search_results_page',
                                              'params': task_params
                                          })

    @responses.activate
    @patch("google.appengine.api.taskqueue.add")
    @patch("recidiviz.ingest.scraper_utils.get_headers")
    @patch("recidiviz.ingest.scraper_utils.get_proxies")
    def test_expected_html_snapshot(self,
                                    mock_proxies,
                                    mock_headers,
                                    mock_taskqueue):
        """Tests the happy path case for snapshot scrapes."""
        scraper = UsNyScraper()
        scrape_type = 'snapshot'

        setup_mocks(mock_proxies, mock_headers, mock_taskqueue)

        responses.add(responses.GET, 'http://nysdoccslookup.doccs.ny.gov',
                      body=SEARCH_PAGE, status=200)

        result = scraper.scrape_search_page({'scrape_type': scrape_type,
                                             'content': ('123abc', [])})
        assert result is None

        verify_mocks(mock_proxies, mock_headers, mock_taskqueue, 1)

        task_params = json.dumps({'first_page': True,
                                  'k01': 'WINQ000',
                                  'token': 'abcdefgh',
                                  'action': '/GCA00P00/WIQ1/WINQ000',
                                  'scrape_type': scrape_type,
                                  'content': ('123abc', [])})
        mock_taskqueue.assert_called_with(url=scraper.scraper_work_url,
                                          queue_name='us-ny-scraper',
                                          params={
                                              'region': 'us_ny',
                                              'task':
                                                  'scrape_person',
                                              'params': task_params
                                          })

    @responses.activate
    @patch("recidiviz.ingest.scraper_utils.get_headers")
    @patch("recidiviz.ingest.scraper_utils.get_proxies")
    def test_unexpected_html_no_session_key(self, mock_proxies, mock_headers):
        """Tests the case where there's no sesion key in the HTML."""
        scraper = UsNyScraper()
        scrape_type = 'background'

        setup_mocks(mock_proxies, mock_headers)

        responses.add(responses.GET, 'http://nysdoccslookup.doccs.ny.gov',
                      body='<p>This is valid but unexpected</p>', status=200)

        result = scraper.scrape_search_page({'scrape_type': scrape_type})
        assert result == -1

        verify_mocks(mock_proxies, mock_headers)

    @responses.activate
    @patch("recidiviz.ingest.scraper_utils.get_headers")
    @patch("recidiviz.ingest.scraper_utils.get_proxies")
    def test_invalid_html(self, mock_proxies, mock_headers):
        """Tests the case where there's an invalid response body."""
        scraper = UsNyScraper()
        scrape_type = 'snapshot'

        setup_mocks(mock_proxies, mock_headers)

        responses.add(responses.GET, 'http://nysdoccslookup.doccs.ny.gov',
                      body='<!<!', status=200)

        result = scraper.scrape_search_page({'scrape_type': scrape_type})
        assert result == -1

        verify_mocks(mock_proxies, mock_headers)

    @responses.activate
    @patch("recidiviz.ingest.scraper_utils.get_headers")
    @patch("recidiviz.ingest.scraper_utils.get_proxies")
    def test_error_response(self, mock_proxies, mock_headers):
        """Tests the case where the server returns an actual error."""
        scraper = UsNyScraper()
        scrape_type = 'snapshot'

        setup_mocks(mock_proxies, mock_headers)

        responses.add(responses.GET, 'http://nysdoccslookup.doccs.ny.gov',
                      body=requests.exceptions.RequestException(), status=400)

        result = scraper.scrape_search_page({'scrape_type': scrape_type})
        assert result == -1

        verify_mocks(mock_proxies, mock_headers)


class TestScrapeSearchResultsPage(object):
    """Tests for the scrape_search_results_page method."""

    def setup_method(self, _test_method):
        # noinspection PyAttributeOutsideInit
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        context = ndb.get_context()
        context.set_memcache_policy(False)
        context.clear_cache()

        # root_path must be set the the location of queue.yaml.
        # Otherwise, only the 'default' queue will be available.
        self.testbed.init_taskqueue_stub(root_path='.')

        # noinspection PyAttributeOutsideInit
        self.taskqueue_stub = self.testbed.get_stub(
            testbed.TASKQUEUE_SERVICE_NAME)

    def teardown_method(self, _test_method):
        self.testbed.deactivate()

    @responses.activate
    @patch("recidiviz.ingest.sessions.update_session")
    @patch("google.appengine.api.taskqueue.add")
    @patch("recidiviz.ingest.scraper_utils.get_headers")
    @patch("recidiviz.ingest.scraper_utils.get_proxies")
    def test_expected_html_first_page(self,
                                      mock_proxies,
                                      mock_headers,
                                      mock_taskqueue,
                                      mock_session):
        """Tests the happy path case for first page search result scrapes."""
        scraper = UsNyScraper()
        scrape_type = 'background'
        action = '/GCA00P00/WIQ1/WINQ000'

        setup_mocks(mock_proxies, mock_headers, mock_taskqueue)
        mock_session.return_value = True

        responses.add(responses.POST, 'http://nysdoccslookup.doccs.ny.gov'
                      + action,
                      body=SEARCH_RESULTS_PAGE, status=200)

        result = scraper.scrape_search_results_page({
            'first_page': True,
            'k01': 'WINQ000',
            'token': 'abcdefgh',
            'action': '/GCA00P00/WIQ1/WINQ000',
            'scrape_type': scrape_type,
            'content': ('AAARDVARK', '')
        })
        assert result is None

        verify_mocks(mock_proxies, mock_headers, mock_taskqueue, 5)
        mock_session.assert_called_with('AARDVARK, ARTHUR',
                                        ScrapeKey('us_ny', scrape_type))

        next_search_task_params = {
            'action': '/GCA00P00/WIQ3/WINQ130',
            'clicki': 'Y',
            'dini': '',
            'k01': 'WINQ130',
            'k02': '1234567',
            'k03': '',
            'k04': '1',
            'k05': '2',
            'k06': '1',
            'token': 'abcdefgh',
            'map_token': '',
            'next': 'Next 4 Inmate Names',
            'content': ['AAARDVARK', ''],
            'scrape_type': scrape_type
        }
        mock_taskqueue.assert_any_call(
            url=scraper.scraper_work_url,
            queue_name='us-ny-scraper',
            params={
                'region': 'us_ny',
                'task': 'scrape_search_results_page',
                'params': DeserializedJson(next_search_task_params)
            }
        )
        self.assert_scrape_person_calls(scraper, scrape_type, mock_taskqueue)

    @responses.activate
    @patch("recidiviz.ingest.sessions.update_session")
    @patch("google.appengine.api.taskqueue.add")
    @patch("recidiviz.ingest.scraper_utils.get_headers")
    @patch("recidiviz.ingest.scraper_utils.get_proxies")
    def test_expected_html_subsequent_page(self,
                                           mock_proxies,
                                           mock_headers,
                                           mock_taskqueue,
                                           mock_session):
        """Tests the happy path case subsequent page search result scrapes."""
        scraper = UsNyScraper()
        scrape_type = 'background'
        action = '/GCA00P00/WIQ3/WINQ130'

        setup_mocks(mock_proxies, mock_headers, mock_taskqueue)
        mock_session.return_value = True

        responses.add(responses.POST, 'http://nysdoccslookup.doccs.ny.gov'
                      + action,
                      body=SEARCH_RESULTS_PAGE, status=200)

        result = scraper.scrape_search_results_page({
            'action': action,
            'clicki': 'Y',
            'dini': '',
            'k01': 'WINQ130',
            'k02': '1234567',
            'k03': '',
            'k04': '1',
            'k05': '2',
            'k06': '1',
            'token': 'abcdefgh',
            'map_token': '',
            'next': 'Next 4 Inmate Names',
            'content': ['AAARDVARK', ''],
            'scrape_type': scrape_type
        })
        assert result is None

        verify_mocks(mock_proxies, mock_headers, mock_taskqueue, 5)
        mock_session.assert_called_with('AARDVARK, ARTHUR',
                                        ScrapeKey('us_ny', scrape_type))

        next_search_task_params = {
            'action': '/GCA00P00/WIQ3/WINQ130',
            'clicki': 'Y',
            'dini': '',
            'k01': 'WINQ130',
            'k02': '1234567',
            'k03': '',
            'k04': '1',
            'k05': '2',
            'k06': '1',
            'token': 'abcdefgh',
            'map_token': '',
            'next': 'Next 4 Inmate Names',
            'content': ['AAARDVARK', ''],
            'scrape_type': scrape_type
        }
        mock_taskqueue.assert_any_call(
            url=scraper.scraper_work_url,
            queue_name='us-ny-scraper',
            params={
                'region': 'us_ny',
                'task': 'scrape_search_results_page',
                'params': DeserializedJson(next_search_task_params)
            }
        )
        self.assert_scrape_person_calls(scraper, scrape_type, mock_taskqueue)

    @responses.activate
    @patch("recidiviz.ingest.scraper_utils.get_headers")
    @patch("recidiviz.ingest.scraper_utils.get_proxies")
    def test_error_response(self, mock_proxies, mock_headers):
        """Tests the case where the server returns an actual error."""
        scraper = UsNyScraper()
        scrape_type = 'snapshot'
        action = '/GCA00P00/WIQ1/WINQ000'

        setup_mocks(mock_proxies, mock_headers)

        responses.add(responses.POST, 'http://nysdoccslookup.doccs.ny.gov'
                      + action,
                      body=requests.exceptions.RequestException(), status=400)

        result = scraper.scrape_search_results_page({
            'first_page': True,
            'k01': 'WINQ000',
            'token': 'abcdefgh',
            'action': '/GCA00P00/WIQ1/WINQ000',
            'scrape_type': scrape_type,
            'content': ('AAARDVARK', '')
        })
        assert result == -1

        verify_mocks(mock_proxies, mock_headers)

    @responses.activate
    @patch("recidiviz.ingest.sessions.update_session")
    @patch("google.appengine.api.taskqueue.add")
    @patch("recidiviz.ingest.scraper_utils.get_headers")
    @patch("recidiviz.ingest.scraper_utils.get_proxies")
    def test_subsequent_page_no_session_to_update(self,
                                                  mock_proxies,
                                                  mock_headers,
                                                  mock_taskqueue,
                                                  mock_session):
        """Tests the subsequent page case where there is no session to update.
        """
        scraper = UsNyScraper()
        scrape_type = 'background'
        action = '/GCA00P00/WIQ3/WINQ130'

        setup_mocks(mock_proxies, mock_headers, mock_taskqueue)
        mock_session.return_value = None

        responses.add(responses.POST, 'http://nysdoccslookup.doccs.ny.gov'
                      + action,
                      body=SEARCH_RESULTS_PAGE, status=200)

        result = scraper.scrape_search_results_page({
            'action': action,
            'clicki': 'Y',
            'dini': '',
            'k01': 'WINQ130',
            'k02': '1234567',
            'k03': '',
            'k04': '1',
            'k05': '2',
            'k06': '1',
            'token': 'abcdefgh',
            'map_token': '',
            'next': 'Next 4 Inmate Names',
            'content': ['AAARDVARK', ''],
            'scrape_type': scrape_type
        })
        assert result == -1

        verify_mocks(mock_proxies, mock_headers, mock_taskqueue, 4)
        mock_session.assert_called_with('AARDVARK, ARTHUR',
                                        ScrapeKey('us_ny', scrape_type))
        self.assert_scrape_person_calls(scraper, scrape_type, mock_taskqueue)

    @responses.activate
    @patch("google.appengine.api.taskqueue.add")
    @patch("recidiviz.ingest.scraper_utils.get_headers")
    @patch("recidiviz.ingest.scraper_utils.get_proxies")
    def test_subsequent_page_parsing_failure(self,
                                             mock_proxies,
                                             mock_headers,
                                             mock_taskqueue):
        """Performs the legwork for the two parsing failure test cases above."""
        scraper = UsNyScraper()
        scrape_type = 'background'
        action = '/GCA00P00/WIQ3/WINQ130'

        setup_mocks(mock_proxies, mock_headers, mock_taskqueue)

        responses.add(responses.POST, 'http://nysdoccslookup.doccs.ny.gov'
                      + action,
                      body=SEARCH_RESULTS_PAGE_MISSING_NEXT, status=200)

        result = scraper.scrape_search_results_page({
            'action': action,
            'clicki': 'Y',
            'dini': '',
            'k01': 'WINQ130',
            'k02': '1234567',
            'k03': '',
            'k04': '1',
            'k05': '2',
            'k06': '1',
            'token': 'abcdefgh',
            'map_token': '',
            'next': 'Next 4 Inmate Names',
            'content': ['AAARDVARK', ''],
            'scrape_type': scrape_type
        })
        assert result is None

        verify_mocks(mock_proxies, mock_headers, mock_taskqueue, 4)
        self.assert_scrape_person_calls(scraper, scrape_type, mock_taskqueue)

    def assert_scrape_person_calls(self, scraper, scrape_type, mock_taskqueue):
        dinis = ['1111aaa', '2222bbb', '3333ccc', '4444ddd']

        for dini in dinis:
            task_params = {
                'action': '/GCA00P00/WIQ3/WINQ130',
                'clicki': '',
                'dini': dini,
                'k01': 'WINQ130',
                'k02': '1234567',
                'k03': '',
                'k04': '1',
                'k05': '2',
                'k06': '1',
                'token': 'abcdefgh',
                'map_token': '',
                'dinx_name': 'din' + dini[0],
                'dinx_val': dini,
                'content': ['AAARDVARK', ''],
                'scrape_type': scrape_type
            }

            mock_taskqueue.assert_any_call(
                url=scraper.scraper_work_url,
                queue_name='us-ny-scraper',
                params={
                    'region': 'us_ny',
                    'task': 'scrape_person',
                    'params': DeserializedJson(task_params)
                }
            )


def test_get_initial_task():
    scraper = UsNyScraper()
    assert scraper.get_initial_task() == 'scrape_search_page'


def test_gather_details_error_missing_tables():
    """Tests that the gathers_detail method properly handles a page without
    an expected table."""
    unexpected_html = """
    <div id="ii">
    <h2 class="aligncenter">Inmate Information</h2>
    <p class="err"></p>
    <table cellpadding="2" cellspacing="0" summary="Inmate Identifying and
     location information">
      <caption>Identifying and Location Information<br>
       <span class="pcap">As of 10/11/18</span></caption>
      <tbody><tr>
        <td scope="row" id="t1a">
         <a href="http://www.doccs.ny.gov/univinq/fpmsdoc.htm#din" 
            title="Definition of DIN (Department Identification Number)">
         Cowabunga</a></td>
        <td headers="t1a">1234567 &nbsp;</td>
      </tr></tbody>
    </table>
    </div>
    """

    page_tree = html.fromstring(unexpected_html)
    details_page = page_tree.xpath('//div[@id="ii"]')

    scraper = UsNyScraper()
    assert scraper.gather_details(page_tree, details_page) == -1


def test_gather_details_error_unexpected_table_content():
    """Tests that the gathers_detail method properly handles a table with
    an unexpected header row."""
    unexpected_html = """
    <div id="ii">
    <h2 class="aligncenter">Inmate Information</h2>
    <p class="err"></p>
    <table cellpadding="2" cellspacing="0" summary="Inmate Identifying and
     location information">
      <caption>Identifying and Location Information<br>
       <span class="pcap">As of 10/11/18</span></caption>
      <tbody><tr>
        <td scope="row" id="t1a">
         <a href="http://www.doccs.ny.gov/univinq/fpmsdoc.htm#din" 
            title="Definition of DIN (Department Identification Number)">
         DIN (Department Identification Number)</a></td>
        <td headers="t1a">1234567 &nbsp;</td>
      </tr></tbody>
    </table>
    <table cellpadding="2" cellspacing="0" summary="Inmate crimes of
     conviction">
      <tbody><tr>
        <th scope="col" id="crime">Crime</th>
        <th scope="col" id="class">Class</th>
      </tr></tbody>
    </table>
    <table cellpadding="2" cellspacing="0" summary="Inmate sentence terms and
     release dates">
      <caption>Sentence Terms and Release Dates<br>
       <span class="pcap">Under certain circumstances, an inmate may be
       released prior to serving his or her minimum term and before the
       earliest release date shown for the inmate.<br>
       As of 10/11/18</span></caption>
      <tbody><tr>
        <td scope="row" id="t3a">
         <a href="http://www.doccs.ny.gov/univinq/fpmsdoc.htm#agg" 
            title="Definition of Aggregate Minimum Sentence">
         MAXIMUM SENTENCE</a></td>
        <td headers="t3a">0008 Years, 04 Months,
         00 Days</td>
      </tr></tbody>
    </table>
    </div>
    """

    page_tree = html.fromstring(unexpected_html)
    details_page = page_tree.xpath('//div[@id="ii"]')

    scraper = UsNyScraper()
    assert scraper.gather_details(page_tree, details_page) == -1


class TestCreatePerson(object):
    """Tests that the creation of new Persons works and captures all
    possible fields."""

    FIELDS_NOT_SET = ['alias', 'suffix']

    def setup_method(self, _test_method):
        # noinspection PyAttributeOutsideInit
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        context = ndb.get_context()
        context.set_memcache_policy(False)
        context.clear_cache()

        # root_path must be set the the location of queue.yaml.
        # Otherwise, only the 'default' queue will be available.
        self.testbed.init_taskqueue_stub(root_path='.')

        # noinspection PyAttributeOutsideInit
        self.taskqueue_stub = self.testbed.get_stub(
            testbed.TASKQUEUE_SERVICE_NAME)

    def teardown_method(self, _test_method):
        self.testbed.deactivate()

    def test_create_person(self):
        """Tests the happy path for create_person."""
        scraper = UsNyScraper()

        page_tree = html.fromstring(PERSON_PAGE)
        details_page = page_tree.xpath('//div[@id="ii"]')
        person_details = scraper.gather_details(page_tree, details_page)

        actual = scraper.create_person(person_details)

        assert actual.birthdate == date(1972, 4, 22)
        assert actual.age >= 46
        assert actual.sex == 'male'
        assert actual.race == 'white'
        assert actual.surname == 'SIMPSON'
        assert actual.given_names == 'BART'
        assert actual.region == 'us_ny'

        # pylint:disable=protected-access
        person_attributes = UsNyPerson._properties
        unset_attributes = [attribute for attribute in person_attributes
                            if attribute != 'class'
                            and getattr(actual, attribute) is None]

        assert all(attr in TestCreatePerson.FIELDS_NOT_SET
                   for attr in unset_attributes)

    def test_create_person_linked_already(self):
        scraper = UsNyScraper()

        page_tree = html.fromstring(PERSON_PAGE)
        details_page = page_tree.xpath('//div[@id="ii"]')
        person_details = scraper.gather_details(page_tree, details_page)
        person_details['linked_records'] = ['55aa66', '66bb77']

        person_a_key = UsNyPerson(person_id='aaaaa').put()
        person_b_key = UsNyPerson(person_id='bbbbb').put()

        UsNyRecord(parent=person_a_key, record_id='55aa66').put()
        UsNyRecord(parent=person_b_key, record_id='6bb77').put()

        actual = scraper.create_person(person_details)
        assert actual.person_id == 'aaaaa'
        assert actual.us_ny_person_id == 'aaaaa'

    def test_create_person_group_id(self):
        group_id = '45678'
        scraper = UsNyScraper()

        page_tree = html.fromstring(PERSON_PAGE)
        details_page = page_tree.xpath('//div[@id="ii"]')
        person_details = scraper.gather_details(page_tree, details_page)
        person_details['group_id'] = group_id

        actual = scraper.create_person(person_details)
        assert actual.person_id == group_id
        assert actual.us_ny_person_id == group_id


class TestCreateRecord(object):
    """Tests that the creation of new Records works and captures all
    possible fields."""

    FIELDS_NOT_SET = ['community_supervision_agency',
                      'status',
                      'case_worker',
                      'committed_by',
                      'offense_date',
                      'parole_officer',
                      'record_id_is_fuzzy',
                      'release_date']

    def setup_method(self, _test_method):
        # noinspection PyAttributeOutsideInit
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        context = ndb.get_context()
        context.set_memcache_policy(False)
        context.clear_cache()

        # root_path must be set the the location of queue.yaml.
        # Otherwise, only the 'default' queue will be available.
        self.testbed.init_taskqueue_stub(root_path='.')

        # noinspection PyAttributeOutsideInit
        self.taskqueue_stub = self.testbed.get_stub(
            testbed.TASKQUEUE_SERVICE_NAME)

    def teardown_method(self, _test_method):
        self.testbed.deactivate()

    def test_create_record(self):
        """Tests the happy path for create_record."""
        scraper = UsNyScraper()

        page_tree = html.fromstring(PERSON_PAGE)
        details_page = page_tree.xpath('//div[@id="ii"]')
        person_details = scraper.gather_details(page_tree, details_page)

        person = scraper.create_person(person_details)
        person_key = person.put()

        _old_record, actual = scraper.create_record(person,
                                                    person_key,
                                                    person_details)

        offenses = [
            Offense(
                crime_description='MANSLAUGHTER 1ST',
                crime_class='B'
            ),
            Offense(
                crime_description='ARMED ROBBERY',
                crime_class='B'
            )
        ]

        assert actual.admission_type == ''
        assert actual.birthdate == date(1972, 4, 22)
        assert actual.cond_release_date == date(2014, 8, 13)
        assert actual.county_of_commit == 'KINGS'
        assert actual.custody_date == date(1991, 5, 16)
        assert actual.custody_status == 'RELEASED'
        assert actual.earliest_release_date == date(1998, 7, 4)
        assert actual.earliest_release_type == ''
        assert actual.is_released
        assert actual.last_custody_date == date(2013, 5, 10)
        assert actual.latest_release_date == date(2014, 4, 7)
        assert actual.latest_release_type == 'PAROLE DIV OF PAROLE'
        assert actual.latest_facility == 'QUEENSBORO'
        assert actual.max_expir_date == date(2015, 6, 1)
        assert actual.max_expir_date_parole == date(2019, 2, 1)
        assert actual.max_expir_date_superv == date(2020, 1, 1)
        assert actual.max_sentence_length == SentenceDuration(
            life_sentence=False,
            years=25,
            months=0,
            days=0)
        assert actual.min_sentence_length == SentenceDuration(
            life_sentence=False,
            years=8,
            months=4,
            days=0)
        assert actual.parole_elig_date == date(1998, 6, 28)
        assert actual.parole_discharge_date == date(2008, 6, 1)
        assert actual.parole_hearing_date == date(2014, 2, 1)
        assert actual.parole_hearing_type == 'PAROLE VIOLATOR ' \
                                             'ASSESSED EXPIRATION'
        assert actual.offense == offenses
        assert actual.race == 'white'
        assert actual.region == 'us_ny'
        assert actual.sex == 'male'
        assert actual.surname == 'SIMPSON'
        assert actual.given_names == 'BART'

        # pylint:disable=protected-access
        record_attributes = UsNyRecord._properties
        unset_attributes = [attribute for attribute in record_attributes
                            if attribute != 'class'
                            and getattr(actual, attribute) is None]

        print unset_attributes

        assert all(attr in TestCreateRecord.FIELDS_NOT_SET
                   for attr in unset_attributes)


class TestRecordToSnapshot(object):
    """Tests that the translation between Records and Snapshots works and
    captures all possible fields."""

    FIELDS_NOT_SET = ['offense_date']

    def setup_method(self, _test_method):
        # noinspection PyAttributeOutsideInit
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        context = ndb.get_context()
        context.set_memcache_policy(False)
        context.clear_cache()

        # root_path must be set the the location of queue.yaml.
        # Otherwise, only the 'default' queue will be available.
        self.testbed.init_taskqueue_stub(root_path='.')

        # noinspection PyAttributeOutsideInit
        self.taskqueue_stub = self.testbed.get_stub(
            testbed.TASKQUEUE_SERVICE_NAME)

    def teardown_method(self, _test_method):
        self.testbed.deactivate()

    def test_record_to_snapshot(self):
        """Tests the happy path for record_to_snapshot."""
        offense = Offense(
            crime_description='MANSLAUGHTER 1ST',
            crime_class='B'
        )

        record = UsNyRecord(
            admission_type='REVOCATION',
            birthdate=datetime(1972, 4, 22),
            cond_release_date=datetime(2006, 8, 14),
            county_of_commit='KINGS',
            custody_date=datetime(1991, 3, 14),
            custody_status='RELEASED',
            earliest_release_date=datetime(2002, 8, 14),
            earliest_release_type='PAROLE',
            is_released=True,
            last_custody_date=datetime(1994, 7, 1),
            latest_release_date=datetime(2002, 10, 28),
            latest_release_type='PAROLE',
            latest_facility='QUEENSBORO',
            max_expir_date=datetime(2010, 1, 14),
            max_expir_date_parole=datetime(2010, 1, 14),
            max_expir_date_superv=datetime(2010, 1, 14),
            max_sentence_length=SentenceDuration(
                life_sentence=False,
                years=18,
                months=10,
                days=0),
            min_sentence_length=SentenceDuration(
                life_sentence=False,
                years=11,
                months=5,
                days=0),
            parole_elig_date=datetime(2002, 8, 14),
            parole_discharge_date=datetime(2002, 8, 14),
            parole_hearing_date=datetime(2002, 6, 17),
            parole_hearing_type='INITIAL HEARING',
            offense=[offense],
            race='WHITE',
            record_id='1234567',
            region='us_ny',
            sex='MALE',
            surname='SIMPSON',
            given_names='BART',
            us_ny_record_id='1234567'
        )

        scraper = UsNyScraper()
        snapshot = scraper.record_to_snapshot(record)
        assert snapshot == Snapshot(
            parent=record.key,
            admission_type=record.admission_type,
            birthdate=record.birthdate,
            cond_release_date=record.cond_release_date,
            county_of_commit=record.county_of_commit,
            custody_date=record.custody_date,
            custody_status=record.custody_status,
            earliest_release_date=record.earliest_release_date,
            earliest_release_type=record.earliest_release_type,
            is_released=record.is_released,
            last_custody_date=record.last_custody_date,
            latest_facility=record.latest_facility,
            latest_release_date=record.latest_release_date,
            latest_release_type=record.latest_release_type,
            max_expir_date=record.max_expir_date,
            max_expir_date_parole=record.max_expir_date_parole,
            max_expir_date_superv=record.max_expir_date_superv,
            max_sentence_length=record.max_sentence_length,
            min_sentence_length=record.min_sentence_length,
            offense=record.offense,
            parole_discharge_date=record.parole_discharge_date,
            parole_elig_date=record.parole_elig_date,
            parole_hearing_date=record.parole_hearing_date,
            parole_hearing_type=record.parole_hearing_type,
            race=record.race,
            region=record.region,
            sex=record.sex,
            surname=record.surname,
            given_names=record.given_names
        )
        snapshot.put()

        # pylint:disable=protected-access
        snapshot_attributes = Snapshot._properties
        unset_attributes = [attribute for attribute in snapshot_attributes
                            if attribute != 'class'
                            and getattr(snapshot, attribute) is None]

        print unset_attributes

        assert all(attr in TestRecordToSnapshot.FIELDS_NOT_SET
                   for attr in unset_attributes)


class TestStoreRecord(object):
    """Tests for the store_record method."""

    def setup_method(self, _test_method):
        # noinspection PyAttributeOutsideInit
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        context = ndb.get_context()
        context.set_memcache_policy(False)
        context.clear_cache()

        # root_path must be set the the location of queue.yaml.
        # Otherwise, only the 'default' queue will be available.
        self.testbed.init_taskqueue_stub(root_path='.')

        # noinspection PyAttributeOutsideInit
        self.taskqueue_stub = self.testbed.get_stub(
            testbed.TASKQUEUE_SERVICE_NAME)

    def teardown_method(self, _test_method):
        self.testbed.deactivate()

    def test_store_record(self):
        """Tests the happy path for store_record."""
        scraper = UsNyScraper()
        person_details = self.prepare_details(scraper)

        result = scraper.store_record(person_details)
        assert result is None

        person = UsNyPerson.query(UsNyPerson.surname == 'SIMPSON').get()
        assert person
        assert person.birthdate == date(1972, 4, 22)

        records = UsNyRecord.query(ancestor=person.key).fetch()
        assert len(records) == 1
        assert records[0].custody_date == date(1991, 5, 16)

        snapshots = Snapshot.query(ancestor=records[0].key).fetch()
        assert len(snapshots) == 1

    @patch("recidiviz.ingest.us_ny.us_ny_person.UsNyPerson.put")
    def test_error_saving_person(self, mock_put):
        mock_put.side_effect = InternalError()

        scraper = UsNyScraper()
        person_details = self.prepare_details(scraper)
        result = scraper.store_record(person_details)

        assert result == -1
        mock_put.assert_called_with()

    @patch("recidiviz.ingest.us_ny.us_ny_record.UsNyRecord.put")
    def test_error_saving_record(self, mock_put):
        mock_put.side_effect = InternalError()

        scraper = UsNyScraper()
        person_details = self.prepare_details(scraper)
        result = scraper.store_record(person_details)

        assert result == -1
        mock_put.assert_called_with()

        person = UsNyPerson.query(UsNyPerson.surname == 'SIMPSON').get()
        assert person
        assert person.birthdate == date(1972, 4, 22)

    @staticmethod
    def prepare_details(scraper):
        page_tree = html.fromstring(PERSON_PAGE)
        details_page = page_tree.xpath('//div[@id="ii"]')
        return scraper.gather_details(page_tree, details_page)


class TestStopScrapeAndMaybeResume(object):
    """Tests for the stop_scrape_and_maybe_resume method."""

    def setup_method(self, _test_method):
        # noinspection PyAttributeOutsideInit
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        context = ndb.get_context()
        context.set_memcache_policy(False)
        context.clear_cache()

        # root_path must be set the the location of queue.yaml.
        # Otherwise, only the 'default' queue will be available.
        self.testbed.init_taskqueue_stub(root_path='.')

        # noinspection PyAttributeOutsideInit
        self.taskqueue_stub = self.testbed.get_stub(
            testbed.TASKQUEUE_SERVICE_NAME)

    def teardown_method(self, _test_method):
        self.testbed.deactivate()


    @patch("recidiviz.ingest.scraper.Scraper.stop_scrape")
    @patch("recidiviz.ingest.sessions.get_open_sessions")
    def test_no_open_sessions(self, mock_sessions, mock_stop_scrape):
        scrape_type = 'background'
        mock_sessions.return_value = []
        mock_stop_scrape.return_value = None

        scraper = UsNyScraper()
        scraper.stop_scrape_and_maybe_resume(scrape_type)

        mock_sessions.assert_called_with('us_ny', most_recent_only=True)
        mock_stop_scrape.assert_called_with([scrape_type])

    @patch("recidiviz.ingest.scraper.Scraper.stop_scrape")
    @patch("recidiviz.ingest.sessions.get_recent_sessions")
    @patch("recidiviz.ingest.sessions.get_open_sessions")
    def test_nothing_scraped_yet(
            self, mock_sessions, mock_recent_sessions, mock_stop_scrape):
        scrape_type = 'background'

        mock_sessions.return_value = ScrapeSession(region='us_ny',
                                                   scrape_type=scrape_type)

        recent_session = ScrapeSession(region='us_ny',
                                       scrape_type=scrape_type,
                                       last_scraped='ZZZZ, ZZ')
        mock_recent_sessions.return_value = [recent_session]

        mock_stop_scrape.return_value = None

        scraper = UsNyScraper()
        scraper.stop_scrape_and_maybe_resume(scrape_type)

        mock_sessions.assert_called_with('us_ny', most_recent_only=True)
        mock_recent_sessions.assert_called_with(
            ScrapeKey('us_ny', scrape_type))
        mock_stop_scrape.assert_called_with([scrape_type])

    @patch("recidiviz.ingest.scraper.Scraper.stop_scrape")
    @patch("google.appengine.ext.deferred.defer")
    @patch("recidiviz.ingest.sessions.get_open_sessions")
    def test_not_finished_yet(
            self, mock_sessions, mock_deferred, mock_stop_scrape):
        scrape_type = 'background'

        mock_sessions.return_value = ScrapeSession(region='us_ny',
                                                   scrape_type=scrape_type,
                                                   last_scraped='SIMPSON, ABE')
        mock_deferred.return_value = None
        mock_stop_scrape.return_value = None

        scraper = UsNyScraper()
        scraper.stop_scrape_and_maybe_resume(scrape_type)

        mock_sessions.assert_called_with('us_ny', most_recent_only=True)
        mock_deferred.assert_called_with(scraper.resume_scrape,
                                         scrape_type,
                                         _countdown=60)
        mock_stop_scrape.assert_called_with([scrape_type])

    @patch("recidiviz.ingest.scraper.Scraper.stop_scrape")
    @patch("recidiviz.ingest.sessions.get_open_sessions")
    def test_finished(self, mock_sessions, mock_stop_scrape):
        scrape_type = 'background'

        mock_sessions.return_value = ScrapeSession(region='us_ny',
                                                   scrape_type=scrape_type,
                                                   last_scraped='ZYTEL, ABC')
        mock_stop_scrape.return_value = None

        scraper = UsNyScraper()
        scraper.stop_scrape_and_maybe_resume(scrape_type)

        mock_sessions.assert_called_with('us_ny', most_recent_only=True)
        mock_stop_scrape.assert_called_with([scrape_type])


def setup_mocks(mock_proxies, mock_headers, mock_taskqueue=None):
    proxies = {'http': 'http://user:password@proxy.biz/'}
    mock_proxies.return_value = proxies
    headers = {'User-Agent': 'test_user_agent'}
    mock_headers.return_value = headers

    if mock_taskqueue is not None:
        mock_taskqueue.return_value = None


def verify_mocks(mock_proxies, mock_headers, mock_taskqueue=None, task_calls=0):
    mock_proxies.assert_called_with()
    mock_headers.assert_called_with()

    if mock_taskqueue is not None and task_calls > 0:
        assert len(mock_taskqueue.mock_calls) == task_calls
