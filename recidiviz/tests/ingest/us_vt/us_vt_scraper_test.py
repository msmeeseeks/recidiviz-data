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

"""Tests for the Vermont scraper: ingest/us_vt/us_vt_scraper.py."""

import json
from copy import deepcopy
from datetime import datetime

from mock import patch
import responses

from google.appengine.ext import ndb
from google.appengine.ext.db import InternalError
from google.appengine.ext import testbed

import requests

from recidiviz.ingest.us_vt.us_vt_person import UsVtPerson
from recidiviz.ingest.us_vt.us_vt_record import UsVtOffense
from recidiviz.ingest.us_vt.us_vt_scraper import UsVtScraper
from recidiviz.ingest.us_vt.us_vt_snapshot import UsVtSnapshot
from recidiviz.models.record import Record
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.ingest.matchers import DeserializedJson

SESSION = 'e47fszhY4nYPfTEGEoZ9QU3R'
SCRAPE_TYPE = 'background'

FRONT_PAGE_HTML = fixtures.as_string('us_vt', 'front_page.html')

ROSTER_PAGE_JSON = fixtures.as_dict('us_vt', 'roster_page.json')

PERSON_JSON = fixtures.as_dict('us_vt', 'person.json')

# So far, all case data has been empty. See us_vt_scraper.store_record.
CASES_JSON = fixtures.as_dict('us_vt', 'cases.json')

CHARGES_JSON = fixtures.as_dict('us_vt', 'charges.json')


def test_get_initial_task():
    scraper = UsVtScraper()
    assert scraper.get_initial_task() == 'scrape_front_page'


class TestScrapeFrontPage(object):
    """Tests for the UsVtScraper.scrape_front_page functionality."""

    @responses.activate
    @patch("google.appengine.api.taskqueue.add")
    @patch("recidiviz.ingest.scraper_utils.get_headers")
    @patch("recidiviz.ingest.scraper_utils.get_proxies")
    def test_expected_html(self, mock_proxies, mock_headers, mock_taskqueue):
        """Tests the happy path case."""

        scraper = UsVtScraper()

        setup_mocks(mock_proxies, mock_headers, mock_taskqueue)

        responses.add(responses.GET, 'https://omsweb.public-safety-cloud.com'
                                     '/jtclientweb//jailtracker/index/Vermont',
                      body=FRONT_PAGE_HTML, status=200)

        result = scraper.scrape_front_page({'scrape_type': SCRAPE_TYPE})
        assert result is None

        verify_mocks(mock_proxies, mock_headers, mock_taskqueue, 1)

        task_params = json.dumps({'session': SESSION,
                                  'start': 0,
                                  'limit': 10,
                                  'scrape_type': SCRAPE_TYPE})
        mock_taskqueue.assert_called_with(url=scraper.scraper_work_url,
                                          queue_name='us-vt-scraper',
                                          params={
                                              'region': 'us_vt',
                                              'task': 'scrape_roster',
                                              'params': task_params
                                          })

    @responses.activate
    @patch("recidiviz.ingest.scraper_utils.get_headers")
    @patch("recidiviz.ingest.scraper_utils.get_proxies")
    def test_unexpected_html_no_session_key(self, mock_proxies, mock_headers):
        """Tests the case where there's no session key in the HTML."""

        scraper = UsVtScraper()

        setup_mocks(mock_proxies, mock_headers)

        responses.add(responses.GET, 'https://omsweb.public-safety-cloud.com'
                                     '/jtclientweb//jailtracker/index/Vermont',
                      body='<p>This is valid but unexpected</p>', status=200)

        result = scraper.scrape_front_page({'scrape_type': SCRAPE_TYPE})
        assert result == -1

        verify_mocks(mock_proxies, mock_headers)

    @responses.activate
    @patch("recidiviz.ingest.scraper_utils.get_headers")
    @patch("recidiviz.ingest.scraper_utils.get_proxies")
    def test_invalid_html(self, mock_proxies, mock_headers):
        """Tests the case where there's an invalid response body."""

        scraper = UsVtScraper()

        setup_mocks(mock_proxies, mock_headers)

        responses.add(responses.GET, 'https://omsweb.public-safety-cloud.com'
                                     '/jtclientweb//jailtracker/index/Vermont',
                      body='<!<!', status=200)

        result = scraper.scrape_front_page({'scrape_type': SCRAPE_TYPE})
        assert result == -1

        verify_mocks(mock_proxies, mock_headers)

    @responses.activate
    @patch("recidiviz.ingest.scraper_utils.get_headers")
    @patch("recidiviz.ingest.scraper_utils.get_proxies")
    def test_error_response(self, mock_proxies, mock_headers):
        """Tests the case where the server returns an actual error."""

        scraper = UsVtScraper()

        setup_mocks(mock_proxies, mock_headers)

        responses.add(responses.GET, 'https://omsweb.public-safety-cloud.com'
                                     '/jtclientweb//jailtracker/index/Vermont',
                      body=requests.exceptions.RequestException(), status=400)

        result = scraper.scrape_front_page({'scrape_type': SCRAPE_TYPE})
        assert result == -1

        verify_mocks(mock_proxies, mock_headers)


class TestScrapeRoster(object):
    """Tests for the UsVtScraper.scrape_roster functionality."""

    @responses.activate
    @patch("google.appengine.api.taskqueue.add")
    @patch("recidiviz.ingest.scraper_utils.get_headers")
    @patch("recidiviz.ingest.scraper_utils.get_proxies")
    def test_expected_json_done_with_roster(self, mock_proxies, mock_headers,
                                            mock_taskqueue):
        """Tests the happy path case."""
        scraper = UsVtScraper()

        setup_mocks(mock_proxies, mock_headers, mock_taskqueue)

        responses.add(responses.GET, 'https://omsweb.public-safety-cloud.com/'
                                     'jtclientweb/'
                                     '(S(e47fszhY4nYPfTEGEoZ9QU3R))//'
                                     '(S(e47fszhY4nYPfTEGEoZ9QU3R))/'
                                     'JailTracker/GetInmates?start={start}'
                                     '&limit={limit}&sort=LastName&dir=ASC',
                      json=ROSTER_PAGE_JSON, status=200)

        input_params = {
            'session': SESSION,
            'start': 0,
            'limit': 10,
            'scrape_type': SCRAPE_TYPE
        }
        result = scraper.scrape_roster(input_params)
        assert result is None

        verify_mocks(mock_proxies, mock_headers, mock_taskqueue, 3)

        for person in ROSTER_PAGE_JSON['data']:
            task_params = {
                'session': SESSION,
                'roster_entry': person,
                'scrape_type': SCRAPE_TYPE
            }

            mock_taskqueue.assert_any_call(url=scraper.scraper_work_url,
                                           queue_name='us-vt-scraper',
                                           params={
                                               'region': 'us_vt',
                                               'task': 'scrape_person',
                                               'params': DeserializedJson(
                                                   task_params)
                                           })

    @responses.activate
    @patch("google.appengine.api.taskqueue.add")
    @patch("recidiviz.ingest.scraper_utils.get_headers")
    @patch("recidiviz.ingest.scraper_utils.get_proxies")
    def test_expected_json_more_roster_to_go(self, mock_proxies, mock_headers,
                                             mock_taskqueue):
        """Tests the happy path case where there is still another roster
        segment to scrape."""

        scraper = UsVtScraper()

        setup_mocks(mock_proxies, mock_headers, mock_taskqueue)

        higher_count_json = deepcopy(ROSTER_PAGE_JSON)
        higher_count_json['totalCount'] = 8000

        responses.add(responses.GET, 'https://omsweb.public-safety-cloud.com/'
                                     'jtclientweb/'
                                     '(S(e47fszhY4nYPfTEGEoZ9QU3R))//'
                                     '(S(e47fszhY4nYPfTEGEoZ9QU3R))/'
                                     'JailTracker/GetInmates?start={start}'
                                     '&limit={limit}&sort=LastName&dir=ASC',
                      json=higher_count_json, status=200)

        input_params = {
            'session': SESSION,
            'start': 0,
            'limit': 10,
            'scrape_type': SCRAPE_TYPE
        }
        result = scraper.scrape_roster(input_params)
        assert result is None

        verify_mocks(mock_proxies, mock_headers, mock_taskqueue, 4)

        for person in higher_count_json['data']:
            task_params = {
                'session': SESSION,
                'roster_entry': person,
                'scrape_type': SCRAPE_TYPE
            }

            mock_taskqueue.assert_any_call(url=scraper.scraper_work_url,
                                           queue_name='us-vt-scraper',
                                           params={
                                               'region': 'us_vt',
                                               'task': 'scrape_person',
                                               'params': DeserializedJson(
                                                   task_params)
                                           })

        next_roster_scrape_params = {
            'start': 3,
            'limit': 10,
            'session': SESSION,
            'scrape_type': SCRAPE_TYPE
        }
        mock_taskqueue.assert_any_call(url=scraper.scraper_work_url,
                                       queue_name='us-vt-scraper',
                                       params={
                                           'region': 'us_vt',
                                           'task': 'scrape_roster',
                                           'params': DeserializedJson(
                                               next_roster_scrape_params)
                                       })

    @responses.activate
    @patch("recidiviz.ingest.scraper_utils.get_headers")
    @patch("recidiviz.ingest.scraper_utils.get_proxies")
    def test_error_response(self, mock_proxies, mock_headers):
        """Tests the case where the server returns an error response."""

        scraper = UsVtScraper()

        setup_mocks(mock_proxies, mock_headers)

        responses.add(responses.GET, 'https://omsweb.public-safety-cloud.com/'
                                     'jtclientweb/'
                                     '(S(e47fszhY4nYPfTEGEoZ9QU3R))//'
                                     '(S(e47fszhY4nYPfTEGEoZ9QU3R))/'
                                     'JailTracker/GetInmates?start={start}'
                                     '&limit={limit}&sort=LastName&dir=ASC',
                      body=requests.exceptions.RequestException(), status=503)

        input_params = {
            'session': SESSION,
            'start': 0,
            'limit': 10,
            'scrape_type': SCRAPE_TYPE
        }
        result = scraper.scrape_roster(input_params)
        assert result == -1

        verify_mocks(mock_proxies, mock_headers)


class TestScrapePerson(object):
    """Tests for the UsVtScraper.scrape_person functionality."""

    @responses.activate
    @patch("google.appengine.api.taskqueue.add")
    @patch("recidiviz.ingest.scraper_utils.get_headers")
    @patch("recidiviz.ingest.scraper_utils.get_proxies")
    def test_expected_json(self, mock_proxies, mock_headers, mock_taskqueue):
        """Tests the happy path case."""

        scraper = UsVtScraper()

        roster_entry = ROSTER_PAGE_JSON['data'][0]

        setup_mocks(mock_proxies, mock_headers, mock_taskqueue)

        responses.add(responses.GET, 'https://omsweb.public-safety-cloud.com/'
                                     'jtclientweb/'
                                     '(S(e47fszhY4nYPfTEGEoZ9QU3R))//'
                                     '(S(e47fszhY4nYPfTEGEoZ9QU3R))/'
                                     'JailTracker/GetInmate?arrestNo={arrest}',
                      json=PERSON_JSON, status=200)

        input_params = {
            'session': SESSION,
            'scrape_type': SCRAPE_TYPE,
            'roster_entry': roster_entry
        }
        result = scraper.scrape_person(input_params)
        assert result is None

        verify_mocks(mock_proxies, mock_headers, mock_taskqueue, 1)

        cases_scrape_params = {
            'session': SESSION,
            'roster_entry': roster_entry,
            'person': PERSON_JSON,
            'scrape_type': SCRAPE_TYPE
        }
        mock_taskqueue.assert_any_call(url=scraper.scraper_work_url,
                                       queue_name='us-vt-scraper',
                                       params={
                                           'region': 'us_vt',
                                           'task': 'scrape_cases',
                                           'params': DeserializedJson(
                                               cases_scrape_params)
                                       })

    @responses.activate
    @patch("recidiviz.ingest.scraper_utils.get_headers")
    @patch("recidiviz.ingest.scraper_utils.get_proxies")
    def test_error_response(self, mock_proxies, mock_headers):
        """Tests the case where the server returns an error response."""

        scraper = UsVtScraper()

        roster_entry = ROSTER_PAGE_JSON['data'][0]

        setup_mocks(mock_proxies, mock_headers)

        responses.add(responses.GET, 'https://omsweb.public-safety-cloud.com/'
                                     'jtclientweb/'
                                     '(S(e47fszhY4nYPfTEGEoZ9QU3R))//'
                                     '(S(e47fszhY4nYPfTEGEoZ9QU3R))/'
                                     'JailTracker/GetInmate?arrestNo={arrest}',
                      body=requests.exceptions.RequestException(), status=503)

        input_params = {
            'session': SESSION,
            'scrape_type': SCRAPE_TYPE,
            'roster_entry': roster_entry
        }
        result = scraper.scrape_person(input_params)
        assert result == -1

        verify_mocks(mock_proxies, mock_headers)


class TestScrapeCases(object):
    """Tests for the UsVtScraper.scrape_cases functionality."""

    @responses.activate
    @patch("google.appengine.api.taskqueue.add")
    @patch("recidiviz.ingest.scraper_utils.get_headers")
    @patch("recidiviz.ingest.scraper_utils.get_proxies")
    def test_expected_json(self, mock_proxies, mock_headers, mock_taskqueue):
        """Tests the happy path case."""

        scraper = UsVtScraper()

        roster_entry = ROSTER_PAGE_JSON['data'][0]
        person = PERSON_JSON

        setup_mocks(mock_proxies, mock_headers, mock_taskqueue)

        responses.add(responses.GET, 'https://omsweb.public-safety-cloud.com/'
                                     'jtclientweb/'
                                     '(S(e47fszhY4nYPfTEGEoZ9QU3R))//'
                                     '(S(e47fszhY4nYPfTEGEoZ9QU3R))/'
                                     'JailTracker/GetCases?arrestNo={arrest}',
                      json=CASES_JSON, status=200)

        input_params = {
            'session': SESSION,
            'scrape_type': SCRAPE_TYPE,
            'roster_entry': roster_entry,
            'person': person
        }
        result = scraper.scrape_cases(input_params)
        assert result is None

        verify_mocks(mock_proxies, mock_headers, mock_taskqueue, 1)

        cases_scrape_params = {
            'session': SESSION,
            'roster_entry': roster_entry,
            'person': PERSON_JSON,
            'cases': CASES_JSON,
            'scrape_type': SCRAPE_TYPE
        }
        mock_taskqueue.assert_any_call(url=scraper.scraper_work_url,
                                       queue_name='us-vt-scraper',
                                       params={
                                           'region': 'us_vt',
                                           'task': 'scrape_charges',
                                           'params': DeserializedJson(
                                               cases_scrape_params)
                                       })

    @responses.activate
    @patch("recidiviz.ingest.scraper_utils.get_headers")
    @patch("recidiviz.ingest.scraper_utils.get_proxies")
    def test_error_response(self, mock_proxies, mock_headers):
        """Tests the case where the server returns an error response."""

        scraper = UsVtScraper()

        roster_entry = ROSTER_PAGE_JSON['data'][0]
        person = PERSON_JSON

        setup_mocks(mock_proxies, mock_headers)

        responses.add(responses.GET, 'https://omsweb.public-safety-cloud.com/'
                                     'jtclientweb/'
                                     '(S(e47fszhY4nYPfTEGEoZ9QU3R))//'
                                     '(S(e47fszhY4nYPfTEGEoZ9QU3R))/'
                                     'JailTracker/GetCases?arrestNo={arrest}',
                      body=requests.exceptions.RequestException(), status=406)

        input_params = {
            'session': SESSION,
            'scrape_type': SCRAPE_TYPE,
            'roster_entry': roster_entry,
            'person': person
        }
        result = scraper.scrape_cases(input_params)
        assert result == -1

        verify_mocks(mock_proxies, mock_headers)


class TestScrapeCharges(object):
    """Tests for the UsVtScraper.scrape_charges functionality."""

    @responses.activate
    @patch("recidiviz.ingest.us_vt.us_vt_scraper.UsVtScraper.store_record")
    @patch("recidiviz.ingest.scraper_utils.get_headers")
    @patch("recidiviz.ingest.scraper_utils.get_proxies")
    def test_expected_json(self, mock_proxies, mock_headers, mock_store_record):
        """Tests the happy path case."""

        scraper = UsVtScraper()

        roster_entry = ROSTER_PAGE_JSON['data'][0]
        person = PERSON_JSON
        cases = CASES_JSON

        setup_mocks(mock_proxies, mock_headers)
        mock_store_record.return_value = None

        responses.add(responses.POST, 'https://omsweb.public-safety-cloud.com/'
                                      'jtclientweb/'
                                      '(S(e47fszhY4nYPfTEGEoZ9QU3R))//'
                                      '(S(e47fszhY4nYPfTEGEoZ9QU3R))/'
                                      'JailTracker/GetCharges',
                      json=CHARGES_JSON, status=200)

        input_params = {
            'session': SESSION,
            'scrape_type': SCRAPE_TYPE,
            'roster_entry': roster_entry,
            'person': person,
            'cases': cases
        }
        result = scraper.scrape_charges(input_params)
        assert result is None

        verify_mocks(mock_proxies, mock_headers)
        mock_store_record.assert_called_with(
            roster_entry, person, cases, CHARGES_JSON)

    @responses.activate
    @patch("recidiviz.ingest.scraper_utils.get_headers")
    @patch("recidiviz.ingest.scraper_utils.get_proxies")
    def test_error_response(self, mock_proxies, mock_headers):
        """Tests the case where the server returns an error response."""

        scraper = UsVtScraper()

        roster_entry = ROSTER_PAGE_JSON['data'][0]
        person = PERSON_JSON
        cases = CASES_JSON

        setup_mocks(mock_proxies, mock_headers)

        responses.add(responses.POST, 'https://omsweb.public-safety-cloud.com/'
                                      'jtclientweb/'
                                      '(S(e47fszhY4nYPfTEGEoZ9QU3R))//'
                                      '(S(e47fszhY4nYPfTEGEoZ9QU3R))/'
                                      'JailTracker/GetCharges',
                      body=requests.exceptions.RequestException(), status=401)

        input_params = {
            'session': SESSION,
            'scrape_type': SCRAPE_TYPE,
            'roster_entry': roster_entry,
            'person': person,
            'cases': cases
        }
        result = scraper.scrape_charges(input_params)
        assert result == -1

        verify_mocks(mock_proxies, mock_headers)

    @responses.activate
    @patch("recidiviz.ingest.us_vt.us_vt_scraper.UsVtScraper.store_record")
    @patch("recidiviz.ingest.scraper_utils.get_headers")
    @patch("recidiviz.ingest.scraper_utils.get_proxies")
    def test_error_on_save(self, mock_proxies, mock_headers, mock_store_record):
        """Tests the case where there is an error while saving the records."""

        scraper = UsVtScraper()

        roster_entry = ROSTER_PAGE_JSON['data'][0]
        person = PERSON_JSON
        cases = CASES_JSON

        setup_mocks(mock_proxies, mock_headers)
        mock_store_record.return_value = -1

        responses.add(responses.POST, 'https://omsweb.public-safety-cloud.com/'
                                      'jtclientweb/'
                                      '(S(e47fszhY4nYPfTEGEoZ9QU3R))//'
                                      '(S(e47fszhY4nYPfTEGEoZ9QU3R))/'
                                      'JailTracker/GetCharges',
                      json=CHARGES_JSON, status=200)

        input_params = {
            'session': SESSION,
            'scrape_type': SCRAPE_TYPE,
            'roster_entry': roster_entry,
            'person': person,
            'cases': cases
        }
        result = scraper.scrape_charges(input_params)
        assert result == -1

        verify_mocks(mock_proxies, mock_headers)
        mock_store_record.assert_called_with(
            roster_entry, person, cases, CHARGES_JSON)


class TestPersonIdToRecordId(object):
    """Tests for the person_id_to_record_id method."""

    def setup_method(self, _test_method):
        # noinspection PyAttributeOutsideInit
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        ndb.get_context().clear_cache()

        # root_path must be set the the location of queue.yaml.
        # Otherwise, only the 'default' queue will be available.
        self.testbed.init_taskqueue_stub(root_path='.')

        # noinspection PyAttributeOutsideInit
        self.taskqueue_stub = self.testbed.get_stub(
            testbed.TASKQUEUE_SERVICE_NAME)

    def teardown_method(self, _test_method):
        self.testbed.deactivate()

    def test_person_id_to_record_id(self):
        person_id = 'us_vt-123456'
        person = UsVtPerson(person_id=person_id)
        person.put()

        record_id = 'us_vt-abcde'
        record = Record(parent=person.key, record_id=record_id)
        record.put()

        scraper = UsVtScraper()
        assert scraper.person_id_to_record_id(person_id) == record_id

    def test_person_id_to_record_id_no_person(self):
        scraper = UsVtScraper()
        assert scraper.person_id_to_record_id('cdefg') is None

    def test_person_id_to_record_id_no_record(self):
        person_id = 'us_vt-123456'
        person = UsVtPerson(person_id=person_id)
        person.put()

        scraper = UsVtScraper()
        assert scraper.person_id_to_record_id(person_id) is None


class TestCreatePerson(object):
    """Tests that the creation of new Persons works and captures all
    possible fields."""

    FIELDS_NOT_SET = ['birthdate']

    def setup_method(self, _test_method):
        # noinspection PyAttributeOutsideInit
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        ndb.get_context().clear_cache()

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
        scraper = UsVtScraper()

        roster_data = ROSTER_PAGE_JSON['data'][0]
        person_data = {
            "Agency": "ST. ALBANS PROBATION & PAROLE",
            "Last Name": "FRIGHTERSON",
            "First Name": "BOO",
            "Middle Name": "",
            "Suffix": "",
            "Current Age": "32",
            "Booking Date": "9/15/2014 2:53:00 PM",
            "Date Released": "",
            "Race": "White",
            "Sex": "F",
            "Alias": "Casper",
            "Parole Officer": "Ghost, Friendly",
            "Case Worker": "",
            "Min Release": "05/02/2018",
            "Max Release": "05/02/2018",
            "Status": "Sentenced"
        }

        actual = scraper.create_person(roster_data, person_data)

        expected = UsVtPerson(
            id="5551",
            person_id="5551",
            surname="LANTERN",
            given_names="JACK O",
            alias="Casper",
            suffix="",
            age=32,
            sex="female",
            race="white",
            us_vt_person_id="5551",
            person_id_is_fuzzy=False,
            region="us_vt"
        )
        expected.put()
        expected.created_on = actual.created_on
        expected.updated_on = actual.updated_on

        assert expected == actual

        # pylint:disable=protected-access
        person_attributes = UsVtPerson._properties
        unset_attributes = [attribute for attribute in person_attributes
                            if attribute != 'class'
                            and getattr(actual, attribute) is None]

        assert all(attr in TestCreatePerson.FIELDS_NOT_SET
                   for attr in unset_attributes)


class TestCreateRecord(object):
    """Tests that the creation of new Records works and captures all
    possible fields."""

    FIELDS_NOT_SET = ['is_released',
                      'admission_type',
                      'birthdate',
                      'cond_release_date',
                      'county_of_commit',
                      'custody_status',
                      'earliest_release_type',
                      'last_custody_date',
                      'latest_release_type',
                      'max_expir_date',
                      'max_expir_date_parole',
                      'max_expir_date_superv',
                      'max_sentence_length',
                      'parole_hearing_type',
                      'min_sentence_length',
                      'offense_date',
                      'parole_discharge_date',
                      'parole_elig_date',
                      'parole_hearing_date',
                      'parole_hearing_type',
                      'release_date']

    def setup_method(self, _test_method):
        # noinspection PyAttributeOutsideInit
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        ndb.get_context().clear_cache()

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
        scraper = UsVtScraper()

        roster_data = ROSTER_PAGE_JSON['data'][0]
        person_data = {
            "Agency": "ST. ALBANS PROBATION & PAROLE",
            "Last Name": "FRIGHTERSON",
            "First Name": "BOO",
            "Middle Name": "",
            "Suffix": "",
            "Current Age": "32",
            "Booking Date": "9/15/2014 2:53:00 PM",
            "Date Released": "",
            "Race": "White",
            "Sex": "F",
            "Alias": "Casper",
            "Parole Officer": "Ghost, Friendly",
            "Case Worker": "",
            "Min Release": "05/02/2018",
            "Max Release": "05/02/2018",
            "Status": "Sentenced"
        }
        charge_data = CHARGES_JSON['data']

        person = UsVtPerson(
            id="5551",
            person_id="5551",
            surname="LANTERN",
            given_names="JACK O",
            alias="Casper",
            suffix="",
            age=32,
            sex="female",
            race="white",
            us_vt_person_id="5551",
            person_id_is_fuzzy=False,
            region="us_vt"
        )
        person.put()

        agencies = scraper.extract_agencies(PERSON_JSON['data'],
                                            person.person_id)

        actual = scraper.create_record(person, agencies,
                                       roster_data, person_data, charge_data)

        offenses = [
            UsVtOffense(
                arresting_agency="",
                arrest_code="ESC",
                arrest_date=None,
                bond_amount=0.00,
                bond_type=None,
                case_number="1111-11-11 Aaaa",
                control_number="0",
                court_time=datetime(2017, 2, 4, 14, 4, 0),
                court_type="Criminal",
                crime_class="12345",
                crime_description="ESCAPE OR WALKAWAY - F",
                crime_type="F",
                modifier=None,
                number_of_counts=1,
                status="Sentenced",
                warrant_number=""
            ),
            UsVtOffense(
                arresting_agency="",
                arrest_code="SA",
                arrest_date=None,
                bond_amount=0.00,
                bond_type=None,
                case_number="2222-22-22 Bbbb",
                control_number="0",
                court_time=datetime(2007, 3, 11, 17, 30, 0),
                court_type="Criminal",
                crime_class="67890",
                crime_description="SIMPLE ASSAULT",
                crime_type="M",
                modifier=None,
                number_of_counts=1,
                status="Sentenced",
                warrant_number=""
            ),
            UsVtOffense(
                arresting_agency="",
                arrest_code="BURGUN",
                arrest_date=None,
                bond_amount=0.00,
                bond_type=None,
                case_number="3333-33-33 Cccc",
                control_number="0",
                court_time=datetime(2009, 3, 23, 22, 21, 0),
                court_type="Criminal",
                crime_class="45678",
                crime_description="BURGLARY",
                crime_type="F",
                modifier=None,
                number_of_counts=1,
                status="Sentenced",
                warrant_number=""
            )
        ]

        expected = Record(
            parent=person.key,
            id="555",
            custody_date=datetime(2014, 9, 15, 14, 53),
            offense=offenses,
            status="Sentenced",
            release_date=None,
            earliest_release_date=datetime(2018, 5, 2),
            latest_release_date=datetime(2018, 5, 2),
            parole_officer="Ghost, Friendly",
            case_worker="",
            surname="LANTERN",
            given_names="JACK O",
            sex="female",
            race="white",
            region="us_vt",
            record_id="555",
            record_id_is_fuzzy=False,
            latest_facility="LOCAL COUNTY JAIL",
            community_supervision_agency="ST. ALBANS PROBATION & PAROLE"
        )
        expected.put()
        expected.created_on = actual.created_on
        expected.updated_on = actual.updated_on

        # assert expected == actual

        # pylint:disable=protected-access
        record_attributes = Record._properties
        unset_attributes = [attribute for attribute in record_attributes
                            if attribute != 'class'
                            and getattr(actual, attribute) is None]

        assert all(attr in TestCreateRecord.FIELDS_NOT_SET
                   for attr in unset_attributes)


class TestRecordToSnapshot(object):
    """Tests that the translation between Records and Snapshots works and
    captures all possible fields."""

    FIELDS_NOT_SET = ['is_released',
                      'admission_type',
                      'birthdate',
                      'cond_release_date',
                      'county_of_commit',
                      'custody_status',
                      'earliest_release_type',
                      'last_custody_date',
                      'latest_release_type',
                      'min_sentence_length',
                      'max_expir_date',
                      'max_expir_date_parole',
                      'max_expir_date_superv',
                      'max_sentence_length',
                      'offense_date',
                      'parole_elig_date',
                      'parole_discharge_date',
                      'parole_hearing_date',
                      'parole_hearing_type']

    def setup_method(self, _test_method):
        # noinspection PyAttributeOutsideInit
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        ndb.get_context().clear_cache()

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
        offense = UsVtOffense(
            arresting_agency='Stockholm Police Department',
            arrest_code='POSS',
            arrest_date=datetime(2016, 7, 2),
            bond_amount=0,
            bond_type=None,
            case_number='67890b',
            control_number='223',
            court_time=datetime(2016, 7, 19, 10, 30),
            court_type='County',
            crime_class='C',
            crime_description='POSSESSION',
            crime_type='NON-VIOLENT',
            modifier=None,
            number_of_counts=1,
            status='CONVICTED',
            warrant_number=None
        )

        record = Record(
            id=12345,
            case_worker='Martin Rohde',
            community_supervision_agency='Lanskrim Malmo',
            custody_date=datetime(2016, 8, 14),
            earliest_release_date=datetime(2018, 4, 1),
            latest_facility='Stockholm Penitentiary',
            latest_release_date=datetime(2019, 6, 10),
            offense=[offense],
            parole_officer='Saga Noren',
            race='Black',
            record_id='us_vt-12345',
            record_id_is_fuzzy=False,
            region='us_vt',
            release_date=datetime(2018, 5, 23),
            sex='M',
            status='Sentenced',
            surname='Sabroe',
            given_names='Henrik'
        )

        scraper = UsVtScraper()
        snapshot = scraper.record_to_snapshot(record)
        assert snapshot == UsVtSnapshot(
            case_worker=record.case_worker,
            community_supervision_agency=record.community_supervision_agency,
            custody_date=record.custody_date,
            earliest_release_date=record.earliest_release_date,
            given_names=record.given_names,
            is_released=record.is_released,
            latest_facility=record.latest_facility,
            latest_release_date=record.latest_release_date,
            latest_release_type=record.latest_release_type,
            max_sentence_length=record.max_sentence_length,
            min_sentence_length=record.min_sentence_length,
            offense=record.offense,
            parent=record.key,
            parole_officer=record.parole_officer,
            race=record.race,
            region=record.region,
            release_date=record.release_date,
            sex=record.sex,
            status=record.status,
            surname=record.surname
        )
        snapshot.put()

        # pylint:disable=protected-access
        snapshot_attributes = UsVtSnapshot._properties
        unset_attributes = [attribute for attribute in snapshot_attributes
                            if attribute != 'class'
                            and getattr(snapshot, attribute) is None]

        assert all(attr in TestRecordToSnapshot.FIELDS_NOT_SET
                   for attr in unset_attributes)


class TestExtractAgencies(object):
    """Tests for the extract_agencies method."""

    def setup_method(self, _test_method):
        # noinspection PyAttributeOutsideInit
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        ndb.get_context().clear_cache()

        # root_path must be set the the location of queue.yaml.
        # Otherwise, only the 'default' queue will be available.
        self.testbed.init_taskqueue_stub(root_path='.')

        # noinspection PyAttributeOutsideInit
        self.taskqueue_stub = self.testbed.get_stub(
            testbed.TASKQUEUE_SERVICE_NAME)

    def teardown_method(self, _test_method):
        self.testbed.deactivate()

    def test_extract_agencies(self):
        """Tests the happy path for extract_agencies."""
        agencies = self.conduct_test(PERSON_JSON['data'])
        assert agencies == ("LOCAL COUNTY JAIL",
                            "ST. ALBANS PROBATION & PAROLE")

    def test_extract_agencies_repeated_names(self):
        person_json = [
            {
                "Field": "Agency:",
                "Value": "ST. ALBANS PROBATION & PAROLE"
            },
            {
                "Field": None,
                "Value": "ST. ALBANS PROBATION & PAROLE"
            },
            {
                "Field": None,
                "Value": "COUNTY PENITENTIARY"
            }
        ]

        agencies = self.conduct_test(person_json)
        assert agencies == ("COUNTY PENITENTIARY",
                            "ST. ALBANS PROBATION & PAROLE")

    def test_extract_agencies_multiple_supervision_agencies(self):
        person_json = [
            {
                "Field": "Agency:",
                "Value": "ST. ALBANS PROBATION & PAROLE"
            },
            {
                "Field": None,
                "Value": "ST. ALBANS PROBATION & PAROLE"
            },
            {
                "Field": None,
                "Value": "A DIFFERENT PAROLE AGENCY"
            }
        ]

        agencies = self.conduct_test(person_json)
        assert agencies is None

    def test_extract_agencies_multiple_facilities(self):
        person_json = [
            {
                "Field": "Agency:",
                "Value": "LOCAL COUNTY JAIL"
            },
            {
                "Field": None,
                "Value": "ST. ALBANS PROBATION & PAROLE"
            },
            {
                "Field": None,
                "Value": "A DIFFERENT FACILITY"
            }
        ]

        agencies = self.conduct_test(person_json)
        assert agencies is None

    @staticmethod
    def conduct_test(person_json):
        scraper = UsVtScraper()

        person = UsVtPerson(
            id="5551",
            person_id="5551",
            surname="LANTERN",
            given_names="JACK O",
            alias="Casper",
            suffix="",
            age=32,
            sex="female",
            race="white",
            us_vt_person_id="5551",
            person_id_is_fuzzy=False,
            region="us_vt"
        )
        person.put()

        return scraper.extract_agencies(person_json, person.person_id)


class TestStoreRecord(object):
    """Tests for the store_record method."""

    def setup_method(self, _test_method):
        # noinspection PyAttributeOutsideInit
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        ndb.get_context().clear_cache()

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
        roster_data = ROSTER_PAGE_JSON['data'][0]
        person_data = PERSON_JSON
        case_data = CASES_JSON
        charge_data = CHARGES_JSON

        scraper = UsVtScraper()
        result = scraper.store_record(roster_data, person_data,
                                      case_data, charge_data)

        assert result is None

        person = UsVtPerson.get_by_id("5551")
        assert person.person_id == "5551"

        records = Record.query(ancestor=person.key).fetch()
        assert len(records) == 1
        assert records[0].record_id == "555"

        snapshots = UsVtSnapshot.query(ancestor=records[0].key).fetch()
        assert len(snapshots) == 1

    def test_found_case_data(self):
        roster_data = ROSTER_PAGE_JSON['data'][0]
        person_data = PERSON_JSON
        case_data = CASES_JSON
        case_data['data'] = [{"Field": "Docket", "Value": "Appellate"}]
        charge_data = CHARGES_JSON

        scraper = UsVtScraper()
        result = scraper.store_record(roster_data, person_data,
                                      case_data, charge_data)

        assert result is None

    @patch("recidiviz.ingest.us_vt.us_vt_person.UsVtPerson.put")
    def test_error_saving_person(self, mock_put):
        mock_put.side_effect = InternalError()

        roster_data = ROSTER_PAGE_JSON['data'][0]
        person_data = PERSON_JSON
        case_data = CASES_JSON
        charge_data = CHARGES_JSON

        scraper = UsVtScraper()
        result = scraper.store_record(roster_data, person_data,
                                      case_data, charge_data)

        assert result == -1
        mock_put.assert_called_with()

    @patch("recidiviz.models.record.Record.put")
    def test_error_saving_record(self, mock_put):
        mock_put.side_effect = InternalError()

        roster_data = ROSTER_PAGE_JSON['data'][0]
        person_data = PERSON_JSON
        case_data = CASES_JSON
        charge_data = CHARGES_JSON

        scraper = UsVtScraper()
        result = scraper.store_record(roster_data, person_data,
                                      case_data, charge_data)

        assert result == -1
        mock_put.assert_called_with()

        person = UsVtPerson.get_by_id("5551")
        assert person.person_id == "5551"


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
