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

from mock import patch
import callee
import responses
import requests

from recidiviz.ingest.us_vt.us_vt_scraper import UsVtScraper

SESSION = 'e47fszhY4nYPfTEGEoZ9QU3R'

FRONT_PAGE_HTML = """
<html xmlns="http://www.w3.org/1999/xhtml">
    <body id="ext-gen5">
    <div id="page">
        <script type="text/javascript">
            Ext.onReady(function() {
                   JailTracker.Web.Settings.init('e47fszhY4nYPfTEGEoZ9QU3R'
                                          , 'False' == 'True' ? true : false
                                          , 'True' == 'True' ? true : false);
            });
        </script>
    </div>
    </body>
</html>
"""

ROSTER_PAGE_JSON = {
    "data": [
        {
            "RowIndex": 2,
            "AgencyName": "St. Albans Probation & Parole",
            "ArrestNo": 555,
            "Jacket": "5551",
            "FirstName": "JACK",
            "MiddleName": "O",
            "LastName": "LANTERN",
            "Suffix": "III",
            "OriginalBookDateTime": "2018-09-19",
            "FinalReleaseDateTime": None
        },
        {
            "RowIndex": 1,
            "AgencyName": "Burlington Probation & Parole",
            "ArrestNo": 777,
            "Jacket": "7772",
            "FirstName": "HAL",
            "MiddleName": "LOWS",
            "LastName": "EVE",
            "Suffix": "",
            "OriginalBookDateTime": "2015-03-18",
            "FinalReleaseDateTime": None
        },
        {
            "RowIndex": 3,
            "AgencyName": "Rutland Probation & Parole",
            "ArrestNo": 999,
            "Jacket": "9993",
            "FirstName": "BOO",
            "MiddleName": "",
            "LastName": "FRIGHTERSON",
            "Suffix": "",
            "OriginalBookDateTime": "2013-08-16",
            "FinalReleaseDateTime": None
        }
    ],
    "totalCount": 3,
    "error": "",
    "success": True
}

PERSON_JSON = {
    "data": [
        {
            "Field": "Agency:",
            "Value": "ST. ALBANS PROBATION & PAROLE"
        },
        {
            "Field": "Last Name:",
            "Value": "FRIGHTERSON"
        },
        {
            "Field": "First Name:",
            "Value": "BOO"
        },
        {
            "Field": "Middle Name:",
            "Value": ""
        },
        {
            "Field": "Suffix:",
            "Value": ""
        },
        {
            "Field": "Current Age:",
            "Value": "32"
        },
        {
            "Field": "Booking Date:",
            "Value": "9/15/2014 2:53:00 PM"
        },
        {
            "Field": "Date Released:",
            "Value": ""
        },
        {
            "Field": "Race:",
            "Value": "White"
        },
        {
            "Field": "Sex:",
            "Value": "F"
        },
        {
            "Field": "Alias:",
            "Value": "Casper"
        },
        {
            "Field": "Parole Officer:",
            "Value": "Ghost, Friendly"
        },
        {
            "Field": "Case Worker:",
            "Value": ""
        },
        {
            "Field": "Min Release:",
            "Value": "05/02/2018"
        },
        {
            "Field": "Max Release:",
            "Value": "05/02/2018"
        },
        {
            "Field": "Status:",
            "Value": "Sentenced"
        }
    ],
    "totalCount": 16,
    "error": "",
    "success": True
}

# So far, all case data has been empty. See us_vt_scraper.store_record.
CASES_JSON = {}

CHARGES_JSON = {
    "data": [
        {
            "ChargeId": 12345,
            "CaseNo": "1111-11-11 Aaaa",
            "CrimeType": "F",
            "Counts": 1,
            "Modifier": None,
            "ControlNumber": "0",
            "WarrantNumber": "",
            "ArrestCode": "ESC",
            "ChargeDescription": "ESCAPE OR WALKAWAY - F",
            "BondType": None,
            "BondAmount": 0.00,
            "CourtType": "Criminal",
            "CourtTime": "Feb  4 2017  2:04PM",
            "ChargeStatus": "Sentenced",
            "OffenseDate": "2014-09-18",
            "ArrestDate": "",
            "ArrestingAgency": ""
        },
        {
            "ChargeId": 67890,
            "CaseNo": "2222-22-22 Bbbb",
            "CrimeType": "M",
            "Counts": 1,
            "Modifier": None,
            "ControlNumber": "0",
            "WarrantNumber": "",
            "ArrestCode": "SA",
            "ChargeDescription": "SIMPLE ASSAULT",
            "BondType": None,
            "BondAmount": 0.00,
            "CourtType": "Criminal",
            "CourtTime": "Mar 11 2007  5:30PM",
            "ChargeStatus": "Sentenced",
            "OffenseDate": "2005-06-12",
            "ArrestDate": "",
            "ArrestingAgency": ""
        },
        {
            "ChargeId": 45678,
            "CaseNo": "3333-33-33 Cccc",
            "CrimeType": "F",
            "Counts": 1,
            "Modifier": None,
            "ControlNumber": "0",
            "WarrantNumber": "",
            "ArrestCode": "BURGUN",
            "ChargeDescription": "BURGLARY",
            "BondType": None,
            "BondAmount": 0.00,
            "CourtType": "Criminal",
            "CourtTime": "Mar 23 2009 10:21PM",
            "ChargeStatus": "Sentenced",
            "OffenseDate": "2006-01-23",
            "ArrestDate": "",
            "ArrestingAgency": ""
        }
    ],
    "totalCount": 3,
    "error": "",
    "success": True
}


class DeserializedJson(callee.Matcher):
    """An argument Matcher which can match serialized json against a
    deserialized object for comparison by deserializing the json.

    This is useful here because we pass around serialized json as strings that
    could have its fields in any order within the string, and we want to match
    it against the actual object that the json represents.
    """

    def __init__(self, comparison_object):
        self.comparison = comparison_object

    def match(self, value):
        return json.loads(value) == self.comparison


class TestScrapeFrontPage(object):

    @responses.activate
    @patch("google.appengine.api.taskqueue.add")
    @patch("recidiviz.ingest.scraper_utils.get_headers")
    @patch("recidiviz.ingest.scraper_utils.get_proxies")
    def test_expected_html(self, mock_proxies, mock_headers, mock_taskqueue):
        scraper = UsVtScraper()
        scrape_type = 'background'

        proxies = {'http': 'http://user:password@proxy.biz/'}
        mock_proxies.return_value = proxies
        headers = {'User-Agent': 'test_user_agent'}
        mock_headers.return_value = headers
        mock_taskqueue.return_value = None

        responses.add(responses.GET, 'https://omsweb.public-safety-cloud.com'
                                     '/jtclientweb//jailtracker/index/Vermont',
                      body=FRONT_PAGE_HTML, status=200)

        result = scraper.scrape_front_page({'scrape_type': scrape_type})
        assert result is None

        mock_proxies.assert_called_with()
        mock_headers.assert_called_with()

        task_params = json.dumps({'session': SESSION,
                                  'start': 0,
                                  'limit': 10,
                                  'scrape_type': scrape_type})
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
        scraper = UsVtScraper()
        scrape_type = 'background'

        proxies = {'http': 'http://user:password@proxy.biz/'}
        mock_proxies.return_value = proxies
        headers = {'User-Agent': 'test_user_agent'}
        mock_headers.return_value = headers

        responses.add(responses.GET, 'https://omsweb.public-safety-cloud.com'
                                     '/jtclientweb//jailtracker/index/Vermont',
                      body='<p>This is valid but unexpected</p>', status=200)

        result = scraper.scrape_front_page({'scrape_type': scrape_type})
        assert result == -1

        mock_proxies.assert_called_with()
        mock_headers.assert_called_with()

    @responses.activate
    @patch("recidiviz.ingest.scraper_utils.get_headers")
    @patch("recidiviz.ingest.scraper_utils.get_proxies")
    def test_invalid_html(self, mock_proxies, mock_headers):
        scraper = UsVtScraper()
        scrape_type = 'background'

        proxies = {'http': 'http://user:password@proxy.biz/'}
        mock_proxies.return_value = proxies
        headers = {'User-Agent': 'test_user_agent'}
        mock_headers.return_value = headers

        responses.add(responses.GET, 'https://omsweb.public-safety-cloud.com'
                                     '/jtclientweb//jailtracker/index/Vermont',
                      body='<!<!', status=200)

        result = scraper.scrape_front_page({'scrape_type': scrape_type})
        assert result == -1

        mock_proxies.assert_called_with()
        mock_headers.assert_called_with()

    @responses.activate
    @patch("recidiviz.ingest.scraper_utils.get_headers")
    @patch("recidiviz.ingest.scraper_utils.get_proxies")
    def test_error_response(self, mock_proxies, mock_headers):
        scraper = UsVtScraper()
        scrape_type = 'background'

        proxies = {'http': 'http://user:password@proxy.biz/'}
        mock_proxies.return_value = proxies
        headers = {'User-Agent': 'test_user_agent'}
        mock_headers.return_value = headers

        responses.add(responses.GET, 'https://omsweb.public-safety-cloud.com'
                                     '/jtclientweb//jailtracker/index/Vermont',
                      body=requests.exceptions.RequestException(), status=400)

        result = scraper.scrape_front_page({'scrape_type': scrape_type})
        assert result == -1

        mock_proxies.assert_called_with()
        mock_headers.assert_called_with()


class TestScrapeRoster(object):

    @responses.activate
    @patch("google.appengine.api.taskqueue.add")
    @patch("recidiviz.ingest.scraper_utils.get_headers")
    @patch("recidiviz.ingest.scraper_utils.get_proxies")
    def test_expected_json_done_with_roster(self, mock_proxies, mock_headers,
                                            mock_taskqueue):
        scraper = UsVtScraper()
        scrape_type = 'background'

        proxies = {'http': 'http://user:password@proxy.biz/'}
        mock_proxies.return_value = proxies
        headers = {'User-Agent': 'test_user_agent'}
        mock_headers.return_value = headers
        mock_taskqueue.return_value = None

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
            'scrape_type': scrape_type
        }
        result = scraper.scrape_roster(input_params)
        assert result is None

        mock_proxies.assert_called_with()
        mock_headers.assert_called_with()

        assert len(mock_taskqueue.mock_calls) == 3

        for person in ROSTER_PAGE_JSON['data']:
            task_params = {
                'session': SESSION,
                'roster_entry': person,
                'scrape_type': scrape_type
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
        scraper = UsVtScraper()
        scrape_type = 'background'

        proxies = {'http': 'http://user:password@proxy.biz/'}
        mock_proxies.return_value = proxies
        headers = {'User-Agent': 'test_user_agent'}
        mock_headers.return_value = headers
        mock_taskqueue.return_value = None

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
            'scrape_type': scrape_type
        }
        result = scraper.scrape_roster(input_params)
        assert result is None

        mock_proxies.assert_called_with()
        mock_headers.assert_called_with()

        assert len(mock_taskqueue.mock_calls) == 4

        for person in higher_count_json['data']:
            task_params = {
                'session': SESSION,
                'roster_entry': person,
                'scrape_type': scrape_type
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
            'scrape_type': scrape_type
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
        scraper = UsVtScraper()
        scrape_type = 'background'

        proxies = {'http': 'http://user:password@proxy.biz/'}
        mock_proxies.return_value = proxies
        headers = {'User-Agent': 'test_user_agent'}
        mock_headers.return_value = headers

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
            'scrape_type': scrape_type
        }
        result = scraper.scrape_roster(input_params)
        assert result == -1

        mock_proxies.assert_called_with()
        mock_headers.assert_called_with()


class TestScrapePerson(object):

    @responses.activate
    @patch("google.appengine.api.taskqueue.add")
    @patch("recidiviz.ingest.scraper_utils.get_headers")
    @patch("recidiviz.ingest.scraper_utils.get_proxies")
    def test_expected_json(self, mock_proxies, mock_headers,
                           mock_taskqueue):
        scraper = UsVtScraper()
        scrape_type = 'background'
        roster_entry = ROSTER_PAGE_JSON['data'][0]

        proxies = {'http': 'http://user:password@proxy.biz/'}
        mock_proxies.return_value = proxies
        headers = {'User-Agent': 'test_user_agent'}
        mock_headers.return_value = headers
        mock_taskqueue.return_value = None

        responses.add(responses.GET, 'https://omsweb.public-safety-cloud.com/'
                                     'jtclientweb/'
                                     '(S(e47fszhY4nYPfTEGEoZ9QU3R))//'
                                     '(S(e47fszhY4nYPfTEGEoZ9QU3R))/'
                                     'JailTracker/GetInmate?arrestNo={arrest}',
                      json=PERSON_JSON, status=200)

        input_params = {
            'session': SESSION,
            'scrape_type': scrape_type,
            'roster_entry': roster_entry
        }
        result = scraper.scrape_person(input_params)
        assert result is None

        mock_proxies.assert_called_with()
        mock_headers.assert_called_with()

        cases_scrape_params = {
            'session': SESSION,
            'roster_entry': roster_entry,
            'person': PERSON_JSON,
            'scrape_type': scrape_type
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
        scraper = UsVtScraper()
        scrape_type = 'background'
        roster_entry = ROSTER_PAGE_JSON['data'][0]

        proxies = {'http': 'http://user:password@proxy.biz/'}
        mock_proxies.return_value = proxies
        headers = {'User-Agent': 'test_user_agent'}
        mock_headers.return_value = headers

        responses.add(responses.GET, 'https://omsweb.public-safety-cloud.com/'
                                     'jtclientweb/'
                                     '(S(e47fszhY4nYPfTEGEoZ9QU3R))//'
                                     '(S(e47fszhY4nYPfTEGEoZ9QU3R))/'
                                     'JailTracker/GetInmate?arrestNo={arrest}',
                      body=requests.exceptions.RequestException(), status=503)

        input_params = {
            'session': SESSION,
            'scrape_type': scrape_type,
            'roster_entry': roster_entry
        }
        result = scraper.scrape_person(input_params)
        assert result == -1

        mock_proxies.assert_called_with()
        mock_headers.assert_called_with()


class TestScrapeCases(object):

    @responses.activate
    @patch("google.appengine.api.taskqueue.add")
    @patch("recidiviz.ingest.scraper_utils.get_headers")
    @patch("recidiviz.ingest.scraper_utils.get_proxies")
    def test_expected_json(self, mock_proxies, mock_headers,
                           mock_taskqueue):
        scraper = UsVtScraper()
        scrape_type = 'background'
        roster_entry = ROSTER_PAGE_JSON['data'][0]
        person = PERSON_JSON

        proxies = {'http': 'http://user:password@proxy.biz/'}
        mock_proxies.return_value = proxies
        headers = {'User-Agent': 'test_user_agent'}
        mock_headers.return_value = headers
        mock_taskqueue.return_value = None

        responses.add(responses.GET, 'https://omsweb.public-safety-cloud.com/'
                                     'jtclientweb/'
                                     '(S(e47fszhY4nYPfTEGEoZ9QU3R))//'
                                     '(S(e47fszhY4nYPfTEGEoZ9QU3R))/'
                                     'JailTracker/GetCases?arrestNo={arrest}',
                      json=CASES_JSON, status=200)

        input_params = {
            'session': SESSION,
            'scrape_type': scrape_type,
            'roster_entry': roster_entry,
            'person': person
        }
        result = scraper.scrape_cases(input_params)
        assert result is None

        mock_proxies.assert_called_with()
        mock_headers.assert_called_with()

        cases_scrape_params = {
            'session': SESSION,
            'roster_entry': roster_entry,
            'person': PERSON_JSON,
            'cases': CASES_JSON,
            'scrape_type': scrape_type
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
        scraper = UsVtScraper()
        scrape_type = 'background'
        roster_entry = ROSTER_PAGE_JSON['data'][0]
        person = PERSON_JSON

        proxies = {'http': 'http://user:password@proxy.biz/'}
        mock_proxies.return_value = proxies
        headers = {'User-Agent': 'test_user_agent'}
        mock_headers.return_value = headers

        responses.add(responses.GET, 'https://omsweb.public-safety-cloud.com/'
                                     'jtclientweb/'
                                     '(S(e47fszhY4nYPfTEGEoZ9QU3R))//'
                                     '(S(e47fszhY4nYPfTEGEoZ9QU3R))/'
                                     'JailTracker/GetCases?arrestNo={arrest}',
                      body=requests.exceptions.RequestException(), status=406)

        input_params = {
            'session': SESSION,
            'scrape_type': scrape_type,
            'roster_entry': roster_entry,
            'person': person
        }
        result = scraper.scrape_cases(input_params)
        assert result == -1

        mock_proxies.assert_called_with()
        mock_headers.assert_called_with()


class TestScrapeCharges(object):

    @responses.activate
    @patch("recidiviz.ingest.us_vt.us_vt_scraper.UsVtScraper.store_record")
    @patch("recidiviz.ingest.scraper_utils.get_headers")
    @patch("recidiviz.ingest.scraper_utils.get_proxies")
    def test_expected_json(self, mock_proxies, mock_headers, mock_store_record):
        scraper = UsVtScraper()
        scrape_type = 'background'
        roster_entry = ROSTER_PAGE_JSON['data'][0]
        person = PERSON_JSON
        cases = CASES_JSON

        proxies = {'http': 'http://user:password@proxy.biz/'}
        mock_proxies.return_value = proxies
        headers = {'User-Agent': 'test_user_agent'}
        mock_headers.return_value = headers
        mock_store_record.return_value = None

        responses.add(responses.POST, 'https://omsweb.public-safety-cloud.com/'
                                      'jtclientweb/'
                                      '(S(e47fszhY4nYPfTEGEoZ9QU3R))//'
                                      '(S(e47fszhY4nYPfTEGEoZ9QU3R))/'
                                      'JailTracker/GetCharges',
                      json=CHARGES_JSON, status=200)

        input_params = {
            'session': SESSION,
            'scrape_type': scrape_type,
            'roster_entry': roster_entry,
            'person': person,
            'cases': cases
        }
        result = scraper.scrape_charges(input_params)
        assert result is None

        mock_proxies.assert_called_with()
        mock_headers.assert_called_with()
        mock_store_record.assert_called_with(
            roster_entry, person, cases, CHARGES_JSON)

    @responses.activate
    @patch("recidiviz.ingest.scraper_utils.get_headers")
    @patch("recidiviz.ingest.scraper_utils.get_proxies")
    def test_error_response(self, mock_proxies, mock_headers):
        scraper = UsVtScraper()
        scrape_type = 'background'
        roster_entry = ROSTER_PAGE_JSON['data'][0]
        person = PERSON_JSON
        cases = CASES_JSON

        proxies = {'http': 'http://user:password@proxy.biz/'}
        mock_proxies.return_value = proxies
        headers = {'User-Agent': 'test_user_agent'}
        mock_headers.return_value = headers

        responses.add(responses.POST, 'https://omsweb.public-safety-cloud.com/'
                                      'jtclientweb/'
                                      '(S(e47fszhY4nYPfTEGEoZ9QU3R))//'
                                      '(S(e47fszhY4nYPfTEGEoZ9QU3R))/'
                                      'JailTracker/GetCharges',
                      body=requests.exceptions.RequestException(), status=401)

        input_params = {
            'session': SESSION,
            'scrape_type': scrape_type,
            'roster_entry': roster_entry,
            'person': person,
            'cases': cases
        }
        result = scraper.scrape_charges(input_params)
        assert result == -1

        mock_proxies.assert_called_with()
        mock_headers.assert_called_with()

    @responses.activate
    @patch("recidiviz.ingest.us_vt.us_vt_scraper.UsVtScraper.store_record")
    @patch("recidiviz.ingest.scraper_utils.get_headers")
    @patch("recidiviz.ingest.scraper_utils.get_proxies")
    def test_error_on_save(self, mock_proxies, mock_headers, mock_store_record):
        scraper = UsVtScraper()
        scrape_type = 'background'
        roster_entry = ROSTER_PAGE_JSON['data'][0]
        person = PERSON_JSON
        cases = CASES_JSON

        proxies = {'http': 'http://user:password@proxy.biz/'}
        mock_proxies.return_value = proxies
        headers = {'User-Agent': 'test_user_agent'}
        mock_headers.return_value = headers
        mock_store_record.return_value = -1

        responses.add(responses.POST, 'https://omsweb.public-safety-cloud.com/'
                                      'jtclientweb/'
                                      '(S(e47fszhY4nYPfTEGEoZ9QU3R))//'
                                      '(S(e47fszhY4nYPfTEGEoZ9QU3R))/'
                                      'JailTracker/GetCharges',
                      json=CHARGES_JSON, status=200)

        input_params = {
            'session': SESSION,
            'scrape_type': scrape_type,
            'roster_entry': roster_entry,
            'person': person,
            'cases': cases
        }
        result = scraper.scrape_charges(input_params)
        assert result == -1

        mock_proxies.assert_called_with()
        mock_headers.assert_called_with()
        mock_store_record.assert_called_with(
            roster_entry, person, cases, CHARGES_JSON)


def test_get_initial_task():
    scraper = UsVtScraper()
    assert scraper.get_initial_task() == 'scrape_front_page'


def test_extract_agencies():
    pass


def test_person_id_to_record_id():
    pass


def test_person_id_to_record_id_no_person():
    pass


def test_person_id_to_record_id_no_record():
    pass


def test_record_to_snapshot():
    pass


def test_compare_and_set_snapshot():
    pass
