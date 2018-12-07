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


"""Generic scraper implementation for regions using JailTracker.

All subclasses of JailtrackerScraper must implement:
- get_jailtracker_index
- process_record

JailTracker exposes data via JSON endpoints, so the scraper only scrapes the
page for the session token required to access those endpoints, and the data
is then requested directly.

Scraper flow:
    1. Requests landing page and scrapes required session token from the page
        content.
    2. Requests roster in increments of _ROSTER_PAGE_SIZE.
    3. For each person on each page of the roster, sequentially requests the
        person, cases, and charges details.
    4. When all requests for an individual person are completed, passes the
        data to process_record.
"""

import abc
import json
import logging
import re

from lxml import html
from recidiviz.ingest.base_scraper import BaseScraper


class JailTrackerScraper(BaseScraper):
    """Generic scraper for regions using JailTracker."""

    # Number of roster entries requested in a single request.
    _ROSTER_PAGE_SIZE = 25

    # URL base for all JailTracker pages and requests.
    _URL_BASE = "https://omsweb.public-safety-cloud.com/jtclientweb/"

    # Template for URL suffix used to request a specific region's landing page.
    #
    # Must be provided with 'index' field before being used.
    _LANDING_PAGE_URL_SUFFIX_TEMPLATE = "jailtracker/index/{index}"

    # Template for URL suffix used to request roster data.
    #
    # Must be provided with 'session', 'start', and 'limit' fields before being
    # used.
    _ROSTER_REQUEST_SUFFIX_TEMPLATE = "(S({session}))//(S({session}))/"
    "JailTracker/GetInmates?start={start}&limit={limit}&sort=LastName&dir=ASC"

    # Template for URL suffix used to request data on a specific person.
    #
    # Must be provided with 'session' and 'arrest' fields before being used.
    _PERSON_REQUEST_SUFFIX_TEMPLATE = "(S({session}))//(S({session}))/"
    "JailTracker/GetInmate?arrestNo={arrest}"

    # Template for URL suffix used to request data on cases associated with a
    # specific person.
    #
    # Must be provided with 'session' and 'arrest' fields before being used.
    _CASES_REQUEST_SUFFIX_TEMPLATE = "(S({session}))//(S({session}))/"
    "JailTracker/GetCases?arrestNo={arrest}"

    # Template for URL suffix used to request data on charges associated with
    # a specific person.
    #
    # Must be provided with 'session' field before being used.
    _CHARGES_REQUEST_SUFFIX_TEMPLATE = "(S({session}))//(S({session}))/"
    "JailTracker/GetCharges"

    # Constant strings needed for param and data dicts.
    #
    # These are only values we're setting internally on our own data structures.
    # Strings needed to parse JailTracker's JSON responses are handled as raw
    # strings within each parsing method.

    # Key in param dict for identifier used for requesting person, cases, and
    # charges.
    _ARREST_NUMBER = "arrest_number"
    # Key in param dict for JSON object containing cases.
    _CASES = "cases"
    # Value in param dict for a request targeting the cases endpoint.
    _CASES_REQUEST = "cases_request"
    # Value in param dict for a request targeting the charges endpoint.
    _CHARGES_REQUEST = "charges_request"
    # Key in param dict for data dict passed to _fetch_content.
    _DATA = "data"
    # Key in param dict for endpoint requested.
    _ENDPOINT = "endpoint"
    # Value in data dict for an expected response type of HTML.
    _HTML = "HTML"
    # Value in data dict for an expected response type of JSON.
    _JSON = "JSON"
    # Key in param dict for JSON object for a person.
    _PERSON = "person"
    # Value in param dict for a request targeting the person endpoint.
    _PERSON_REQUEST = "person_request"
    # Key in param dict specifying the type of endpoint requested.
    _REQUEST_TARGET = "request_target"
    # Key in data dict for the expected response type.
    _RESPONSE_TYPE = "response_type"
    # Value in param dict for a request targeting the roster endpoint.
    _ROSTER_REQUEST = "roster_request"
    # Key in param dict for scrape type.
    _SCRAPE_TYPE = "scrape_type"
    # Key in param dict for session token.
    _SESSION_TOKEN = "session_token"
    # Key in param dict for task type.
    _TASK_TYPE = "task_type"

    def __init__(self, region_name):
        super(JailTrackerScraper, self).__init__(region_name)

        # Set initial endpoint using region-specific landing page index.
        landing_page_url_suffix = self._LANDING_PAGE_URL_SUFFIX_TEMPLATE.format(
            index=self.get_jailtracker_index())
        self._initial_endpoint = "/".join(
            [self._URL_BASE, landing_page_url_suffix])

    @abc.abstractmethod
    def get_jailtracker_index(self):
        """Returns the index used in the JailTracker URL to request a specific
        region's landing page.

        A JailTracker landing page URL ends with: "/jailtracker/index/<INDEX>".
        This value can either be text or a number. In either case, this method
        should return the value as a string.
        """
        pass

    @abc.abstractmethod
    def process_record(self, person, cases, charges):
        """Performs region-specific processing on the fetched data for a
        specific person.

        Args:
            person: JSON object for a specific person
            cases: JSON object for that person's cases
            charges: JSON object for that person's charges
        """
        pass

    def get_initial_endpoint(self):
        """Returns the initial endpoint to hit on the first call."""

        return self._initial_endpoint

    def get_initial_data(self):
        """Returns the initial data to send on the first call."""

        # First request will be for landing page, which gives an HTML response.
        return {self._RESPONSE_TYPE: self._HTML}

    def get_more_tasks(self, content, params):
        """Gets more tasks based on the content and params passed in.

        Args:
            content: An lxml html tree on the first request, and a JSON
                object for all following requests.
            params: dict of parameters passed from the last scrape session.

        Returns:
            A list of param dicts, one for each task we want to run.
        """

        task_type = params.get(self._TASK_TYPE, self.get_initial_task_type())
        next_tasks = []

        if self.is_initial_task(task_type):
            roster_request_params = \
                self._process_landing_page_and_get_next_task(content, params)
            if roster_request_params == -1:
                return -1
            next_tasks.append(roster_request_params)

        elif params[self._REQUEST_TARGET] == self._ROSTER_REQUEST:
            next_tasks.extend(
                self._process_roster_response_and_get_next_tasks(
                    content, params))

        elif params[self._REQUEST_TARGET] == self._PERSON_REQUEST:
            next_tasks.append(
                self._process_person_response_and_get_next_task(
                    content, params))

        elif params[self._REQUEST_TARGET] == self._CASES_REQUEST:
            next_tasks.append(
                self._process_cases_response_and_get_next_task(
                    content, params))

        elif params[self._REQUEST_TARGET] == self._CHARGES_REQUEST:
            # Once all three requests have been made for a specific person, the
            # data can be passed to the region-specific scraper for handling.
            self.process_record(
                params[self._PERSON], params[self._CASES], content)

        return next_tasks

    def _process_landing_page_and_get_next_task(self, content, params):
        """Scrapes session token from landing page and creates params for
        first roster request.

        Args:
            content: lxml html tree of landing page content
            params: dict of parameters used for last request

        Returns:
            Param dict for first roster request on success or -1 on failure.
        """

        session_token = None
        try:
            body_script = html.tostring(content.xpath("//body/div/script")[0])
            session_token = re.search(r"JailTracker.Web.Settings.init\('(.*)'",
                                      body_script).group(1)
        except Exception, exception:
            logging.error("Error, could not parse session token from the "
                          "landing page HTML. Error: %s\nPage content:\n\n%s",
                          exception, content)
            logging.error(exception)
            return -1

        roster_request_suffix = self._ROSTER_REQUEST_SUFFIX_TEMPLATE.format(
            session=session_token,
            start=0,
            limit=self._ROSTER_PAGE_SIZE)
        roster_request_endpoint = "/".join(
            [self._URL_BASE, roster_request_suffix])

        return {
            self._DATA: {self._RESPONSE_TYPE: self._JSON},
            self._ENDPOINT: roster_request_endpoint,
            self._REQUEST_TARGET: self._ROSTER_REQUEST,
            self._SCRAPE_TYPE: params[self._SCRAPE_TYPE],
            self._SESSION_TOKEN: session_token
        }

    def _process_roster_response_and_get_next_tasks(self, response, params):
        """Returns next tasks from a single roster response.

        One task will be created for each person in the response. Additionally,
        if the response does not represent the end of the roster, a task will
        be created to fetch the next page of the roster.

        Args:
            response: JSON object of one page of roster
            params: dict of parameters used for last request

        Returns:
            List of param dicts, one for each task to run.
        """

        next_tasks = []

        max_index_present = 0
        for roster_entry in response["data"]:
            row_index = roster_entry["RowIndex"]
            if row_index > max_index_present:
                max_index_present = row_index

            arrest_number = roster_entry["ArrestNo"]

            person_request_suffix = self._PERSON_REQUEST_SUFFIX_TEMPLATE.format(
                session=params[self._SESSION_TOKEN],
                arrest=arrest_number)
            person_request_endpoint = "/".join(
                [self._URL_BASE, person_request_suffix])

            next_tasks.append({
                self._ARREST_NUMBER: arrest_number,
                self._DATA: {self._RESPONSE_TYPE: self._JSON},
                self._ENDPOINT: person_request_endpoint,
                self._REQUEST_TARGET: self._PERSON_REQUEST,
                self._SCRAPE_TYPE: params[self._SCRAPE_TYPE],
                self._SESSION_TOKEN: params[self._SESSION_TOKEN]
            })

        # If we aren't done reading the roster, request another page
        if response["totalCount"] > max_index_present:
            roster_request_suffix = self._ROSTER_REQUEST_SUFFIX_TEMPLATE.format(
                session=params[self._SESSION_TOKEN],
                # max_index_present doesn't need to be incremented because the
                # returned RowIndex is 1-based while the index in the request
                # is 0-based.
                start=max_index_present,
                limit=self._ROSTER_PAGE_SIZE)
            roster_request_endpoint = "/".join(
                [self._URL_BASE, roster_request_suffix])

            next_tasks.append({
                self._DATA: {self._RESPONSE_TYPE: self._JSON},
                self._ENDPOINT: roster_request_endpoint,
                self._REQUEST_TARGET: self._ROSTER_REQUEST,
                self._SCRAPE_TYPE: params[self._SCRAPE_TYPE],
                self._SESSION_TOKEN: params[self._SESSION_TOKEN]
            })

        return next_tasks

    def _process_person_response_and_get_next_task(self, response, params):
        """Creates cases request task for the person provided in the response.

        The returned params will also include the JSON person object from the
        response, so it can be combined with cases and charges once both are
        fetched.

        Args:
            response: JSON object of one person
            params: dict of parameters used for last request

        Returns:
            Dict of parameters for cases request
        """

        cases_request_suffix = self._CASES_REQUEST_SUFFIX_TEMPLATE.format(
            session=params[self._SESSION_TOKEN],
            arrest=params[self._ARREST_NUMBER])
        cases_request_endpoint = "/".join(
            [self._URL_BASE, cases_request_suffix])

        return {
            self._ARREST_NUMBER: params[self._ARREST_NUMBER],
            self._DATA: {self._RESPONSE_TYPE: self._JSON},
            self._ENDPOINT: cases_request_endpoint,
            self._PERSON: response,
            self._REQUEST_TARGET: self._CASES_REQUEST,
            self._SCRAPE_TYPE: params[self._SCRAPE_TYPE],
            self._SESSION_TOKEN: params[self._SESSION_TOKEN]
        }

    def _process_cases_response_and_get_next_task(self, response, params):
        """Creates charges request task for the person provided in params.

        The returned params will also include the JSON objects for both person
        and cases, so they can be combined with charges once those are fetched.

        Args:
            response: JSON object of cases for one person
            params: dict of parameters used for last request

        Returns:
            Dict of parameters for charges request
        """

        charges_request_suffix = self._CHARGES_REQUEST_SUFFIX_TEMPLATE.format(
            session=params[self._SESSION_TOKEN])
        charges_request_endpoint = "/".join(
            [self._URL_BASE, charges_request_suffix])

        return {
            self._CASES: response,
            # For this request, arrest number must be passed in the request data
            # and not via the endpoint URL.
            self._DATA: {
                "arrestNo": params[self._ARREST_NUMBER],
                self._RESPONSE_TYPE: self._JSON
            },
            self._ENDPOINT: charges_request_endpoint,
            self._PERSON: params[self._PERSON],
            self._REQUEST_TARGET: self._CHARGES_REQUEST,
            self._SCRAPE_TYPE: params[self._SCRAPE_TYPE],
            self._SESSION_TOKEN: params[self._SESSION_TOKEN]
        }

    # Overrides method in GenericScraper to handle JSON responses.
    def _fetch_content(self, endpoint, post_data=None, json_data=None):
        """Returns the response content, either HTML or JSON.

        'data' is expected to contain an entry keyed on '_RESPONSE_TYPE',
        indicating whether the expected response type is HTML or JSON.

        Args:
            endpoint: the endpoint to make a request to.
            post_data: dict of parameters to pass into the request.

        Returns:
            Returns the content of the response on success or -1 on failure.
        """

        response_type = post_data.get(self._RESPONSE_TYPE, None)
        if response_type is None:
            logging.error(
                "Missing response type for endpoint %s. Data:\n%s"
                % (endpoint, post_data))
            return -1

        # Remove response type before passing on data.
        post_data.pop(self._RESPONSE_TYPE)

        if response_type == self._HTML:
            # Fall back on GenericScraper behavior.
            return super(JailTrackerScraper, self)._fetch_content(
                endpoint, post_data)
        elif response_type == self._JSON:
            response = self.fetch_page(endpoint)
            if response == -1:
                return -1
            return json.loads(response.content)
        else:
            logging.error("Unexpected response type %s for endpoint %s"
                          % (response_type, endpoint))
            return -1
