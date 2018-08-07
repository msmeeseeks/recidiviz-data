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


"""This file defines a scraper base class that all regional scrapers
inherit from.
"""

import abc
from datetime import date
import json
import logging
import string
import random

import dateutil.parser as parser
import requests

from google.appengine.ext import deferred
from google.appengine.ext import ndb
from google.appengine.api import taskqueue
from recidiviz.models import env_vars
from recidiviz.ingest import sessions
from recidiviz.ingest.models.scrape_key import ScrapeKey
from recidiviz.ingest import tracker
from recidiviz.utils import environment
from recidiviz.utils.regions import Region


class Scraper(object):
    """The base for all scraper objects. It handles basic setup, scrape
    process control (start, pause, resume, stop), web requests, task
    queueing, state tracking, and a bunch of static convenience
    methods for data manipulation.

    Note that all child classes must implement the inmate_id_to_record_id
    method, which is used to iterate docket items.

    """

    def __init__(self, region_name):

        self.REGION = Region(region_name)
        self.FAIL_COUNTER = (
            self.REGION.region_code + "_next_page_fail_counter")
        self.SCRAPER_WORK_URL = '/scraper/work'


    @abc.abstractmethod
    def inmate_id_to_record_id(self, inmate_id):
        pass


    def start_scrape(self, scrape_type):
        """Start new scrape session / query against corrections site

        Retrieves first docket item, enqueues task for initial search
        page scrape to start the new scraping session.

        Args:
            scrape_type: (string) The type of scrape to start

        Returns:
            N/A

        """
        docket_item = self.iterate_docket_item(scrape_type)
        if not docket_item:
            logging.error("Found no %s docket items for %s, shutting down." % (
                scrape_type, self.REGION.region_code))
            sessions.end_session(ScrapeKey(self.REGION.region_code,
                                           scrape_type))
            return

        params = {"scrape_type": scrape_type, "content": docket_item}

        self.add_task(self.INITIAL_TASK, params)


    def stop_scrape(self, scrape_types):
        """Stops all active scraping tasks, resume non-targeted scrape types

        Stops the scraper, even if in the middle of a session. In
        production, this is called by a cron job scheduled to prevent
        interference with the normal operation of the scraped site.

        We share the scraping taskqueue between snapshot and
        background scraping to be certain of our throttling for the
        third-party service. As a result, cleaning up / purging the
        taskqueue necessarily kills all scrape types.  We kick off
        resume_scrape for any ongoing scraping types that aren't
        targets.

        Args:
            scrape_types: (list of strings) Scrape types to terminate

        Returns:
            N/A

        """
        # Check for other running scrapes, and if found kick off a delayed
        # resume for them since the taskqueue purge below will kill them.
        other_scrapes = set([])

        open_sessions = sessions.get_open_sessions(self.REGION.region_code)
        for session in open_sessions:
            if session.scrape_type not in scrape_types:
                other_scrapes.add(session.scrape_type)

        for scrape in other_scrapes:
            logging.info("Setting 60s deferred task to resume unaffected "
                         "scrape type: %s." % str(scrape))
            deferred.defer(self.resume_scrape, scrape, _countdown=60)

        q = taskqueue.Queue(self.REGION.queues[0])
        q.purge()


    def resume_scrape(self, scrape_type):
        """Resume a stopped scrape from where it left off

        Starts the scraper up again at the same place (roughly) as it had been
        stopped previously. This allows for cron jobs to start/stop scrapers at
        different times of day.

        Args:
            scrape_type: (string) Type of scraping to resume

        Returns:
            N/A
        """
        recent_sessions = sessions.get_recent_sessions(ScrapeKey(
            self.REGION.region_code, scrape_type))

        if scrape_type == "background":
            # Background scrape

            # In most scrapers, background scrapes will use
            # short-lived docket items. However, some background
            # scrapes use only one docket item to run a giant scrape,
            # which may run for months. Limitations in GAE Pull Queues
            # make it difficult to keep track of a leased task for
            # that long, so we don't try. Resuming a background scrape
            # simply resumes from session data, and the task stays in
            # the docket un-leased. It will get deleted the next time
            # we start a new background scrape.

            last_scraped = None
            for session in recent_sessions:
                if session.last_scraped:
                    last_scraped = session.last_scraped
                    break

            if last_scraped:
                content = last_scraped.split(', ')
            else:
                logging.error("No earlier session with last_scraped found; "
                              "cannot resume.")

        else:
            # Snapshot scrape

            # Get an item from the docket and continue from there. These queries
            # are very quick, so we don't bother trying to resume the same task
            # we left off on.

            content = self.iterate_docket_item(scrape_type)
            if not content:
                sessions.end_session(
                    ScrapeKey(self.REGION.region_code, scrape_type))
                return

        params = {'scrape_type': scrape_type, 'content': content}
        self.add_task(self.INITIAL_TASK, params)


    def fetch_page(self, url, data=None):
        """Fetch content from a URL. If data is None (the default), we perform
        a GET for the page. If the data is set, it must be a dict of parameters
        to use as POST data in a POST request to the url.

        Args:
            url: URL to fetch content from
            data: POST data to send

        Returns:
            The content if successful, -1 if fails.

        """
        proxies = self.get_proxies()
        headers = self.get_headers()

        try:
            if data is None:
                page = requests.get(url, proxies=proxies, headers=headers)
            else:
                page = requests.post(url, proxies=proxies, headers=headers,
                                     data=data)
        except requests.exceptions.RequestException as ce:
            log_error = "Error: {0}".format(ce)

            if ce.request:
                log_error += ("\n\nRequest headers: \n{0}"
                              "\n\nMethod: {1}"
                              "\n\nBody: \n{2} ")
                log_error = log_error.format(
                    ce.request.headers,
                    ce.request.method,
                    ce.request.body)

            if ce.response:
                log_error += ("\n\nResponse: \n{0} / {1}"
                              "\n\nHeaders: \n{2}"
                              "\n\nText: \n{3}")
                log_error = log_error.format(
                    ce.response.status_code,
                    ce.response.reason,
                    ce.response.headers,
                    ce.response.text)

            logging.warning("Problem retrieving page, failing task to "
                            "retry. \n\n%s" % log_error)
            return -1

        return page


    def add_task(self, task_name, params):
        """ Add a task to the task queue.
        """

        params_serial = json.dumps(params)

        taskqueue.add(url=self.SCRAPER_WORK_URL,
                      # TODO (Issue #88): Replace this with dynamic
                      # queue selection
                      queue_name=self.REGION.queues[0],
                      params={'region': self.REGION.region_code,
                              'task': task_name,
                              'params': params_serial})


    def iterate_docket_item(self, scrape_type):
        """Leases new docket item, updates current session, returns item
        contents

        Returns an entity to scrape as provided by the docket item.

        Args:
            scrape_type: (string) Type of docket item to retrieve

        Returns:
            False if there was any failure to retrieve a new docket item.
            If successful:
                Background scrape: ("surname", "given names")
                Snapshot scrape:   ("record_id", ["records to ignore", ...])

        """

        item_content = tracker.iterate_docket_item(
            ScrapeKey(self.REGION.region_code, scrape_type))

        if not item_content:
            return False

        if scrape_type == "snapshot":
            # Content will be in the form (inmate ID, [list of records
            # to ignore]); allow the child class to convert inmate to
            # record
            record_id = self.inmate_id_to_record_id(item_content[0])

            if not record_id:
                logging.error("Couldn't convert docket item [%s] to record"
                              % str(item_content))
                return False

            return (record_id, item_content[1])

        return item_content


    @staticmethod
    def parse_date_string(date_string, inmate_id):
        """Converts string describing date to Python date object

        Dates are expressed differently in different records,
        typically following one of these patterns:

            "07/2001",
            "12/21/1991",
            "06/14/13", etc.

        This function parses several common variants and returns a datetime.

        Args:
            date_string: (string) Scraped string containing a date
            inmate_id: (string) Inmate ID this date is for, for logging

        Returns:
            Python date object representing the date parsed from the string, or
            None if string wasn't one of our expected values (this is common,
            often NONE or LIFE are put in for these if life sentence).

        """
        if date_string:
            try:
                result = parser.parse(date_string)
                result = result.date()
            except ValueError:
                logging.debug("Couldn't parse date string '%s' for inmate: %s" %
                              (date_string, inmate_id))
                return None

            # If month-only date, manually force date to first of the month.
            if len(date_string.split("/")) == 2:
                result = result.replace(day=1)

        else:
            return None

        return result


    @staticmethod
    def generate_id(entity_kind):
        """Generate unique, 10-digit alphanumeric ID for entity kind provided

        Generates a new 10-digit alphanumeric ID, checks it for uniqueness for
        the entity type provided, and retries if needed until unique before
        returning a new ID.

        Args:
            entity_kind: (ndb model class, e.g. us_ny.UsNyInmate) Entity kind to
                check uniqueness of generated id.

        Returns:
            The new ID / key name (string)
        """
        new_id = ''.join(random.choice(string.ascii_uppercase +
                                       string.ascii_lowercase +
                                       string.digits) for _ in range(10))

        test_key = ndb.Key(entity_kind, new_id)
        key_result = test_key.get()

        if key_result is not None:
            # Collision, try again
            return Scraper.generate_id(entity_kind)

        return new_id


    @staticmethod
    def normalize_key_value_row(row_data):
        """Removes extraneous whitespace from scraped data

        Removes extraneous (leading, trailing, internal) whitespace from scraped
        data.

        Args:
            row_data: (list) One row of data in a list (key/value pair)

        Returns:
            Tuple of cleaned strings, in the order provided.
        """
        key = ' '.join(row_data[0].text_content().split())
        value = ' '.join(row_data[1].text_content().split())
        return key, value


    @staticmethod
    def calculate_age(birth_date):
        """Converts birth date to age during current scrape.

        Determines age of inmate based on her or his birth date. Note: We don't
        know the timezone of birth, so we use local time for us. Result may be
        off by up to a day.

        Args:
            birth_date: (date) Date of birth as reported by prison system

        Returns:
            (int) Age of inmate
        """
        today = date.today()
        age = today.year - birth_date.year - ((today.month, today.day) <
                                              (birth_date.month,
                                               birth_date.day))

        return age


    @staticmethod
    def get_proxies(use_test=False):
        """Retrieves proxy username/pass from environment variables

        Retrieves proxy information to use in requests to third-party
        services. If not in production environment, defaults to test proxy
        credentials (so problems during test runs don't risk our main proxy
        IP's reputation).

        Args:
            use_test: (bool) Use test proxy credentials, not prod

        Returns:
            Proxies dict for requests library, in the form:
                {'<protocol>': '<http://<proxy creds>@<proxy url>'}

        Raises:
            Exception: General exception, since scraper cannot
            proceed without this

        """
        in_prod = environment.in_prod()

        if not in_prod or use_test:
            user_var = "test_proxy_user"
            pass_var = "test_proxy_password"
        else:
            user_var = "proxy_user"
            pass_var = "proxy_password"

        proxy_url = env_vars.get_env_var("proxy_url", None)

        proxy_user = env_vars.get_env_var(user_var, None)
        proxy_password = env_vars.get_env_var(pass_var, None)

        if (proxy_user is None) or (proxy_password is None):
            raise Exception("No proxy user/pass")

        proxy_credentials = proxy_user + ":" + proxy_password
        proxy_request_url = 'http://' + proxy_credentials + "@" + proxy_url

        proxies = {'http': proxy_request_url}

        return proxies


    @staticmethod
    def get_headers():
        """Retrieves headers (e.g., user agent string) from environment
        variables

        Retrieves user agent string information to use in requests to
        third-party services.

        Args:
            N/A

        Returns:
            Headers dict for the requests library, in the form:
                {'User-Agent': '<user agent string>'}

        Raises:
            Exception: General exception, since scraper cannot
            proceed without this

        """
        user_agent_string = env_vars.get_env_var("user_agent", None)

        if not user_agent_string:
            raise Exception("No user agent string")

        headers = {'User-Agent': (user_agent_string)}
        return headers
