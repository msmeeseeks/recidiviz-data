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


"""Scraper implementation for the state of New York (NYS DOCCS)

Region-specific notes:
    - DOCCS includes historical data (back to ~1920s, reliably back to ~1970s)
    - DOCCS allows for surname-only search
    - DOCCS returns the nearest match alphabetically, and then all subsequent
      names in the alphabet.
        - As a result, the us-ny names list is one name, 'aaardvark', since
          this query will return all inmates in the DOCCS system.
    - DOCCS attempts to de-duplicate inmates, and show a disambiguation page
      for multiple records it believes to be of the same person.

Background scraping procedure:
    1. A starting search page (to get session vars) --> (search, see 2)
    2. A results list (4x/page) --> (parse, see 2a - 2c)
        (2a) A list of inmate results --> (follow, each entry leads to 3)
        (2b) The 'next page' of results for the query --> (follow it, see 2)
        (2c) The main search page --> Reached end of the list, stop scrape
    3. EITHER
        (3a) A disambiguation page (which record would you like to see for this
             inmate?)
        (3b) A details page for the inmate, about a specific incarceration
             event
"""

from copy import deepcopy
import logging
import re

from lxml import html
from lxml.etree import XMLSyntaxError  # pylint:disable=no-name-in-module

from google.appengine.ext import deferred
from google.appengine.ext import ndb
from google.appengine.ext.db import InternalError
from google.appengine.ext.db import Timeout, TransactionFailedError
from google.appengine.api import memcache

from recidiviz.models.record import Offense, SentenceDuration
from recidiviz.ingest import sessions
from recidiviz.ingest.models.scrape_key import ScrapeKey
from recidiviz.ingest.scraper import Scraper
from recidiviz.ingest import scraper_utils
from recidiviz.ingest import tracker
from recidiviz.ingest.sessions import ScrapedRecord
from recidiviz.ingest.us_ny.us_ny_inmate import UsNyInmate
from recidiviz.ingest.us_ny.us_ny_record import UsNyRecord
from recidiviz.ingest.us_ny.us_ny_snapshot import UsNySnapshot


class UsNyScraper(Scraper):
    """Class to scrape info from the NY state DOCCS searcher.
    """

    def __init__(self):
        super(UsNyScraper, self).__init__('us_ny')

    def get_initial_task(self):
        """The name of the initial task to run for this scraper.
        """
        return 'scrape_search_page'

    def scrape_search_page(self, params):
        """Scrapes the search page for DOCCS to get session variables

        Gets the main search page, pulls form details and session
        variables.  Enqueues task with params from search page form,
        to fetch the first page of results.

        Args:
            params: Dict of parameters, includes:
                first_name: (string) First name for inmate query
                    (empty string if last-name only search)
                last_name: (string) Last name for inmate query (required)

        Returns:
            Nothing if successful, -1 if fails.

        """
        search_page = self.fetch_page(self.get_region().base_url)
        if search_page == -1:
            return -1

        try:
            search_tree = html.fromstring(search_page.content)
        except XMLSyntaxError as e:
            logging.error("Error parsing search page. Error: %s\nPage:\n\n%s",
                          str(e), str(search_page.content))
            return -1

        try:
            # Extract said tokens and construct the URL for searches
            session_K01 = search_tree.cssselect('[name=K01]')[0].get("value")
            session_token = search_tree.cssselect(
                '[name=DFH_STATE_TOKEN]')[0].get("value")
            session_action = search_tree.xpath(
                '//div[@id="content"]/form/@action')[0]
        except IndexError:
            logging.error("Search page not understood. HTML:\n\n%s",
                          search_page.content)
            return -1

        search_results_params = {
            'first_page': True,
            'k01': session_K01,
            'token': session_token,
            'action': session_action,
            'scrape_type': params['scrape_type'],
            'content': params['content']}

        if params["scrape_type"] == "background":
            task_name = "scrape_search_results_page"
        else:
            task_name = "scrape_inmate"

        self.add_task(task_name, search_results_params)

        return None

    def scrape_search_results_page(self, params):
        """Scrapes page of search results, follows each listing and 'Next page'

        Fetches search results page, parses results to extract inmate listings
        and params for the next page of results. Enqueues tasks to scrape both
        the individual listings and next page of results.

        Args:

            params: Dict of parameters. This is how DOCCS keeps track
            of where you are and what you're requesting. Not all of
            the parameters are understood, and some are frequently
            empty values.

                First page of results only:
                first_page: (bool) True / exists only if this is requesting the
                    first page of results. Added in scrape_search_page(), not
                    scraped.

                Subsequent pages of results only:

                clicki: (string) A request parameter for results,
                    scraped from form
                dini: (string) A request parameter for results,
                    scraped from the form
                k01,k02,k03,k04,k05,k06: (strings) Request parameters
                    for results, scraped from form
                map_token: (string) A request parameter for results,
                    scraped from form
                next: (string) A request parameter for results,
                    scraped from form

                All results pages:
                first_name: (string) Given names for inmate query
                    (empty string if surname-only search)
                last_name: (string) Surname for inmate query (required)
                k01: (string) A request parameter for results, scraped from
                    form
                token: (string) DFH_STATE_TOKEN - session info from the form
                action: (string) The 'action' attr from the scraped
                    form - URL to POST our request to
                scrape_type: (string) Type of scrape to perform
                content: (tuple) Search target, stored as a tuple:
                    Background scrape: ("surname", "given names")
                    Snapshot scrape:   ("record_id", ["records to ignore", ...])

        Returns:
            Nothing if successful, -1 if fails.

        """

        url = self.get_region().base_url + str(params['action'])
        scrape_type = params['scrape_type']
        scrape_content = params['content']

        # Request for the first results page is unique
        if 'first_page' in params:
            surname = scrape_content[0]
            given_names = scrape_content[1]

            data = {
                'K01': params['k01'],
                'DFH_STATE_TOKEN': params['token'],
                'DFH_MAP_STATE_TOKEN': '',
                'M00_LAST_NAMEI': surname,
                'M00_FIRST_NAMEI': given_names,
                'M00_MID_NAMEI': '',
                'M00_NAME_SUFXI': '',
                'M00_DOBCCYYI': '',
                'M00_DIN_FLD1I': '',
                'M00_DIN_FLD2I': '',
                'M00_DIN_FLD3I': '',
                'M00_NYSID_FLD1I': '',
                'M00_NYSID_FLD2I': ''
            }

        else:
            data = {
                'M13_PAGE_CLICKI': params['clicki'],
                'M13_SEL_DINI': params['dini'],
                'K01': params['k01'],
                'K02': params['k02'],
                'K03': params['k03'],
                'K04': params['k04'],
                'K05': params['k05'],
                'K06': params['k06'],
                'DFH_STATE_TOKEN': params['token'],
                'DFH_MAP_STATE_TOKEN': params['map_token'],
                'next': params['next'],
            }

        url = self.get_region().base_url + str(params['action'])
        results_page = self.fetch_page(url, data=data)
        if results_page == -1:
            return -1

        results_tree = html.fromstring(results_page.content)

        # Parse the rows in the search results list
        result_list = results_tree.xpath('//table[@id="dinlist"]/tr/td/form')

        for row in result_list:
            result_params = {}

            # Parse result rows, pull form info to follow link
            result_params['action'] = row.xpath('attribute::action')[0]
            result_params['clicki'] = row.xpath(
                './div/input[@name="M13_PAGE_CLICKI"]/@value')[0]
            result_params['dini'] = row.xpath(
                './div/input[@name="M13_SEL_DINI"]/@value')[0]
            result_params['k01'] = row.xpath(
                './div/input[@name="K01"]/@value')[0]
            result_params['k02'] = row.xpath(
                './div/input[@name="K02"]/@value')[0]
            result_params['k03'] = row.xpath(
                './div/input[@name="K03"]/@value')[0]
            result_params['k04'] = row.xpath(
                './div/input[@name="K04"]/@value')[0]
            result_params['k05'] = row.xpath(
                './div/input[@name="K05"]/@value')[0]
            result_params['k06'] = row.xpath(
                './div/input[@name="K06"]/@value')[0]
            result_params['token'] = row.xpath(
                './div/input[@name="DFH_STATE_TOKEN"]/@value')[0]
            result_params['map_token'] = row.xpath(
                './div/input[@name="DFH_MAP_STATE_TOKEN"]/@value')[0]
            result_params['dinx_name'] = row.xpath(
                './div/input[@type="submit"]/@name')[0]
            result_params['dinx_val'] = row.xpath(
                './div/input[@type="submit"]/@value')[0]

            # Enqueue tasks to follow link / get results
            result_params['content'] = scrape_content
            result_params['scrape_type'] = scrape_type

            self.add_task('scrape_inmate', result_params)

        # Parse the 'next' button's embedded form
        next_params = {}

        try:
            next_button = results_tree.xpath('//div[@id="content"]/form')[0]
            next_params['action'] = next_button.xpath('attribute::action')[0]
            next_params['clicki'] = next_button.xpath(
                './div/input[@name="M13_PAGE_CLICKI"]/@value')[0]
            next_params['dini'] = next_button.xpath(
                './div/input[@name="M13_SEL_DINI"]/@value')[0]
            next_params['k01'] = next_button.xpath(
                './div/input[@name="K01"]/@value')[0]
            next_params['k02'] = next_button.xpath(
                './div/input[@name="K02"]/@value')[0]
            next_params['k03'] = next_button.xpath(
                './div/input[@name="K03"]/@value')[0]
            next_params['k04'] = next_button.xpath(
                './div/input[@name="K04"]/@value')[0]
            next_params['k05'] = next_button.xpath(
                './div/input[@name="K05"]/@value')[0]
            next_params['k06'] = next_button.xpath(
                './div/input[@name="K06"]/@value')[0]
            next_params['token'] = next_button.xpath(
                './div/input[@name="DFH_STATE_TOKEN"]/@value')[0]
            next_params['map_token'] = next_button.xpath(
                './div/input[@name="DFH_MAP_STATE_TOKEN"]/@value')[0]
            next_params['next'] = next_button.xpath(
                './div/div/input[@name="next"]/@value')[0]

            # Update session to remember the first person on this
            # results page, to ensure we can pick up where we left off
            # later if desired. Note that us_ny scraper can only crawl
            # 1x search results page at a time, so there is no risk of
            # contention.
            last_scraped = results_tree.xpath(
                '//td[@headers="name"]')[0].text_content().strip()
            update_session_result = sessions.update_session(
                last_scraped, ScrapeKey(self.get_region().region_code,
                                        scrape_type))

            if not update_session_result:
                return -1

        except IndexError:
            # We got a page we didn't expect - results_parsing_failure will
            # attempt to determine why and whether to wind down current scrape.
            wind_down = self.results_parsing_failure()

            if wind_down:
                return None

            return -1

        # Enqueue task to follow that link / get next results page
        next_params['content'] = scrape_content
        next_params['scrape_type'] = scrape_type

        self.add_task('scrape_search_results_page', next_params)

        return None

    # INMATE PAGES #

    def scrape_inmate(self, params):
        """Fetches inmate listing page, parses results

        Fetches inmate listing page. This sometimes turns out to be a
        disambiguation page, as there may be multiple incarceration
        records in DOCCS for the person we've selected. If so, shunts
        details over to scrape_disambiguation(). If actual results
        page, pulls details from page and sends to store_record.

        Args:
            params: Dict of parameters. This is how DOCCS keeps track
            of where you are and what you're requesting. Not all of
            the parameters are understood, and some are frequently
            empty values.
                scrape_type: (string) Type of scrape to perform
                content: (tuple) Search target, stored as a tuple:
                    Background scrape: ("surname", "given names")
                    Snapshot scrape:   ("record_id", ["records to ignore", ...])
                first_name: (string) First name for inmate query
                    (empty string if last-name only search)
                last_name: (string) Last name for inmate query (required)
                token: (string) DFH_STATE_TOKEN - session info from scraped
                    form
                action: (string) the 'action' attr from the scraped form -
                    URL to POST to
                dini: (string) A request parameter for results,
                    scraped from form
                k01,k02,k03,k04,k05,k06: (strings) Request parameters
                    for results, scraped from form
                map_token: (string) request parameter for results, scraped from
                    the form
                dinx_name: (dict key) Form results are d1-d4, this is
                    the name attr which we store as a key in the
                    params (result: {dinx_name: dinx_val})
                dinx_val: (dict val) value for the dinx_name parameter
                ---params below only apply if we did NOT arrive from a
                    disambiguation page / this inmate only has one listing---
                clicki: (string) a request parameter for results,
                scraped from the form

        Returns:
            Nothing if successful, -1 if fails.

        """

        next_docket_item = False

        url = self.get_region().base_url + str(params['action'])
        scrape_type = params['scrape_type']
        inmate_details = {}
        ignore_list = []

        # Create a request using the provided params. How we structure
        # this varies a bit depending on how we got here.
        if "first_page" in params and scrape_type == "snapshot":
            # Arriving from snapshot scrape / main search page
            ignore_list = params["content"][1]
            next_docket_item = True

            # Decompose record ID / DIN into the three individual
            # fields used in the DOCCS web search.
            din1 = params["content"][0][:2]
            din2 = params["content"][0][2:3]
            din3 = params["content"][0][3:]
            data = {
                'K01': params['k01'],
                'DFH_STATE_TOKEN': params['token'],
                'DFH_MAP_STATE_TOKEN': '',
                'M00_LAST_NAMEI': '',
                'M00_FIRST_NAMEI': '',
                'M00_MID_NAMEI': '',
                'M00_NAME_SUFXI': '',
                'M00_DOBCCYYI': '',
                'M00_DIN_FLD1I': din1,
                'M00_DIN_FLD2I': din2,
                'M00_DIN_FLD3I': din3,
                'M00_NYSID_FLD1I': '',
                'M00_NYSID_FLD2I': ''
            }

        elif 'group_id' in params:
            # Arriving from disambiguation page
            next_docket_item = params['next_docket_item']
            inmate_details['group_id'] = params['group_id']
            inmate_details['linked_records'] = params['linked_records']

            data = {
                'M12_SEL_DINI': params['dini'],
                'K01': params['k01'],
                'K02': params['k02'],
                'K03': params['k03'],
                'K04': params['k04'],
                'K05': params['k05'],
                'K06': params['k06'],
                'DFH_STATE_TOKEN': params['token'],
                'DFH_MAP_STATE_TOKEN': params['map_token'],
                params['dinx_name']: params['dinx_val'],
            }

        else:
            # Arriving from background search / search results page
            data = {
                'M13_PAGE_CLICKI': params['clicki'],
                'M13_SEL_DINI': params['dini'],
                'K01': params['k01'],
                'K02': params['k02'],
                'K03': params['k03'],
                'K04': params['k04'],
                'K05': params['k05'],
                'K06': params['k06'],
                'DFH_STATE_TOKEN': params['token'],
                'DFH_MAP_STATE_TOKEN': params['map_token'],
                params['dinx_name']: params['dinx_val'],
            }

        inmate_page = self.fetch_page(url, data=data)
        if inmate_page == -1:
            return -1

        page_tree = html.fromstring(inmate_page.content)

        # First, test if we got a disambiguation page (inmate has had more than
        # one stay in prison system, page wants to know which you want to see)
        details_page = page_tree.xpath('//div[@id="ii"]')

        if details_page:

            # Create xpath selectors for the parts of the page we want
            # to scrape
            id_and_locale_rows = details_page[0].xpath(
                "./table[contains(@summary, 'Identifying')]/tr")
            crimes_rows = details_page[0].xpath(
                "./table[contains(@summary, 'crimes')]/tr")
            sentence_rows = details_page[0].xpath(
                "./table[contains(@summary, 'sentence')]/tr")

            # Make sure the tables on the page are what we're expecting
            expected_first_row_id = "DIN (Department Identification Number)"
            expected_first_row_crimes = "Crime"
            expected_first_row_sentences = "Aggregate Minimum Sentence"

            actual_first_row_id = (
                id_and_locale_rows[0].xpath("./td")[0].text_content().strip())
            actual_first_row_crimes = (
                crimes_rows[0].xpath("./th")[0].text_content().strip())
            actual_first_row_sentences = (
                sentence_rows[0].xpath("./td")[0].text_content().strip())

            if (actual_first_row_id != expected_first_row_id or
                    actual_first_row_crimes != expected_first_row_crimes or
                    actual_first_row_sentences != expected_first_row_sentences):

                # This isn't the page we were expecting
                logging.warning("Did not find expected tables on inmates page."
                                " Page received: \n%s",
                                html.tostring(page_tree, pretty_print=True))
                return -1

            else:
                crimes = []

                # Capture table data from each details table on the page
                for row in id_and_locale_rows:
                    row_data = row.xpath('./td')
                    key, value = scraper_utils.normalize_key_value_row(
                        row_data)
                    inmate_details[key] = value

                for row in crimes_rows:
                    row_data = row.xpath('./td')

                    # One row is the table headers / has no <td> elements
                    if not row_data:
                        pass
                    else:
                        crime_description, crime_class = (
                            scraper_utils.normalize_key_value_row(row_data))
                        crime = {'crime': crime_description,
                                 'class': crime_class}

                        # Only add to the list if row is non-empty
                        if crime['crime']:
                            crimes.append(crime)

                for row in sentence_rows:
                    row_data = row.xpath('./td')
                    key, value = scraper_utils.normalize_key_value_row(
                        row_data)
                    inmate_details[key] = value

                inmate_details['crimes'] = crimes

            # Kick off next docket item if this concludes our last one
            if next_docket_item:
                # Delete current docket item, update session
                tracker.remove_item_from_session_and_docket(scrape_type)

                # Start the next docket item
                self.start_scrape(params["scrape_type"])

            logging_name = "%s" % (str(params['content']))
            logging_name = logging_name.strip()
            logging.info("(%s) Scraped inmate: %s",
                         logging_name, inmate_details['Inmate Name'])

            return self.store_record(inmate_details)

        else:
            # We're on a disambiguation page, not an actual details
            # page. Scrape the disambig page and follow each link.

            # We can call this one without creating a task, it doesn't
            # create a new network request to DOCCS / doesn't need to
            # be throttled.
            return self.scrape_disambiguation(
                page_tree, params['content'], scrape_type, ignore_list)

    def scrape_disambiguation(self, page_tree, query_content,
                              scrape_type, ignore_list):
        """Scraped record disambiguation page for an inmate in DOCCS

        In attempting to fetch an inmate, the scrape_inmate received a
        disambig page - asking which incarceration event for that inmate they
        wanted. This function takes that result page, parses it, and enqueues a
        new task to scrape each entry from the disambig page.

        Args:
            page_tree: lxml.html parsed object of the disambig page
            query_content: (tuple) The query we're after, in the form:
                Background scrape: ("surname", "given names")
                Snapshot scrape:   ("record_id", ["records to ignore", ...])
            scrape_type: (string) Type of scrape we're performing
            ignore_list: (list of strings) Set of record IDs not to
                follow (set if in snapshot scrape, and some records
                are outside of our metrics / time period of interest
                window)

        Returns:
            Nothing if successful, -1 if fails.
        """
        # Create an ID to group these entries with - DOCCS doesn't
        # tell us how it groups these / give us a persistent ID for
        # inmates, but we want to know each of the entries scraped
        # from this page were about the same person.
        group_id = scraper_utils.generate_id(UsNyInmate)
        new_tasks = []
        department_identification_numbers = []

        # We detect the end of a normal 'background' scrape by noting
        # that we're at the end of the alphabet. In the case of
        # snapshot scrapes, that won't be the case - so we add a
        # marker into one and only one of the inmate scrapes that
        # signifies we should kick off the next task.
        first_task = True if scrape_type == "snapshot" else False

        # Parse the results list
        result_list = page_tree.xpath('//div[@id="content"]/table/tr/td/form')

        if not result_list:
            logging.warning("Malformed inmate or disambig page, failing task "
                            "to re-queue.")
            # We got a page we didn't expect - results_parsing_failure will
            # attempt to disambiguate why.
            wind_down = self.results_parsing_failure()

            if wind_down:
                return None

            return -1

        for row in result_list:
            result_params = {}

            result_params['group_id'] = group_id

            # Parse result row and relevant form info to follow link
            result_params['action'] = row.xpath('attribute::action')[0]
            result_params['dini'] = row.xpath(
                './div/input[@name="M12_SEL_DINI"]/@value')[0]
            result_params['k01'] = row.xpath(
                './div/input[@name="K01"]/@value')[0]
            result_params['k02'] = row.xpath(
                './div/input[@name="K02"]/@value')[0]
            result_params['k03'] = row.xpath(
                './div/input[@name="K03"]/@value')[0]
            result_params['k04'] = row.xpath(
                './div/input[@name="K04"]/@value')[0]
            result_params['k05'] = row.xpath(
                './div/input[@name="K05"]/@value')[0]
            result_params['k06'] = row.xpath(
                './div/input[@name="K06"]/@value')[0]
            result_params['token'] = row.xpath(
                './div/input[@name="DFH_STATE_TOKEN"]/@value')[0]
            result_params['map_token'] = row.xpath(
                './div/input[@name="DFH_MAP_STATE_TOKEN"]/@value')[0]
            result_params['dinx_name'] = row.xpath(
                './div/input[@type="submit"]/@name')[0]
            result_params['dinx_val'] = row.xpath(
                './div/input[@type="submit"]/@value')[0]

            department_identification_numbers.append(result_params['dinx_val'])
            new_tasks.append(result_params)

        for task_params in new_tasks:
            # The disambig page produces far more rows than are
            # visible, each with a form element you can click -
            # programmatically, these look nearly identical to actual
            # result rows.
            #
            # The only differences are
            #   a) they have empty 'value' attr in the 'submit' element /
            #     (thus are empty / invisible in the UI), and
            #   b) they go back to the search results instead of
            #     listing another record.
            #
            # To avoid the latter mucking up our parsing, we test
            # dinx_val to skip these entries.

            if task_params['dinx_val']:

                dept_id_number = task_params['dinx_val']

                # Double-check that we haven't already processed this
                # entry while scraping another disambig page for this
                # same person
                current_session = sessions.get_open_sessions(
                    self.get_region().region_code, most_recent_only=True)

                if current_session is None:
                    # The session's been closed, we should peacefully
                    # end scraping
                    logging.info("No open sessions, exiting without creating "
                                 "new tasks for disambig page results.")
                    return None

                scraped_record = ScrapedRecord.query(ndb.AND(
                    ScrapedRecord.region == self.get_region().region_code,
                    ScrapedRecord.record_id == dept_id_number,
                    ScrapedRecord.created_on > current_session.start)).get()

                if scraped_record:
                    logging.info("We already scraped record %s, skipping.",
                                 dept_id_number)
                    continue

                if dept_id_number in ignore_list:
                    logging.info(
                        "Record %s outside of snapshot range, skipping.",
                        dept_id_number)
                    continue

                # Otherwise, schedule scraping it and add it to the list
                new_scraped_record = ScrapedRecord(
                    region=self.get_region().region_code,
                    record_id=dept_id_number)
                try:
                    new_scraped_record.put()
                except (Timeout, TransactionFailedError, InternalError):
                    logging.warning("Couldn't persist ScrapedRecord entry, "
                                    "record_id: %s", dept_id_number)

                # Set the next_docket_item to True for only one inmate page
                task_params['next_docket_item'] = first_task
                first_task = False

                # Enqueue task to follow that link / scrape record
                task_params['content'] = query_content
                task_params[
                    'linked_records'] = department_identification_numbers
                task_params['scrape_type'] = scrape_type
                self.add_task('scrape_inmate', task_params)

        return None

    def store_record(self, inmate_details):
        """Store scraped data from a results page

        We've scraped an incarceration details page, and want to store
        the data we found. This function does some post-processing on
        the scraped data, and feeds it into the datastore in a way
        that can be indexed / queried in the future.

        Args:
            inmate_details: (dict) Key/value results parsed from the scrape

        Returns:
            Nothing if successful, -1 if fails.

        """

        # INMATE LISTING

        department_identification_numbers = []
        if 'linked_records' in inmate_details:
            department_identification_numbers.extend(
                inmate_details['linked_records'])
        else:
            department_identification_numbers.append(
                inmate_details['DIN (Department Identification Number)'])

        old_id = self.link_inmate(department_identification_numbers)

        if old_id:
            inmate_id = old_id
        else:
            if 'group_id' in inmate_details:
                # If we find no prior records, use the group_id
                # generated earlier in this scraping session as the
                # inmate_id, so as to tie this record to the linked
                # ones for the same inmate.
                inmate_id = inmate_details['group_id']
            else:
                inmate_id = scraper_utils.generate_id(UsNyInmate)

        inmate = UsNyInmate.get_or_insert(inmate_id)

        # Some pre-work to massage values out of the data
        inmate_name = inmate_details['Inmate Name'].split(', ')
        inmate_dob = inmate_details['Date of Birth']
        inmate_age = None
        if inmate_dob:
            inmate_dob = scraper_utils.parse_date_string(inmate_dob, inmate_id)
            if inmate_dob:
                inmate_age = scraper_utils.calculate_age(inmate_dob)
        inmate_sex = inmate_details['Sex'].lower()
        inmate_race = inmate_details['Race / Ethnicity'].lower()

        # NY-specific fields
        inmate.us_ny_inmate_id = inmate_id

        # General Inmate fields
        if inmate_dob:
            inmate.birthday = inmate_dob
        if inmate_age:
            inmate.age = inmate_age
        if inmate_sex:
            inmate.sex = inmate_sex
        if inmate_race:
            inmate.race = inmate_race
        inmate.inmate_id = inmate_id
        inmate.inmate_id_is_fuzzy = True
        inmate.last_name = inmate_name[0]
        if len(inmate_name) > 1:
            inmate_given_name = inmate_name[1]
        else:
            inmate_given_name = ""
        inmate.given_names = inmate_given_name
        inmate.region = self.get_region().region_code

        try:
            inmate_key = inmate.put()
        except (Timeout, TransactionFailedError, InternalError):
            # Datastore error - fail task to trigger queue retry + backoff
            logging.warning("Couldn't persist inmate: %s", inmate_id)
            return -1

        # CRIMINAL RECORD ENTRY

        record_id = inmate_details['DIN (Department Identification Number)']

        record = UsNyRecord.get_or_insert(record_id, parent=inmate_key)
        old_record = deepcopy(record)

        # Some pre-work to massage values out of the data
        last_custody = inmate_details['Date Received (Current)']
        last_custody = scraper_utils.parse_date_string(last_custody, inmate_id)
        first_custody = inmate_details['Date Received (Original)']
        first_custody = scraper_utils.parse_date_string(first_custody,
                                                        inmate_id)
        admission_type = inmate_details['Admission Type']
        county_of_commit = inmate_details['County of Commitment']
        custody_status = inmate_details['Custody Status']
        released = (custody_status != "IN CUSTODY")
        min_sentence = inmate_details['Aggregate Minimum Sentence']
        min_sentence = self.parse_sentence_duration(min_sentence,
                                                    inmate_id)
        max_sentence = inmate_details['Aggregate Maximum Sentence']
        max_sentence = self.parse_sentence_duration(max_sentence,
                                                    inmate_id)
        earliest_release_date = inmate_details['Earliest Release Date']
        earliest_release_date = scraper_utils.parse_date_string(
            earliest_release_date, inmate_id)
        earliest_release_type = inmate_details['Earliest Release Type']
        parole_hearing_date = inmate_details['Parole Hearing Date']
        parole_hearing_date = scraper_utils.parse_date_string(
            parole_hearing_date, inmate_id)
        parole_hearing_type = inmate_details['Parole Hearing Type']
        parole_elig_date = inmate_details['Parole Eligibility Date']
        parole_elig_date = scraper_utils.parse_date_string(parole_elig_date,
                                                           inmate_id)
        cond_release_date = inmate_details['Conditional Release Date']
        cond_release_date = scraper_utils.parse_date_string(cond_release_date,
                                                            inmate_id)
        max_expir_date = inmate_details['Maximum Expiration Date']
        max_expir_date = scraper_utils.parse_date_string(max_expir_date,
                                                         inmate_id)
        max_expir_date_parole = (
            inmate_details['Maximum Expiration Date for Parole Supervision'])
        max_expir_date_parole = scraper_utils.parse_date_string(
            max_expir_date_parole, inmate_id)
        max_expir_date_superv = (
            inmate_details['Post Release Supervision Maximum Expiration Date'])
        max_expir_date_superv = scraper_utils.parse_date_string(
            max_expir_date_superv, inmate_id)
        parole_discharge_date = inmate_details['Parole Board Discharge Date']
        parole_discharge_date = scraper_utils.parse_date_string(
            parole_discharge_date, inmate_id)
        scraped_facility = inmate_details['Housing / Releasing Facility']
        last_release = (
            inmate_details[
                'Latest Release Date / Type (Released Inmates Only)'])
        if last_release:
            release_info = last_release.split(" ", 1)
            last_release_date = scraper_utils.parse_date_string(
                release_info[0], inmate_id)
            last_release_type = release_info[1]
        else:
            last_release_date = None
            last_release_type = None

        record_offenses = []
        for crime in inmate_details['crimes']:
            crime = Offense(
                crime_description=crime['crime'],
                crime_class=crime['class'])
            record_offenses.append(crime)

        # NY-specific Inmate fields
        #   (None)

        # NY-specific record fields
        record.last_custody_date = last_custody
        record.admission_type = admission_type
        record.county_of_commit = county_of_commit
        record.custody_status = custody_status
        record.earliest_release_date = earliest_release_date
        record.earliest_release_type = earliest_release_type
        record.parole_hearing_date = parole_hearing_date
        record.parole_hearing_type = parole_hearing_type
        record.parole_elig_date = parole_elig_date
        record.cond_release_date = cond_release_date
        record.max_expir_date = max_expir_date
        record.max_expir_date_parole = max_expir_date_parole
        record.max_expir_date_superv = max_expir_date_superv
        record.parole_discharge_date = parole_discharge_date

        if min_sentence:
            min_sentence_duration = SentenceDuration(
                life_sentence=min_sentence['Life'],
                years=min_sentence['Years'],
                months=min_sentence['Months'],
                days=min_sentence['Days'])
        else:
            min_sentence_duration = None

        if max_sentence:
            max_sentence_duration = SentenceDuration(
                life_sentence=max_sentence['Life'],
                years=max_sentence['Years'],
                months=max_sentence['Months'],
                days=max_sentence['Days'])
        else:
            max_sentence_duration = None

        # General Record fields
        if record_offenses:
            record.offense = record_offenses
        record.custody_date = first_custody
        record.min_sentence_length = min_sentence_duration
        record.max_sentence_length = max_sentence_duration
        record.birthday = inmate.birthday
        record.sex = inmate.sex
        record.race = inmate.race
        if last_release:
            record.latest_release_type = last_release_type
            record.latest_release_date = last_release_date
        record.last_name = inmate.last_name
        record.given_names = inmate.given_names
        record.record_id = record_id
        record.is_released = released
        record.latest_facility = scraped_facility

        try:
            record.put()
        except (Timeout, TransactionFailedError, InternalError):
            logging.warning("Couldn't persist record: %s", record_id)
            return -1

        # INMATE RECORD SNAPSHOT
        new_snapshot = self.record_to_snapshot(record)

        self.compare_and_set_snapshot(old_record, new_snapshot)

        if 'group_id' in inmate_details:
            logging.info("Checked record for %s %s, inmate %s, in group %s, "
                         "for record %s.", inmate_name[1], inmate_name[0],
                         inmate_id, inmate_details['group_id'], record_id)
        else:
            logging.info("Checked record for %s %s, inmate %s, (no group), for"
                         " record %s.", inmate_name[1], inmate_name[0],
                         inmate_id, record_id)

        return None

    def parse_sentence_duration(self, term_string, inmate_id):
        """Converts string describing sentence duration to
        models.SentenceDuration

        For the 'Maximum Aggregate Sentence' and 'Minimum Aggregate Sentence'
        results, the scraped string often looks like one of these:

            "00 Years, 000 Months, 000 Days",
            "04 Years, 002 Months, 000 Days",
            "LIFE Years, 999 Months, 999 Days", etc.

        There is a bit of inconsistency on number of digits or exact string.
        This function takes the string, and turns it into a dictionary with
        year/month/day values and a 'Life Sentence' boolean.

        Args:
            term_string: (string) Scraped sentence duration string
            inmate_id: (string) Inmate ID this date is for, for logging

        Returns:
            dict of values -
                'Life' (bool) whether sentence is a life term,
                'Years' (int) # years
                'Months' (int) # months
                'Days' (int) # days
            Or None, if string is empty or not parsable.

        """
        if term_string.startswith("LIFE"):

            result = {'Life': True,
                      'Years': 0,
                      'Months': 0,
                      'Days': 0}

        else:
            parsed_nums = re.findall(r'\d+', term_string)

            if not term_string:
                return None
            elif (not parsed_nums) or (len(parsed_nums) < 3):
                logging.debug("Couldn't parse term string '%s' for inmate: %s",
                              term_string, inmate_id)
                return None
            else:
                years = int(parsed_nums[0])
                months = int(parsed_nums[1])
                days = int(parsed_nums[2])

                result = {'Life': False,
                          'Years': years,
                          'Months': months,
                          'Days': days}

        return result

    def link_inmate(self, record_list):
        """Checks for prior records matching newly scraped ones, returns
        inmate ID

        Matches DIN (record IDs) to previously scraped records, looks
        up associated inmates, then returns that inmate_id so we can
        update the same person rather than duplicating the inmate.

        Args:
            record_list: (list of strings) List of DINs (record IDs) to check

        Returns:
            The found inmate_id from prior instance, or None if not found

        """

        for linked_record in record_list:
            query = UsNyRecord.query(UsNyRecord.record_id == linked_record)
            result = query.get()
            if result:
                prior_inmate_key = result.key.parent()
                prior_inmate = prior_inmate_key.get()
                inmate_id = prior_inmate.inmate_id

                logging.info("Found an earlier record with an inmate ID %s,"
                             "using that.", inmate_id)
                return inmate_id

        return None

    def results_parsing_failure(self):
        """Determines cause, handles retries for parsing problems

        We didn't get the page we expected while retrieving a results,
        inmate, or disambiguation page. We retry three times (keeping
        track in memcache), which is long enough for most transient
        errors to go away.

        If we fail three times in a row (with backoff), we first check
        if we've completed the alphabet. On the last page, when we
        click 'Next 4 results', DOCCS just takes the user back to the
        main search page (which could be why trying to parse it like a
        result page failed). If we did, we just shut down the scraper
        because this is success.

        If not, we assume DOCCS has lost state and no longer knows
        what we're asking for. In this case we clean up the current
        scraping session, purge all other tasks in the queue, and kick
        off a new scraping session to get new state in DOCCS to
        continue scraping using.

        Args:
            None

        Returns:
            True if calling function should 'succeed' (not be retried)
            False if calling function should 'fail' (be retried)

        """
        fail_count = memcache.get(key=self.fail_counter)

        fail_count = 0 if not fail_count else fail_count

        if fail_count < 3:
            logging.warning("Couldn't parse next page of results (attempt %d)."
                            " Failing task to force retry.", fail_count)
            fail_count += 1
            memcache.set(key=self.fail_counter, value=fail_count, time=600)
            return False
        else:
            # This is a hacky check for whether we finished the
            # alphabet. The last name in DOCCS as of 11/13/2017 is
            # 'ZYTEL', who's sentenced to life and has no other crimes
            # / disambig.
            current_session = sessions.get_open_sessions(
                self.get_region().region_code, most_recent_only=True)
            if current_session:
                last_scraped = current_session.last_scraped
                scrape_type = current_session.scrape_type
            else:
                logging.error(
                    "No open sessions found! Bad state, ending scrape.")
                return True

            if not last_scraped:
                logging.error(
                    "Session isn't old enough to have a last_scraped "
                    "name yet, but no search results are coming back. "
                    "Finding last scraped name from earlier session.")

                # Get most recent sessions, including closed ones, and find
                # the last one to have a last_scraped name in it. These will
                # come back most-recent-first.
                recent_sessions = sessions.get_recent_sessions(
                    ScrapeKey(self.get_region().region_code, scrape_type))

                for session in recent_sessions:
                    if session.last_scraped:
                        last_scraped = session.last_scraped
                        break

            # TODO (#113): we need a more robust method for detecting
            # the end of the roster.
            if last_scraped[0:3] < "ZYT":

                # We haven't finished the alphabet yet. Most likely,
                # we're failing repeatedly because the server has lost
                # state (e.g., went through a maintenance period). End
                # current scrape / purge tasks, start again from where
                # we left off.
                logging.warning(
                    "Server has lost state. Kicking off new scrape "
                    "task for last name seen in results, and removing "
                    "other tasks with old state.")

                deferred.defer(self.resume_scrape, scrape_type,
                               _countdown=60)
                self.stop_scrape([scrape_type])
                return True

            # We've run out of names, and the 'Next 4 results'
            # button dumped us back at the original search
            # page. Log it, and end the query.

            # Note: In another region (where we search for names
            #       one-by-one), we'd iterate the background
            #       scrape docket item here.
            logging.info("Looped all results. Ending scraping session.")
            self.stop_scrape([scrape_type])

            return True

    def inmate_id_to_record_id(self, inmate_id):
        """Convert provided inmate_id to record_id of any record for that inmate

        The general snapshot logic creates dockets of inmate IDs to snapshot,
        but in the us_ny case this means 'fuzzy' inmate IDs, which were
        generated as a substitute for anything state-provided. Since we can't
        search DOCCS with those, we need to convert them to record IDs instead,
        which DOCCS does allow querying by.

        We only need one DOCCS record ID per inmate ID, because DOCCS
        will take any record ID query to the disambiguation page for
        that inmate if they have more records than just the one
        searched for.

        Args:
            inmate_id: (string) Inmate ID for the inmate

        Returns:
            None if query returns None
            Record ID if a record is found for the inmate in the docket item

        """
        inmate = UsNyInmate.query(UsNyInmate.inmate_id == inmate_id).get()
        if not inmate:
            return None

        record = UsNyRecord.query(ancestor=inmate.key).get()
        if not record:
            return None

        return record.record_id

    def record_to_snapshot(self, record):
        """Mirrors record fields into a Snapshot instance

        Takes in a new Record entity, and mirrors its fields into a
        Snapshot entity for comparison against the last-collected
        snapshot entity for this Record.

        Args:
            record: A Record object to mirror

        Returns:
            A Snapshot entity populated with the same details as the Record

        """
        snapshot = UsNySnapshot(
            parent=record.key,
            latest_facility=record.latest_facility,
            offense=record.offense,
            custody_date=record.custody_date,
            birthday=record.birthday,
            sex=record.sex,
            race=record.race,
            last_name=record.last_name,
            given_names=record.given_names,
            latest_release_date=record.latest_release_date,
            latest_release_type=record.latest_release_type,
            is_released=record.is_released,
            min_sentence_length=record.min_sentence_length,
            max_sentence_length=record.max_sentence_length,
            last_custody_date=record.last_custody_date,
            admission_type=record.admission_type,
            county_of_commit=record.county_of_commit,
            custody_status=record.custody_status,
            earliest_release_date=record.earliest_release_date,
            earliest_release_type=record.earliest_release_type,
            parole_hearing_date=record.parole_hearing_date,
            parole_hearing_type=record.parole_hearing_type,
            parole_elig_date=record.parole_elig_date,
            cond_release_date=record.cond_release_date,
            max_expir_date=record.max_expir_date,
            max_expir_date_superv=record.max_expir_date_superv,
            max_expir_date_parole=record.max_expir_date_parole,
            parole_discharge_date=record.parole_discharge_date)

        return snapshot

    def compare_and_set_snapshot(self, old_record, snapshot):
        """Check for updates since last scrape, if found persist new snapshot
        Checks the last snapshot taken for this record, and if fields
        have changed since the last time the record was updated (or
        the record has no old snapshots) stores new snapshot.

        The new snapshot will only include those fields which have changed.
        Args:
            old_record: (UsNyRecord) The record entity this snapshot pertains
                to
            snapshot: (UsNySnapshot) Snapshot object with details from current
                scrape.
        Returns:
            True if successful
            False if datastore errors

        """
        new_snapshot = False

        # pylint:disable=protected-access
        snapshot_class = ndb.Model._kind_map['UsNySnapshot']
        snapshot_attrs = snapshot_class._properties

        last_snapshot = UsNySnapshot.query(
            ancestor=old_record.key).order(-UsNySnapshot.created_on).get()

        if last_snapshot:
            for attribute in snapshot_attrs:
                if attribute not in ["class", "created_on", "offense"]:
                    current_value = getattr(snapshot, attribute)
                    last_value = getattr(old_record, attribute)
                    if current_value != last_value:
                        new_snapshot = True
                        logging.info("Found change in inmate snapshot: field "
                                     "%s was '%s', is now '%s'.",
                                     attribute, last_value, current_value)
                    else:
                        setattr(snapshot, attribute, None)

                elif attribute == "offense":
                    # Offenses have to be treated differently -
                    # setting them to None means an empty list or
                    # tuple instead of None, and an unordered list of
                    # them can't be compared to another unordered list
                    # of them.
                    offense_changed = False

                    # Check if any new offenses have been added
                    for charge in snapshot.offense:
                        if charge in old_record.offense:
                            pass
                        else:
                            offense_changed = True

                    # Check if any old offenses have been removed
                    for charge in old_record.offense:
                        if charge in snapshot.offense:
                            pass
                        else:
                            offense_changed = True

                    if offense_changed:
                        new_snapshot = True
                        logging.info("Found change in inmate snapshot: field "
                                     "%s was '%s', is now '%s'.", attribute,
                                     old_record.offense, snapshot.offense)
                    else:
                        setattr(snapshot, attribute, [])

        else:
            # This is the first snapshot, store everything
            new_snapshot = True

        if new_snapshot:
            try:
                snapshot.put()
            except (Timeout, TransactionFailedError, InternalError):
                logging.warning("Couldn't store new snapshot for record %s",
                                old_record.record_id)

        return True
