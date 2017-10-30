# Copyright 2017 Andrew Corp <andrew@andrewland.co> 
# All rights reserved.

"""
us-ny-scraper.py
Recidivism data crawler for the state of New York, in the US.

Department: 
New York State - Dept of Corrections and Community Supervision (DOCCS)
 
Search type: 
- NYS-DOCCS allows for search by last name only
- However, we use a custon name list for DOCCS instead of the standard 
    'last_names' list - see Notes.

Region-specific notes: 
    - DOCCS shows 'rough matches' that seem to iterate through the whole inmate
        database - making it much easier to query than most prison systems. 
            - As a result, the us-ny names list is custom 'aaardvark', since this
            query should be enough to get all inmates in the entire system.
            - Also as a result, we have a catch an exception in results parsing
            to know when we ran out of names / it looped back to the search page.
    - DOCCS is based on incarceration events, not prisoners. As a result, each 
        record produced by the scraper is for one stay in prison for one prisoner.
            - We add the field 'group_id' to entries which DOCCS says are for the 
            same person to aid in deduping people later on.


General scraping procedure:
    1. A starting search page --> (search, see 2)
    2. A results list (4x/page) --> (parse, see 2a and 2b)
        (2a) A list of inmate results --> (follow, each entry leads to 3)
        (2b) The 'next page' of results for the query --> (follow it, see 2)
    3. EITHER
        (3a) A disambiguation page (which time in jail would you like to see 
            for this inmate?)
        (3b) A details page for the inmate, about a specific incarceration event
"""


from datetime import datetime
import json
import logging
import requests
import requests_toolbelt.adapters.appengine
import string
import random
from lxml import html # Useful docs @ http://lxml.de/lxmlhtml.html
import re

import models

#from google.appengine.api import memcache  # See 'Session pages'
from google.appengine.api import taskqueue
from google.appengine.ext import ndb
from google.appengine.ext.ndb import polymodel


# Use the App Engine Requests adapter. This makes sure that Requests uses
# URLFetch.
requests_toolbelt.adapters.appengine.monkeypatch()

START_URL = "http://nysdoccslookup.doccs.ny.gov/"
BASE_RESULTS_URL = "http://nysdoccslookup.doccs.ny.gov"
QUEUE_NAME = 'us-ny'
REGION = 'us_ny'
REQUEST_TIMEOUT = 5


# DB MODELS

"""
UsNyInmate

Datastore model for a snapshot of a record of a particular sentence being 
served (a DIN, in NY's DOCCS system). This extends the general Inmate,
which has the following fields:

See models.Inmate for pre-populated fields. UsNyInmate extends 
these by adding the following:
    - us_ny_record_id: (string) Same as record_id, but used as key for this entity type
        to force uniqueness / prevent collisions within the us_ny records

Note the duplicated record ID - this allows us to use this field as a key,
forcing uniqueness within the UsNy record space without forcing it across
all regions (as there may be record ID collisions between states).

"""
class UsNyInmate(models.Inmate):
    us_ny_record_id = ndb.StringProperty()

"""
UsNyRecordEntry

Datastore model for historical violation records (e.g. 
ROBB. WPN-NOT DEADLY, 04/21/1995, sentence: 20mo). Multiple records may map
to one inmate.

See models.RecordEntry for pre-populated fields. UsNyRecordEntry extends 
these by adding the following:
    - last_custody_date: (date) Most recent date inmate returned for this 
        sentence (may not be the initial custody date - e.g., if parole
        was violated, may be readmitted for remainder of prison term)
    - admission_type: (string) 'New commitment' is beginning to serve a term,
        other reasons are usually after term has started (e.g. parole issue)
    - county_of_commit: (string) County the inmate was convicted/committed in
    - custody_status: (string) Whether still in custody or released (and how)
    - last_release_type: (string) If released from prison, the reason
    - last_release_date: (date) If released from prison, the date of release
    - earliest_release_date: (date) Earliest date to be released based on 
        min_sentence. In certain circumstances, may be released before this.
    - earliest_release_type: (string) The reason for the earliest possible
        release date.
    - parole_hearing_date: (date) Date of next hearing before Parole Board 
    - parole_hearing_type: (string) Type of hearing for next PB appearance.
    - parole_elig_date: (date) Date inmate will be eligible for parole
    - cond_release_date: (date) Release date based on prison discretion for
        'good time off' based on behavior. Releases prisoner on parole, but
        bypasses PB review.
    - max_expir_date: (date) Date of release if no PB or conditional release,
        maximum obligation to the state.
    - max_expir_date_parole: (date) Last possible date of ongoing parole
        supervision. Doesn't apply to all inmates.
    - max_expir_date_superv: (date) Last possible date of post-release 
        supervision. Doesn't apply to all inmates.
    - parole_discharge_date: (date) Final date of parole supervision, based on
        the parole board's decision to end supervision before max expiration.
    (Note: for the three 'max...'s, the latest date is considered controlling.)

See 'DOCCS Data Definitions' for full descriptions:
http://www.doccs.ny.gov/univinq/fpmsdoc.htm

"""
class UsNyRecordEntry(models.RecordEntry):
    last_custody_date = ndb.DateProperty()
    admission_type = ndb.StringProperty()
    county_of_commit = ndb.StringProperty()
    custody_status = ndb.StringProperty()
    last_release_type = ndb.StringProperty()
    last_release_date = ndb.DateProperty()
    earliest_release_date = ndb.DateProperty()
    earliest_release_type = ndb.StringProperty()
    parole_hearing_date = ndb.DateProperty()
    parole_hearing_type = ndb.StringProperty()
    parole_elig_date = ndb.DateProperty()
    cond_release_date = ndb.DateProperty()
    max_expir_date = ndb.DateProperty()
    max_expir_date_parole = ndb.DateProperty()
    max_expir_date_superv = ndb.DateProperty()
    parole_discharge_date = ndb.DateProperty()



# TODO(andrew) - Create class to surround this with, with requirements for 
# the functions that will be called from recidiviz and worker such as setup()
# and start_query.

# TODO(andrew) - Move this into a scrapers/ subdir, with other regional 
# scrapers

def setup():

    """
    setup()
    Required by Scraper class. Is run prior to any specific scraping tasks for
    the region, and is allowed to trigger a small number of third-party network
    requests synchronously (without queues) to allow for initial session info, 
    etc. to be configured prior to scraping.     

    Args:
        None

    Returns:
        Nothing if successful, -1 if fails.
    """

    # Note: Since DOCCS creates a new form for every link to follow, it doesn't
    # look like we need to worry about session vars for the us-ny scraper.
    #get_session_vars()

    return


def start_query(first_name, last_name):
    """
    start_query()
    Required by Scraper class. Is run to kick off scraping for a particular 
    query (i.e., a particular name search). 

    Args:
        first_name: string, first name for inmate query (empty string if 
            last-name only search)
        last_name: string, last name for inmate query (required)

    Returns:
        Nothing if successful, -1 if fails.

        Enqueues task for initial search page scrape to get form/session params
        for the initial search request.
    """

    logging.info(REGION + " // Scraper starting up for name: %s %s" % (first_name, last_name))

    # Search for the provided name
    params = {'first_name': first_name, 'last_name': last_name}

    serial_params = json.dumps(params)

    # Enqueue task to get new session vars
    task = taskqueue.add(
        url='/scraper',
        queue_name=QUEUE_NAME,
        params={'region': REGION,
                'task': "scrape_search_page",
                'params': serial_params})

    return


# SESSION MANAGEMENT #

# NOTE: US_NY DOESN'T NEED SESSION MGT, SO COMMENTING THIS SECTION OUT FOR THIS SCRAPER 
#       IF USED IN FUTURE, ALSO UNCOMMENT IMPORT OF MEMCACHE.

"""
def get_session():
    ### Replace me with three double-quotes if uncommented
    get_session()
    Checks for session vars in memcache - if found, returns them. If not, 
    enqueues task to get new ones and store them in memcache. 

    Args:
        None

    Returns:
        Session info if successful, -1 if fails.

        Enqueues task to refresh cache if doesn't find current info in 
        memcache
    ### Replace me with three double-quotes if uncommented


    # Pull the memcache session info
    id = memcache.get('us_ny_session_var1')
    token = memcache.get('us_ny_session_var2')

    if (id is None) or (token is None):

        logging.info(REGION + " // Session cache miss, refreshing")


        # If any are missing, refresh the session info
        queue_session_refresh()

        # Fail the task, it'll get requeued with a delay (allowing time for the
        # session refresh task to finish)
        return -1

    else:
        results = {'token': token, 'id': id}
        return results

def queue_session_refresh():
    ### Replace me with three double-quotes if uncommented
    session_refresh()
    Forces refresh of session info, even if memcache still has prior
    values. 

    Args:
        None

    Returns:
        None

        Enqueues task to scrape search page for new session info
    ### Replace me with three double-quotes if uncommented

    # Session refreshes get their own (cross-region) queue, so they don't
    # get stuck behind tasks which are blocked on needing new session info.
    queue_name = "session-refresh"

    # Enqueue task to get new session vars
    task = taskqueue.add(
        url='/scraper',
        queue_name=queue_name,
        params={'region': REGION,
                'task': "get_session_vars"})

    return 

def get_session_vars():
    ### Replace me with three double-quotes if uncommented
    get_session_vars()
    Scrapes main search page to retrieve hidden form info for session

    Args:
        None

    Returns:
        None

        Stores new session info in memcache with TTL=1hr for fast access
    ### Replace me with three double-quotes if uncommented

    # Load the Search page on DOCCS, which will generate some tokens in the form
    search_page = requests.get(START_URL, timeout=REQUEST_TIMEOUT)
    search_tree = html.fromstring(search_page.content)

    # Extract said tokens and construct the URL for searches
    session_token = search_tree.cssselect("[name=K01]")[0].get("value")
    session_id = search_tree.cssselect("[name=DFH_STATE_TOKEN]")[0].get("value")

    # Store this info in memcache so tasks can easily get to it, with expiration
    # of an hour. This may be tweaked on a per-scraper basis, and this function
    # can be called at any time to refresh the info if stale.

    id_key = REGION + "_session_id"
    token_key = REGION + "_session_id"

    memcache.set(key=id_key, value=session_id, time=3600)
    memcache.set(key=token_key, value=session_token, time=3600)

    return
"""

# SEARCH AND SEARCH RESULTS PAGES #

def scrape_search_page(params):
    """
    scrape_search_page()
    Gets the main search page, pulls form details, and queues request for
    inmate listings given our query. 

    Args:
        params: Dict of parameters
            first_name: string, first name for inmate query (empty string if 
            last-name only search)
            last_name: string, last name for inmate query (required)

    Returns:
        Nothing if successful, -1 if fails.

        Enqueues task with params from search page form, to fetch the first
        page of results.
    """

    params = json.loads(params)

    logging.info(REGION + " //   Starting search for name in list: %s %s" % 
                 (params['first_name'], params['last_name']))


    # Load the Search page on DOCCS, which will generate some tokens in the <form>
    search_page = requests.get(START_URL, timeout=REQUEST_TIMEOUT)
    search_tree = html.fromstring(search_page.content)

    # Extract said tokens and construct the URL for searches
    session_K01 = search_tree.cssselect('[name=K01]')[0].get("value")
    session_token = search_tree.cssselect('[name=DFH_STATE_TOKEN]')[0].get("value")
    session_action = search_tree.xpath('//div[@id="content"]/form/@action')[0]

    search_results_params = {
        'first_page': True,
        'k01': session_K01, 
        'token': session_token, 
        'action': session_action,
        'first_name': params['first_name'],
        'last_name': params['last_name']}

    # Serialize params so they can make it through a POST request
    search_results_params_serial = json.dumps(search_results_params)

    # Enqueue task to get results and parse them
    task = taskqueue.add(
        url='/scraper',
        queue_name=QUEUE_NAME,
        params={'region': REGION,
                'task': "scrape_search_results_page",
                'params': search_results_params_serial})

    return


def scrape_search_results_page(params):
    """
    get scrape_search_page()
    Fetches search results page, parses results to extract inmate listings
    and params for the next page of results. Enqueues tasks to scrape both
    the individual listings and next page of results.

    Args:
        params: Dict of parameters. This is how DOCCS keeps track of where you
        are and what you're requesting. Not all of the parameters are
        understood, and some are frequently empty values.
            first_name: string, first name for inmate query (empty string if 
            last-name only search)
            last_name: string, last name for inmate query (required)
            k01: a request parameter for results, scraped from the form
            token: DFH_STATE_TOKEN - session info from the scraped form
            action: the 'action' attr from the scraped form - URL to POST to
            ---params below only apply for the first page of search results---
            first_page: True / exists only if this is requesting the first page
                of results
            ---params below only apply for 'next page' results---
            clicki: a request parameter for results, scraped from the form
            dini: a request parameter for results, scraped from the form
            k01,k02,k03,k04,k05,k06: request parameters for results, scraped
                from the form
            map_token: a request parameter for results, scraped from the form
            next: a request parameter for results, scraped from the form


    Returns:
        Nothing if successful, -1 if fails.

        Enqueues task with params from search page form, to fetch the first
        page of results.
    """

    params = json.loads(params)

    url = BASE_RESULTS_URL + str(params['action'])

    # The request for the first results page is a little unique 
    if 'first_page' in params:

        # Create a request using the provided params
        results_page = requests.post(url, data = {
                            'K01': params['k01'],
                            'DFH_STATE_TOKEN': params['token'],
                            'DFH_MAP_STATE_TOKEN': '',
                            'M00_LAST_NAMEI': params['last_name'],
                            'M00_FIRST_NAMEI': params['first_name'],
                            'M00_MID_NAMEI': '',
                            'M00_NAME_SUFXI': '', 
                            'M00_DOBCCYYI': '', 
                            'M00_DIN_FLD1I': '', 
                            'M00_DIN_FLD2I': '', 
                            'M00_DIN_FLD3I': '', 
                            'M00_NYSID_FLD1I': '', 
                            'M00_NYSID_FLD2I': ''
                            }, timeout=REQUEST_TIMEOUT)

    else:

        # Create a request using the provided params
        results_page = requests.post(url, data = {
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
                            }, timeout=REQUEST_TIMEOUT)


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

        # Enqueue tasks to follow that link / get result
        result_params['first_name'] = params['first_name']
        result_params['last_name'] = params['last_name']
        result_params = json.dumps(result_params)

        task = taskqueue.add(
        url='/scraper',
        queue_name=QUEUE_NAME,
        params={'region': REGION,
                'task': "scrape_inmate_entry",
                'params': result_params})


    # Parse the 'next' button's embedded form
    next_params = {}
    next_button = results_tree.xpath('//div[@id="content"]/form')[0]

    try:
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
    except IndexError:
        # We've run out of names, and the 'Next 4 results' button dumped us
        # back at the original search page. Log it, and end the query.
        logging.info(REGION + " //   Looped. Ending scraping session.")
        return


    # Enqueue task to follow that link / get result
    next_params['first_name'] = params['first_name']
    next_params['last_name'] = params['last_name']
    next_params = json.dumps(next_params)

    task = taskqueue.add(
    url='/scraper',
    queue_name=QUEUE_NAME,
    params={'region': REGION,
            'task': "scrape_search_results_page",
            'params': next_params})

    return


# INMATE PAGES #

def scrape_inmate_entry(params):
    """
    scrape_inmate_entry()
    Fetches inmate listing page. This sometimes turns out to be a
    disambiguation page, as there may be multiple incarceration records in
    DOCCS for the person you've selected. If so, shunts details over to
    scrape_disambiguaton(). If actual results page, pulls details from page
    and sends to store_record.

    Args:
        params: Dict of parameters. This is how DOCCS keeps track of where you
        are and what you're requesting. Not all of the parameters are
        understood, and some are frequently empty values.
            first_name: string, first name for inmate query (empty string if 
            last-name only search)
            last_name: string, last name for inmate query (required)
            token: DFH_STATE_TOKEN - session info from the scraped form
            action: the 'action' attr from the scraped form - URL to POST to
            dini: a request parameter for results, scraped from the form
            k01,k02,k03,k04,k05,k06: request parameters for results, scraped
                from the form
            map_token: a request parameter for results, scraped from the form
            dinx_name: form results are d1-d4, this is the name attr which we 
                store as a key in the params (result: {dinx_name: dinx_val})
            dinx_val: value for the dinx_name parameter
            ---params below only apply if we did NOT arrive from a 
                disambiguation page / this inmate only has one listing---
            clicki: a request parameter for results, scraped from the form


    Returns:
        Nothing if successful, -1 if fails.

        Enqueues task with params from search page form, to fetch the first
        page of results.
    """

    params = json.loads(params)

    inmate_details = {}
    url = BASE_RESULTS_URL + str(params['action'])

    # Create a request using the provided params. How we structure this varies
    # a bit depending on whether we got here from a results page or a 
    # disambiguation page.
    if 'group_id' in params:

        inmate_details['group_id'] = params['group_id']
        inmate_details['linked_records'] = params['linked_records']

        # Create a request using the provided params
        inmate_page = requests.post(url, data = {
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
                            }, timeout=REQUEST_TIMEOUT)

    else:

        # Create a request using the provided params
        inmate_page = requests.post(url, data = {
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
                            }, timeout=REQUEST_TIMEOUT)

    page_tree = html.fromstring(inmate_page.content)
    
    # First, test if we got a disambiguation page (inmate has had more than 
    # one stay in prison system, page wants to know which you want to see)
    details_page = page_tree.xpath('//div[@id="ii"]')

    if details_page:

        # Create xpath selectors for the parts of the page we want to scrape
        id_and_locale_rows = details_page[0].xpath("./table[contains(@summary, 'Identifying')]/tr")
        crimes_rows = details_page[0].xpath("./table[contains(@summary, 'crimes')]/tr")
        sentence_rows = details_page[0].xpath("./table[contains(@summary, 'sentence')]/tr")

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
            logging.warning("Did not find expected tables on inmates page. "
                "Page received: \n" + html.tostring(page_tree, pretty_print=True))
            return -1

        else:

            # Make a list to store convictions in, since they're key:value 
            # data and we don't know the number of them in advance
            crime_list = []

            # Capture table data from each details table
            for row in id_and_locale_rows:
                row_data = row.xpath('./td')

                # Get rid of weird internal, leading, and trailing whitespace
                data0 = ' '.join(row_data[0].text_content().split()).strip()
                data1 = ' '.join(row_data[1].text_content().split()).strip()

                inmate_details[data0] = data1

            for row in crimes_rows:
                row_data = row.xpath('./td')

                # One row is the table headers / has no <td> elements
                if not row_data: 
                    pass
                else:
                    # Get rid of weird internal, leading, and trailing whitespace
                    crime = ' '.join(row_data[0].text_content().split()).strip()
                    crime_class = ' '.join(row_data[1].text_content().split()).strip()

                    inmate_details[data0] = data1

                    crime_entry = {'crime': crime,
                                   'class': crime_class}

                    # Only add to the list if row is non-empty
                    if crime_entry['crime']:
                        crime_list.append(crime_entry)

            for row in sentence_rows:
                row_data = row.xpath('./td')

                # Get rid of weird internal, leading, and trailing whitespace
                data0 = ' '.join(row_data[0].text_content().split()).strip()
                data1 = ' '.join(row_data[1].text_content().split()).strip()

                inmate_details[data0] = data1


            inmate_details['crimes'] = crime_list

        logging.info(REGION + " //     (%s %s) Scraped inmate: %s" % 
             (params['first_name'], params['last_name'], inmate_details['Inmate Name']))


        return store_record(inmate_details)

    else:
        # We're on a disambiguation page, not an actual details page. Scrape the disambig page 
        # and follow each link.

        # We can call this one without creating a task, it doesn't need a new 
        # network call
        scrape_disambiguation(page_tree, 
                                params['first_name'], 
                                params['last_name'])

        return


def scrape_disambiguation(page_tree, first_name, last_name):
    """
    scrape_disambiguation()
    In attempting to fetch an inmate, the scrape_inmate_entry received a 
    disambig page - asking which incarceration event for that inmate they
    wanted. This function takes that result page, parses it, and enqueues a
    new task to scrape each entry from the disambig page.

    Args:
        page_tree: lxml.html parsed version of the disambig page
        first_name: string, first name for inmate query (empty string if 
            last-name only search)
        last_name: string, last name for inmate query (required)


    Returns:
        Nothing if successful, -1 if fails.

        Enqueues task for each incarceration event listed on the page.
    """

    # Create an ID to group these entries with - DOCCS doesn't tell us how it 
    # groups these / give us a persistent ID for inmates, but we want to know
    # each of the entries scraped from this page were about the same person.
    group_id = generate_id(UsNyInmate)
    new_tasks = []
    din_list = []


    # Parse the results list
    result_list = page_tree.xpath('//div[@id="content"]/table/tr/td/form')

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

        din_list.append(result_params['dinx_val'])
        new_tasks.append(result_params)


    for task_params in new_tasks:

        """ 
        The disambig page has produces far more rows than are visible, each 
        with a form element you can click - programmatically, these look nearly
        identical to actual result rows. 

        The only differences are 
          a) they have empty 'value' attr in the 'submit' element / (thus are
          empty / invisible in the UI), and 
          b) they go back to the search results instead of listing another 
          record. 

        To avoid the latter mucking up our parsing, we test dinx_val to skip 
        these entries.
        """
        if task_params['dinx_val']:

            # Enqueue task to follow that link / get result
            task_params['first_name'] = first_name
            task_params['last_name'] = last_name
            task_params['linked_records'] = din_list
            task_params = json.dumps(task_params)

            task = taskqueue.add(
            url='/scraper',
            queue_name=QUEUE_NAME,
            params={'region': REGION,
                    'task': "scrape_inmate_entry",
                    'params': task_params})

        else: pass

    return


def store_record(inmate_details):

    """
    store_record()
    We've scraped a incarceration details page, and want to store the data we
    found. This function does some post-processing on the scraped data, and
    feeds it into the datastore in a way that can be indexed / queried in the
    future.

    Args:
        inmate_details: the results of the scrape, stored in a dict object.


    Returns:
        Nothing if successful, -1 if fails.
    """

    
    # INMATE LISTING

    # NY doesn't provide an inmate ID to link records. If DOCCS linked records
    # for us, we already generated an ID to tie them together - use that. If 
    # not, generate one.
    if 'group_id' in inmate_details:

        # By default (if we find no prior records below), use the group_id 
        # generated earlier in this scraping session as the inmate_id, so as
        # to tie this record to the linked ones for the same inmate.
        inmate_id = inmate_details['group_id']

        # Check if this or any of the linked records have a previously scraped 
        # version in datastore, and if so tie this record to the same inmate_id.
        for linked_record in inmate_details['linked_records']:
            query = UsNyRecordEntry.query(UsNyRecordEntry.record_id == linked_record)
            result = query.get()
            if result:
                # Set inmate_id to the inmate_id of the Inmate referenced by that record
                prior_inmate_key = result.associated_listing
                prior_inmate_listing = prior_inmate_key.get()
                inmate_id = prior_inmate_listing.record_id

                logging.info("Found an earlier record with an inmate ID, using that.")
        
    else:
        inmate_id = generate_id(UsNyInmate)


    listing = UsNyInmate.get_or_insert(inmate_id)


    # Some pre-work to massage values out of the data
    inmate_name = inmate_details['Inmate Name'].split(', ')
    inmate_dob = inmate_details['Date of Birth']
    if inmate_dob: inmate_dob = parse_date_string(inmate_dob)
    inmate_sex = inmate_details['Sex'].capitalize()
    inmate_race = inmate_details['Race / Ethnicity'].capitalize()

    # NY-specific fields
    listing.us_ny_record_id = inmate_id

    # General Inmate fields
    listing.record_id = inmate_id
    listing.record_id_is_fuzzy = True
    listing.last_name = inmate_name[0]
    listing.given_names = inmate_name[1]
    listing.birthday = inmate_dob
    listing.region = REGION
    listing.sex = inmate_sex
    listing.race = inmate_race

    listing_key = listing.put()


    # CRIMINAL RECORD ENTRY

    record_id = inmate_details['DIN (Department Identification Number)']

    record = UsNyRecordEntry.get_or_insert(record_id)

    # Some pre-work to massage values out of the data
    last_custody = inmate_details['Date Received (Current)']
    if last_custody: 
        last_custody = parse_date_string(last_custody)
    first_custody = inmate_details['Date Received (Original)']
    if first_custody: 
        first_custody = parse_date_string(first_custody)
    admission_type = inmate_details['Admission Type']
    county_of_commit = inmate_details['County of Commitment']
    custody_status = inmate_details['Custody Status']
    released = (custody_status != "IN CUSTODY")
    record_offenses = json.dumps(inmate_details['crimes'])
    min_sentence = inmate_details['Aggregate Minimum Sentence']
    if min_sentence: 
        min_sentence = json.dumps(parse_term_string(min_sentence))
    max_sentence = inmate_details['Aggregate Maximum Sentence']
    if max_sentence:
        max_sentence = json.dumps(parse_term_string(max_sentence))
    earliest_release_date = inmate_details['Earliest Release Date']
    if earliest_release_date: 
        earliest_release_date = parse_date_string(earliest_release_date)
    earliest_release_type = inmate_details['Earliest Release Type']
    parole_hearing_date = inmate_details['Parole Hearing Date']
    if parole_hearing_date: 
        parole_hearing_date = parse_date_string(parole_hearing_date)
    parole_hearing_type = inmate_details['Parole Hearing Type']
    parole_elig_date = inmate_details['Parole Eligibility Date']
    if parole_elig_date: 
        parole_elig_date = parse_date_string(parole_elig_date)
    cond_release_date = inmate_details['Conditional Release Date']
    if cond_release_date: 
        cond_release_date = parse_date_string(cond_release_date)
    max_expir_date = inmate_details['Maximum Expiration Date']
    if max_expir_date: 
        max_expir_date = parse_date_string(max_expir_date)
    max_expir_date_parole = (
        inmate_details['Maximum Expiration Date for Parole Supervision'])
    if max_expir_date_parole: 
        max_expir_date_parole = parse_date_string(max_expir_date_parole)
    max_expir_date_superv = (
        inmate_details['Post Release Supervision Maximum Expiration Date'])
    if max_expir_date_superv: 
        max_expir_date_superv = parse_date_string(max_expir_date_superv)
    parole_discharge_date = inmate_details['Parole Board Discharge Date']
    if parole_discharge_date: 
        parole_discharge_date = parse_date_string(parole_discharge_date)
    last_release = (
        inmate_details['Latest Release Date / Type (Released Inmates Only)'])
    if last_release:
        release_info = last_release.split(" ", 1)
        last_release_date = parse_date_string(release_info[0])
        last_release_type = release_info[1] 

    # NY-specific record fields
    if last_custody: record.last_custody_date = last_custody
    if admission_type: record.admission_type = admission_type
    if county_of_commit: record.county_of_commit = county_of_commit
    if custody_status: record.custody_status = custody_status
    if last_release: 
        record.last_release_type = last_release_type
        record.last_release_date = last_release_date
    if min_sentence: record.min_sentence = min_sentence
    if max_sentence: record.max_sentence = max_sentence
    if earliest_release_date: record.earliest_release_date = earliest_release_date
    if earliest_release_type: record.earliest_release_type = earliest_release_type
    if parole_hearing_date: record.parole_hearing_date = parole_hearing_date
    if parole_hearing_type: record.parole_hearing_type = parole_hearing_type
    if parole_elig_date: record.parole_elig_date = parole_elig_date
    if cond_release_date: record.cond_release_date = cond_release_date
    if max_expir_date: record.max_expir_date = max_expir_date
    if max_expir_date_parole: record.max_expir_date_parole = max_expir_date_parole
    if max_expir_date_superv: record.max_expir_date_superv = max_expir_date_superv
    if parole_discharge_date: record.parole_discharge_date = parole_discharge_date

    # General RecordEntry fields
    record.offense = record_offenses
    record.record_id = record_id
    record.custody_date = first_custody
    record.min_sentence_length = min_sentence
    record.max_sentence_length = max_sentence
    record.is_released = released
    record.associated_listing = listing_key

    record_key = record.put()


    # FACILITY SNAPSHOT

    facility_snapshot = models.InmateFacilitySnapshot()

    facility = inmate_details['Housing / Releasing Facility']

    facility_snapshot.facility = facility
    facility_snapshot.associated_listing = listing_key
    facility_snapshot.associated_record = record_key

    facility_snapshot.put()

    if 'group_id' in inmate_details:
        logging.info(REGION + " //    Stored record for "
            "%s %s,  in group %s, for record %s." % (
                inmate_name[1], 
                inmate_name[0], 
                inmate_details['group_id'], 
                record_id))
    else:
        logging.info(REGION + " //    Stored record for "
            "%s %s,  (no group), for record %s." % (
                inmate_name[1], 
                inmate_name[0], 
                record_id))

    return 


def parse_term_string(term_string):
    if term_string.startswith("LIFE"):

        result = {'Life': True,
                  'Years': 0,
                  'Months': 0,
                  'Days': 0}

    else:
        parsed_nums = re.findall('\d+', term_string)

        if not parsed_nums:
            result = ""
        else:

            years = parsed_nums[0]
            months = parsed_nums[1]
            days = parsed_nums[2]

            result = {'Life': False,
                      'Years': years,
                      'Months': months,
                      'Days': days}

    return result


def parse_date_string(date_string):
    try:
        if len(date_string) == 7:
            result = datetime.strptime(date_string, '%m/%Y')
        elif len(date_string) == 8:
            result = datetime.strptime(date_string, '%m/%d/%y')
        else:
            result = datetime.strptime(date_string, '%m/%d/%Y')
    except ValueError:
        logging.warning("Couldn't parse date string: %s" % date_string)
        result = ""

    return result


def generate_id(entity_kind):

    # Generate new key
    new_key = ''.join(random.choice(string.ascii_uppercase + 
                      string.ascii_lowercase + 
                      string.digits) for _ in range(10))

    # Double-check it isn't a collision
    test_key = ndb.Key(entity_kind, new_key)
    key_result = test_key.get()

    if key_result is not None:

        # Collision, try again
        return generate_id(entity_kind)

    else:

        return new_key
