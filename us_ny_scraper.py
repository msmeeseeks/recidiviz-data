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


import json
import logging
import requests
import requests_toolbelt.adapters.appengine
import string
import random
from lxml import html # Useful docs @ http://lxml.de/lxmlhtml.html

#from google.appengine.api import memcache  # See 'Session pages'
from google.appengine.api import taskqueue


# Use the App Engine Requests adapter. This makes sure that Requests uses
# URLFetch.
requests_toolbelt.adapters.appengine.monkeypatch()

START_URL = "http://nysdoccslookup.doccs.ny.gov/"
BASE_RESULTS_URL = "http://nysdoccslookup.doccs.ny.gov"
QUEUE_NAME = 'us-ny'
REGION = 'us_ny'
REQUEST_TIMEOUT = 5


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
                inmate_details[row_data[0].text_content().strip()] = (
                    row_data[1].text_content().strip())

            for row in crimes_rows:
                row_data = row.xpath('./td')

                # One row is the table headers / has no <td> elements
                if not row_data: 
                    pass
                else:
                    crime_entry = {'crime': row_data[0].text_content().strip(),
                                   'class': row_data[1].text_content().strip()}

                    # Only add to the list if row is non-empty
                    if crime_entry['crime']:
                        crime_list.append(crime_entry)

            for row in sentence_rows:
                row_data = row.xpath('./td')
                inmate_details[row_data[0].text_content().strip()] = (
                    row_data[1].text_content().strip())

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
    group_id = ''.join(random.choice(string.ascii_uppercase + 
                    string.ascii_lowercase + 
                    string.digits) for _ in range(10))


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
        if result_params['dinx_val']:

            # Enqueue task to follow that link / get result
            result_params['first_name'] = first_name
            result_params['last_name'] = last_name
            result_params = json.dumps(result_params)

            task = taskqueue.add(
            url='/scraper',
            queue_name=QUEUE_NAME,
            params={'region': REGION,
                    'task': "scrape_inmate_entry",
                    'params': result_params})

        else: pass

    return
        


def store_record(results_tree):

    """
    Looks like this:

    {'Housing / Releasing Facility': u'', 'Parole Board Discharge Date': u'', 'Aggregate Maximum Sentence': 'Years,  Months,\r\n          Days', 
    'Admission Type': u'', 'Inmate Name': u'AAKJAR, KELUIN', 'Maximum Expiration Date for Parole Supervision': u'', 'Parole Hearing Type': u'MINIMUM PERIOD OF IMPRISONMENT', 
    'County of Commitment': u'DUTCHESS', 'Maximum Expiration Date': u'04/10/1985', 'Parole Eligibility Date': u'', 'Earliest Release Date': u'', 'Race / Ethnicity': u'WHITE', 
    'Earliest Release Type': u'', 'Custody Status': u'DISCHARGED', 'Parole Hearing Date': u'07/1980', 'Post Release Supervision Maximum Expiration Date': u'', 
    'Date Received (Original)': u'05/23/1980', 'Date of Birth': u'09/21/1959', 'Date Received (Current)': u'05/23/1980', 
    'Latest Release Date / Type (Released Inmates Only)': u'04/10/83 DISCH - MAXIMUM EXPIRATION', 'DIN (Department Identification Number)': u'80B0876', 
    'Sex': u'MALE', 'crimes': [{'crime': u'MANSLAUGHTER 2ND', 'class': u'C'}], 'Aggregate Minimum Sentence': 'Years,  Months,\r\n          Days', 
    'Conditional Release Date': u'08/10/1983'}
    """

    logging.info("In process_results")

    return 
