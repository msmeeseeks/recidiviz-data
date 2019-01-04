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

"""Scraper implementation for us_pa.
The Pennsylvania DOC roster exposes search results for people via JSON POST
requests. Each person has a unique ID that can be used to scrape person details
by appending the ID to the _DETAILS_PAGE url. Because searches are limited to
500, this scraper searches over combinations of committing county and current
location.
"""

import re
import json
from copy import deepcopy
from lxml import etree

from recidiviz.ingest.base_scraper import BaseScraper
from recidiviz.ingest import constants

_SEARCH_RESULTS_PAGE = 'https://captorapi.cor.pa.gov/InmLocAPI/' + \
    'api/v1/InmateLocator/SearchResults'
_DETAILS_PAGE = 'https://captorapi.cor.pa.gov/InmLocAPI/' + \
    'api/v1/InmateLocator/InmateDetailsbyID'

class UsPaScraper(BaseScraper):
    """Scraper implementation for us_pa."""
    def __init__(self):
        super(UsPaScraper, self).__init__('us_pa')


    def populate_data(self, content, params, ingest_info):
        response = json.loads(content.text)
        # 'inmatedetails' may contain multiple entries if this person has
        #  multiple aliases; the fields are otherwise identical.
        person = response['inmatedetails'][0]

        ingest_person = ingest_info.create_person()
        ingest_person.given_names = person['inm_firstname']
        ingest_person.surname = person['inm_lastname']
        ingest_person.person_id = person['inmate_number']
        ingest_person.birthdate = person['dob']
        ingest_person.race = person['race']
        ingest_person.gender = person['sex']

        ingest_booking = ingest_person.create_booking()
        ingest_booking.facility = person['currloc']

        ingest_sentence = ingest_booking.create_charge().create_sentence()
        ingest_sentence.sentencing_region = person['cnty_name']

        return ingest_info


    def get_more_tasks(self, content, params):
        task_type = params.get('task_type', self.get_initial_task_type())

        if self.is_initial_task(task_type):
            return self._get_js_params(content)

        if 'endpoint' in params and params['endpoint'].endswith('.js'):
            return self._get_people_params(content)

        if 'endpoint' in params and params['endpoint'] == _SEARCH_RESULTS_PAGE:
            return self._get_person_params(content, params)

        raise ValueError('unexpected params: %s' % str(params))

    def _get_js_params(self, content):
        homepage = etree.tostring(content)
        jsfile_pattern = re.compile(r'src=\"(main\.\w+?\.js)\"')
        matches = jsfile_pattern.findall(homepage)

        if len(matches) != 1:
            raise ValueError('Was not able to parse main.*.js filename.')

        js_filename = matches[0]
        js_url = '{}/{}'.format(self.get_region().base_url, js_filename)

        return [{
            'endpoint': js_url,
            'task_type': constants.GET_MORE_TASKS,
        }]


    def _get_people_params(self, content):
        """From the main.*.js file, find all committing counties and facility
        locations.
        By default, we search for all people in a given location. Searches
        are limited to 500 results, so if a location has more than 500 people
        we split searches for that location by the county of committment.
        """
        input_text = etree.tostring(content)
        keyvalue_pattern = re.compile(
            r'\[\[\"value\",\"(\w+)\"\]\].+?\[\"(.+?)\"\]\)\)')

        _, remaining_text = input_text.split('"inmateGender"')
        _, remaining_text = remaining_text.split('"inmateRace"')
        _, remaining_text = remaining_text.split('"committingCounty"')
        county_text, remaining_text = remaining_text.split('"currentLocation"')
        location_text, _ = remaining_text.split('"inmateCitizen"')

        county_options = dict(keyvalue_pattern.findall(county_text))
        location_options = dict(keyvalue_pattern.findall(location_text))

        params_list = [
            {
                'endpoint': _SEARCH_RESULTS_PAGE,
                'task_type': constants.GET_MORE_TASKS,
                'json': {
                    'age': '',
                    'citizenlistkey': '---',
                    'countylistkey': '---',
                    'dateofbirth': None,
                    'firstName': '  ',
                    'id': '',
                    'lastName': '  ',
                    'locationlistkey': location,
                    'middleName': ' ',
                    'racelistkey': '---',
                    'sexlistkey': '---',
                    'sortBy': '1'
                },
                # Store the list of counties for more granular search.
                'county_options': county_options,
                # Store the location name for debugging.
                'location': location_options[location]
            } for location in location_options.keys()
        ]

        return params_list


    def _get_people_params_by_county(self, params):
        """Returns a list of GET_MORE_TASKS params with the JSON data's
        |countylistkey| field set to each of the counties scraped from the
        search page.
        """
        if not params['county_options']:
            raise ValueError("Didn't scrape county names from search page")

        params_list = list()
        for county_code, county_name in params['county_options'].items():
            params_copy = deepcopy(params)
            params_copy['json']['countylistkey'] = county_code
            params_copy['county'] = county_name
            params_list.append(params_copy)
        return params_list


    def _get_people_params_by_age(self, params):
        """Returns a list of GET_MORE_TASKS params with the JSON data's |age|
        field set in the range(150).
        Because of the order this is called, |params| should already have a
        'countylistkey' field set in the JSON data.
        """
        params_list = list()
        for i in range(150):
            params_copy = deepcopy(params)
            params_copy['json']['age'] = str(i)
            params_list.append(params_copy)
        return params_list


    def _get_person_params(self, content, params):
        """Search results are encoded in JSON. The JSON response contains a
        list of person IDs ('inmate_number') which we pass as SCRAPE_DATA
        tasks. However, if the search returns more than 500 results, we retry
        the search with the additional 'age' parameter to get fewer results
        per search.
        """
        response = json.loads(content.text)

        total_records = int(response['pagestats'][0]['totalrecords'])
        if total_records <= 500:
            params_list = [
                {
                    'endpoint': '{}/{}'.format(_DETAILS_PAGE,
                                               person['inmate_number']),
                    'task_type': constants.SCRAPE_DATA
                } for person in response['inmates']
            ]
            return params_list

        # There are more than 500 results. Fall back on searching for:
        #   - people committed from each county
        #   - people committed from each county, by age
        search_county = params['json']['countylistkey']
        search_age = params['json']['age']
        if search_county == '---':
            return self._get_people_params_by_county(params)
        if search_age == '':
            return self._get_people_params_by_age(params)

        # If the search for people of a one age, committed from one county, in
        # one location, is greater than 500, give up.
        raise ValueError(('Search can only recieve 500 results but '
                          'got {} results for '
                          'people age {} in'
                          'Current Location {} and '
                          'Committing County {}').format(total_records,
                                                         search_age,
                                                         params['location'],
                                                         params['county']))
