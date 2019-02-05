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
from typing import Optional
from typing import List

from recidiviz.ingest.scrape import constants
from recidiviz.ingest.scrape.base_scraper import BaseScraper
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.scrape.task_params import ScrapedData, Task

_SEARCH_RESULTS_PAGE = 'https://captorapi.cor.pa.gov/InmLocAPI/' + \
    'api/v1/InmateLocator/SearchResults'
_DETAILS_PAGE = 'https://captorapi.cor.pa.gov/InmLocAPI/' + \
    'api/v1/InmateLocator/InmateDetailsbyID'

class UsPaScraper(BaseScraper):
    """Scraper implementation for us_pa."""
    def __init__(self):
        super(UsPaScraper, self).__init__('us_pa')


    def populate_data(self, content, task: Task,
                      ingest_info: IngestInfo) -> Optional[ScrapedData]:
        # 'inmatedetails' may contain multiple entries if this person has
        #  multiple aliases; the fields are otherwise identical.
        person = content['inmatedetails'][0]

        ingest_person = ingest_info.create_person()
        ingest_person.person_id = person['inmate_number']
        ingest_person.birthdate = person['dob']
        ingest_person.race = person['race']
        ingest_person.gender = person['sex']

        ingest_booking = ingest_person.create_booking()
        ingest_booking.facility = person['currloc']

        ingest_sentence = ingest_booking.create_charge().create_sentence()
        ingest_sentence.sentencing_region = person['cnty_name']

        return ScrapedData(ingest_info=ingest_info, persist=True)


    def get_more_tasks(self, content, task: Task) -> List[Task]:
        if self.is_initial_task(task.task_type):
            return self._get_js_tasks(content)

        if task.endpoint and task.endpoint.endswith('.js'):
            return self._get_people_tasks(content)

        if task.endpoint == _SEARCH_RESULTS_PAGE:
            return self._get_person_tasks(content, task)

        raise ValueError('unexpected task: %s' % str(task))

    def _get_js_tasks(self, content) -> List[Task]:
        matches = content.xpath('//script[starts-with(@src, "main.")]')

        if len(matches) != 1:
            raise ValueError('Was not able to parse main.*.js filename.')

        js_filename = matches[0].attrib['src']
        js_url = '{}/{}'.format(self.get_region().base_url, js_filename)

        return [Task(
            task_type=constants.TaskType.GET_MORE_TASKS,
            endpoint=js_url,
            response_type=constants.ResponseType.TEXT,
        )]


    def _get_people_tasks(self, content) -> List[Task]:
        """From the main.*.js file, find all committing counties and facility
        locations.
        By default, we search for all people in a given location. Searches
        are limited to 500 results, so if a location has more than 500 people
        we split searches for that location by the county of committment.
        """
        keyvalue_pattern = re.compile(
            r'\[\[\"value\",\"(\w+)\"\]\].+?\[\"(.+?)\"\]\)\)')

        _, remaining_text = content.split('"inmateGender"')
        _, remaining_text = remaining_text.split('"inmateRace"')
        _, remaining_text = remaining_text.split('"committingCounty"')
        county_text, remaining_text = remaining_text.split('"currentLocation"')
        location_text, _ = remaining_text.split('"inmateCitizen"')

        county_options = dict(keyvalue_pattern.findall(county_text))
        location_options = dict(keyvalue_pattern.findall(location_text))

        return [
            Task(
                task_type=constants.TaskType.GET_MORE_TASKS,
                endpoint=_SEARCH_RESULTS_PAGE,
                response_type=constants.ResponseType.JSON,
                json={
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
                custom={
                    # Store the list of counties for more granular search.
                    'county_options': county_options,
                    # Store the location name for debugging.
                    'location': location_options[location]
                }
            ) for location in location_options.keys()
        ]


    def _get_people_tasks_by_county(self, task: Task) -> List[Task]:
        """Returns a list of GET_MORE_TASKS params with the JSON data's
        |countylistkey| field set to each of the counties scraped from the
        search page.
        """
        if task.json is None:
            raise ValueError('Expected JSON in prior task: {}'.format(task))
        if not task.custom['county_options']:
            raise ValueError("Didn't scrape county names from search page")

        task_list = list()
        for county_code, county_name in task.custom['county_options'].items():
            new_json = task.json.copy()
            new_json['countylistkey'] = county_code
            new_custom = task.custom.copy()
            new_custom['county'] = county_name
            task_list.append(Task.evolve(
                task,
                json=new_json,
                custom=new_custom,
            ))
        return task_list


    def _get_people_tasks_by_age(self, task: Task) -> List[Task]:
        """Returns a list of GET_MORE_TASKS params with the JSON data's |age|
        field set in the range(150).
        Because of the order this is called, |params| should already have a
        'countylistkey' field set in the JSON data.
        """
        if task.json is None:
            raise ValueError('Expected JSON in prior task: {}'.format(task))

        task_list = list()
        for i in range(150):
            new_json = task.json.copy()
            new_json['age'] = str(i)
            task_list.append(Task.evolve(
                task,
                json=new_json
            ))
        return task_list


    def _get_person_tasks(self, content, task: Task) -> List[Task]:
        """Search results are encoded in JSON. The JSON response contains a
        list of person IDs ('inmate_number') which we pass as SCRAPE_DATA
        tasks. However, if the search returns more than 500 results, we retry
        the search with the additional 'age' parameter to get fewer results
        per search.
        """
        total_records = int(content['pagestats'][0]['totalrecords'])
        if total_records <= 500:
            return [
                Task(
                    task_type=constants.TaskType.SCRAPE_DATA,
                    endpoint='{}/{}'.format(_DETAILS_PAGE,
                                            person['inmate_number']),
                    response_type=constants.ResponseType.JSON,
                ) for person in content['inmates']
            ]

        if task.json is None:
            raise ValueError('Expected JSON in prior task: {}'.format(task))

        # There are more than 500 results. Fall back on searching for:
        #   - people committed from each county
        #   - people committed from each county, by age
        search_county = task.json['countylistkey']
        search_age = task.json['age']
        if search_county == '---':
            return self._get_people_tasks_by_county(task)
        if search_age == '':
            return self._get_people_tasks_by_age(task)

        # If the search for people of a one age, committed from one county, in
        # one location, is greater than 500, give up.
        raise ValueError(('Search can only recieve 500 results but '
                          'got {} results for '
                          'people age {} in'
                          'Current Location {} and '
                          'Committing County {}').format(
                              total_records, search_age,
                              task.custom['location'], task.custom['county']))
