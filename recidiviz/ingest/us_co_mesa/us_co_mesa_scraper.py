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


"""Scraper implementation for CO Mesa County

Region-specific notes:
    - No historical data is kept, only data on current people
    - Allows for search by surname, birthdate, or booking id
    - Allows for empty surname search which returns all people
    - Has no stable person identifier, only current bookings
    - Can search by booking identifier to find current information
    - Only an unstructured name and birthdate are provided about a particular
      person
    - Bookings contain multiple bonds and each bond can have multiple charges


Background scraping procedure:
    1. A starting home page GET
    2. A primary search POST with empty surname --> (parse, see 2a)
         (2a) A list of person results --> (follow, each entry leads to 3)
    3. A details page for a person's current booking
"""

from recidiviz.ingest import constants, scraper_utils
from recidiviz.ingest.generic_scraper import GenericScraper
from recidiviz.models.person import Person
from recidiviz.models.record import Offense, Record


class UsCoMesaScraper(GenericScraper):
    """Scraper Mesa County, Colorado jails."""

    def __init__(self):
        super(UsCoMesaScraper, self).__init__('us_co_mesa')

        self._base_endpoint = self.get_region().base_url
        self._front_url = 'default.asp'
        self._initial_endpoint = '/'.join(
            [self._base_endpoint, self._front_url])

    def get_more_tasks(self, content, params):
        task_type = params.get('task_type', self.get_initial_task_type())
        params_list = []

        if self.is_initial_task(task_type):
            params_list.append(self._get_all_people_params())
        elif self.should_get_more_tasks(task_type):
            params_list.extend(self._get_person_params(content))

        if self.should_scrape_person(task_type):
            self._person = UsCoMesaScraper._scrape_person(content)
        if self.should_scrape_record(task_type):
            self._record = UsCoMesaScraper._scrape_record(content)

        return params_list

    def _get_all_people_params(self):
        params = {
            'endpoint': self._initial_endpoint,
            'task_type': constants.GET_MORE_TASKS,
            'data': {
                'SearchField': 'LName',
                'SearchVal': '',
            },
        }
        return params

    def _get_person_params(self, content):
        params_list = []

        form_inputs = content.cssselect('[name=SearchVal]')
        for form_input in form_inputs:
            params_list.append({
                'endpoint': self._initial_endpoint,
                'task_type': constants.SCRAPE_PERSON_AND_RECORD_AND_MORE,
                'data': {
                    'SearchField': 'BookingNo',
                    'SearchVal': form_input.get('value')
                },
            })

        return params_list

    # TODO(colincadams): Parse free form text in first table row, current
    # possibilities:
    # - 'Inmate has been sentenced and cannot post bond for release at this
    #    time.'
    # - 'The inmate currently has open charges. Inmate may have a hold or bond
    #    has not been set at this time...'
    # - 'The inmate currently is eligible for release after posting bond...'
    # - 'The inmate currently has a hold that could prevent him from being
    #    eligible for release after posting bond...'

    @staticmethod
    def _scrape_person(content):
        person = Person()

        table = content.cssselect('table')[0]
        for row in table:
            if len(row) == 2:
                if row[0].text_content().startswith('Name'):
                    # TODO: move to scraper_utils
                    name = row[1].text_content().split()
                    person.given_names = ' '.join(name[:-1])
                    person.surname = name[-1]
                if row[0].text_content().startswith('DOB'):
                    person.birthdate = scraper_utils.parse_date_string(
                        row[1].text_content())
                    person.age = scraper_utils.calculate_age(person.birthdate)

        return person

    @staticmethod
    def _scrape_record(content):
        record = Record()

        table = content.cssselect('table')[0]
        for row in table:
            if row[0].text_content().startswith('Booking#'):
                record.record_id = row[1].text_content()
            if row[0].text_content().startswith('Bonds'):
                # TODO: for now we are just putting all the bond info in a
                # single offense
                offense = Offense()
                # Bond information is stored in the next row.
                offense.description = row.getnext()[1].text_content()
                record.offenses.append(offense)
        return record

    def get_initial_endpoint(self):
        """Returns the initial endpoint to hit on the first call
        Returns:
            A string representing the initial endpoint to hit
        """
        return self._initial_endpoint

    def set_initial_vars(self, content, params):
        """
        Sets initial vars in the params that it will pass on to future scrapes

        Args:
            content: An lxml html tree.
            params: dict of parameters passed from the last scrape session.
        """
        pass

    def get_person_class(self):
        """Returns the person subclass to use for this scraper.

        Returns:
            A class representing the person DB object.
        """
        return Person

    def get_record_class(self):
        return Record

    def get_person_id(self, content, params):
        return self.get_region().region_code + scraper_utils.generate_id(Person)

    def get_record_id(self, content, params):
        return self._record.record_id

    def person_id_is_fuzzy(self):
        return True

    def get_given_names(self, content, params):
        return self._person.given_names

    def get_surname(self, content, params):
        return self._person.surname

    def get_birthdate(self, content, params):
        return self._person.birthdate

    def get_age(self, content, params):
        return self._person.age

    def get_offenses(self, content, params):
        return self._record.offenses
