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

from recidiviz.ingest import constants
from recidiviz.ingest.base_scraper import BaseScraper
from recidiviz.ingest.models.ingest_info import _Bond


class UsCoMesaScraper(BaseScraper):
    """Scraper Mesa County, Colorado jails."""

    def __init__(self):
        super(UsCoMesaScraper, self).__init__('us_co_mesa')

    def set_initial_vars(self, content, params):
        pass

    def get_more_tasks(self, content, params):
        task_type = params.get('task_type', self.get_initial_task_type())

        if self.is_initial_task(task_type):
            return [self._get_all_people_params()]

        return self._get_person_params(content)

    def _get_all_people_params(self):
        params = {
            'endpoint': self.get_region().base_url,
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
                'endpoint': self.get_region().base_url,
                'task_type': constants.SCRAPE_DATA,
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

    def populate_data(self, content, params, ingest_info):
        person = ingest_info.create_person()
        booking = person.create_booking()

        table = content.cssselect('table')[0]
        for row in table:
            if row[0].text_content().startswith('Name'):
                # TODO(#206): move name parsing to data converter
                name = row[1].text_content().split()
                person.given_names = ' '.join(name[:-1])
                person.surname = name[-1]
            if row[0].text_content().startswith('DOB'):
                person.birthdate = row[1].text_content()
            if row[0].text_content().startswith('Booking#'):
                booking.booking_id = row[1].text_content()
            if row[0].text_content().startswith('Bonds'):
                bond_list = row.getnext()[1][0]
                total_bond_amount = 0
                # Each "bond" occupies two elements in the list, a header
                # element and a list of relevant charges. We iterate over the
                # bonds list in twos to handle this.
                for bond_element in bond_list[::2]:
                    bond_text = bond_element.text_content()
                    assert bond_text.startswith('Bond#')
                    bond_text = bond_text[5:]
                    bond_id, bond = bond_text.split(' - ')
                    bond_type, bond_amount = bond.split(':')

                    bond = _Bond(
                        bond_id=bond_id.strip(),
                        amount=bond_amount.strip(),
                        bond_type=bond_type,
                    )
                    total_bond_amount += int(bond_amount.strip(' $'))

                    # Get list of charges
                    charge_list = bond_element.getnext()
                    for charge_element in charge_list:
                        charge_text = charge_element.text_content()
                        assert charge_text.startswith('Charge:')
                        assert charge_text.endswith(')')
                        charge_text = charge_text[7:-1]
                        charge_name, charge_meta = charge_text.split('(', 1)
                        charge_meta = charge_meta.split(' ')
                        charge_statute = charge_meta[0]
                        charge_class = charge_meta[1] if len(
                            charge_meta) > 1 else None

                        booking.create_charge(
                            name=charge_name.strip(),
                            statute=charge_statute,
                            charge_class=charge_class,
                            bond=bond,
                        )
                booking.total_bond_amount = '${}'.format(total_bond_amount)
