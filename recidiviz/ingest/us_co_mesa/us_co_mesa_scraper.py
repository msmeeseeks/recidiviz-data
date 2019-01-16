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
import enum

from recidiviz.common.constants.bond import BondType
from recidiviz.common.constants.charge import ChargeClass
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
        if self.is_initial_task(params['task_type']):
            return [self._get_all_people_params()]

        return self._get_person_params(content)

    def _get_all_people_params(self):
        params = {
            'endpoint': self.get_region().base_url,
            'task_type': constants.GET_MORE_TASKS,
            'post_data': {
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
                'post_data': {
                    'SearchField': 'BookingNo',
                    'SearchVal': form_input.get('value')
                },
            })

        return params_list

    def populate_data(self, content, params, ingest_info):
        person = ingest_info.create_person()
        booking = person.create_booking()

        table = content.cssselect('table')[0]

        # Get person status
        person_status = get_person_status(table[0].text_content())
        if person_status is _PersonStatus.HOLD:
            booking.hold = 'True'

        # Parse rest of data from table
        for row in table[1:]:
            if row[0].text_content().startswith('Name'):
                person.full_name = row[1].text_content()
            if row[0].text_content().startswith('DOB'):
                person.birthdate = row[1].text_content()
            if row[0].text_content().startswith('Booking#'):
                booking.booking_id = row[1].text_content()
            if row[0].text_content().startswith('Bonds'):
                bond_list = row.getnext()[1][0]
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

                    # Get list of charges
                    charge_list = bond_element.getnext()
                    for charge_element in charge_list:
                        charge_text = charge_element.text_content()
                        assert charge_text.startswith('Charge: ')
                        charge_text = charge_text[8:]

                        # TODO(617): Parse charge name for any holds or degrees
                        charge_name, charge_meta = \
                            _split_charge_text(charge_text)

                        # By default, assume the whole parentheses is the
                        # statute
                        charge_statute = charge_meta
                        charge_class = charge_level = None

                        charge_meta_split = charge_meta.rsplit(' ', maxsplit=1)
                        # If the last part starts with a '#' then it is not the
                        # full class code, look before it.
                        if len(charge_meta_split) > 1 and \
                                charge_meta_split[1].startswith('#'):
                            # TODO(617): determine how this should be used
                            _something = charge_meta_split[1]
                            charge_meta_split = charge_meta_split[0].rsplit(
                                ' ', maxsplit=1)

                        # If the last part doesn't start with a paren then it is
                        # the class
                        if len(charge_meta_split) > 1 and \
                                not charge_meta_split[1].startswith('('):
                            charge_statute = charge_meta_split[0]
                            charge_class, charge_level = \
                                _parse_class_and_level(charge_meta_split[1])

                        charge_status = 'Sentenced' if person_status is \
                            _PersonStatus.SENTENCED else None

                        booking.create_charge(
                            name=charge_name.strip(),
                            statute=charge_statute,
                            charge_class=charge_class,
                            level=charge_level,
                            bond=bond,
                            status=charge_status,
                        )

        # TODO(617): For sentenced people the site no longer shows their
        # charges, we should create an empty charge to hold this information.

        # TODO(617): For people 'with open charges', the site doesn't show their
        # charges. Not sure what we should do here, just leave it blank?

        return ingest_info

    def get_enum_overrides(self):
        return {
            # Charge Classes
            'PO': ChargeClass.INFRACTION,  # Petty Offense
            'TI': ChargeClass.CIVIL,  # Traffic Infraction

            # Bond Types
            '(CASH) CASH': BondType.CASH,
            '(CS) CASH/SURETY': BondType.UNSECURED,
            '(CSP) CASH/SURETY/PROPERTY': BondType.UNSECURED,
            '(SURETY) SURETY': BondType.UNSECURED,
            '(PR) PERSONAL RECOGNIZANCE': BondType.NO_BOND,
            '(PRS) PERSONAL RECOGNIZANCE CO-SIGN': BondType.NO_BOND,
        }


def _parse_class_and_level(text):
    """Parses the charge class and level from the given text"""
    charge_class = charge_level = None

    # Class starts with 'D' if a Drug offense.
    # TODO(617): encode this in the charge
    _is_drug = False
    if text.startswith('D'):
        _is_drug = True
        text = text[1:]

    # Sometimes these have an unused suffix
    text_split = text.split('#')
    if len(text_split) > 1:
        # TODO(617): determine how this should be used
        _something = text_split[1]
        text = text_split[0]

    if text:
        print('charge_class: %s' % text)
        # If it ends in a digit, that is the level
        if text[-1].isdigit():
            charge_class = text[:-1]
            charge_level = text[-1:]
        # Sometimes we fatfinger them in the wrong order
        elif text[0].isdigit():
            charge_class = text[1:]
            charge_level = text[:1]
        # If it is a 'Traffic Infraction', then the suffix is the class.
        elif text.startswith('TI') and len(text) > len('TI'):
            charge_class = text[:2]
            charge_level = text[2:]
        else:
            charge_class = text

    return charge_class, charge_level


def _split_charge_text(text):
    assert text[-1] == ')'

    # Find matching open paren
    num_unmatched_closed = 0
    split_index = None
    for i, c in enumerate(reversed(text)):
        if c == '(':
            num_unmatched_closed -= 1
            if not num_unmatched_closed:
                split_index = 0 - (i + 1)
                break
        elif c == ')':
            num_unmatched_closed += 1

    assert split_index is not None
    charge_name = text[:split_index]
    charge_meta = text[split_index:][1:-1]
    return charge_name, charge_meta


class _PersonStatus(enum.Enum):
    SENTENCED = 'sentenced'                      # Sets charge status
    OPEN_CHARGES = 'open'                        # TODO(616): bond -> pending
    ELIGIBLE_AFTER_POST = 'eligible_after_post'  # No effect
    HOLD = 'hold'                                # Sets hold on booking


STATUS_SUFFIX = ('Please contact the Mesa County Sheriff\'s Office at 970 '
                 '244-3500 for more information.')
def get_person_status(status_text):
    status_text = status_text.strip()
    if status_text.endswith(STATUS_SUFFIX):
        status_text = status_text[:-len(STATUS_SUFFIX)]
    status_text = status_text.strip()

    if not status_text:
        return None
    if status_text == ('Inmate has been sentenced and cannot post bond for '
                       'release at this time.'):
        return _PersonStatus.SENTENCED
    if status_text == ('The inmate currently has open charges.   Inmate may '
                       'have a hold or bond has not been set at this time.  '
                       'Arraignment times are 1pm Monday through Friday.'):
        return _PersonStatus.OPEN_CHARGES
    if status_text == ('The inmate currently is eligible for release after '
                       'posting bond.'):
        return _PersonStatus.ELIGIBLE_AFTER_POST
    if status_text == ('The inmate currently has a hold that could prevent him '
                       'from being eligible for release after posting bond.'):
        return _PersonStatus.HOLD
    raise ValueError(
        'Unexpected status text in us_co_mesa: "{}"'.format(status_text))
