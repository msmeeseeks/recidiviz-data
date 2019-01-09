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

"""Represents data scraped for a single individual."""
from typing import List, Optional


class IngestInfo:
    """Class for information about multiple people."""

    def __init__(self, people=None):
        self.person: List[_Person] = people or []

    def __eq__(self, other):
        if other is None:
            return False
        return self.__dict__ == other.__dict__

    def __bool__(self):
        return to_bool(self)

    def __str__(self):
        return to_string(self)

    def __repr__(self):
        return to_repr(self)

    def __setattr__(self, name, value):
        restricted_setattr(self, 'person', name, value)

    def create_person(self, **kwargs) -> '_Person':
        person = _Person(**kwargs)
        self.person.append(person)
        return person

    def get_recent_person(self) -> Optional['_Person']:
        if self.person:
            return self.person[-1]
        return None

    def prune(self) -> 'IngestInfo':
        self.person = [person.prune() for person in self.person if person]
        return self


class _Person:
    """Class for information about a person.
    Referenced from IngestInfo.
    """

    def __init__(self, person_id=None, full_name=None, surname=None,
                 given_names=None, birthdate=None, gender=None, age=None,
                 race=None, ethnicity=None, place_of_residence=None,
                 bookings=None):
        self.person_id: str = person_id
        self.surname: str = surname
        self.given_names: str = given_names
        self.full_name: str = full_name
        self.birthdate: str = birthdate
        self.gender: str = gender
        self.age: str = age
        self.race: str = race
        self.ethnicity: str = ethnicity
        self.place_of_residence: str = place_of_residence

        self.booking: List[_Booking] = bookings or []

    def __eq__(self, other):
        if other is None:
            return False
        return self.__dict__ == other.__dict__

    def __bool__(self):
        return to_bool(self)

    def __str__(self):
        return to_string(self)

    def __repr__(self):
        return to_repr(self)

    def __setattr__(self, name, value):
        restricted_setattr(self, 'booking', name, value)

    def create_booking(self, **kwargs) -> '_Booking':
        booking = _Booking(**kwargs)
        self.booking.append(booking)
        return booking

    def get_recent_booking(self) -> Optional['_Booking']:
        if self.booking:
            return self.booking[-1]
        return None

    def prune(self) -> '_Person':
        self.booking = [booking.prune() for booking in self.booking if booking]
        return self


class _Booking:
    """Class for information about a booking.
    Referenced from Person.
    """

    def __init__(self, booking_id=None, admission_date=None,
                 projected_release_date=None, release_date=None,
                 release_reason=None,
                 custody_status=None,
                 hold=None,
                 facility=None, classification=None,
                 total_bond_amount=None,
                 arrest=None, charges=None):
        self.booking_id: str = booking_id
        self.admission_date: str = admission_date
        self.projected_release_date: str = projected_release_date
        self.release_date: str = release_date
        self.release_reason: str = release_reason
        self.custody_status: str = custody_status
        self.hold: str = hold
        self.facility: str = facility
        self.classification: str = classification
        self.total_bond_amount: str = total_bond_amount

        self.arrest: Optional[_Arrest] = arrest
        self.charge: List[_Charge] = charges or []

    def __eq__(self, other):
        if other is None:
            return False
        return self.__dict__ == other.__dict__

    def __bool__(self):
        return to_bool(self)

    def __str__(self):
        return to_string(self)

    def __repr__(self):
        return to_repr(self)

    def __setattr__(self, name, value):
        restricted_setattr(self, 'charge', name, value)

    def create_arrest(self, **kwargs) -> '_Arrest':
        self.arrest = _Arrest(**kwargs)
        return self.arrest

    def create_charge(self, **kwargs) -> '_Charge':
        charge = _Charge(**kwargs)
        self.charge.append(charge)
        return charge

    def get_recent_charge(self) -> Optional['_Charge']:
        if self.charge:
            return self.charge[-1]
        return None

    def get_recent_arrest(self) -> Optional['_Arrest']:
        return self.arrest

    def prune(self) -> '_Booking':
        self.charge = [charge.prune() for charge in self.charge if charge]
        if not self.arrest:
            self.arrest = None
        return self


class _Arrest:
    """Class for information about an arrest.
    Referenced from Booking.
    """

    def __init__(self, arrest_id=None, date=None, location=None,
                 officer_name=None, officer_id=None, agency=None):
        self.arrest_id: str = arrest_id
        self.date: str = date
        self.location: str = location
        self.officer_name: str = officer_name
        self.officer_id: str = officer_id
        self.agency: str = agency

    def __eq__(self, other):
        if other is None:
            return False
        return self.__dict__ == other.__dict__

    def __bool__(self):
        return to_bool(self)

    def __str__(self):
        return to_string(self)

    def __repr__(self):
        return to_repr(self)

    def __setattr__(self, name, value):
        restricted_setattr(self, 'agency', name, value)


class _Charge:
    """Class for information about a charge.
    Referenced from Booking.
    """

    def __init__(self, charge_id=None, offense_date=None, statute=None,
                 name=None, attempted=None, degree=None,
                 charge_class=None, level=None, fee_dollars=None,
                 charging_entity=None, status=None,
                 number_of_counts=None, court_type=None,
                 case_number=None, next_court_date=None, judge_name=None,
                 bond=None, sentence=None):
        self.charge_id: str = charge_id
        self.offense_date: str = offense_date
        self.statute: str = statute
        self.name: str = name
        self.attempted: str = attempted
        self.degree: str = degree
        self.charge_class: str = charge_class
        self.level: str = level
        self.fee_dollars: str = fee_dollars
        self.charging_entity: str = charging_entity
        self.status: str = status
        self.number_of_counts: str = number_of_counts
        self.court_type: str = court_type
        self.case_number: str = case_number
        self.next_court_date: str = next_court_date
        self.judge_name: str = judge_name

        self.bond: Optional[_Bond] = bond
        self.sentence: Optional[_Sentence] = sentence

    def __eq__(self, other):
        if other is None:
            return False
        return self.__dict__ == other.__dict__

    def __bool__(self):
        return to_bool(self)

    def __str__(self):
        return to_string(self)

    def __repr__(self):
        return to_repr(self)

    def __setattr__(self, name, value):
        restricted_setattr(self, 'sentence', name, value)

    def create_bond(self, **kwargs) -> '_Bond':
        self.bond = _Bond(**kwargs)
        return self.bond

    def create_sentence(self, **kwargs) -> '_Sentence':
        self.sentence = _Sentence(**kwargs)
        return self.sentence

    def get_recent_bond(self) -> Optional['_Bond']:
        return self.bond

    def get_recent_sentence(self) -> Optional['_Sentence']:
        return self.sentence

    def prune(self) -> '_Charge':
        if not self.bond:
            self.bond = None
        if not self.sentence:
            self.sentence = None
        return self


class _Bond:
    """Class for information about a bond.
    Referenced from Charge.
    """

    def __init__(self, bond_id=None, amount=None, bond_type=None, status=None):
        self.bond_id: str = bond_id
        self.amount: str = amount
        self.bond_type: str = bond_type
        self.status: str = status

    def __eq__(self, other):
        if other is None:
            return False
        return self.__dict__ == other.__dict__

    def __bool__(self):
        return to_bool(self)

    def __str__(self):
        return to_string(self)

    def __repr__(self):
        return to_repr(self)

    def __setattr__(self, name, value):
        restricted_setattr(self, 'status', name, value)


class _Sentence:
    """Class for information about a sentence.
    Referenced from Charge.
    """

    def __init__(self, sentence_id=None, date_imposed=None,
                 sentencing_region=None, min_length=None, max_length=None,
                 is_life=None, is_probation=None, is_suspended=None,
                 fine_dollars=None, parole_possible=None,
                 post_release_supervision_length=None):
        self.sentence_id: str = sentence_id
        self.date_imposed: str = date_imposed
        self.sentencing_region: str = sentencing_region
        self.min_length: str = min_length
        self.max_length: str = max_length
        self.is_life: str = is_life
        self.is_probation: str = is_probation
        self.is_suspended: str = is_suspended
        self.fine_dollars: str = fine_dollars
        self.parole_possible: str = parole_possible

        self.post_release_supervision_length: str = \
            post_release_supervision_length

    def __eq__(self, other):
        if other is None:
            return False
        return self.__dict__ == other.__dict__

    def __bool__(self):
        return to_bool(self)

    def __str__(self):
        return to_string(self)

    def __repr__(self):
        return to_repr(self)

    def __setattr__(self, name, value):
        restricted_setattr(self, 'post_release_supervision_length', name, value)


def to_bool(obj):
    return any(any(v) if isinstance(v, list) else v
               for v in obj.__dict__.values())


def to_string(obj):
    out = [obj.__class__.__name__ + ":"]
    for key, val in vars(obj).items():
        if isinstance(val, list):
            for index, elem in enumerate(val):
                out += '{}[{}]: {}'.format(key, index, elem).split('\n')
        elif val:
            out += '{}: {}'.format(key, val).split('\n')
    return '\n   '.join(out)


def to_repr(obj):
    plurals = {'person': 'people', 'booking': 'bookings', 'charge': 'charges'}
    args = []
    for key, val in vars(obj).items():
        if val:
            key_arg = plurals[key] if key in plurals else key
            args.append('{}={}'.format(key_arg, repr(val)))

    return '{}({})'.format(obj.__class__.__name__, ', '.join(args))


def restricted_setattr(self, last_field, name, value):
    if hasattr(self, last_field) and not hasattr(self, name):
        raise AttributeError('No field {} in object {}'.format(name,
                                                               type(self)))
    self.__dict__[name] = value
