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
"""Contains helpers to interact with the database."""
import attr

from recidiviz.persistence import entities
from recidiviz.persistence.database import schema


def convert_people(people_src):
    """Converts the given list of people to the correct objects

    Args:
        people_src: list of schema.Person or entities.Person
    Returns:
        The converted list, a schema.Person or entities.Person
    """
    return [convert_person(p) for p in people_src]
    

def convert_person(person_src):
    """Converts the given person to the correct object.

    Args:
        person_src: A schema Person or entity Person object
    Returns:
        The converted object, a schema or entity object.
    """
    if not person_src:
        return None

    entity_to_db = isinstance(person_src, entities.Person)
    if entity_to_db:
        person_dst = schema.Person()
    else:
        person_dst = entities.Person.builder()
    for k in attr.fields_dict(entities.Person).keys():
        if k == 'bookings':
            person_dst.bookings = [convert_booking(b) for b in
                                   person_src.bookings]
        else:
            setattr(person_dst, k, getattr(person_src, k))

    if entity_to_db:
        return person_dst

    return person_dst.build()


def convert_bookings(bookings_src):
    """Converts the given list of bookings to the correct objects

    Args:
        bookings_src: list of schema.Booking or entities.Booking
    Returns:
        The converted list, a schema.Booking or entities.Booking
    """
    return [convert_booking(b) for b in bookings_src]


def convert_booking(booking_src):
    """Converts the given booking to the correct object.

    Args:
        booking_src: A schema Booking or entity Booking object
    Returns:
        The converted object, a schema or entity object
    """
    if not booking_src:
        return None

    entity_to_db = isinstance(booking_src, entities.Booking)
    fields = vars(entities.Booking()).keys()
    if entity_to_db:
        dst_module = schema
        booking_dst = schema.Booking()
    else:
        dst_module = entities
        booking_dst = entities.Booking()
    for k in fields:
        if k == 'holds':
            booking_dst.holds = [_convert_object(
                h, dst_module.Hold(), entity_to_db)
                                 for h in booking_src.holds]
        elif k == 'arrest':
            booking_dst.arrest = _convert_object(
                booking_src.arrest, dst_module.Arrest(), entity_to_db)
        elif k == 'charges':
            booking_dst.charges = [convert_charge(c) for c in
                                   booking_src.charges]
        else:
            setattr(booking_dst, k, getattr(booking_src, k))
    return booking_dst


def convert_charges(charges_src):
    """Converts the given list of charges to the correct objects

    Args:
        charges_src: list of schema.Charge or entities.Charge
    Returns:
        The converted list, a schema.Charge or entities.Charge
    """
    return [convert_charge(c) for c in charges_src]


def convert_charge(charge_src):
    """Converts the given charge to the correct object.

    Args:
        charge_src: A schema Charge or entity Charge object
    Returns:
        The converted object, a schema or entity object
    """
    if not charge_src:
        return None

    entity_to_db = isinstance(charge_src, entities.Charge)
    fields = vars(entities.Charge()).keys()
    if entity_to_db:
        dst_module = schema
        charge_dst = schema.Charge()
    else:
        dst_module = entities
        charge_dst = entities.Charge()

    for k in fields:
        if k == 'bond':
            charge_dst.bond = _convert_object(
                charge_src.bond, dst_module.Bond(), entity_to_db)
        elif k == 'sentence':
            charge_dst.sentence = convert_sentence(charge_src.sentence)
        else:
            setattr(charge_dst, k, getattr(charge_src, k))
    return charge_dst


def convert_sentence(sentence_src):
    """Converts the given sentence to the correct object.

    Args:
        sentence_src: A schema Sentence or entity Sentence object
    Returns:
        The converted object, a schema or entity object
    """
    if not sentence_src:
        return None

    entity_to_db = isinstance(sentence_src, entities.Sentence)
    fields = vars(entities.Sentence()).keys()
    fields.remove('related_sentences')
    if entity_to_db:
        sentence_dst = schema.Sentence()
    else:
        sentence_dst = entities.Sentence()

    for k in fields:
        setattr(sentence_dst, k, getattr(sentence_src, k))
    return sentence_dst


def _convert_object(src, dst, entity_to_db=True):
    """Converts the given source to the given dst.  The fields should exist
    in both the source and destination.

    Args:
        booking_src: A schema Booking or entity Booking object
    Returns:
        The converted object, a schema or entity object
    """
    if src is None:
        return None

    if entity_to_db:
        fields = vars(src).keys()
    else:
        fields = vars(dst).keys()
    for k in fields:
        setattr(dst, k, getattr(src, k))
    return dst
