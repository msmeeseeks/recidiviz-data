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
# ============================================================================
"""Define the ORM schema objects that map directly to the database."""

from sqlalchemy import Column, Integer, String, DateTime, Boolean, ForeignKey
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Person(Base):
    __tablename__ = 'person'

    person_id = Column('person_id', Integer, primary_key=True)

    scraped_person_id = Column('scraped_person_id', Integer)
    surname = Column('surname', String)
    given_names = Column('given_names', String)
    birthdate = Column('birthdate', DateTime)
    birthdate_inferred_from_age = Column('birthdate_inferred_from_age',
                                         Boolean)
    race = Column('race', String)
    ethnicity = Column('ethnicity', String)
    place_of_residence = Column('place_of_residence', String)


class Booking(Base):
    __tablename__ = 'booking'

    booking_id = Column('booking_id', Integer, primary_key=True)

    person_id = Column('person_id', Integer,
                       ForeignKey("person.person_id"))

    admission_date = Column('admission_date', DateTime)
    release_date = Column('release_date', DateTime)
    release_reason = Column('release_reason', String)
    custody_status = Column('custody_status', String)
    held_for_other_jurisdiction = Column('held_for_other_jurisdiction',
                                         Boolean)
    hold = Column('hold', String)
    facility = Column('facility', String)
    classification = Column('classification', String)


class Arrest(Base):
    __tablename__ = 'arrest'

    arrest_id = Column('arrest_id', Integer, primary_key=True)

    booking_id = Column('booking_id', Integer,
                        ForeignKey("booking.booking_id"))

    date = Column('date', DateTime)
    location = Column('location', String)
    agency = Column('agency', String)
    officer_name = Column('officer_name', String)
    officer_id = Column('officer_id', String)


class Charge(Base):
    __tablename__ = 'charge'

    charge_id = Column('charge_id', Integer, primary_key=True)

    booking_id = Column('booking_id', Integer,
                        ForeignKey("booking.booking_id"))

    offence_date = Column('offence_date', DateTime)
    statute = Column('statute', String)
    offense_code = Column('offense_code', Integer)
    name = Column('name', String)
    attempted = Column('attempted', Boolean)
    degree = Column('degree', String)
    charge_class = Column('charge_class', String)
    level = Column('level', String)
    fee = Column('fee', Integer)
    charging_entity = Column('charging_entity', String)
    status = Column('status', String)
    number_of_counts = Column('number_of_counts', Integer)
    court_type = Column('court_type', String)
    case_number = Column('case_number', String)
    next_court_date = Column('next_court_date', DateTime)
    judge_name = Column('judge_name', String)


class Bond(Base):
    __tablename__ = 'bond'

    bond_id = Column('bond_id', Integer, primary_key=True)

    charge_id = Column('charge_id', Integer,
                       ForeignKey("charge.charge_id"))

    amount = Column('amount', Integer)
    bond_type = Column('bond_type', String)
    status = Column('status', String)


class Sentence(Base):
    __tablename__ = 'sentences'

    sentence_id = Column('sentence_id', Integer, primary_key=True)

    charge_id = Column('charge_id', Integer,
                       ForeignKey("charge.charge_id"))

    date_imposed = Column('date_imposed', DateTime)
    min_length_days = Column('min_length_days', Integer)
    max_length_days = Column('max_length_days', Integer)
    is_life = Column('is_life', Boolean)
    is_probation = Column('is_probation', Boolean)
    is_suspended = Column('is_suspended', Boolean)
    fine = Column('fine', Integer)
    parole_possible = Column('parole_possible', Boolean)
    post_release_supervision_length_days = Column(
        'post_release_supervision_length_days',
        Integer)
    concurrent_with = Column('concurrent_with',
                             ForeignKey('sentences.sentence_id'))
    consecutive_with = Column('consecutive_with',
                              ForeignKey('sentences.sentence_id'))
