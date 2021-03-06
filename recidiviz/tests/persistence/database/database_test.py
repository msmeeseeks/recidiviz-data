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
"""Tests for database.py."""

import datetime
from copy import deepcopy
from unittest import TestCase

from more_itertools import one
import pandas as pd
from sqlalchemy import func

from recidiviz import Session
from recidiviz.common.constants import enum_canonical_strings as enum_strings
from recidiviz.common.constants.bond import BondStatus
from recidiviz.common.constants.booking import CustodyStatus
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.person import Race
from recidiviz.common.constants.sentence import SentenceStatus
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.persistence import entities
from recidiviz.persistence.database import database, database_utils
from recidiviz.persistence.database.schema import Bond, Booking, Person, \
    Sentence, FlCountyAggregate, FlFacilityAggregate
from recidiviz.persistence.database.schema import (
    BookingHistory,
    Charge, ChargeHistory,
    PersonHistory)
from recidiviz.tests.utils import fakes

_REGION = 'region'
_REGION_ANOTHER = 'wrong region'
_FULL_NAME = 'full_name'
_EXTERNAL_ID = 'external_id'
_BIRTHDATE = datetime.date(year=2012, month=1, day=2)
_LAST_SEEN_TIME = datetime.datetime(year=2020, month=7, day=4)
_FACILITY = 'facility'

DATE_SCRAPED = datetime.date(year=2019, month=1, day=1)


class TestDatabase(TestCase):
    """Test that the methods in database.py correctly read from the SQL
    database """

    def setup_method(self, _test_method):
        fakes.use_in_memory_sqlite_database()

    def test_readPeopleWithOpenBookingsBeforeDate(self):
        # Arrange
        person = Person(person_id=8, region=_REGION)
        person_resolved_booking = Person(person_id=9, region=_REGION)
        person_most_recent_scrape = Person(person_id=10, region=_REGION)
        person_wrong_region = Person(person_id=11, region=_REGION_ANOTHER)

        release_date = datetime.date(2018, 7, 20)
        most_recent_scrape_date = datetime.datetime(2018, 6, 20)
        date_in_past = most_recent_scrape_date - datetime.timedelta(days=1)

        # Bookings that should be returned
        open_booking_before_last_scrape = Booking(
            person_id=person.person_id,
            custody_status=CustodyStatus.IN_CUSTODY.value,
            last_seen_time=date_in_past)

        # Bookings that should not be returned
        open_booking_incorrect_region = Booking(
            person_id=person_wrong_region.person_id,
            custody_status=CustodyStatus.IN_CUSTODY.value,
            last_seen_time=date_in_past)
        open_booking_most_recent_scrape = Booking(
            person_id=person_most_recent_scrape.person_id,
            custody_status=CustodyStatus.IN_CUSTODY.value,
            last_seen_time=most_recent_scrape_date)
        resolved_booking = Booking(
            person_id=person_resolved_booking.person_id,
            custody_status=CustodyStatus.IN_CUSTODY.value,
            release_date=release_date,
            last_seen_time=date_in_past)

        session = Session()
        session.add(person)
        session.add(person_resolved_booking)
        session.add(person_most_recent_scrape)
        session.add(person_wrong_region)
        session.add(open_booking_before_last_scrape)
        session.add(open_booking_incorrect_region)
        session.add(open_booking_most_recent_scrape)
        session.add(resolved_booking)
        session.commit()

        # Act
        people = database.read_people_with_open_bookings_scraped_before_time(
            session, person.region, most_recent_scrape_date)

        # Assert
        self.assertEqual(people, [database_utils.convert(person)])

    def test_readPeopleByExternalId(self):
        admission_date = datetime.datetime(2018, 6, 20)
        release_date = datetime.date(2018, 7, 20)
        closed_booking = Booking(
            custody_status=CustodyStatus.IN_CUSTODY.value,
            admission_date=admission_date,
            release_date=release_date,
            last_seen_time=admission_date)

        person_no_match = Person(person_id=1, region=_REGION,
                                 bookings=[deepcopy(closed_booking)])
        person_match_external_id = Person(person_id=2, region=_REGION,
                                          bookings=[closed_booking],
                                          external_id=_EXTERNAL_ID)

        session = Session()
        session.add(person_no_match)
        session.add(person_match_external_id)
        session.commit()

        ingested_person = entities.Person.new_with_defaults(
            external_id=_EXTERNAL_ID)
        people = database.read_people_by_external_ids(session, _REGION,
                                                      [ingested_person])

        expected_people = [
            database_utils.convert(person_match_external_id)]
        self.assertCountEqual(people, expected_people)

    def test_readPeopleWithOpenBookings(self):
        admission_date = datetime.datetime(2018, 6, 20)
        release_date = datetime.date(2018, 7, 20)

        open_booking = Booking(
            custody_status=CustodyStatus.IN_CUSTODY.value,
            admission_date=admission_date,
            last_seen_time=admission_date)
        closed_booking = Booking(
            custody_status=CustodyStatus.IN_CUSTODY.value,
            admission_date=admission_date,
            release_date=release_date,
            last_seen_time=admission_date)

        person_no_match = Person(person_id=1, region=_REGION,
                                 bookings=[deepcopy(open_booking)])
        person_match_full_name = Person(person_id=2, region=_REGION,
                                        bookings=[deepcopy(open_booking)],
                                        full_name=_FULL_NAME)
        person_no_open_bookings = Person(person_id=6, region=_REGION,
                                         full_name=_FULL_NAME,
                                         bookings=[closed_booking])

        session = Session()
        session.add(person_no_match)
        session.add(person_no_open_bookings)
        session.add(person_match_full_name)
        session.commit()

        info = IngestInfo()
        info.create_person(full_name=_FULL_NAME, person_id=_EXTERNAL_ID)
        people = database.read_people_with_open_bookings(session, _REGION,
                                                         info.people)

        expected_people = [database_utils.convert(p) for p in
                           [person_match_full_name]]
        self.assertCountEqual(people, expected_people)

    def testWritePerson_noExistingSnapshots_createsSnapshots(self):
        act_session = Session()

        person = entities.Person.new_with_defaults(
            region=_REGION, race=Race.OTHER)

        booking = entities.Booking.new_with_defaults(
            custody_status=CustodyStatus.IN_CUSTODY,
            last_seen_time=_LAST_SEEN_TIME)
        person.bookings = [booking]

        charge_1 = entities.Charge.new_with_defaults(
            status=ChargeStatus.PENDING)
        charge_2 = entities.Charge.new_with_defaults(
            status=ChargeStatus.PRETRIAL)
        booking.charges = [charge_1, charge_2]

        persisted_person = database.write_person(
            act_session, person, IngestMetadata(
                "default_region", _LAST_SEEN_TIME, {}))
        act_session.commit()

        person_id = persisted_person.person_id
        booking_id = persisted_person.bookings[0].booking_id
        charge_1_id = persisted_person.bookings[0].charges[0].charge_id
        charge_2_id = persisted_person.bookings[0].charges[1].charge_id

        act_session.close()

        assert_session = Session()

        person_snapshots = assert_session.query(PersonHistory).filter(
            PersonHistory.person_id == person_id).all()
        booking_snapshots = assert_session.query(BookingHistory).filter(
            BookingHistory.booking_id == booking_id).all()
        charge_snapshots = assert_session.query(ChargeHistory).filter(
            ChargeHistory.charge_id.in_([charge_1_id, charge_2_id])).all()

        self.assertEqual(len(person_snapshots), 1)
        self.assertEqual(len(booking_snapshots), 1)
        self.assertEqual(len(charge_snapshots), 2)

        person_snapshot = person_snapshots[0]
        booking_snapshot = booking_snapshots[0]
        charge_snapshot_statuses = {charge_snapshot.status for charge_snapshot
                                    in charge_snapshots}
        snapshot_time = person_snapshot.valid_from

        self.assertEqual(person_snapshot.region, _REGION)
        self.assertEqual(person_snapshot.race, Race.OTHER.value)
        self.assertIsNone(person_snapshot.valid_to)

        self.assertEqual(
            booking_snapshot.custody_status, CustodyStatus.IN_CUSTODY.value)
        self.assertEqual(booking_snapshot.valid_from, snapshot_time)
        self.assertIsNone(booking_snapshot.valid_to)

        self.assertEqual(
            charge_snapshot_statuses,
            {ChargeStatus.PENDING.value, ChargeStatus.PRETRIAL.value})
        self.assertEqual(charge_snapshots[0].valid_from, snapshot_time)
        self.assertEqual(charge_snapshots[1].valid_from, snapshot_time)
        self.assertIsNone(charge_snapshots[0].valid_to)
        self.assertIsNone(charge_snapshots[1].valid_to)

        assert_session.close()

    def testWritePerson_allExistingSnapshots_onlyUpdatesOnChange(self):
        charge_name_1 = 'charge_name_1'
        charge_name_2 = 'charge_name_2'
        updated_last_seen_time = datetime.datetime(year=2020, month=7, day=6)
        # Pick a date in the past so the assigned snapshot date will always be
        # later
        valid_from = datetime.datetime(year=2015, month=1, day=1)
        existing_person_id = 1
        existing_booking_id = 14
        existing_charge_id = 47

        arrange_session = Session()

        existing_person = Person(
            person_id=existing_person_id,
            region=_REGION,
            race=Race.OTHER.value)
        existing_person_snapshot = PersonHistory(
            person_history_id=1000,
            person_id=existing_person_id,
            region=_REGION,
            race=Race.OTHER.value,
            valid_from=valid_from)

        existing_booking = Booking(
            booking_id=existing_booking_id,
            custody_status=CustodyStatus.IN_CUSTODY.value,
            facility=_FACILITY,
            last_seen_time=_LAST_SEEN_TIME)
        existing_booking_snapshot = BookingHistory(
            booking_history_id=1000,
            person_id=existing_person_id,
            booking_id=existing_booking_id,
            custody_status=CustodyStatus.IN_CUSTODY.value,
            facility=_FACILITY,
            valid_from=valid_from)
        existing_person.bookings = [existing_booking]

        existing_charge = Charge(
            charge_id=existing_charge_id,
            status=ChargeStatus.PENDING.value,
            name=charge_name_1)
        existing_charge_snapshot = ChargeHistory(
            charge_history_id=1000,
            charge_id=existing_charge_id,
            booking_id=existing_booking_id,
            status=ChargeStatus.PENDING.value,
            name=charge_name_1,
            valid_from=valid_from)
        existing_booking.charges = [existing_charge]

        arrange_session.add(existing_person)
        arrange_session.flush()
        # Snapshots must be added separately, as they are not included in ORM
        # relationships
        arrange_session.add(existing_person_snapshot)
        arrange_session.add(existing_booking_snapshot)
        arrange_session.add(existing_charge_snapshot)
        arrange_session.commit()
        arrange_session.close()

        act_session = Session()

        # Ingested record tree has updates to person and charge but not booking
        ingested_person = entities.Person.new_with_defaults(
            person_id=existing_person_id,
            region=_REGION,
            race=Race.EXTERNAL_UNKNOWN)

        # Ingested booking has new last_seen_time but this is ignored, as it
        # is not included on the historical table.
        ingested_booking = entities.Booking.new_with_defaults(
            booking_id=existing_booking_id,
            custody_status=CustodyStatus.IN_CUSTODY,
            facility=_FACILITY,
            last_seen_time=updated_last_seen_time)
        ingested_person.bookings = [ingested_booking]

        ingested_charge = entities.Charge.new_with_defaults(
            charge_id=existing_charge_id,
            status=ChargeStatus.PENDING,
            name=charge_name_2)
        ingested_booking.charges = [ingested_charge]

        database.write_person(
            act_session, ingested_person, IngestMetadata(
                "default_region", _LAST_SEEN_TIME, {}))
        act_session.commit()
        act_session.close()

        assert_session = Session()

        person_snapshots = assert_session.query(PersonHistory) \
            .filter(PersonHistory.person_id == existing_person_id) \
            .order_by(PersonHistory.valid_from.asc()) \
            .all()
        booking_snapshots = assert_session.query(BookingHistory).filter(
            BookingHistory.booking_id == existing_booking_id).all()
        charge_snapshots = assert_session.query(ChargeHistory) \
            .filter(ChargeHistory.charge_id == existing_charge_id) \
            .order_by(ChargeHistory.valid_from.asc()) \
            .all()

        self.assertEqual(len(person_snapshots), 2)
        self.assertEqual(len(booking_snapshots), 1)
        self.assertEqual(len(charge_snapshots), 2)

        old_person_snapshot = person_snapshots[0]
        new_person_snapshot = person_snapshots[1]
        booking_snapshot = booking_snapshots[0]
        old_charge_snapshot = charge_snapshots[0]
        new_charge_snapshot = charge_snapshots[1]
        snapshot_time = old_person_snapshot.valid_to

        self.assertEqual(old_person_snapshot.race, Race.OTHER.value)
        self.assertEqual(
            new_person_snapshot.race, Race.EXTERNAL_UNKNOWN.value)

        self.assertEqual(old_charge_snapshot.name, charge_name_1)
        self.assertEqual(new_charge_snapshot.name, charge_name_2)

        self.assertEqual(old_person_snapshot.valid_from, valid_from)
        self.assertEqual(new_person_snapshot.valid_from, snapshot_time)
        self.assertEqual(booking_snapshot.valid_from, valid_from)
        self.assertEqual(old_charge_snapshot.valid_from, valid_from)
        self.assertEqual(new_charge_snapshot.valid_from, snapshot_time)

        self.assertEqual(old_person_snapshot.valid_to, snapshot_time)
        self.assertIsNone(new_person_snapshot.valid_to)
        self.assertIsNone(booking_snapshot.valid_to)
        self.assertEqual(old_charge_snapshot.valid_to, snapshot_time)
        self.assertIsNone(new_charge_snapshot.valid_to)

        assert_session.close()

    def testWritePerson_someExistingSnapshots_createsAndExtends(self):
        charge_name = 'charge_name'
        updated_last_seen_time = datetime.datetime(year=2020, month=7, day=6)
        # Pick a date in the past so the assigned snapshot date will always be
        # later
        valid_from = datetime.datetime(year=2015, month=1, day=1)
        existing_person_id = 1
        existing_booking_id = 14

        arrange_session = Session()

        # Person and booking already exist, while charge is new.
        existing_person = Person(
            person_id=existing_person_id,
            region=_REGION,
            race=Race.OTHER.value)
        existing_person_snapshot = PersonHistory(
            person_history_id=1000,
            person_id=existing_person_id,
            region=_REGION,
            race=Race.OTHER.value,
            valid_from=valid_from)

        existing_booking = Booking(
            booking_id=existing_booking_id,
            custody_status=CustodyStatus.IN_CUSTODY.value,
            facility=_FACILITY,
            last_seen_time=_LAST_SEEN_TIME)
        existing_booking_snapshot = BookingHistory(
            booking_history_id=1000,
            person_id=existing_person_id,
            booking_id=existing_booking_id,
            custody_status=CustodyStatus.IN_CUSTODY.value,
            facility=_FACILITY,
            valid_from=valid_from)
        existing_person.bookings = [existing_booking]

        arrange_session.add(existing_person)
        arrange_session.flush()
        # Snapshots must be added separately, as they are not included in ORM
        # relationships
        arrange_session.add(existing_person_snapshot)
        arrange_session.add(existing_booking_snapshot)
        arrange_session.commit()
        arrange_session.close()

        act_session = Session()

        # Ingested record tree has update to person, no updates to booking, and
        # a new charge
        ingested_person = entities.Person.new_with_defaults(
            person_id=existing_person_id,
            region=_REGION,
            race=Race.EXTERNAL_UNKNOWN)

        # Ingested booking has new last_seen_time but this is ignored, as it
        # is not included on the historical table.
        ingested_booking = entities.Booking.new_with_defaults(
            booking_id=existing_booking_id,
            custody_status=CustodyStatus.IN_CUSTODY,
            facility=_FACILITY,
            last_seen_time=updated_last_seen_time)
        ingested_person.bookings = [ingested_booking]

        ingested_charge = entities.Charge.new_with_defaults(
            status=ChargeStatus.PENDING,
            name=charge_name)
        ingested_booking.charges = [ingested_charge]

        persisted_person = database.write_person(
            act_session, ingested_person, IngestMetadata(
                "default_region", _LAST_SEEN_TIME, {}))
        act_session.commit()

        charge_id = persisted_person.bookings[0].charges[0].charge_id

        act_session.close()

        assert_session = Session()

        person_snapshots = assert_session.query(PersonHistory) \
            .filter(PersonHistory.person_id == existing_person_id) \
            .order_by(PersonHistory.valid_from.asc()) \
            .all()
        booking_snapshots = assert_session.query(BookingHistory).filter(
            BookingHistory.booking_id == existing_booking_id).all()
        charge_snapshots = assert_session.query(ChargeHistory).filter(
            ChargeHistory.charge_id == charge_id).all()

        self.assertEqual(len(person_snapshots), 2)
        self.assertEqual(len(booking_snapshots), 1)
        self.assertEqual(len(charge_snapshots), 1)

        old_person_snapshot = person_snapshots[0]
        new_person_snapshot = person_snapshots[1]
        booking_snapshot = booking_snapshots[0]
        charge_snapshot = charge_snapshots[0]
        snapshot_time = old_person_snapshot.valid_to

        self.assertEqual(old_person_snapshot.race, Race.OTHER.value)
        self.assertEqual(
            new_person_snapshot.race, Race.EXTERNAL_UNKNOWN.value)

        self.assertEqual(charge_snapshot.name, charge_name)

        self.assertEqual(old_person_snapshot.valid_from, valid_from)
        self.assertEqual(new_person_snapshot.valid_from, snapshot_time)
        self.assertEqual(booking_snapshot.valid_from, valid_from)
        self.assertEqual(charge_snapshot.valid_from, snapshot_time)

        self.assertEqual(old_person_snapshot.valid_to, snapshot_time)
        self.assertIsNone(new_person_snapshot.valid_to)
        self.assertIsNone(booking_snapshot.valid_to)
        self.assertIsNone(charge_snapshot.valid_to)

        assert_session.close()

    def testWritePeople_duplicatePeople_raisesError(self):
        shared_id = 48
        session = Session()

        person_1 = entities.Person.new_with_defaults(
            region=_REGION, race=Race.OTHER, person_id=shared_id)
        person_2 = entities.Person.new_with_defaults(
            region=_REGION, race=Race.EXTERNAL_UNKNOWN, person_id=shared_id)

        self.assertRaises(
            AssertionError,
            database.write_people,
            session,
            [person_1, person_2],
            IngestMetadata("default_region", _LAST_SEEN_TIME, {}))

        session.close()

    def test_removeBondFromCharge_shouldNotOrphanOldBond(self):
        arrange_session = Session()

        person = entities.Person.new_with_defaults(
            region=_REGION, race=Race.OTHER)
        booking = entities.Booking.new_with_defaults(
            custody_status=CustodyStatus.IN_CUSTODY,
            last_seen_time=_LAST_SEEN_TIME)
        person.bookings = [booking]
        charge = entities.Charge.new_with_defaults(
            status=ChargeStatus.PENDING)
        booking.charges = [charge]
        bond = entities.Bond.new_with_defaults(
            status=BondStatus.UNKNOWN_FOUND_IN_SOURCE)
        charge.bond = bond

        persisted_person = database.write_person(
            arrange_session, person, IngestMetadata(
                "default_region",
                datetime.datetime(year=2020, month=7, day=6),
                {}))
        arrange_session.commit()
        persisted_person_id = persisted_person.person_id
        persisted_booking_id = persisted_person.bookings[0].booking_id
        arrange_session.close()

        act_session = Session()
        person_query = act_session.query(Person) \
            .filter(Person.person_id == persisted_person_id)
        fetched_person = database_utils.convert(person_query.first())
        # Remove bond from charge so bond is no longer directly associated
        # with ORM copy of the record tree
        fetched_charge = fetched_person.bookings[0].charges[0]
        fetched_charge.bond = None
        database.write_person(
            act_session, fetched_person, IngestMetadata(
                "default_region",
                datetime.datetime(year=2020, month=7, day=7),
                {}))
        act_session.commit()
        act_session.close()

        assert_session = Session()

        # Bond should still be associated with booking, even though it is no
        # longer associated with the charge
        bonds = assert_session.query(Bond) \
            .filter(Bond.booking_id == persisted_booking_id) \
            .all()
        self.assertEqual(len(bonds), 1)

        assert_session.commit()
        assert_session.close()

    def test_removeSentenceFromCharge_shouldNotOrphanOldSentence(self):
        arrange_session = Session()

        person = entities.Person.new_with_defaults(
            region=_REGION, race=Race.OTHER)
        booking = entities.Booking.new_with_defaults(
            custody_status=CustodyStatus.IN_CUSTODY,
            last_seen_time=_LAST_SEEN_TIME)
        person.bookings = [booking]
        charge = entities.Charge.new_with_defaults(
            status=ChargeStatus.PENDING)
        booking.charges = [charge]
        sentence = entities.Sentence.new_with_defaults(
            status=SentenceStatus.UNKNOWN_FOUND_IN_SOURCE)
        charge.sentence = sentence

        persisted_person = database.write_person(
            arrange_session, person, IngestMetadata(
                "default_region",
                datetime.datetime(year=2020, month=7, day=6),
                {}))
        arrange_session.commit()
        persisted_person_id = persisted_person.person_id
        persisted_booking_id = persisted_person.bookings[0].booking_id
        arrange_session.close()

        act_session = Session()
        person_query = act_session.query(Person) \
            .filter(Person.person_id == persisted_person_id)
        fetched_person = database_utils.convert(person_query.first())
        # Remove sentence from charge so sentence is no longer directly
        # associated with ORM copy of the record tree
        fetched_charge = fetched_person.bookings[0].charges[0]
        fetched_charge.sentence = None
        database.write_person(
            act_session, fetched_person, IngestMetadata(
                "default_region",
                datetime.datetime(year=2020, month=7, day=7),
                {}))
        act_session.commit()
        act_session.close()

        assert_session = Session()

        # Sentence should still be associated with booking, even though it is no
        # longer associated with the charge
        sentences = assert_session.query(Sentence) \
            .filter(Sentence.booking_id == persisted_booking_id) \
            .all()
        self.assertEqual(len(sentences), 1)

        assert_session.commit()
        assert_session.close()

    def test_addBondToExistingBooking_shouldSetBookingIdOnBond(self):
        arrange_session = Session()

        person = entities.Person.new_with_defaults(
            region=_REGION, race=Race.OTHER)
        booking = entities.Booking.new_with_defaults(
            custody_status=CustodyStatus.IN_CUSTODY,
            last_seen_time=_LAST_SEEN_TIME)
        person.bookings = [booking]
        charge = entities.Charge.new_with_defaults(
            status=ChargeStatus.PENDING)
        booking.charges = [charge]

        persisted_person = database.write_person(
            arrange_session, person, IngestMetadata(
                "default_region",
                datetime.datetime(year=2020, month=7, day=6),
                {}))
        arrange_session.commit()
        persisted_person_id = persisted_person.person_id
        persisted_booking_id = persisted_person.bookings[0].booking_id
        arrange_session.close()

        act_session = Session()
        person_query = act_session.query(Person) \
            .filter(Person.person_id == persisted_person_id)
        fetched_person = database_utils.convert(person_query.first())
        new_bond = entities.Bond.new_with_defaults(
            status=BondStatus.UNKNOWN_FOUND_IN_SOURCE)
        fetched_person.bookings[0].charges[0].bond = new_bond
        database.write_person(
            act_session, fetched_person, IngestMetadata(
                "default_region",
                datetime.datetime(year=2020, month=7, day=7),
                {}))
        act_session.commit()
        act_session.close()

        assert_session = Session()

        assert_person_query = assert_session.query(Person) \
            .filter(Person.person_id == persisted_person_id)
        final_fetched_person = database_utils.convert(
            assert_person_query.first())
        self.assertEqual(
            final_fetched_person.bookings[0].charges[0].bond.booking_id,
            persisted_booking_id)

        assert_session.commit()
        assert_session.close()


    def testWriteDf(self):
        # Arrange
        subject = pd.DataFrame({
            'county_name': ['Alachua', 'Baker', 'Bay', 'Bradford', 'Brevard'],
            'county_population': [257062, 26965, 176016, 27440, 568919],
            'average_daily_population': [799, 478, 1015, 141, 1547],
            'date_reported': [pd.NaT, pd.NaT,
                              datetime.datetime(year=2017, month=9, day=1),
                              pd.NaT, pd.NaT],
            'fips': ['0', '1', '2', '3', '4'],
            'report_date': 5 * [DATE_SCRAPED],
            'report_granularity': 5 * [enum_strings.monthly_granularity]
        })

        # Act
        database.write_df(FlCountyAggregate, subject)

        # Assert
        query = Session() \
            .query(FlCountyAggregate) \
            .filter(FlCountyAggregate.county_name == 'Bay')
        result = one(query.all())

        self.assertEqual(result.county_name, 'Bay')
        self.assertEqual(result.county_population, 176016)
        self.assertEqual(result.average_daily_population, 1015)
        self.assertEqual(result.date_reported,
                         datetime.date(year=2017, month=9, day=1))
        self.assertEqual(result.fips, '2')
        self.assertEqual(result.report_date, DATE_SCRAPED)
        self.assertEqual(result.report_granularity,
                         enum_strings.monthly_granularity)

    def testWriteDf_doesNotOverrideMatchingColumnNames(self):
        # Arrange
        subject = pd.DataFrame({
            'county_name': ['Alachua', 'Baker', 'Bay', 'Bradford', 'Brevard'],
            'county_population': [257062, 26965, 176016, 27440, 568919],
            'average_daily_population': [799, 478, 1015, 141, 1547],
            'date_reported': [pd.NaT, pd.NaT,
                              datetime.datetime(year=2017, month=9, day=1),
                              pd.NaT, pd.NaT],
            'fips': ['0', '1', '2', '3', '4'],
            'report_date': 5 * [DATE_SCRAPED],
            'report_granularity': 5 * [enum_strings.monthly_granularity]
        })
        database.write_df(FlCountyAggregate, subject)

        subject = pd.DataFrame({
            'facility_name': ['One', 'Two', 'Three', 'Four', 'Five'],
            'average_daily_population': [13, 14, 15, 16, 17],
            'number_felony_pretrial': [23, 24, 25, 26, 27],
            'number_misdemeanor_pretrial': 5 * [pd.NaT],
            'fips': ['000', '111', '222', '333', '444'],
            'report_date': 5 * [DATE_SCRAPED],
            'report_granularity': 5 * [enum_strings.monthly_granularity]
        })

        # Act
        database.write_df(FlFacilityAggregate, subject)

        # Assert
        query = Session() \
            .query(FlCountyAggregate) \
            .filter(FlCountyAggregate.county_name == 'Bay')
        result = one(query.all())

        fips_not_overridden_by_facility_table = '2'
        self.assertEqual(result.county_name, 'Bay')
        self.assertEqual(result.fips, fips_not_overridden_by_facility_table)

    def testWriteDf_rowsWithSameColumnsThatMustBeUnique_onlyWritesOnce(self):
        # Arrange
        subject = pd.DataFrame({
            'county_name': ['Alachua', 'Baker'],
            'county_population': [257062, 26965],
            'average_daily_population': [799, 478],
            'date_reported': [pd.NaT, pd.NaT],
            'fips': 2 * ['SAME_FIPS'],
            'report_date': 2 * [DATE_SCRAPED],
            'report_granularity': 2 * [enum_strings.monthly_granularity]
        })

        # Act
        database.write_df(FlCountyAggregate, subject)

        # Assert
        query = Session().query(FlCountyAggregate)
        self.assertEqual(len(query.all()), 1)

    def testWriteDf_OverlappingData_WritesNewAndIgnoresDuplicateRows(self):
        # Arrange
        initial_df = pd.DataFrame({
            'county_name': ['Alachua', 'Baker', 'Bay', 'Bradford', 'Brevard'],
            'county_population': [257062, 26965, 176016, 27440, 568919],
            'average_daily_population': [799, 478, 1015, 141, 1547],
            'date_reported': [pd.NaT, pd.NaT,
                              datetime.datetime(year=2017, month=9, day=1),
                              pd.NaT, pd.NaT],
            'fips': ['0', '1', '2', '3', '4'],
            'report_date': 5 * [DATE_SCRAPED],
            'report_granularity': 5 * [enum_strings.monthly_granularity]
        })
        database.write_df(FlCountyAggregate, initial_df)

        subject = pd.DataFrame({
            'county_name': ['Alachua', 'NewCounty', 'Baker'],
            'county_population': [0, 1000000000, 0],
            'average_daily_population': [0, 50, 0],
            'date_reported': [pd.NaT, pd.NaT, pd.NaT],
            'fips': ['0', '1000', '2'],
            'report_date': 3 * [DATE_SCRAPED],
            'report_granularity': 3 * [enum_strings.monthly_granularity]
        })

        # Act
        database.write_df(FlCountyAggregate, subject)

        # Assert
        query = Session().query(func.sum(FlCountyAggregate.county_population))
        result = one(one(query.all()))

        # This sum includes intial_df + NewCounty and ignores other changes in
        # the subject (eg. county_population = 0 for 'Alachua')
        expected_sum_county_populations = 1001056402
        self.assertEqual(result, expected_sum_county_populations)
