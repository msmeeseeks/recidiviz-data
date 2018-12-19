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

from recidiviz import Session
import recidiviz.common.constants.enum_canonical_strings as enum_strings
from recidiviz.persistence.database import database, database_utils
from recidiviz.persistence.database.schema import Booking, Person
from recidiviz.tests.utils import fakes


class TestDatabase(object):
    """Test that the methods in database.py correctly read from the SQL
    database """

    def setup_method(self, _test_method):
        fakes.use_in_memory_sqlite_database()

    def test_readOpenBookingsBeforeDate(self):
        # Arrange
        person = Person(person_id=8, region='region')
        person_wrong_region = Person(person_id=9, region='wrong_region')

        release_date = datetime.date(2018, 7, 20)
        most_recent_scrape_date = datetime.datetime(2018, 6, 20)
        date_in_past = most_recent_scrape_date - datetime.timedelta(days=1)

        # TODO(176): Replace enum_strings with schema enum values once we've
        # migrated to Python 3.

        # Bookings that should be returned
        open_booking_before_last_scrape = Booking(
            person_id=person.person_id,
            custody_status=enum_strings.custody_status_in_custody,
            last_seen_time=date_in_past)

        # Bookings that should not be returned
        open_booking_incorrect_region = Booking(
            person_id=person_wrong_region.person_id,
            custody_status=enum_strings.custody_status_in_custody,
            last_seen_time=date_in_past)
        open_booking_most_recent_scrape = Booking(
            person_id=person.person_id,
            custody_status=enum_strings.custody_status_in_custody,
            last_seen_time=most_recent_scrape_date)
        resolved_booking = Booking(
            person_id=person.person_id,
            custody_status=enum_strings.custody_status_in_custody,
            release_date=release_date,
            last_seen_time=date_in_past)

        session = Session()
        session.add(person)
        session.add(person_wrong_region)
        session.add(open_booking_before_last_scrape)
        session.add(open_booking_incorrect_region)
        session.add(open_booking_most_recent_scrape)
        session.add(resolved_booking)
        session.commit()

        # Act
        bookings = database.read_open_bookings_scraped_before_time(
            session, person.region, most_recent_scrape_date)

        # Assert
        assert bookings == [
            database_utils.convert_booking(open_booking_before_last_scrape)]
