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

# pylint: disable=protected-access

"""Tools for migrating data after changing Datastore entities.

These are for migrating data for Datastore entities after backwards incompatible
changes have been made.
"""


import logging
import webapp2
from google.appengine.ext import deferred
from google.appengine.ext import ndb
from utils.auth import authenticate_request
from ingest.models.snapshot import Snapshot
from ingest.us_ny.us_ny_record import UsNyRecord
from ingest.us_ny.us_ny_person import UsNyInmate, UsNyPerson
from models.snapshot import InmateFacilitySnapshot


DEFAULT_BATCH_SIZE = 15
REGION = "us_ny"


class DataMigration(webapp2.RequestHandler):
    """Request handler for requests related to data migration."""

    @authenticate_request
    def get(self):
        """Request handler to kick off migration work

        Request handler for data migration tasks. Migrates last_release_date
        and last_release_type from UsNyRecord model to Record model, and old
        InmateFacilitySnapshot entities to new Snapshot entities.

        Example queries:

            # First pass, 15 inmates and their child records/snapshots only,
                non-destructive
            http://localhost:8080/data_migration?test_only=true&migration_type=first_pass

            # Second pass, 15 records and their child snapshots only,
                non-destructive
            http://localhost:8080/data_migration?test_only=false&migration_type=second_pass

            # To migrate all inmates or records, and delete migrated entities, change
                test_only parameter to 'false'

        URL Params:
            migration_type: (string) 'Record' or 'Snapshot' migration
            test_only: (string) "true" or "false", whether to non-
                destructively migrate only 10 records

        Returns:
            N/A

        Response Codes:
            HTTP 200 if successful
            HTTP 400 if not
        """
        test_only = self.request.get('test_only', "true").lower()
        migration_type = self.request.get('migration_type', None).lower()

        test_only = True if test_only == "true" else False

        if migration_type == "first_pass":
            deferred.defer(migration_pass1, test_only=test_only)
        elif migration_type == "second_pass":
            deferred.defer(migration_pass2, test_only=test_only)
        else:
            logging.error("Migration type '%s' not recognized. Exiting." %
                          migration_type)
            self.response.write("Invalid parameters, see logs.")
            self.response.set_status(500)
            return

        self.response.write("Kicked off %s." % migration_type)
        logging.info("Kicked off migration." % migration_type)


def migration_pass1(cursor=None, num_updated=0, batch_size=DEFAULT_BATCH_SIZE,
                    test_only=True):
    """ Migrate a batch of UsNyInmates and their child records and snapshots

    Migrates several Inmate entities and their child entities (Records and
    InmateFacilitySnapshots) into temp datastore entities that resemble the new
    datastore schema, then deletes the old entities.

    Specific changes made during the transition to temp entities:
        - Migrates all Inmate fields and us_ny_person_id to a new UsNyPerson
            entity
        - Migrates UsNyRecord and Record fields to new TempRecord63 entities
            as child entities of that Person object
        - Creates child Snapshot objects under the new TempRecord entity, which
            includes info from both the old Record and old
            InmateFacilitySnapshot entities
        - Persists all of these new entities, and deletes the source
            UsNyInmate, UsNyRecord, and InmateFacilitySnapshot entities

    Args:
        cursor: (ndb cursor) Query cursor for where we are in the migration
        num_updated: (int) Current number of Inmates updated
        batch_size: (int) Number of Inmates to handle during this run
        test_only: (bool) If true, performs non-destructive migration only

    Returns:
        N/A
    """
    # Get (batch_size) UsNyInmate entities. If <(batch_size) returned, set
    # migration_complete to True.
    inmate_query = UsNyInmate.query()
    inmates, next_cursor, more = \
        inmate_query.fetch_page(batch_size, start_cursor=cursor)

    to_put = []
    to_del = []

    # For each UsNyInmate entity, pull inmate info and construct new UsNyPerson
    for inmate in inmates:
        new_person = convert_inmate(inmate)
        to_put.append(new_person)

        # Get all UsNyRecord entities which are children of this, and migrate
        # to TempRecords under the new UsNyPerson
        old_records = UsNyRecord.query(ancestor=inmate.key).fetch()
        for old_record in old_records:
            temp_record = convert_record_to_temp(new_person, old_record)
            to_put.append(temp_record)

            # Get all inmate_facility_snapshot entities which are children of
            # this, and migrate to temp Snapshots under the TempRecord above
            inmate_facility_snapshots = InmateFacilitySnapshot.query(
                ancestor=old_record.key).fetch()
            for old_snapshot in inmate_facility_snapshots:
                temp_snapshot = convert_snapshot_to_temp(temp_record,
                                                         old_record,
                                                         old_snapshot)
                to_put.append(temp_snapshot)
                to_del.append(old_snapshot)
            to_del.append(old_record)
        to_del.append(inmate)

    num_updated += len(to_del)

    transactional_put_multi(to_put, to_del, test_only)

    if more and not test_only:
        deferred.defer(migration_pass1,
                       cursor=next_cursor,
                       num_updated=num_updated,
                       test_only=test_only)
    else:
        logging.debug("""Migration pass #1 complete with %d inmate entities
                         updated. You must now take the following steps and
                         re-deploy the app prior to initiating pass 2:
                         - Delete the fields marked in
                           recividiz_data/ingest/us_ny/us_ny_record.py
                         - Update the current indices for UsNyRecord or Record
                           in index.yaml
                         - Uncomment new Record fields in
                           recividiz_data/ingest/models/record.py
                      """ %
                      num_updated)


def convert_inmate(inmate):
    """Creates UsNyPerson entity from UsNyInmate entity

    Args:
        inmate: The UsNyInmate entity to copy data from

    Returns:
        The new UsNyPerson entity build from the Inmate data
    """
    entity_id = REGION + inmate.inmate_id
    new_person = UsNyPerson(
        id=entity_id,
        person_id=inmate.inmate_id,
        person_id_is_fuzzy=inmate.inmate_id_is_fuzzy,
        given_names=inmate.given_names,
        surname=inmate.last_name,
        birthdate=inmate.birthday,
        age=inmate.age,
        region=inmate.region,
        sex=inmate.sex,
        race=inmate.race,
        created_on=inmate.created_on,
        updated_on=inmate.updated_on,
    )

    return new_person


def convert_record_to_temp(new_person, old_record):
    """Creates TempRecord entity from UsNyRecord entity

    Args:
        new_person: The new UsNyPerson entity to make a child for
        old_record: The old UsNyRecord entity to clone data from

    Returns:
        The new TempRecord63 entity
    """
    new_temp_record = TempRecord63(
        parent=new_person.key,
        offense=old_record.offense,
        record_id=old_record.record_id,
        record_id_is_fuzzy=False,
        min_sentence_length=old_record.min_sentence_length,
        max_sentence_length=old_record.max_sentence_length,
        custody_date=old_record.custody_date,
        offense_date=old_record.offense_date,
        latest_facility=old_record.latest_facility,
        latest_release_date=old_record.last_release_date,
        latest_release_type=old_record.last_release_type,
        is_released=old_record.is_released,
        given_names=old_record.given_names,
        surname=old_record.last_name,
        birthdate=old_record.birthday,
        sex=old_record.sex,
        race=old_record.race,
        last_custody_date=old_record.last_custody_date,
        admission_type=old_record.admission_type,
        county_of_commit=old_record.county_of_commit,
        custody_status=old_record.custody_status,
        earliest_release_date=old_record.earliest_release_date,
        earliest_release_type=old_record.earliest_release_type,
        parole_hearing_date=old_record.parole_hearing_date,
        parole_hearing_type=old_record.parole_hearing_type,
        parole_elig_date=old_record.parole_elig_date,
        cond_release_date=old_record.cond_release_date,
        max_expir_date=old_record.max_expir_date,
        max_expir_date_parole=old_record.max_expir_date_parole,
        max_expir_date_superv=old_record.max_expir_date_superv,
        parole_discharge_date=old_record.parole_discharge_date,
        region=REGION,
        created_on=old_record.created_on,
        updated_on=old_record.updated_on,
        us_ny_record_id=old_record.record_id
    )

    return new_temp_record


def convert_snapshot_to_temp(temp_record, old_record, old_snapshot):
    """Creates Snapshot entity from InmateFacilitySnapshot entity

    Args:
        temp_record: The TempRecord63 entity to create a child of
        old_record: The old UsNyRecord entity to copy data from
        old_snapshot: The old InmateFacilitySnapshot entity to copy data from

    Returns:
        The new Snapshot entity
    """
    new_snapshot = Snapshot(
        parent=temp_record.key,
        admission_type=old_record.admission_type,
        birthdate=old_record.birthday,
        cond_release_date=old_record.cond_release_date,
        county_of_commit=old_record.county_of_commit,
        custody_date=old_record.custody_date,
        custody_status=old_record.custody_status,
        latest_facility=old_snapshot.facility,
        earliest_release_date=old_record.earliest_release_date,
        earliest_release_type=old_record.earliest_release_type,
        given_names=old_record.given_names,
        is_released=old_record.is_released,
        latest_release_date=old_record.last_release_date,
        latest_release_type=old_record.last_release_type,
        last_custody_date=old_record.last_custody_date,
        surname=old_record.last_name,
        max_expir_date=old_record.max_expir_date,
        max_expir_date_superv=old_record.max_expir_date_superv,
        max_expir_date_parole=old_record.max_expir_date_parole,
        max_sentence_length=old_record.max_sentence_length,
        min_sentence_length=old_record.min_sentence_length,
        offense=old_record.offense,
        offense_date=old_record.offense_date,
        parole_discharge_date=old_record.parole_discharge_date,
        parole_elig_date=old_record.parole_elig_date,
        parole_hearing_date=old_record.parole_hearing_date,
        parole_hearing_type=old_record.parole_hearing_type,
        race=old_record.race,
        region=REGION,
        sex=old_record.sex,
        created_on=old_snapshot.snapshot_date
    )

    return new_snapshot


def migration_pass2(cursor=None, num_updated=0, batch_size=DEFAULT_BATCH_SIZE,
                      test_only=True):
    """ Migrate a batch of TempRecords back into new-schema Record entities

    Migrates several TempRecord entities and their child Snapshots into Record
    and child snapshot entities that are now in the new datastore schema, then
    deletes the old temporary entities.

    Specific changes made during the transition to temp entities:
        - Migrates all TempRecord entities into new UsNyRecord entities which
            are children of the same Person as the TempRecord was
        - Creates new Snapshots for each Snapshot under the migrated
            TempRecord, which are children of the new UsNyRecord instead of the
            TempRecord.
        - Deletes the temporary Snapshots (children of TempRecords), TempRecords

    Args:
        cursor: (ndb cursor) Query cursor for where we are in the migration
        num_updated: (int) Current number of records updated
        batch_size: (int) Number of TempRecord entities to handle during run
        test_only: (bool) If true, performs non-destructive migration only

    Returns:
        N/A
    """
    # Get (batch_size) UsNyInmate entities. If <(batch_size) returned, set
    # migration_complete to True.
    temp_record_query = TempRecord63.query()
    temp_records, next_cursor, more = \
        temp_record_query.fetch_page(batch_size, start_cursor=cursor)

    to_put = []
    to_del = []

    # For each TempRecord entity, pull record info and construct new UsNyRecord
    for temp_record in temp_records:
        new_record = convert_temp_to_record(temp_record)
        to_put.append(new_record)

        # For each Snapshot entity which is a child of this TempRecord, migrate
        # it to a new Snapshot with the new / real UsNyRecord as parent
        temp_snapshots = Snapshot.query(ancestor=temp_record.key).fetch()
        for temp_snapshot in temp_snapshots:
            new_snapshot = convert_temp_to_snapshot(new_record, temp_snapshot)
            to_put.append(new_snapshot)
            to_del.append(temp_snapshot)

        to_del.append(temp_record)

    num_updated += len(to_del)

    transactional_put_multi(to_put, to_del, test_only)

    if more and not test_only:
        deferred.defer(migration_pass2,
                       cursor=next_cursor,
                       num_updated=num_updated,
                       test_only=test_only)
    else:
        logging.debug('Migration pass #2 complete with %d record entities '
                      'updated.' % num_updated)


def convert_temp_to_record(temp_record):
    """Creates Record entity from TempRecord63 entity

    Args:
        temp_record: The TempRecord63 entity currently holding data for this
            UsNyRecord

    Returns:
        The new Record entity
    """
    entity_id = REGION + temp_record.record_id
    new_record = UsNyRecord(
        id=entity_id,
        parent=temp_record.key.parent(),
        offense=temp_record.offense,
        record_id=temp_record.record_id,
        record_id_is_fuzzy=temp_record.record_id_is_fuzzy,
        min_sentence_length=temp_record.min_sentence_length,
        max_sentence_length=temp_record.max_sentence_length,
        custody_date=temp_record.custody_date,
        offense_date=temp_record.offense_date,
        latest_facility=temp_record.latest_facility,
        latest_release_date=temp_record.latest_release_date,
        latest_release_type=temp_record.latest_release_type,
        is_released=temp_record.is_released,
        given_names=temp_record.given_names,
        surname=temp_record.surname,
        birthdate=temp_record.birthdate,
        sex=temp_record.sex,
        race=temp_record.race,
        last_custody_date=temp_record.last_custody_date,
        admission_type=temp_record.admission_type,
        county_of_commit=temp_record.county_of_commit,
        custody_status=temp_record.custody_status,
        earliest_release_date=temp_record.earliest_release_date,
        earliest_release_type=temp_record.earliest_release_type,
        parole_hearing_date=temp_record.parole_hearing_date,
        parole_hearing_type=temp_record.parole_hearing_type,
        parole_elig_date=temp_record.parole_elig_date,
        cond_release_date=temp_record.cond_release_date,
        max_expir_date=temp_record.max_expir_date,
        max_expir_date_parole=temp_record.max_expir_date_parole,
        max_expir_date_superv=temp_record.max_expir_date_superv,
        parole_discharge_date=temp_record.parole_discharge_date,
        region=temp_record.region,
        created_on=temp_record.created_on,
        updated_on=temp_record.updated_on,
        us_ny_record_id=temp_record.us_ny_record_id
    )

    return new_record


def convert_temp_to_snapshot(new_record, temp_snapshot):
    """Creates Snapshot entity from InmateFacilitySnapshot entity

    Args:
        new_record: The new UsNyRecord entity this should be a child of
        temp_snapshot: The current Snapshot entity holding this Snapshot's data

    Returns:
        The new Snapshot entity
    """
    new_snapshot = Snapshot(
        parent=new_record.key,
        admission_type=temp_snapshot.admission_type,
        birthdate=temp_snapshot.birthdate,
        cond_release_date=temp_snapshot.cond_release_date,
        county_of_commit=temp_snapshot.county_of_commit,
        custody_date=temp_snapshot.custody_date,
        custody_status=temp_snapshot.custody_status,
        latest_facility=temp_snapshot.latest_facility,
        earliest_release_date=temp_snapshot.earliest_release_date,
        earliest_release_type=temp_snapshot.earliest_release_type,
        given_names=temp_snapshot.given_names,
        is_released=temp_snapshot.is_released,
        latest_release_date=temp_snapshot.latest_release_date,
        latest_release_type=temp_snapshot.latest_release_type,
        last_custody_date=temp_snapshot.last_custody_date,
        surname=temp_snapshot.surname,
        max_expir_date=temp_snapshot.max_expir_date,
        max_expir_date_superv=temp_snapshot.max_expir_date_superv,
        max_expir_date_parole=temp_snapshot.max_expir_date_parole,
        max_sentence_length=temp_snapshot.max_sentence_length,
        min_sentence_length=temp_snapshot.min_sentence_length,
        offense=temp_snapshot.offense,
        offense_date=temp_snapshot.offense_date,
        parole_discharge_date=temp_snapshot.parole_discharge_date,
        parole_elig_date=temp_snapshot.parole_hearing_date,
        parole_hearing_date=temp_snapshot.parole_hearing_date,
        parole_hearing_type=temp_snapshot.parole_hearing_type,
        race=temp_snapshot.race,
        region=temp_snapshot.region,
        sex=temp_snapshot.sex,
        created_on=temp_snapshot.created_on
    )

    return new_snapshot


@ndb.transactional(xg=True)  # pylint: disable=no-value-for-parameter
def transactional_put_multi(to_put, to_del=None, test_only=True):
    """Transactionally persist updated entiies and delete those they replace

    Saves a set of entities, and deletes a set of entities, in the same
    transaction. Note that this can only be used for up to 25 entity groups
    at a time (GAE constraint).

    Args:
        to_put: List of entities to persist to datastore
        to_del: List of entities to delete from datastore, if applicable
        test_only: (bool) whether to delete the old entities

    Returns:
        True if success
    """
    if to_put:
        ndb.put_multi(to_put)

    if to_del and not test_only:
        ndb.delete_multi(to_del)

    return True


app = webapp2.WSGIApplication([
    ('/data_migration', DataMigration)
], debug=False)
