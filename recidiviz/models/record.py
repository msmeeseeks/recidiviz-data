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

"""Records corresponding to individual sentences in prison."""


from google.appengine.ext import ndb
from google.appengine.ext.ndb import polymodel


class Offense(ndb.Model):
    """Model to describe a particular crime of conviction

    Datastore model for a specific crime that led to an incarceration event.
    Note that incarceration events often result from multiple crimes (e.g.
    larceny AND evading arrest, etc.) so there may be multiple Offense entities
    in a single Record.

    One or more Offense entities are stored in the 'offense' property of a
    Record.

    Attributes:
        crime_description: (string) Scraped from prison site describing the
            crime
        crime_class: (string) Scraped from prison site describing class of crime
        case_number: (string) Case number of this offense.
        bond_amount: (float) Dollar amount of bond on this charge, if any.
    """
    crime_description = ndb.StringProperty()
    crime_class = ndb.StringProperty()
    case_number = ndb.StringPropert()
    bond_amount = ndb.FloatProperty()


class SentenceDuration(ndb.Model):
    """Duration of time, used to represent possible sentence durations (min/max)

    Describes a duration of time for a sentence (could be minimum duration,
    could be maximum - this is used for both fields in Record entities).

    A SentenceDuration entity is stored as one of the sentence duration
    properties of a Record entity.

    Attributes:
        life_sentence: (bool) Whether or not this is a life sentence
        years: (int) Number of years in addition to the above for sentence
        months: (int) Number of months in addition to the above for sentence
        days: (int) Number of days in addition to the above for sentence
    """
    life_sentence = ndb.BooleanProperty()
    years = ndb.IntegerProperty()
    months = ndb.IntegerProperty()
    days = ndb.IntegerProperty()


class Record(polymodel.PolyModel):
    """Top-level PolyModel class to describe the record of a criminal event

    Datastore model for a record of a particular criminal event. This is
    intended to be a 1:1 mapping to human beings in the prison system.

    Record entities are never duplicated - if a change is discovered during re-
    scraping (e.g., the parole date has been pushed back), the Record entity is
    updated to reflect the new details and a Snapshot entity is created to note
    the changed fields / new values.

    Because prison systems frequently collect new (and sometimes conflicting)
    information each time a person is entered into custody, potentially
    mutable Person field names are duplicated here (to ensure we capture all
    variants of e.g. a Person's birth date).

    Individual region scrapers are expected to create their own subclasses which
    inherit these common properties, then add more if their region has unique
    information. See us_ny_scraper.py for an example.

    Attributes:
        created_on: (datetime) Creation date of this record. If data is
            migrated in the future, effort will be made to preserve this field
        updated_on: (date) Date of last change / update to this record
        status: (string) Status of the person at this time (sentenced,
            detained, released, etc.)
        offense: (record.Offense) State-provided strings describing the crimes
            of conviction and (if available) class of crimes.
        record_id: (string) The identifier the state site uses for this crime
        record_id_is_fuzzy: (bool) Whether the ID is generated by us (True) or
            provided by the corrections system (False)
        min_sentence: (record.SentenceDuration) Minimum sentence to be served
        max_sentence: (record.SentenceDuration) Maximum sentence to be served
        custody_date: (date) Date the person's sentence started
        offense_date: (date) Date the offense was committed
        latest_facility: (string) The name of the most recent facility the
            person has been held in
        release_date: (date) Date of actual release.
        latest_release_date: (date) Most recent date of release
        latest_release_type: (string) Reason given for most recent release
        is_released: (bool) Whether the person has been released from this
            sentence
        community_supervision_agency: (string) The parole or probation office
            that does or will have jurisdiction over this person after release.
        parole_officer: (string) Name of the parole officer.
        case_worker: (string) Name of the case worker.
        given_names: (string) Any given names provided by the source
        surname: (string) The person's surname, as provided by the source
        birthdate: (date) Date of birth for the person as provided by the source
        sex: (string) Sex of the prisoner as provided by the prison system
        race: (string) Race of the prisoner as provided by prison system
        last_custody_date: (date) Most recent date person returned for this
            sentence (may not be the initial custody date - e.g., if parole
            was violated, may be readmitted for remainder of prison term)
        admission_type: (string) 'New commitment' is beginning to serve a term,
            other reasons are usually after term has started (e.g. parole issue)
        committed_by: (string) The entity that committed the person.
        county_of_commit: (string) County the person was convicted/committed in
        custody_status: (string) Scraped string on custody status (more granular
            than just 'released' / 'not-released')
        earliest_release_date: (date) Earliest date to be released based on
            min_sentence. In certain circumstances, may be released before this.
        earliest_release_type: (string) The reason for the earliest possible
            release date.
        parole_hearing_date: (date) Date of next hearing before Parole Board
        parole_hearing_type: (string) Type of hearing for next PB appearance.
        parole_elig_date: (date) Date person will be eligible for parole
        cond_release_date: (date) Release date based on prison discretion for
            'good time off' based on behavior. Releases prisoner on parole, but
            bypasses PB review.
        max_expir_date: (date) Date of release if no PB or conditional release,
            maximum obligation to the state.
        max_expir_date_parole: (date) Last possible date of ongoing parole
            supervision. Doesn't apply to all people.
        max_expir_date_superv: (date) Last possible date of post-release
            supervision. Doesn't apply to all people.
        parole_discharge_date: (date) Final date of parole supervision, based on
            the parole board's decision to end supervision before max
            expiration.
        region: (string) The Recidiviz region code that this Record belongs to
    """
    admission_type = ndb.StringProperty()
    birthdate = ndb.DateProperty()
    case_worker = ndb.StringProperty()
    community_supervision_agency = ndb.StringProperty()
    cond_release_date = ndb.DateProperty()
    county_of_commit = ndb.StringProperty()
    committed_by = ndb.StringProperty()
    custody_date = ndb.DateProperty()
    custody_status = ndb.StringProperty()
    earliest_release_date = ndb.DateProperty()
    earliest_release_type = ndb.StringProperty()
    is_released = ndb.BooleanProperty()
    last_custody_date = ndb.DateProperty()
    latest_facility = ndb.StringProperty()
    latest_release_date = ndb.DateProperty()
    latest_release_type = ndb.StringProperty()
    max_expir_date = ndb.DateProperty()
    max_expir_date_parole = ndb.DateProperty()
    max_expir_date_superv = ndb.DateProperty()
    max_sentence_length = ndb.StructuredProperty(SentenceDuration,
                                                 repeated=False)
    min_sentence_length = ndb.StructuredProperty(SentenceDuration,
                                                 repeated=False)
    offense = ndb.StructuredProperty(Offense, repeated=True)
    offense_date = ndb.DateProperty()
    parole_discharge_date = ndb.DateProperty()
    parole_elig_date = ndb.DateProperty()
    parole_hearing_date = ndb.DateProperty()
    parole_hearing_type = ndb.StringProperty()
    parole_officer = ndb.StringProperty()
    race = ndb.StringProperty()
    record_id = ndb.StringProperty()
    record_id_is_fuzzy = ndb.BooleanProperty()
    region = ndb.StringProperty()
    release_date = ndb.DateProperty()
    sex = ndb.StringProperty()
    status = ndb.StringProperty()
    surname = ndb.StringProperty()
    given_names = ndb.StringProperty()

    created_on = ndb.DateTimeProperty(auto_now_add=True)
    updated_on = ndb.DateProperty(auto_now=True)
