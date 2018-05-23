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

"""Identifies instances of recidivism and non-recidivism for calculation.

This contains the core logic for identifying recidivism events on an
inmate-by-inmate basis, transform raw records for a given inmate into instances
of recidivism or non-recidivism as appropriate.

This class is paired with calculator.py, together providing the ability to
transform an inmate into an array of recidivism metrics.

Example:
    recidivism_events = identification.find_recidivism(inmate)
    metric_combinations = calculator.map_recidivism_combinations(
        inmate, recidivism_events)
"""


import logging
from collections import defaultdict
from dateutil.relativedelta import relativedelta
from models.snapshot import Snapshot
from models.record import Record
# These are required to permit access to PolyModel attributes on UsNy models
from scraper.us_ny.us_ny_record import UsNyRecord #pylint: disable=unused-import
from scraper.us_ny.us_ny_snapshot import UsNySnapshot #pylint: disable=unused-import
from .recidivism_event import RecidivismEvent


def find_recidivism(inmate, include_conditional_violations=False):
    """Classifies all individual sentences for the inmate as either leading to
    recidivism or not.

    Transforms each sentence from which the inmate has been released into a
    mapping from a release cohort to the details of all event in that cohort.
    The release cohort is an integer for the year, e.g. 2006. The event details
    are RecidivismEvent objects, which represents events of both recidivism and
    non-recidivism. That is, each inmate sentence is transformed into one or
    more recidivism event unless it is the most recent sentence and they are
    still incarcerated.

    Example output for someone who went to prison in 2006, was released in 2008,
    went back in 2010, was released in 2012, and never returned:
    {
      2008:[RecidivismEvent(recidivated=True, original_entry_date="2006-04-05",
                            ...)],
      2012:[RecidivismEvent(recidivated=False, original_entry_date="2010-09-17",
                            ...)]
    }

    The reason the value in the returned dictionary is a list of
    RecidivismEvents instead of a single event is that, in rare corner cases,
    it is possible for an individual to have multiple events worth tracking in
    the same calendar year. For example, an individual could be released
    conditionally early in the year, have their release revoked and be
    reincarcerated, then be released again (conditionally or otherwise), and be
    reincarcerated again, all in the same year. This would include two events
    in one release cohort.

    Args:
        inmate: an inmate to determine recidivism for.
        include_conditional_violations: a boolean indicating whether or not
                to include violations of conditional release in recidivism
                calculations.

    Returns:
        A dictionary from release cohorts to recidivism events for the given
        inmate in that cohort. One event will be returned per release cohort
        in almost every case. Rarely, there could be multiple events per cohort.
    """
    records = Record.query(ancestor=inmate.key)\
        .order(Record.custody_date)\
        .fetch()

    snapshots = Snapshot.query(ancestor=inmate.key)\
        .order(-Snapshot.created_on)\
        .fetch()

    recidivism_events = defaultdict(list)

    for index, record in enumerate(records):
        if record.custody_date is None:
            # If there is no entry date on the record,
            # there is nothing we can process. Skip it. See Issue #49.
            continue

        if record.is_released and not record.latest_release_date:
            # If the record is marked as released but there is no release date,
            # there is nothing we can process. Skip it. See Issue #51.
            continue

        original_entry_date = first_entrance(record)
        release_date = final_release(record)
        release_cohort = release_date.year if release_date else None
        release_facility = last_facility(record, snapshots)

        if len(records) - index == 1:
            event = for_last_record(record, original_entry_date,
                                    release_date, release_facility)
            if event:
                recidivism_events[release_cohort].append(event)
        else:
            # If there is a record after this one and they have been released,
            # then they recidivated. Capture the details.
            if record.is_released:
                event = for_intermediate_record(record, records[index + 1],
                                                snapshots, original_entry_date,
                                                release_date, release_facility)
                recidivism_events[release_cohort].append(event)

                if include_conditional_violations:
                    conditional_events = for_conditional_release(
                        record, original_entry_date, original_entry_date,
                        release_date, release_facility)

                    merge_recidivism_events(recidivism_events,
                                            conditional_events)

    return recidivism_events


def first_entrance(record):
    """The date of first entrance into prison for the given record.

    The first entrance is when an inmate first when to prison for a new
    sentence. An inmate may be conditionally released and returned to prison on
    violation of conditions for the same sentence, yielding a subsequent
    entrance date.

    Args:
        record: a single record

    Returns:
        A Date for when the inmate first entered into prison for this record.
    """
    return record.custody_date


def subsequent_entrance(record):
    """The date of most recent entrance into prison for the given record.

    A subsequent entrance is when an inmate returns to prison for a given
    sentence, after having already been released for that same sentence, e.g.
    onto parole or a conditional release. It is not returning to prison for a
    brand new sentence.

    Args:
        record: a single record

    Returns:
        A Date for when the inmate re-entered prison for this record.
    """
    return record.last_custody_date


def final_release(record):
    """The date of final release from prison for the given record.

    An inmate can be released from prison multiple times for a given record if
    they are sent back in the interim for conditional violations. This
    represents the last time they were released for this particular record.

    Args:
        record: a single record

    Returns:
        A Date for when the inmate was released from prison for this record
        for the last time. None if the inmate is still in custody.
    """
    if not record.is_released:
        return None

    return record.latest_release_date


def first_facility(record, snapshots):
    """The facility that the inmate first occupied for the given record.

    Returns the facility that the inmate was first in for the given record.
    That is, the facility that they started that record in, whether or not they
    have since been released.

    This assumes the snapshots are provided in descending order by snapshot
    date, i.e. it picks the facility in the last snapshot in the collection that
    matches the given record. Thus, this also assumes that the given list of
    snapshots contains the full history for the given record. If that is not
    true, this simply returns the earliest facility we are aware of.

    Args:
        record: a single record
        snapshots: a list of all facility snapshots for the inmate.

    Returns:
        The facility that the inmate first occupied for the record. None if we
        have no apparent history available for the record.
    """
    return last_facility(record, reversed(snapshots))


def last_facility(record, snapshots):
    """The facility that the inmate last occupied for the given record.

    Returns the facility that the inmate was last in for the given record.
    That is, the facility that they are currently in if still incarcerated, or
    that they were released from on their final release for that record.

    This assumes the snapshots are provided in descending order by snapshot
    date, i.e. it picks the facility in the first snapshot in the collection
    that matches the given record. Thus, this also assumes that the given list
    of snapshots contains the full history for the given record. If that is not
    true, this simply returns the earliest facility we are aware of.

    Args:
        record: a single record
        snapshots: a list of all facility snapshots for the inmate.

    Returns:
        The facility that the inmate last occupied for the record. None if we
        have no apparent history available for the record.
    """
    return next((snapshot.latest_facility for snapshot in snapshots
                 if snapshot.key.parent().id() == record.key.id()), None)


def incarcerated_multiple_times(record):
    """Returns whether or not the individual was incarcerated multiple distinct
    times for this given record.

    A person is incarcerated multiple times for the same record if they are
    released to some form of supervision or with some conditions, and then
    violate the terms of the release and are reincarcerated.

    This works by checking if the original entry date for the record is equal to
    the last known entry date. If they are the same, they have only been
    incarcerated a single time for this record. If they are different, then
    there are at least two distinct incarcerations for the record.

    Args:
        record: a single record

    Returns:
        True if the person was incarcerated multiple times for the record.
        False otherwise.
    """
    return record.custody_date == record.last_custody_date


def for_last_record(record, original_entry_date,
                    release_date, release_facility,
                    was_conditional=False):
    """Returns any non-recidivism event relevant to the person's last record.

    If the person has been released from their last record, there is an instance
    of non-recidivism to count. If they are still incarcerated, there is nothing
    to count.

    Args:
        record: the last record for the person
        original_entry_date: when they entered for this record
        release_date: when they were last released for this record
        release_facility: the facility they were last released from for this
            record
        was_conditional: whether this release was conditional

    Returns:
        A non-recidivism event if released from this record. None otherwise.
    """
    # There is only something to capture
    # if they are out of prison from this last record
    if record.is_released:
        logging.debug('Inmate was released from last or only '
                      'record %s. No recidivism.', record.key.id())

        return RecidivismEvent.non_recidivism_event(original_entry_date,
                                                    release_date,
                                                    release_facility,
                                                    was_conditional)

    logging.debug('Inmate is still incarcerated for last or only '
                  'record %s. Nothing to track', record.key.id())
    return None


def for_intermediate_record(record, recidivism_record, snapshots,
                            original_entry_date, release_date,
                            release_facility):
    """Returns the recidivism event relevant to the person's intermediate
    (not last) record.

    There is definitely an instance of recidivism to count if this is not the
    person's last record and they have been released.

    Args:
        record: a record for some person
        recidivism_record: the next record after this, through which recidivism
            has occurred
        snapshots: all snapshots for this person
        original_entry_date: when they entered for this record
        release_date: when they were last released for this record
        release_facility: the facility they were last released from for this
            record

    Returns:
        A recidivism event.
    """
    logging.debug('Inmate was released from record %s and went '
                  'back again. Yes recidivism.', record.key.id())

    reincarceration_date = first_entrance(recidivism_record)
    reincarceration_facility = first_facility(recidivism_record,
                                              snapshots)

    return RecidivismEvent.recidivism_event(
        original_entry_date, release_date, release_facility,
        reincarceration_date, reincarceration_facility, False)


def for_conditional_release(record, snapshots, original_entry_date,
                            release_date, release_facility):
    """Returns recidivism events mapped to their release cohorts if the person
    was conditionally released as part of this record.

    This includes both events where the individual was conditionally released
    and sent back (regardless of whether they were subsequently released again),
    i.e. revocation, and instances where they were conditionally released and
    did not return for this record (regardless of whether they have returned for
    a new, separate incarceration), i.e. non-revocation.

    Args:
        record: a record for some person
        snapshots: all snapshots for this person
        original_entry_date: when they entered for this record
        release_date: when they were last released for this record
        release_facility: the facility they were last released from for this
            record

    Returns:
        A dictionary from release cohorts to conditional release-related
        recidivism events for the given inmate in that cohort. One event will be
        returned per release cohort in almost every case. Rarely, there could be
        multiple events per cohort.
    """
    if incarcerated_multiple_times(record):
        return conditional_recidivism_events(record, snapshots)
    else:
        if released_early(record):
            # They were released conditionally and never returned, no revocation
            event = for_last_record(record, original_entry_date, release_date,
                                    release_facility, True)
            return {release_date.year: [event]}

    return {}


def conditional_recidivism_events(record, snapshots):
    """Returns revocation recidivism events.

    Revocation events are when an individual is released per some conditions,
    e.g. to parole, and is reincarcerated due to a violation of those
    conditions, i.e. their parole is revoked.

    This attempts to capture those events, though there are deficiencies.
    Because this implementation relies on custody and release date changes
    captured in successive snapshots, we need enough historical (assuming the
    historical data does not commit data loss, as New York State's DOCCS does)
    and/or ongoing data to reach meaningful quantities.

    Additionally, in the case where there is no subsequent custody date after a
    final detected release date, i.e. apparent non-revocation, we check if the
    overall time spent incarcerated is less than the max sentence length to
    detect if this was an actual conditional release and not an unconditional
    release. This may not be accurate in all jurisdictions.

    Args:
        record: a record for some person
        snapshots: all snapshots for this person

    Returns:
        A dictionary from release cohorts to recidivism events for the given
        inmate in that cohort. One event will be returned per release cohort
        in almost every case. Rarely, there could be multiple events per cohort.
    """

    # TODO rework all conditional language around "revocation"?

    def has_relevant_change(record_snapshot):
        return record_snapshot.custody_date \
               or record_snapshot.latest_custody_date \
               or record_snapshot.latest_release_date

    conditional_events = defaultdict(list)
    prior_entry_date = record.custody_date
    most_recent_release_date = None
    re_entry_date = None
    prior_facility = None
    most_recent_facility = None

    for snapshot in snapshots:
        if not has_relevant_change(snapshot):
            continue

        if re_entry_date and not snapshot.latest_custody_date:
            # We previously determined a most recent release date and
            # it was not updated here, so let's capture it
            if most_recent_release_date and \
                    re_entry_date > most_recent_release_date:
                most_recent_release_cohort = most_recent_release_date.year

                conditional_events[most_recent_release_cohort].append(
                    RecidivismEvent.recidivism_event(
                        prior_entry_date, most_recent_release_date,
                        prior_facility, re_entry_date,
                        most_recent_facility, True))

                prior_facility = most_recent_facility
                prior_entry_date = re_entry_date
                re_entry_date = None
                most_recent_release_date = None
            else:
                # Do nothing yet. Still waiting on a release date update
                pass

        if snapshot.latest_custody_date and \
                snapshot.latest_custody_date != re_entry_date:
            # Snapshot introduces new custody date or updates previously
            # introduced custody date if it is a manual fix in source data
            re_entry_date = snapshot.latest_custody_date

        elif snapshot.custody_date and \
                snapshot.custody_date != prior_entry_date and \
                not most_recent_release_date and not conditional_events:
            # Update to the original custody date (unlikely but could be a
            # manual fix applied in source system)
            prior_entry_date = snapshot.custody_date

        if snapshot.latest_release_date:
            # Snapshot introduces new release date or updates previously
            # introduced release date if it is a manual fix in source data
            most_recent_release_date = snapshot.latest_release_date

        if snapshot.latest_facility:
            # Snapshot introduced a change to the facility
            if not prior_facility:
                prior_facility = snapshot.latest_facility
            most_recent_facility = snapshot.latest_facility

    # Check if anything to be finalized after last snapshot passed
    if most_recent_release_date:
        most_recent_release_cohort = most_recent_release_date.year

        if re_entry_date:
            conditional_events[most_recent_release_cohort].append(
                RecidivismEvent.recidivism_event(
                    prior_entry_date, most_recent_release_date,
                    prior_facility, re_entry_date,
                    most_recent_facility, True))
        elif released_early(record):
            conditional_events[most_recent_release_cohort].append(
                RecidivismEvent.non_recidivism_event(
                    prior_entry_date, most_recent_release_date,
                    most_recent_facility, True))

    return conditional_events


def released_early(record):
    """Returns whether or not the individual was released early from their
    sentence.

    Args:
        record: a record for some person

    Returns:
        True if they were released early. False if they were released having
        served the full sentence, if not yet released, or if we cannot clearly
        make a determination.
    """
    if not record.is_released:
        return False

    max_sentence = record.max_sentence_length

    if not max_sentence or (not max_sentence.years and
                            not max_sentence.months and
                            not max_sentence.days):
        return False

    return max_sentence.life_sentence \
        or record.custody_date + relativedelta(years=max_sentence.years,
                                               months=max_sentence.months,
                                               days=max_sentence.days) \
        > record.last_release_date


def merge_recidivism_events(first, second):
    for cohort, events in second:
        for event in events:
            first[cohort].append(event)
