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

"""Tests for the New York scraper: ingest/us_ny/us_ny_scraper.py."""

from datetime import date
from datetime import datetime

from lxml import html
from mock import patch

from google.appengine.api import memcache
from google.appengine.ext import ndb
from google.appengine.ext.db import InternalError
from google.appengine.ext import testbed

from recidiviz.ingest.models.scrape_key import ScrapeKey
from recidiviz.ingest.sessions import ScrapeSession
from recidiviz.ingest.us_ny.us_ny_person import UsNyPerson
from recidiviz.ingest.us_ny.us_ny_record import UsNyRecord
from recidiviz.ingest.us_ny.us_ny_scraper import UsNyScraper
from recidiviz.models.record import Offense, SentenceDuration
from recidiviz.models.snapshot import Snapshot


PERSON_PAGE = """
<div id="ii">
    <h2 class="aligncenter">Inmate Information</h2>
    <p class="err"></p>
    <table cellpadding="2" cellspacing="0" summary="Inmate Identifying and
     location information">
      <caption>Identifying and Location Information<br>
       <span class="pcap">As of 10/11/18</span></caption>
      <tbody><tr>
        <td scope="row" id="t1a">
         <a href="http://www.doccs.ny.gov/univinq/fpmsdoc.htm#din" 
            title="Definition of DIN (Department Identification Number)">
         DIN (Department Identification Number)</a></td>
        <td headers="t1a">1234567 &nbsp;</td>
      </tr>
      <tr>
        <td scope="row" id="t1b">Inmate Name</td>
        <td headers="t1b">SIMPSON, BART                            &nbsp;</td>
      </tr>
      <tr>
        <td scope="row" id="t1c">Sex</td>
        <td headers="t1c">MALE   &nbsp;</td>
      </tr>
      <tr>
        <td scope="row" id="t1d">Date of Birth</td>
        <td headers="t1d">04/22/1972 &nbsp;</td>
      </tr>
      <tr>
        <td scope="row" id="t1e">
         <a href="http://www.doccs.ny.gov/univinq/fpmsdoc.htm#raceth" 
            title="Definition of Race / Ethnicity">
         Race / Ethnicity</a></td>
        <td headers="t1e">WHITE                  &nbsp;</td>
      </tr>
      <tr>
        <td scope="row" id="t1f">
         <a href="http://www.doccs.ny.gov/univinq/fpmsdoc.htm#custstat" 
            title="Definition of  Custody Status">
         Custody Status</a></td>
        <td headers="t1f">RELEASED       &nbsp;</td>
      </tr>
      <tr>
        <td scope="row" id="t1g">
         <a href="http://www.doccs.ny.gov/univinq/fpmsdoc.htm#fac" 
            title="Definition of Housing / Releasing Facility">
         Housing / Releasing Facility</a></td>
        <td headers="t1g">QUEENSBORO                &nbsp;</td>
      </tr>
      <tr>
        <td scope="row" id="t1h">
         <a href="http://www.doccs.ny.gov/univinq/fpmsdoc.htm#origdate" 
            title="Definition of Date Received (Original)">
         Date Received (Original)</a></td>
        <td headers="t1h">05/16/1991 &nbsp;</td>
      </tr>
      <tr>
        <td scope="row" id="t1i">
         <a href="http://www.doccs.ny.gov/univinq/fpmsdoc.htm#currdate" 
            title="Definition of Date Received (Current)">
         Date Received (Current)</a></td>
        <td headers="t1i">05/10/2013 &nbsp;</td>
      </tr>
      <tr>
        <td scope="row" id="t1j">
         <a href="http://www.doccs.ny.gov/univinq/fpmsdoc.htm#admtype" 
            title="Definition of Admission Type">
         Admission Type</a></td>
        <td headers="t1j"> &nbsp;</td>
      </tr>
      <tr>
        <td scope="row" id="t1k">
         <a href="http://www.doccs.ny.gov/univinq/fpmsdoc.htm#county" 
            title="Definition of County of Commitment">
         County of Commitment</a></td>
        <td headers="t1k">KINGS        &nbsp;</td>
      </tr>
      <tr>
        <td scope="row" id="t1l">
         <a href="http://www.doccs.ny.gov/univinq/fpmsdoc.htm#reldate" 
            title="Definition of Latest Release Date / Type (Released Inmates
         Only)">
         Latest Release Date / Type (Released Inmates Only)</a></td>
        <td headers="t1l">04/07/14 PAROLE DIV OF PAROLE        &nbsp;</td>
      </tr>
    </tbody></table>
    <table cellpadding="2" cellspacing="0" summary="Inmate crimes of
     conviction">
      <caption>Crimes of Conviction<br>
       <span class="pcap">If all 4
       <a href="http://www.doccs.ny.gov/univinq/fpmsdoc.htm#crime" 
          title="Definition of Crime">
       crime</a> fields contain data, there may be additional crimes not shown
       here. In this case, the crimes shown here are those with the longest
       sentences.<br>
       As of 10/11/18</span></caption>
      <tbody><tr>
        <th scope="col" id="crime">Crime</th>
        <th scope="col" id="class">Class</th>
      </tr>
      <tr>
        <td headers="crime"> MANSLAUGHTER 1ST               
         &nbsp;</td>
        <td headers="class">B  &nbsp;</td>
      </tr>
      <tr>
        <td headers="crime">  ARMED ROBBERY
         &nbsp;</td>
        <td headers="class">B &nbsp;</td>
      </tr>
      <tr>
        <td headers="crime">  
         &nbsp;</td>
        <td headers="class"> &nbsp;</td>
      </tr>
      <tr>
        <td headers="crime">  
         &nbsp;</td>
        <td headers="class"> &nbsp;</td>
      </tr>
    </tbody></table>
    <table cellpadding="2" cellspacing="0" summary="Inmate sentence terms and
     release dates">
      <caption>Sentence Terms and Release Dates<br>
       <span class="pcap">Under certain circumstances, an inmate may be
       released prior to serving his or her minimum term and before the
       earliest release date shown for the inmate.<br>
       As of 10/11/18</span></caption>
      <tbody><tr>
        <td scope="row" id="t3a">
         <a href="http://www.doccs.ny.gov/univinq/fpmsdoc.htm#agg" 
            title="Definition of Aggregate Minimum Sentence">
         Aggregate Minimum Sentence</a></td>
        <td headers="t3a">0008 Years, 04 Months,
         00 Days</td>
      </tr>
      <tr>
        <td scope="row" id="t3b">
         <a href="http://www.doccs.ny.gov/univinq/fpmsdoc.htm#agg" 
            title="Definition of Aggregate Maximum Sentence">
         Aggregate Maximum Sentence</a></td>
        <td headers="t3b">0025 Years, 00 Months,
         00 Days</td>
      </tr>
      <tr>
        <td scope="row" id="t3c">
         <a href="http://www.doccs.ny.gov/univinq/fpmsdoc.htm#erd" 
            title="Definition of Earliest Release Date">
         Earliest Release Date</a></td>
        <td headers="t3c">07/04/1998 &nbsp;</td>
      </tr>
      <tr>
        <td scope="row" id="t3d">
         <a href="http://www.doccs.ny.gov/univinq/fpmsdoc.htm#ert" 
            title="Definition of Earliest Release Type">
         Earliest Release Type</a></td>
        <td headers="t3d"> &nbsp;</td>
      </tr>
      <tr>
        <td scope="row" id="t3e">
         <a href="http://www.doccs.ny.gov/univinq/fpmsdoc.htm#phd" 
            title="Definition of Parole Hearing Date">
         Parole Hearing Date</a></td>
        <td headers="t3e">02/2014 &nbsp;</td>
      </tr>
      <tr>
        <td scope="row" id="t3f">
         <a href="http://www.doccs.ny.gov/univinq/fpmsdoc.htm#pht" 
            title="Definition of Parole Hearing Type">
         Parole Hearing Type</a></td>
        <td headers="t3f">PAROLE VIOLATOR ASSESSED EXPIRATION       &nbsp;</td>
      </tr>
      <tr>
        <td scope="row" id="t3g">
         <a href="http://www.doccs.ny.gov/univinq/fpmsdoc.htm#pe" 
            title="Definition of Parole Eligibility Date">
         Parole Eligibility Date</a></td>
        <td headers="t3g">06/28/1998 &nbsp;</td>
      </tr>
      <tr>
        <td scope="row" id="t3h">
         <a href="http://www.doccs.ny.gov/univinq/fpmsdoc.htm#cr" 
            title="Definition of Conditional Release Date">
         Conditional Release Date</a></td>
        <td headers="t3h">08/13/2014 &nbsp;</td>
      </tr>
      <tr>
        <td scope="row" id="t3i">
         <a href="http://www.doccs.ny.gov/univinq/fpmsdoc.htm#me" 
            title="Definition of Maximum Expiration Date">
         Maximum Expiration Date</a></td>
        <td headers="t3i">06/01/2015 &nbsp;</td>
      </tr>
      <tr>
        <td scope="row" id="t3j">
         <a href="http://www.doccs.ny.gov/univinq/fpmsdoc.htm#meps" 
            title="Definition of Maximum Expiration Date for 
            Parole Supervision">
         Maximum Expiration Date for Parole Supervision</a></td>
        <td headers="t3j">02/2019 &nbsp;</td>
      </tr>
      <tr>
        <td scope="row" id="t3k">
         <a href="http://www.doccs.ny.gov/univinq/fpmsdoc.htm#prsme" 
            title="Definition of Post Release Supervision Maximum 
            Expiration Date">
         Post Release Supervision Maximum Expiration Date</a></td>
        <td headers="t3k">01/01/2020 &nbsp;</td>
      </tr>
      <tr>
        <td scope="row" id="t3l">
         <a href="http://www.doccs.ny.gov/univinq/fpmsdoc.htm#pbdisch" 
            title="Definition of Parole Board Discharge Date">
         Parole Board Discharge Date</a></td>
        <td headers="t3l"> 06/2008 &nbsp;</td>
      </tr>
    </tbody></table>
  </div>
  """


def test_get_initial_task():
    scraper = UsNyScraper()
    assert scraper.get_initial_task() == 'scrape_search_page'


def test_gather_details_error_missing_tables():
    """Tests that the gathers_detail method properly handles a page without
    an expected table."""
    unexpected_html = """
    <div id="ii">
    <h2 class="aligncenter">Inmate Information</h2>
    <p class="err"></p>
    <table cellpadding="2" cellspacing="0" summary="Inmate Identifying and
     location information">
      <caption>Identifying and Location Information<br>
       <span class="pcap">As of 10/11/18</span></caption>
      <tbody><tr>
        <td scope="row" id="t1a">
         <a href="http://www.doccs.ny.gov/univinq/fpmsdoc.htm#din" 
            title="Definition of DIN (Department Identification Number)">
         Cowabunga</a></td>
        <td headers="t1a">1234567 &nbsp;</td>
      </tr></tbody>
    </table>
    </div>
    """

    page_tree = html.fromstring(unexpected_html)
    details_page = page_tree.xpath('//div[@id="ii"]')

    scraper = UsNyScraper()
    assert scraper.gather_details(page_tree, details_page) == -1


def test_gather_details_error_unexpected_table_content():
    """Tests that the gathers_detail method properly handles a table with
    an unexpected header row."""
    unexpected_html = """
    <div id="ii">
    <h2 class="aligncenter">Inmate Information</h2>
    <p class="err"></p>
    <table cellpadding="2" cellspacing="0" summary="Inmate Identifying and
     location information">
      <caption>Identifying and Location Information<br>
       <span class="pcap">As of 10/11/18</span></caption>
      <tbody><tr>
        <td scope="row" id="t1a">
         <a href="http://www.doccs.ny.gov/univinq/fpmsdoc.htm#din" 
            title="Definition of DIN (Department Identification Number)">
         DIN (Department Identification Number)</a></td>
        <td headers="t1a">1234567 &nbsp;</td>
      </tr></tbody>
    </table>
    <table cellpadding="2" cellspacing="0" summary="Inmate crimes of
     conviction">
      <tbody><tr>
        <th scope="col" id="crime">Crime</th>
        <th scope="col" id="class">Class</th>
      </tr></tbody>
    </table>
    <table cellpadding="2" cellspacing="0" summary="Inmate sentence terms and
     release dates">
      <caption>Sentence Terms and Release Dates<br>
       <span class="pcap">Under certain circumstances, an inmate may be
       released prior to serving his or her minimum term and before the
       earliest release date shown for the inmate.<br>
       As of 10/11/18</span></caption>
      <tbody><tr>
        <td scope="row" id="t3a">
         <a href="http://www.doccs.ny.gov/univinq/fpmsdoc.htm#agg" 
            title="Definition of Aggregate Minimum Sentence">
         MAXIMUM SENTENCE</a></td>
        <td headers="t3a">0008 Years, 04 Months,
         00 Days</td>
      </tr></tbody>
    </table>
    </div>
    """

    page_tree = html.fromstring(unexpected_html)
    details_page = page_tree.xpath('//div[@id="ii"]')

    scraper = UsNyScraper()
    assert scraper.gather_details(page_tree, details_page) == -1


class TestCreatePerson(object):
    """Tests that the creation of new Persons works and captures all
    possible fields."""

    FIELDS_NOT_SET = ['alias', 'suffix']

    def setup_method(self, _test_method):
        # noinspection PyAttributeOutsideInit
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        ndb.get_context().clear_cache()

        # root_path must be set the the location of queue.yaml.
        # Otherwise, only the 'default' queue will be available.
        self.testbed.init_taskqueue_stub(root_path='.')

        # noinspection PyAttributeOutsideInit
        self.taskqueue_stub = self.testbed.get_stub(
            testbed.TASKQUEUE_SERVICE_NAME)

    def teardown_method(self, _test_method):
        self.testbed.deactivate()

    def test_create_person(self):
        """Tests the happy path for create_person."""
        scraper = UsNyScraper()

        page_tree = html.fromstring(PERSON_PAGE)
        details_page = page_tree.xpath('//div[@id="ii"]')
        person_details = scraper.gather_details(page_tree, details_page)

        actual = scraper.create_person(person_details)

        assert actual.birthdate == date(1972, 4, 22)
        assert actual.age >= 46
        assert actual.sex == 'male'
        assert actual.race == 'white'
        assert actual.surname == 'SIMPSON'
        assert actual.given_names == 'BART'
        assert actual.region == 'us_ny'

        # pylint:disable=protected-access
        person_attributes = UsNyPerson._properties
        unset_attributes = [attribute for attribute in person_attributes
                            if attribute != 'class'
                            and getattr(actual, attribute) is None]

        assert all(attr in TestCreatePerson.FIELDS_NOT_SET
                   for attr in unset_attributes)

    def test_create_person_linked_already(self):
        scraper = UsNyScraper()

        page_tree = html.fromstring(PERSON_PAGE)
        details_page = page_tree.xpath('//div[@id="ii"]')
        person_details = scraper.gather_details(page_tree, details_page)
        person_details['linked_records'] = ['55aa66', '66bb77']

        person_a_key = UsNyPerson(person_id='aaaaa').put()
        person_b_key = UsNyPerson(person_id='bbbbb').put()

        UsNyRecord(parent=person_a_key, record_id='55aa66').put()
        UsNyRecord(parent=person_b_key, record_id='6bb77').put()

        actual = scraper.create_person(person_details)
        assert actual.person_id == 'aaaaa'
        assert actual.us_ny_person_id == 'aaaaa'

    def test_create_person_group_id(self):
        group_id = '45678'
        scraper = UsNyScraper()

        page_tree = html.fromstring(PERSON_PAGE)
        details_page = page_tree.xpath('//div[@id="ii"]')
        person_details = scraper.gather_details(page_tree, details_page)
        person_details['group_id'] = group_id

        actual = scraper.create_person(person_details)
        assert actual.person_id == group_id
        assert actual.us_ny_person_id == group_id


class TestCreateRecord(object):
    """Tests that the creation of new Records works and captures all
    possible fields."""

    FIELDS_NOT_SET = ['community_supervision_agency',
                      'status',
                      'case_worker',
                      'offense_date',
                      'parole_officer',
                      'record_id_is_fuzzy',
                      'release_date']

    def setup_method(self, _test_method):
        # noinspection PyAttributeOutsideInit
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        ndb.get_context().clear_cache()

        # root_path must be set the the location of queue.yaml.
        # Otherwise, only the 'default' queue will be available.
        self.testbed.init_taskqueue_stub(root_path='.')

        # noinspection PyAttributeOutsideInit
        self.taskqueue_stub = self.testbed.get_stub(
            testbed.TASKQUEUE_SERVICE_NAME)

    def teardown_method(self, _test_method):
        self.testbed.deactivate()

    def test_create_record(self):
        """Tests the happy path for create_record."""
        scraper = UsNyScraper()

        page_tree = html.fromstring(PERSON_PAGE)
        details_page = page_tree.xpath('//div[@id="ii"]')
        person_details = scraper.gather_details(page_tree, details_page)

        person = scraper.create_person(person_details)
        person_key = person.put()

        _old_record, actual = scraper.create_record(person,
                                                    person_key,
                                                    person_details)

        offenses = [
            Offense(
                crime_description='MANSLAUGHTER 1ST',
                crime_class='B'
            ),
            Offense(
                crime_description='ARMED ROBBERY',
                crime_class='B'
            )
        ]

        assert actual.admission_type == ''
        assert actual.birthdate == date(1972, 4, 22)
        assert actual.cond_release_date == date(2014, 8, 13)
        assert actual.county_of_commit == 'KINGS'
        assert actual.custody_date == date(1991, 5, 16)
        assert actual.custody_status == 'RELEASED'
        assert actual.earliest_release_date == date(1998, 7, 4)
        assert actual.earliest_release_type == ''
        assert actual.is_released
        assert actual.last_custody_date == date(2013, 5, 10)
        assert actual.latest_release_date == date(2014, 4, 7)
        assert actual.latest_release_type == 'PAROLE DIV OF PAROLE'
        assert actual.latest_facility == 'QUEENSBORO'
        assert actual.max_expir_date == date(2015, 6, 1)
        assert actual.max_expir_date_parole == date(2019, 2, 1)
        assert actual.max_expir_date_superv == date(2020, 1, 1)
        assert actual.max_sentence_length == SentenceDuration(
            life_sentence=False,
            years=25,
            months=0,
            days=0)
        assert actual.min_sentence_length == SentenceDuration(
            life_sentence=False,
            years=8,
            months=4,
            days=0)
        assert actual.parole_elig_date == date(1998, 6, 28)
        assert actual.parole_discharge_date == date(2008, 6, 1)
        assert actual.parole_hearing_date == date(2014, 2, 1)
        assert actual.parole_hearing_type == 'PAROLE VIOLATOR ' \
                                             'ASSESSED EXPIRATION'
        assert actual.offense == offenses
        assert actual.race == 'white'
        assert actual.region == 'us_ny'
        assert actual.sex == 'male'
        assert actual.surname == 'SIMPSON'
        assert actual.given_names == 'BART'

        # pylint:disable=protected-access
        record_attributes = UsNyRecord._properties
        unset_attributes = [attribute for attribute in record_attributes
                            if attribute != 'class'
                            and getattr(actual, attribute) is None]

        print unset_attributes

        assert all(attr in TestCreateRecord.FIELDS_NOT_SET
                   for attr in unset_attributes)


class TestRecordToSnapshot(object):
    """Tests that the translation between Records and Snapshots works and
    captures all possible fields."""

    FIELDS_NOT_SET = ['offense_date']

    def setup_method(self, _test_method):
        # noinspection PyAttributeOutsideInit
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        ndb.get_context().clear_cache()

        # root_path must be set the the location of queue.yaml.
        # Otherwise, only the 'default' queue will be available.
        self.testbed.init_taskqueue_stub(root_path='.')

        # noinspection PyAttributeOutsideInit
        self.taskqueue_stub = self.testbed.get_stub(
            testbed.TASKQUEUE_SERVICE_NAME)

    def teardown_method(self, _test_method):
        self.testbed.deactivate()

    def test_record_to_snapshot(self):
        """Tests the happy path for record_to_snapshot."""
        offense = Offense(
            crime_description='MANSLAUGHTER 1ST',
            crime_class='B'
        )

        record = UsNyRecord(
            admission_type='REVOCATION',
            birthdate=datetime(1972, 4, 22),
            cond_release_date=datetime(2006, 8, 14),
            county_of_commit='KINGS',
            custody_date=datetime(1991, 3, 14),
            custody_status='RELEASED',
            earliest_release_date=datetime(2002, 8, 14),
            earliest_release_type='PAROLE',
            is_released=True,
            last_custody_date=datetime(1994, 7, 1),
            latest_release_date=datetime(2002, 10, 28),
            latest_release_type='PAROLE',
            latest_facility='QUEENSBORO',
            max_expir_date=datetime(2010, 1, 14),
            max_expir_date_parole=datetime(2010, 1, 14),
            max_expir_date_superv=datetime(2010, 1, 14),
            max_sentence_length=SentenceDuration(
                life_sentence=False,
                years=18,
                months=10,
                days=0),
            min_sentence_length=SentenceDuration(
                life_sentence=False,
                years=11,
                months=5,
                days=0),
            parole_elig_date=datetime(2002, 8, 14),
            parole_discharge_date=datetime(2002, 8, 14),
            parole_hearing_date=datetime(2002, 6, 17),
            parole_hearing_type='INITIAL HEARING',
            offense=[offense],
            race='WHITE',
            record_id='1234567',
            region='us_ny',
            sex='MALE',
            surname='SIMPSON',
            given_names='BART',
            us_ny_record_id='1234567'
        )

        scraper = UsNyScraper()
        snapshot = scraper.record_to_snapshot(record)
        assert snapshot == Snapshot(
            parent=record.key,
            admission_type=record.admission_type,
            birthdate=record.birthdate,
            cond_release_date=record.cond_release_date,
            county_of_commit=record.county_of_commit,
            custody_date=record.custody_date,
            custody_status=record.custody_status,
            earliest_release_date=record.earliest_release_date,
            earliest_release_type=record.earliest_release_type,
            is_released=record.is_released,
            last_custody_date=record.last_custody_date,
            latest_facility=record.latest_facility,
            latest_release_date=record.latest_release_date,
            latest_release_type=record.latest_release_type,
            max_expir_date=record.max_expir_date,
            max_expir_date_parole=record.max_expir_date_parole,
            max_expir_date_superv=record.max_expir_date_superv,
            max_sentence_length=record.max_sentence_length,
            min_sentence_length=record.min_sentence_length,
            offense=record.offense,
            parole_discharge_date=record.parole_discharge_date,
            parole_elig_date=record.parole_elig_date,
            parole_hearing_date=record.parole_hearing_date,
            parole_hearing_type=record.parole_hearing_type,
            race=record.race,
            region=record.region,
            sex=record.sex,
            surname=record.surname,
            given_names=record.given_names
        )
        snapshot.put()

        # pylint:disable=protected-access
        snapshot_attributes = Snapshot._properties
        unset_attributes = [attribute for attribute in snapshot_attributes
                            if attribute != 'class'
                            and getattr(snapshot, attribute) is None]

        print unset_attributes

        assert all(attr in TestRecordToSnapshot.FIELDS_NOT_SET
                   for attr in unset_attributes)


class TestStoreRecord(object):
    """Tests for the store_record method."""

    def setup_method(self, _test_method):
        # noinspection PyAttributeOutsideInit
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        ndb.get_context().clear_cache()

        # root_path must be set the the location of queue.yaml.
        # Otherwise, only the 'default' queue will be available.
        self.testbed.init_taskqueue_stub(root_path='.')

        # noinspection PyAttributeOutsideInit
        self.taskqueue_stub = self.testbed.get_stub(
            testbed.TASKQUEUE_SERVICE_NAME)

    def teardown_method(self, _test_method):
        self.testbed.deactivate()

    def test_store_record(self):
        """Tests the happy path for store_record."""
        scraper = UsNyScraper()
        person_details = self.prepare_details(scraper)

        result = scraper.store_record(person_details)
        assert result is None

        person = UsNyPerson.query(UsNyPerson.surname == 'SIMPSON').get()
        assert person
        assert person.birthdate == date(1972, 4, 22)

        records = UsNyRecord.query(ancestor=person.key).fetch()
        assert len(records) == 1
        assert records[0].custody_date == date(1991, 5, 16)

        snapshots = Snapshot.query(ancestor=records[0].key).fetch()
        assert len(snapshots) == 1

    @patch("recidiviz.ingest.us_ny.us_ny_person.UsNyPerson.put")
    def test_error_saving_person(self, mock_put):
        mock_put.side_effect = InternalError()

        scraper = UsNyScraper()
        person_details = self.prepare_details(scraper)
        result = scraper.store_record(person_details)

        assert result == -1
        mock_put.assert_called_with()

    @patch("recidiviz.ingest.us_ny.us_ny_record.UsNyRecord.put")
    def test_error_saving_record(self, mock_put):
        mock_put.side_effect = InternalError()

        scraper = UsNyScraper()
        person_details = self.prepare_details(scraper)
        result = scraper.store_record(person_details)

        assert result == -1
        mock_put.assert_called_with()

        person = UsNyPerson.query(UsNyPerson.surname == 'SIMPSON').get()
        assert person
        assert person.birthdate == date(1972, 4, 22)

    @staticmethod
    def prepare_details(scraper):
        page_tree = html.fromstring(PERSON_PAGE)
        details_page = page_tree.xpath('//div[@id="ii"]')
        return scraper.gather_details(page_tree, details_page)


class TestCompareAndSetSnapshot(object):
    """Tests for the compare_and_set_snapshot method.
    TODO: #121 these tests should be shifted up to scraper_test.py.
    """

    def setup_method(self, _test_method):
        # noinspection PyAttributeOutsideInit
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        ndb.get_context().clear_cache()

        # root_path must be set the the location of queue.yaml.
        # Otherwise, only the 'default' queue will be available.
        self.testbed.init_taskqueue_stub(root_path='.')

        # noinspection PyAttributeOutsideInit
        self.taskqueue_stub = self.testbed.get_stub(
            testbed.TASKQUEUE_SERVICE_NAME)

    def teardown_method(self, _test_method):
        self.testbed.deactivate()

    def test_first_snapshot_for_record(self):
        """Tests the happy path for compare_and_set_snapshot."""
        scraper = UsNyScraper()

        record = self.prepare_record(scraper)
        snapshot = scraper.record_to_snapshot(record)

        scraper.compare_and_set_snapshot(record, snapshot)
        snapshots = Snapshot.query(ancestor=record.key).fetch()
        assert len(snapshots) == 1

    def test_no_changes(self):
        """Tests that a lack of changes in any field does not lead to a new
        snapshot."""
        scraper = UsNyScraper()

        record = self.prepare_record(scraper)
        snapshot = scraper.record_to_snapshot(record)

        scraper.compare_and_set_snapshot(record, snapshot)
        snapshots = Snapshot.query(ancestor=record.key).fetch()
        assert len(snapshots) == 1

        # There should still only be one snapshot, because nothing changes
        scraper.compare_and_set_snapshot(record, snapshot)
        snapshots = Snapshot.query(ancestor=record.key).fetch()
        assert len(snapshots) == 1

    def test_changes_in_flat_field(self):
        """Tests that changes in a flat field lead to a new snapshot."""
        scraper = UsNyScraper()

        record = self.prepare_record(scraper)
        snapshot = scraper.record_to_snapshot(record)

        scraper.compare_and_set_snapshot(record, snapshot)
        snapshots = Snapshot.query(ancestor=record.key).fetch()
        assert len(snapshots) == 1

        second_snapshot = scraper.record_to_snapshot(record)
        second_snapshot.latest_facility = "ANOTHER FACILITY"

        scraper.compare_and_set_snapshot(record, second_snapshot)
        snapshots = Snapshot.query(ancestor=record.key).fetch()
        assert len(snapshots) == 2

        self.assert_proper_fields_set(snapshots[1], ["latest_facility"])

    def test_changes_in_nested_field_offense(self):
        """Tests that changes in a nested field, i.e. offense,
        lead to a new snapshot."""
        scraper = UsNyScraper()

        record = self.prepare_record(scraper)
        snapshot = scraper.record_to_snapshot(record)

        scraper.compare_and_set_snapshot(record, snapshot)
        snapshots = Snapshot.query(ancestor=record.key).fetch()
        assert len(snapshots) == 1

        second_snapshot = scraper.record_to_snapshot(record)
        updated_offenses = [
            # This one remained the same
            Offense(
                crime_description="MANSLAUGHTER 1ST",
                crime_class="B"
            ),
            # This one remained the same
            Offense(
                crime_description="ARMED ROBBERY",
                crime_class="B"
            ),
            # This one is new
            Offense(
                crime_description="INTIMIDATION",
                crime_class="D"
            )
        ]
        second_snapshot.offense = updated_offenses

        scraper.compare_and_set_snapshot(record, second_snapshot)
        snapshots = Snapshot.query(ancestor=record.key).fetch()
        assert len(snapshots) == 2

        self.assert_proper_fields_set(snapshots[1], [])

        new_offenses = snapshots[1].offense
        assert new_offenses == updated_offenses

    @patch("recidiviz.models.snapshot.Snapshot.put")
    def test_error_saving_snapshot(self, mock_put):
        mock_put.side_effect = InternalError()

        scraper = UsNyScraper()

        record = self.prepare_record(scraper)
        snapshot = scraper.record_to_snapshot(record)

        result = scraper.compare_and_set_snapshot(record, snapshot)
        assert not result

        mock_put.assert_called_with()

    @staticmethod
    def prepare_record(scraper):
        """Prepares a Record suitable for comparing snapshots to."""
        page_tree = html.fromstring(PERSON_PAGE)
        details_page = page_tree.xpath('//div[@id="ii"]')
        person_details = scraper.gather_details(page_tree, details_page)

        person = scraper.create_person(person_details)
        person_key = person.put()

        _old_record, record = scraper.create_record(
            person, person_key, person_details)
        record.put()

        before_compare = Snapshot.query(ancestor=record.key).fetch()
        assert not before_compare

        return record

    @staticmethod
    def assert_proper_fields_set(snapshot, fields):
        # pylint:disable=protected-access
        snapshot_class = ndb.Model._kind_map['Snapshot']
        snapshot_attrs = snapshot_class._properties
        for attribute in snapshot_attrs:
            # Offense defaults to an empty array, so it's always set
            if attribute not in ["class", "created_on", "offense"]\
                    and attribute not in fields:
                assert getattr(snapshot, attribute) is None

            if attribute in fields:
                assert getattr(snapshot, attribute) is not None


class TestResultsParsingFailure(object):
    """Tests for the results_parsing_failure method."""

    def setup_method(self, _test_method):
        # noinspection PyAttributeOutsideInit
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        ndb.get_context().clear_cache()

        # root_path must be set the the location of queue.yaml.
        # Otherwise, only the 'default' queue will be available.
        self.testbed.init_taskqueue_stub(root_path='.')

        # noinspection PyAttributeOutsideInit
        self.taskqueue_stub = self.testbed.get_stub(
            testbed.TASKQUEUE_SERVICE_NAME)

    def teardown_method(self, _test_method):
        self.testbed.deactivate()

    def test_early_fail_count(self):
        scraper = UsNyScraper()
        memcache.set(scraper.fail_counter, 0)

        assert not scraper.results_parsing_failure()

        assert memcache.get(scraper.fail_counter) == 1

    @patch("recidiviz.ingest.sessions.get_open_sessions")
    def test_third_failure_no_open_sessions(self, mock_sessions):
        mock_sessions.return_value = []

        scraper = UsNyScraper()
        memcache.set(scraper.fail_counter, 3)

        assert scraper.results_parsing_failure()

        mock_sessions.assert_called_with('us_ny', most_recent_only=True)

    @patch("recidiviz.ingest.scraper.Scraper.stop_scrape")
    @patch("recidiviz.ingest.sessions.get_recent_sessions")
    @patch("recidiviz.ingest.sessions.get_open_sessions")
    def test_third_failure_nothing_scraped_yet(self,
                                               mock_sessions,
                                               mock_recent_sessions,
                                               mock_stop_scrape):
        """Tests the case where we've failed at least 3 times, and nothing has
        been scraped yet."""
        scrape_type = 'background'

        mock_sessions.return_value = ScrapeSession(region='us_ny',
                                                   scrape_type=scrape_type)

        recent_session = ScrapeSession(region='us_ny',
                                       scrape_type=scrape_type,
                                       last_scraped='ZZZZ, ZZ')
        mock_recent_sessions.return_value = [recent_session]

        mock_stop_scrape.return_value = None

        scraper = UsNyScraper()
        memcache.set(scraper.fail_counter, 3)

        assert scraper.results_parsing_failure()

        mock_sessions.assert_called_with('us_ny', most_recent_only=True)
        mock_recent_sessions.assert_called_with(
            ScrapeKey('us_ny', scrape_type))
        mock_stop_scrape.assert_called_with([scrape_type])

    @patch("recidiviz.ingest.scraper.Scraper.stop_scrape")
    @patch("google.appengine.ext.deferred.defer")
    @patch("recidiviz.ingest.sessions.get_open_sessions")
    def test_third_failure_not_finished_yet(self,
                                            mock_sessions,
                                            mock_deferred,
                                            mock_stop_scrape):
        scrape_type = 'background'

        mock_sessions.return_value = ScrapeSession(region='us_ny',
                                                   scrape_type=scrape_type,
                                                   last_scraped='SIMPSON, ABE')
        mock_deferred.return_value = None
        mock_stop_scrape.return_value = None

        scraper = UsNyScraper()
        memcache.set(scraper.fail_counter, 3)

        assert scraper.results_parsing_failure()

        mock_sessions.assert_called_with('us_ny', most_recent_only=True)
        mock_deferred.assert_called_with(scraper.resume_scrape,
                                         scrape_type,
                                         _countdown=60)
        mock_stop_scrape.assert_called_with([scrape_type])

    @patch("recidiviz.ingest.scraper.Scraper.stop_scrape")
    @patch("recidiviz.ingest.sessions.get_open_sessions")
    def test_third_failure_finished(self,
                                    mock_sessions,
                                    mock_stop_scrape):
        scrape_type = 'background'

        mock_sessions.return_value = ScrapeSession(region='us_ny',
                                                   scrape_type=scrape_type,
                                                   last_scraped='ZYTEL, ABC')
        mock_stop_scrape.return_value = None

        scraper = UsNyScraper()
        memcache.set(scraper.fail_counter, 3)

        assert scraper.results_parsing_failure()

        mock_sessions.assert_called_with('us_ny', most_recent_only=True)
        mock_stop_scrape.assert_called_with([scrape_type])
