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

"""Tests for ingest/docket.py."""


import json
import time

from datetime import datetime
from dateutil.relativedelta import relativedelta
from google.appengine.api import taskqueue
from google.appengine.ext import ndb
from google.appengine.ext import testbed
from ingest import docket
from ingest import sessions
from ingest.models.scrape_session import ScrapeSession
from ingest.us_ny.us_ny_record import UsNyRecord
from ingest.us_ny.us_ny_snapshot import UsNySnapshot
from models.inmate import Inmate


class TestPopulation(object):
    """Tests for the methods related to population of items in the docket."""

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

    def test_add_to_query_docket_background(self):
        docket.add_to_query_docket("us_ny",
                                   "background",
                                   get_payload(as_json=False))

        tasks = [docket.get_new_docket_item("us_ny", "background"),
                 docket.get_new_docket_item("us_ny", "background")]
        assert len(tasks) == 2

        for i, task in enumerate(tasks):
            assert task.payload == json.dumps(
                get_payload(as_json=False)[i])
            assert task.tag == "us_ny-background"

    def test_add_to_query_docket_snapshot(self):
        docket.add_to_query_docket("us_ny",
                                   "snapshot",
                                   get_snapshot_payload(as_json=False))

        tasks = [docket.get_new_docket_item("us_ny", "snapshot"),
                 docket.get_new_docket_item("us_ny", "snapshot")]
        assert len(tasks) == 2

        for i, task in enumerate(tasks):
            assert task.payload == json.dumps(
                get_snapshot_payload(as_json=False)[i])
            assert task.tag == "us_ny-snapshot"

    def test_load_target_list_background_happy_path(self):
        docket.load_target_list("background", "us_ny", {})

        task = docket.get_new_docket_item("us_ny", "background")
        assert task.payload == json.dumps(('aaardvark', ''))
        assert not docket.get_new_docket_item("us_ny", "background", back_off=1)

    def test_load_target_list_snapshot_never_ingested(self):
        docket.load_target_list("snapshot", "us_ny", {})

        assert not docket.get_new_docket_item("us_ny", "snapshot", back_off=1)

    def test_load_target_list_snapshot_happy_path(self):
        halfway = datetime.now() - relativedelta(years=5)

        inmate = get_inmate("clem", "12345")
        record = get_record(inmate.key, "56789", halfway)
        get_record(inmate.key, "89012", halfway - relativedelta(years=10))
        get_snapshot(record.key, halfway)

        docket.load_target_list("snapshot", "us_ny", {})

        task = docket.get_new_docket_item("us_ny", "snapshot")
        assert task.payload == json.dumps(("12345", ["89012"]))
        assert task.tag == "us_ny-snapshot"


class TestRetrieval(object):
    """Tests for the methods related to retrieval of items from the docket."""

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

    def test_get_new_docket_item(self):
        taskqueue.Task(tag='us_ny-background',
                       payload=get_payload(),
                       method='PULL').add(docket.DOCKET_QUEUE_NAME)

        docket_item = docket.get_new_docket_item("us_ny", "background")
        assert docket_item.tag == 'us_ny-background'
        assert docket_item.payload == get_payload()

    def test_get_new_docket_item_no_matching_items(self):
        taskqueue.Task(tag='us_ny-background',
                       payload=get_payload(),
                       method='PULL').add(docket.DOCKET_QUEUE_NAME)

        docket_item = docket.get_new_docket_item("us_fl",
                                                 "background",
                                                 back_off=1)
        assert not docket_item

    def test_get_new_docket_item_no_items_at_all(self):
        docket_item = docket.get_new_docket_item("us_ny",
                                                 "background",
                                                 back_off=1)
        assert not docket_item

    def test_iterate_docket_item(self):
        create_open_session("us_ny", "background", datetime(2009, 6, 17), "a")

        taskqueue.Task(tag='us_ny-background',
                       payload=get_payload(),
                       method='PULL').add(docket.DOCKET_QUEUE_NAME)

        payload = docket.iterate_docket_item("us_ny", "background")
        assert payload == get_payload(as_json=False)

    def test_iterate_docket_item_no_open_session_to_update(self):
        taskqueue.Task(tag='us_ny-background',
                       payload=get_payload(),
                       method='PULL').add(docket.DOCKET_QUEUE_NAME)

        payload = docket.iterate_docket_item("us_ny", "background")
        assert not payload

    def test_iterate_docket_item_no_matching_items(self):
        taskqueue.Task(tag='us_ny-background',
                       payload=get_payload(),
                       method='PULL').add(docket.DOCKET_QUEUE_NAME)

        payload = docket.iterate_docket_item("us_fl", "background", back_off=1)
        assert not payload

    def test_iterate_docket_item_no_items_at_all(self):
        payload = docket.iterate_docket_item("us_ny", "background", back_off=1)
        assert not payload


class TestRemoval(object):
    """Tests for the methods related to removal of items from the docket."""

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

    def test_purge_query_docket(self):
        taskqueue.Task(tag='us_va-background',
                       payload=get_payload(),
                       method='PULL').add(docket.DOCKET_QUEUE_NAME)
        taskqueue.Task(tag='us_ut-snapshot',
                       payload=get_payload(),
                       method='PULL').add(docket.DOCKET_QUEUE_NAME)

        docket.purge_query_docket("us_ut", "snapshot")
        assert not docket.get_new_docket_item("us_ut", "snapshot", back_off=1)
        assert docket.get_new_docket_item("us_va", "background", back_off=1)

    def test_purge_query_docket_sessions_leased(self):
        create_open_session("us_va", "background", datetime(2016, 11, 20), "a")
        create_open_session("us_va", "background", datetime(2016, 11, 20), "b")

        taskqueue.Task(tag='us_va-background',
                       payload=get_payload(),
                       method='PULL').add(docket.DOCKET_QUEUE_NAME)

        docket.purge_query_docket("us_va", "background")
        assert not docket.get_new_docket_item("us_va", "background", back_off=1)
        assert not sessions.get_sessions_with_leased_docket_items(
            "us_va", "background")

    def test_purge_query_docket_nothing_matching(self):
        taskqueue.Task(tag='us_va-background',
                       payload=get_payload(),
                       method='PULL').add(docket.DOCKET_QUEUE_NAME)

        docket.purge_query_docket("us_ny", "background")
        assert not docket.get_new_docket_item("us_ny", "background", back_off=1)

    def test_purge_query_docket_already_empty(self):
        docket.purge_query_docket("us_ny", "background")
        assert not docket.get_new_docket_item("us_ny", "background", back_off=1)

    def test_remove_item_from_session_and_docket(self):
        create_open_session("us_va", "background", datetime(2016, 11, 20), "a")

        taskqueue.Task(name="a",
                       tag='us_va-background',
                       payload=get_payload(),
                       method='PULL').add(docket.DOCKET_QUEUE_NAME)

        docket.remove_item_from_session_and_docket("us_va", "background")
        assert not docket.get_new_docket_item("us_va", "background", back_off=1)
        assert not sessions.get_sessions_with_leased_docket_items(
            "us_va", "background")

    def test_remove_item_from_session_and_docket_no_docket_item(self):
        create_open_session("us_va", "background", datetime(2016, 11, 20), "a")

        taskqueue.Task(name="something-else",
                       tag='us_va-background',
                       payload=get_payload(),
                       method='PULL').add(docket.DOCKET_QUEUE_NAME)

        docket.remove_item_from_session_and_docket("us_va", "background")
        assert docket.get_new_docket_item("us_va", "background", back_off=1)
        assert sessions.get_sessions_with_leased_docket_items(
            "us_va", "background")

    def test_remove_item_from_session_and_docket_no_open_sessions(self):
        taskqueue.Task(name="a",
                       tag='us_va-background',
                       payload=get_payload(),
                       method='PULL').add(docket.DOCKET_QUEUE_NAME)

        docket.remove_item_from_session_and_docket("us_va", "background")
        assert docket.get_new_docket_item("us_va", "background", back_off=1)


def test_get_task_name_same_within_minute():
    first = docket.get_task_name("us_ny", "snapshot")
    time.sleep(2)
    second = docket.get_task_name("us_ny", "snapshot")

    assert first == second


def get_payload(as_json=True):
    body = [{'name': 'Jacoby, Mackenzie'}, {'name': 'Jacoby, Clementine'}]
    if as_json:
        return json.dumps(body)
    return body


def get_snapshot_payload(as_json=True):
    body = [('123', ['456']), ('789', ['012', '234'])]
    if as_json:
        return json.dumps(body)
    return body


def create_open_session(region_code, scrape_type, start, docket_item):
    session = ScrapeSession(region=region_code,
                            scrape_type=scrape_type,
                            docket_item=docket_item,
                            start=start)
    session.put()
    return session


def get_inmate(key, inmate_id):
    new_inmate = Inmate(id=key, inmate_id=inmate_id)
    new_inmate.put()
    return new_inmate


def get_record(parent_key, record_id, latest_release_date):
    new_record = UsNyRecord(parent=parent_key,
                            record_id=record_id,
                            latest_release_date=latest_release_date,
                            is_released=False)
    new_record.put()
    return new_record


def get_snapshot(parent_key, snapshot_date):
    new_snapshot = UsNySnapshot(parent=parent_key,
                                created_on=snapshot_date,
                                is_released=False)
    new_snapshot.put()
    return new_snapshot
