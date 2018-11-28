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

# pylint: disable=unused-import,wrong-import-order

"""Tests for utils/environment.py."""


import pytest

from ..context import models
from google.appengine.ext import ndb
from google.appengine.ext import testbed
from recidiviz.models import env_vars


class TestGetEnvVar(object):
    """Tests for the get_env_var method in the module."""

    def setup_method(self, _test_method):
        # noinspection PyAttributeOutsideInit
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.testbed.init_memcache_stub()
        ndb.get_context().clear_cache()

    def teardown_method(self, _test_method):
        self.testbed.deactivate()
        env_vars.LOCAL_VARS.clear()

    def test_in_cache(self):
        write_to_local('top_track', 'Olson')

        actual = env_vars.get_env_var('top_track')
        assert actual == 'Olson'

    def test_in_datastore(self):
        write_to_datastore('top_track', 'An Eagle In Your Mind')

        actual = env_vars.get_env_var('top_track')
        assert actual == 'An Eagle In Your Mind'

    def test_in_neither_with_different_cahce_and_datastore(self):
        write_to_local('top_track', 'Wildlife Analysis')
        write_to_local('solid_track', 'Telephasic Workshop')

        actual = env_vars.get_env_var('other_track')
        assert actual is None

    def test_in_datastore_with_different_cache(self):
        write_to_local('top_track', 'Wildlife Analysis')
        write_to_datastore('solid_track', 'Kaini Industries')

        actual = env_vars.get_env_var('solid_track')
        assert actual == 'Kaini Industries'

    def test_in_neither(self):
        actual = env_vars.get_env_var('top_track')
        assert actual is None


def write_to_datastore(name, value):
    env_var = env_vars.EnvironmentVariable(region='all', name=name, value=value)
    env_var.put()


def write_to_local(name, value):
    env_vars.LOCAL_VARS[name] = value
