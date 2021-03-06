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
"""Tests for converting arrests."""
import unittest
from datetime import date

from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.persistence import entities
from recidiviz.persistence.converter import arrest


class ArrestConverterTest(unittest.TestCase):
    """Tests for converting arrests."""

    def testParseArrest(self):
        # Arrange
        ingest_arrest = ingest_info_pb2.Arrest(
            arrest_id='ARREST_ID',
            arrest_date='1/2/1111',
            location='FAKE_LOCATION',
            officer_name='FAKE_NAME',
            officer_id='FAKE_ID',
            agency='FAKE_AGENCY'
        )

        # Act
        result = arrest.convert(ingest_arrest)

        # Assert
        expected_result = entities.Arrest(
            external_id='ARREST_ID',
            arrest_date=date(year=1111, month=1, day=2),
            location='FAKE_LOCATION',
            officer_name='FAKE_NAME',
            officer_id='FAKE_ID',
            agency='FAKE_AGENCY'
        )

        self.assertEqual(result, expected_result)
