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
"""Tests for converting people."""
import unittest
from datetime import date, datetime

from mock import patch

from recidiviz.common.constants.person import Gender, Race, Ethnicity
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.ingest.models import ingest_info_pb2
from recidiviz.persistence import entities
from recidiviz.persistence.converter import person

_NOW = datetime(2000, 5, 15)


class PersonConverterTest(unittest.TestCase):
    """Tests for converting people."""

    def setUp(self):
        self.subject = entities.Person.builder()

    def testParsesPerson(self):
        # Arrange
        metadata = IngestMetadata.new_with_defaults(region='REGION')
        ingest_person = ingest_info_pb2.Person(
            full_name='FULL_NAME',
            birthdate='12-31-1999',
            gender='MALE',
            race='WHITE',
            ethnicity='HISPANIC',
            place_of_residence='NNN\n  STREET \t ZIP')

        # Act
        person.copy_fields_to_builder(self.subject, ingest_person, metadata)
        result = self.subject.build()

        # Assert
        expected_result = entities.Person.new_with_defaults(
            full_name='FULL_NAME',
            birthdate=date(year=1999, month=12, day=31),
            birthdate_inferred_from_age=False,
            gender=Gender.MALE,
            gender_raw_text='MALE',
            race=Race.WHITE,
            race_raw_text='WHITE',
            ethnicity=Ethnicity.HISPANIC,
            ethnicity_raw_text='HISPANIC',
            place_of_residence='NNN STREET ZIP',
            region='REGION',
        )

        self.assertEqual(result, expected_result)

    def testParsePerson_WithSurnameAndFullname_ThrowsException(self):
        # Arrange
        metadata = IngestMetadata.new_with_defaults()
        ingest_person = ingest_info_pb2.Person(
            full_name='LAST,FIRST',
            surname='LAST'
        )

        # Arrange + Act
        with self.assertRaises(ValueError):
            person.copy_fields_to_builder(self.subject, ingest_person, metadata)

    def testParsePerson_WithSurnameAndGivenNames_UsesFullNameAsCsv(self):
        # Arrange
        metadata = IngestMetadata.new_with_defaults()
        ingest_person = ingest_info_pb2.Person(
            surname='UNESCAPED,SURNAME"WITH-CHARS"',
            given_names='GIVEN_NAMES',
            middle_names='MIDDLE_NAMES'
        )

        # Act
        person.copy_fields_to_builder(self.subject, ingest_person, metadata)
        result = self.subject.build()

        # Assert
        expected_escaped_surname = '"UNESCAPED,SURNAME""WITH-CHARS"""'
        expected_result = entities.Person.new_with_defaults(
            full_name='{},{},{}'.format('GIVEN_NAMES', 'MIDDLE_NAMES',
                                        expected_escaped_surname),
        )

        self.assertEqual(result, expected_result)

    @patch('recidiviz.persistence.converter.converter_utils.datetime.datetime')
    def testParsePerson_InfersBirthdateFromAge(self, mock_datetime):
        # Arrange
        mock_datetime.now.return_value = _NOW
        metadata = IngestMetadata.new_with_defaults()
        ingest_person = ingest_info_pb2.Person(age='27')

        # Act
        person.copy_fields_to_builder(self.subject, ingest_person, metadata)
        result = self.subject.build()

        # Assert
        expected_result = entities.Person.new_with_defaults(
            birthdate=datetime(year=_NOW.year - 27, month=1, day=1).date(),
            birthdate_inferred_from_age=True
        )

        self.assertEqual(result, expected_result)

    def testParsePerson_RaceIsEthnicity(self):
        # Arrange
        metadata = IngestMetadata.new_with_defaults()
        ingest_person = ingest_info_pb2.Person(race='HISPANIC')

        # Act
        person.copy_fields_to_builder(self.subject, ingest_person, metadata)
        result = self.subject.build()

        # Assert
        expected_result = entities.Person.new_with_defaults(
            ethnicity=Ethnicity.HISPANIC,
            race_raw_text='HISPANIC',
        )

        self.assertEqual(result, expected_result)
