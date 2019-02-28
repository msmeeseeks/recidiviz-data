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
# ============================================================================
"""Converts an ingest_info proto Person to a persistence entity."""
import json
from typing import Optional

from recidiviz.common.constants.person import Ethnicity, Gender, Race
from recidiviz.persistence.converter import converter_utils
from recidiviz.persistence.converter.converter_utils import \
    calculate_birthdate_from_age, fn, normalize, parse_date, parse_external_id


def copy_fields_to_builder(person_builder, proto, metadata):
    """Mutates the provided |person_builder| by converting an ingest_info proto
     Person.

     Note: This will not copy children into the Builder!
     """
    new = person_builder

    new.external_id = fn(parse_external_id, 'person_id', proto)
    new.full_name = _parse_name(proto)
    new.birthdate, new.birthdate_inferred_from_age = _parse_birthdate(proto)
    new.race, new.ethnicity = _parse_race_and_ethnicity(proto,
                                                        metadata.enum_overrides)
    new.race_raw_text = fn(normalize, 'race', proto)
    new.ethnicity_raw_text = fn(normalize, 'ethnicity', proto)
    new.gender = fn(Gender.parse, 'gender', proto, metadata.enum_overrides)
    new.gender_raw_text = fn(normalize, 'gender', proto)
    new.place_of_residence = fn(normalize, 'place_of_residence', proto)

    new.region = metadata.region


def _parse_name(proto) -> Optional[str]:
    """Parses name into a single string."""
    full_name = fn(normalize, 'full_name', proto)
    given_names = fn(normalize, 'given_names', proto)
    middle_names = fn(normalize, 'middle_names', proto)
    surname = fn(normalize, 'surname', proto)
    name_suffix = fn(normalize, 'name_suffix', proto)

    _validate_names(
        full_name=full_name, given_names=given_names, middle_names=middle_names,
        surname=surname, name_suffix=name_suffix)
    return _combine_names(
        full_name=full_name, given_names=given_names, middle_names=middle_names,
        surname=surname, name_suffix=name_suffix)


def _combine_names(**names: Optional[str]) -> str:
    """Writes the names out as a json string, skipping fields that are None.

    Note: We don't have any need for parsing these back into their parts, but
    this gives us other advantages. It handles escaping the names, and allows us
    to add fields in the future without changing the serialization of existing
    names.
    """
    return json.dumps({k: v for k, v in names.items() if v}, sort_keys=True)


def _validate_names(*, full_name: str, given_names: str, middle_names: str,
                    surname: str, name_suffix: str):
    if full_name and any((given_names, middle_names, surname, name_suffix)):
        raise ValueError(
            'Cannot have full_name and surname/middle/given_names/name_suffix')

    if not any((full_name, given_names, surname)):
        raise ValueError('full_name, given_names, or surname must be set.')


def _parse_birthdate(proto):
    parsed_birthdate = None
    parsed_birthdate_is_inferred = None

    birthdate = fn(parse_date, 'birthdate', proto)
    birthdate_inferred_by_age = fn(calculate_birthdate_from_age, 'age', proto)
    if birthdate is not None:
        parsed_birthdate = birthdate
        parsed_birthdate_is_inferred = False
    elif birthdate_inferred_by_age is not None:
        parsed_birthdate = birthdate_inferred_by_age
        parsed_birthdate_is_inferred = True

    return parsed_birthdate, parsed_birthdate_is_inferred


def _parse_race_and_ethnicity(proto, enum_overrides):
    if converter_utils.race_is_actually_ethnicity(proto, enum_overrides):
        race = None
        ethnicity = fn(Ethnicity.parse, 'race', proto, enum_overrides)
    else:
        race = fn(Race.parse, 'race', proto, enum_overrides)
        ethnicity = fn(Ethnicity.parse, 'ethnicity', proto, enum_overrides)

    return race, ethnicity
