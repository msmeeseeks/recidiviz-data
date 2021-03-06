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
"""Parse the HI Aggregated Statistics PDF."""
import datetime
from typing import Dict, List, Iterable

import dateparser
import more_itertools
import pandas as pd
import tabula
from sqlalchemy.ext.declarative import DeclarativeMeta

import recidiviz.common.constants.enum_canonical_strings as enum_strings
from recidiviz.ingest.aggregate.errors import FipsMergingError, \
    AggregateIngestError
from recidiviz.persistence.database.schema import HiFacilityAggregate

_COLUMN_NAMES = [
    'facility_name',
    'design_bed_capacity',
    'operation_bed_capacity',
    'total_population',
    'male_population',
    'female_population',
    'sentenced_felony_male_population',
    'sentenced_felony_female_population',
    'sentenced_felony_probation_male_population',
    'sentenced_felony_probation_female_population',
    'sentenced_misdemeanor_male_population',
    'sentenced_misdemeanor_female_population',
    'sentenced_pretrial_felony_male_population',
    'sentenced_pretrial_felony_female_population',
    'sentenced_pretrial_misdemeanor_male_population',
    'sentenced_pretrial_misdemeanor_female_population',
    'held_for_other_jurisdiction_male_population',
    'held_for_other_jurisdiction_female_population',
    'parole_violation_male_population',
    'parole_violation_female_population',
    'probation_violation_male_population',
    'probation_violation_female_population'
]

_FACILITY_ACRONYM_TO_NAME = {
    'HCCC': 'Hawaii Community Correctional Center',
    'SNF': 'Halawa Correctional Facility - Special Needs',
    'HMSF': 'Halawa Correctional Facility - Medium Security',
    'KCCC': 'Kauai Community Correctional Center',
    'KCF': ' Kulani Correctional Facility',
    'MCCC': 'Maui Community Correctional Center',
    'OCCC': 'Oahu Community Correctional Center',
    'WCCC': 'Women’s Community Correctional Center',
    'WCF': 'Waiawa Correctional Facility',
    'RED ROCK CC, AZ': 'Red Rock Correctional Center, AZ',
    'SAGUARO CC, AZ': 'Saguaro Correctional Center, AZ',
    'FEDERAL DET. CTR.': 'Federal Detention Center, Honolulu',
    'FEDERAL DET. CTR. 1': 'Federal Detention Center, Honolulu',
}

_FACILITY_ACRONYM_TO_FIPS = {
    'HCCC': 15001,  # Hawaii
    'SNF': 15003,  # Honolulu
    'HMSF': 15003,  # Honolulu
    'KCCC': 15007,  # Kauai
    'KCF': 15001,  # Hawaii
    'MCCC': 15009,  # Maui
    'OCCC': 15003,  # Honolulu
    'WCCC': 15003,  # Honolulu
    'WCF': 15003,  # Honolulu
    'RED ROCK CC, AZ': 4021,  # Pinal
    'SAGUARO CC, AZ': 4021,  # Pinal
    'FEDERAL DET. CTR.': 15003,  # Honolulu
    'FEDERAL DET. CTR. 1': 15003,  # Honolulu
}

DATE_PARSE_ANCHOR_FILENAME = 'pop-reports-eom-'


def parse(filename: str) -> Dict[DeclarativeMeta, pd.DataFrame]:
    table = _parse_table(filename)

    table['report_date'] = parse_date(filename)
    table['report_granularity'] = enum_strings.monthly_granularity

    return {
        HiFacilityAggregate: table
    }


def parse_date(filename: str) -> datetime.date:
    end = filename.index('.pdf')
    start = end - 10
    d = dateparser.parse(filename[start:end])
    # There are two formats for hawaiis dates:
    # _wp-content_uploads_2018_12_pop-reports-eom-2018-11-30.pdf and
    # _wp-content_uploads_2017_10_pop-reports-eom-2017-09-30-17.pdf
    if not d:
        start = end - 8
        d = dateparser.parse(filename[start:end])
    return d.date()


def _parse_table(filename: str) -> pd.DataFrame:
    """Parse the Head Count Endings and Contracted Facilities Tables."""
    all_dfs = tabula.read_pdf(
        filename,
        multiple_tables=True,
        lattice=True,
        pandas_options={
            'header': [0, 1],
        })

    head_count_ending_df = _df_matching_substring(
        all_dfs, {'total', 'head count ending'})
    head_count_ending_df = _format_head_count_ending(head_count_ending_df)

    facilities_df = _df_matching_substring(
        all_dfs, {'contracted facilities'})
    facilities_df = _format_contracted_facilities(facilities_df)

    result = head_count_ending_df.append(facilities_df, ignore_index=True)

    result['fips'] = result.facility_name.map(_facility_acronym_to_fips)
    result['facility_name'] = \
        result.facility_name.map(_facility_acronym_to_name)

    # Rows that may be NaN need to be cast as a float, otherwise use int
    string_columns = {'facility_name'}
    nullable_columns = {'design_bed_capacity', 'operation_bed_capacity'}
    int_columns = set(result.columns) - string_columns - nullable_columns

    for column_name in int_columns:
        result[column_name] = result[column_name].astype(int)
    for column_name in nullable_columns:
        result[column_name] = result[column_name].astype(float)

    return result


def _df_matching_substring(dfs: List[pd.DataFrame], strings: Iterable[str]) \
        -> pd.DataFrame:
    """Get the one df containing all the matching substrings."""
    matches: List[pd.DataFrame] = []
    for df in dfs:
        if all([_df_contains_substring(df, string) for string in strings]):
            matches.append(df)

    return more_itertools.one(matches)


def _df_contains_substring(df: pd.DataFrame, substring: str) -> bool:
    """Returns True if any entity in |df| contains the substring."""
    df_contains_str_mask = df.applymap(
        lambda element: substring.lower() in str(element).lower())
    return df_contains_str_mask.any().any()


def _format_head_count_ending(df: pd.DataFrame) -> pd.DataFrame:
    # Throw away the incorrectly parsed header rows
    df = df.iloc[3:].reset_index(drop=True)

    # Last row contains the totals
    df = df.iloc[:-1].reset_index(drop=True)

    # The pdf leaves an empty cell when nobody exists for that section
    df = df.fillna(0)

    # Since we can't parse the column_headers, just set them ourselves
    df.columns = _COLUMN_NAMES

    return df


def _format_contracted_facilities(df: pd.DataFrame) -> pd.DataFrame:
    # Throw away the incorrectly parsed header rows
    df = df.iloc[3:].reset_index(drop=True)

    # Last row contains the totals
    df = df.iloc[:-1].reset_index(drop=True)

    # The pdf leaves an empty cell when nobody exists for that section
    df = df.fillna(0)

    # Design/Operational Bed Capacity is never set for Contracted Facilities
    df.insert(1, 'design_bed_capacity', None)
    df.insert(2, 'operation_bed_capacity', None)

    # The last column is sometimes empty
    if len(df.columns) == len(_COLUMN_NAMES) + 1:
        df = df.drop(df.columns[-1], axis='columns')

    # Since we can't parse the column_headers, just set them ourselves
    df.columns = _COLUMN_NAMES

    return df


def _facility_acronym_to_name(facility_acronym: str) -> str:
    if facility_acronym not in _FACILITY_ACRONYM_TO_FIPS:
        raise AggregateIngestError(
            'Failed to match facility acronym "{}" to facility_name'.format(
                facility_acronym))

    return _FACILITY_ACRONYM_TO_NAME[facility_acronym]


def _facility_acronym_to_fips(facility_acronym: str) -> int:
    if facility_acronym not in _FACILITY_ACRONYM_TO_FIPS:
        raise FipsMergingError(
            'Failed to match facility acronym "{}" to fips'.format(
                facility_acronym))

    return _FACILITY_ACRONYM_TO_FIPS[facility_acronym]
