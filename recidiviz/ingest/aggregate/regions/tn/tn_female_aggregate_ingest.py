# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Ingest TN female aggregate jail data.
"""
import os
from typing import Dict

import datetime
import dateparser
import pandas as pd
import tabula
import us

import recidiviz.common.constants.enum_canonical_strings as enum_strings
from recidiviz.ingest.aggregate import aggregate_ingest_utils, fips
from recidiviz.persistence.database.schema import TnFacilityFemaleAggregate


_MANUAL_FACILITY_TO_COUNTY_MAP = {
    'Johnson City (F)': 'Washington',
    'Kingsport City': 'Sullivan',
}


def parse(filename: str) -> Dict:

    table = tabula.read_pdf(filename, pages=[2, 3, 4], multiple_tables=True)

    formatted_dfs = []
    for df in table:
        formatted_dfs.append(_format_table(df))

    table = pd.concat(formatted_dfs, ignore_index=True)

    # Discard 'TOTAL' row.
    table = table.iloc[:-1]

    table = aggregate_ingest_utils.cast_columns_to_int(
        table, ignore_columns={'facility_name'})

    names = table.facility_name.apply(_pretend_facility_is_county)
    table = fips.add_column_to_df(table, names, us.states.TN)

    table['report_date'] = _parse_date(filename)
    table['report_granularity'] = enum_strings.monthly_granularity

    return {
        TnFacilityFemaleAggregate: table
    }


def _parse_date(filename: str) -> datetime.date:
    base_filename = os.path.basename(filename)
    end = base_filename.index('.pdf')
    start = 10
    d = dateparser.parse(base_filename[start:end]).date()
    return aggregate_ingest_utils.on_last_day_of_month(d)


def _format_table(df: pd.DataFrame) -> pd.DataFrame:
    """Format the dataframe that comes from one page of the PDF."""

    # The first four rows are parsed containing the column names.
    df.columns = df.iloc[:4].apply(lambda rows: ' '.join(rows.dropna()).strip())
    df = df.iloc[4:]

    rename = {
        r'FACILITY': 'facility_name',
        r'TDOC Backup.*': 'tdoc_backup_population',
        r'FEMALE POPULATION': 'smashed',
        r'Pre- trial.*': 'pretrial_misdemeanor_population',
        r'Female Jail Pop\.': 'female_jail_population',
        r'Female Beds\*\*': 'female_beds',
    }

    df = aggregate_ingest_utils.rename_columns_and_select(
        df, rename, use_regex=True)

    df = df.dropna(how='all')

    # Five columns get smashed together on parse
    smashed_cols = [
        'local_felons_population',
        'other_convicted_felons_population',
        'federal_and_other_population',
        'convicted_misdemeanor_population',
        'pretrial_felony_population',
    ]

    for ind, col_name in enumerate(smashed_cols):
        df[col_name] = df['smashed'].map(lambda element: element.split()[ind])

    df = df.drop('smashed', axis=1)

    return df


def _pretend_facility_is_county(facility_name: str) -> str:
    """Format facility_name like a county_name to match each to a fips."""
    if facility_name in _MANUAL_FACILITY_TO_COUNTY_MAP:
        return _MANUAL_FACILITY_TO_COUNTY_MAP[facility_name]

    words_after_county_name = [
        '-',
        'Annex',
        'Co. Det. Center',
        'Det. Center',
        'Det, Center',
        'Extension',
        'Jail',
        'SCCC',
        'Work Center',
        'Workhouse',
    ]
    for delimiter in words_after_county_name:
        facility_name = facility_name.split(delimiter)[0]

    return facility_name
