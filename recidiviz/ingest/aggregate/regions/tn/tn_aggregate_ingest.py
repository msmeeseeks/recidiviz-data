import datetime
import numpy as np
import pandas as pd
import sys
import tabula
import us

import recidiviz.common.constants.enum_canonical_strings as enum_strings
from recidiviz.ingest.aggregate import aggregate_ingest_utils, fips
from recidiviz.persistence.database.schema import TnFacilityAggregate


_MANUAL_FACILITY_TO_COUNTY_MAP = {
    'Johnson City (F)': 'Washington',
    'Kingsport City': 'Sullivan',
}


def parse(filename):

    table = tabula.read_pdf(filename, pages=[2, 3, 4], multiple_tables=True, pandas_options={'dtype': 'int64'})

    formatted_dfs = []
    for df in table:
        formatted_dfs.append(format_table(df))

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
        TnFacilityAggregate: table
    }

def _parse_date(filename):
    return datetime.date(year=2019, month=1, day=31)

def format_table(df):

    # The first four rows are parsed containing the column names.
    df.columns = df.iloc[:4].apply(lambda rows: ' '.join(rows.dropna()).strip())
    df = df.iloc[4:]

    rename = {
        r'FACILITY': 'facility_name',
        r'TDOC Backup.*': 'tdoc_backup_population',
        r'Local': 'local_felons_population',
        r'Other .* Conv.*': 'other_convicted_felons_population',
        r'Conv\. Misd\.': 'convicted_misdemeanor_population',
        r'Pre- trial Felony': 'pretrial_felony_population',
        r'Pre- trial Misd\.': 'pretrial_misdemeanor_population',
        r'Total Jail Pop\.': 'total_jail_population',
        r'Total Beds\*\*': 'total_beds',
    }

    df = aggregate_ingest_utils.rename_columns_and_select(
        df, rename, use_regex=True)

    df = df.dropna(how='all')

    df['federal_and_other_population'] = df[
        'other_convicted_felons_population'].map(
            lambda element: element.split()[1])
    df['other_convicted_felons_population'] = df[
        'other_convicted_felons_population'].map(
            lambda element: element.split()[0])

    return df


def _pretend_facility_is_county(facility_name: str):
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
