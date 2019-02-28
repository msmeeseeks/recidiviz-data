import numpy as np
import pandas as pd
import sys
import tabula
import us

from recidiviz.ingest.aggregate import aggregate_ingest_utils, fips
from recidiviz.persistence.database.schema import TnFacilityAggregate


def parse(filename):

    table = tabula.read_pdf(filename,
                            # pandas_options={'header': [0, 1, 2, 3]},
                            # pages=[2, 3],
                            pages=[2, 3, 4],
                            multiple_tables=True
    )

    formatted_dfs = []
    for df in table:
        formatted_dfs.append(format_table(df))

    table = pd.concat(formatted_dfs, ignore_index=True)

    # Discard 'TOTAL' row.
    table = table.iloc[:-1]

    table = fips.add_column_to_df(table, table['facility_name'], us.states.TN)

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


def main(argv):
    parse('~/q/vera/analysis/tn_aggregate/JailJanuary2019.pdf')

if __name__ == "__main__":
    sys.exit(main(sys.argv))
