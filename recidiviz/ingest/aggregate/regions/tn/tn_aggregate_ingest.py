import numpy as np
import pandas as pd
import sys
import tabula

from recidiviz.ingest.aggregate import aggregate_ingest_utils


def parse(filename):

    tn_df = tabula.read_pdf(filename,
                            pages=[2, 3, 4], multiple_tables=True)

    formatted_dfs = []
    for df in tn_df:
        formatted_dfs.append(format_table(df))

    tn_df = pd.concat(formatted_dfs, reindex=True)

    #df.columns = df.iloc[0

    tn_df.to_csv('/tmp/shit.csv')

    import ipdb; ipdb.set_trace()


def format_table(df):
    import ipdb; ipdb.set_trace()
    df.columns = pd.MultiIndex.from_frame(df.iloc[0:4])
    df = df.iloc[4:]

    import ipdb; ipdb.set_trace()

    df.columns = aggregate_ingest_utils.collapse_header(df.columns)

    rename = {
        r'FACILITY': 'facility_name',
        r'TDOC Backup.*': 'tdoc_backup_population',
        r'Local': 'local_felons_population',
        r'Other .* Conv': 'other_convicted_felons_population',
        r'Federal & Others': 'federal_and_other_population',
        r'Conv\. Misd\.': 'convicted_misdemeanor_population',
        r'Pre- trial Felony': 'pretrial_felony_population',
        r'Pre- trial Misd\.': 'pretrial_misdemeanor_population',
        r'Total Jail Pop\.': 'total_jail_population',
        r'Total Beds\*\*': 'total_beds',
    }

    df = aggregate_ingest_utils.rename_columns_and_select(
        df, rename, use_regex=True)

    df = df.dropna(how='all')

    return df


def main(argv):
    parse('~/q/vera/analysis/tn_aggregate/JailJanuary2019.pdf')

if __name__ == "__main__":
    sys.exit(main(sys.argv))
