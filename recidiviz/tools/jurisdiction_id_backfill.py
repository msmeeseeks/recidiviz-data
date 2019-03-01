import glob
import os
from typing import Optional

import pandas as pd
import us
import yaml
from more_itertools import one

import recidiviz.ingest.scrape.regions
from recidiviz.ingest.aggregate import fips
from recidiviz.tests.ingest.fixtures import as_filepath

_REGIONS_DIR = os.path.dirname(recidiviz.ingest.scrape.regions.__file__)
_DATA_SETS_DIR = os.path.dirname(
    recidiviz.ingest.aggregate.__file__) + '/data_sets'

_JID = pd.read_csv(_DATA_SETS_DIR + '/jid.csv')


def main():
    all_manifests_filenames = glob.glob(_REGIONS_DIR + '/**/manifest.yaml')

    for manifest_filename in all_manifests_filenames:
        # Read county_name from manifest & state
        region_name = manifest_filename.split('/')[-2]
        state_name = region_name.split('_')[1]
        county_name = ' '.join(region_name.split('_')[2:])

        # Skip state level manifest files
        if not county_name:
            continue

        _to_jurisdiction_id(county_name, us.states.lookup(state_name))


# def _get_jid(county_name: str, state: us.states) -> int:
    # Join each county_name/state to County FIPS
    # county_fips = _to_county_fips(county_name, state)

    # Join FIPS to jid.csv

    # Fuzzy match agency_name to jurisdiction_name

    # y = yaml.load(open(manifest))
    # print(y)

    # If confident match, append jurisdiction_name to manifest, else debug


def _to_county_fips(manifest_county_name: str, state: us.states) -> int:
    """Lookup fips by manifest_county_name, filtering within the given state"""
    fips_for_state = fips._get_fips_for(state)
    actual_county_name = fips._best_match(manifest_county_name,
                                          fips_for_state.index)

    return fips_for_state.at[actual_county_name, 'fips']


def _to_jurisdiction_id(agency_name: str, county_fips: int) -> str:
    """Lookup jurisdction_id by manifest_agency_name, filtering within the
    given county_fips"""
    jids_matching_county_fips = _JID.loc[_JID['fips'] == county_fips]

    # If only one jurisdiction in the county, assume it's a match
    if len(jids_matching_county_fips) == 1:
        return one(jids_matching_county_fips['jid'])

    actual_jurisdcition_name = fips._best_match(
        agency_name, jids_matching_county_fips['name'])

    print(agency_name + ' | ' + actual_jurisdcition_name)


if __name__ == "__main__":
    main()
