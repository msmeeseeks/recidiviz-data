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

"""Tests for utils/regions.py."""
import builtins
from unittest import TestCase

import pytest
from mock import mock_open, patch

from recidiviz.utils import regions

MANIFEST_CONTENTS = """
    regions:
      us_ny:
        agency_name: Department of Corrections and Community Supervision
        agency_type: prison
        base_url: http://nysdoccslookup.doccs.ny.gov
        names_file: us_ny_names.csv
        queue: us-ny-scraper
        region_code: us_ny
        scraper_package: us_ny
        timezone: America/New_York
      us_in:
        agency_name: Department of Corrections
        agency_type: prison
        base_url: https://www.in.gov/apps/indcorrection/ofs/ofs
        names_file: us_in_names.csv
        params:
          foo: bar
          sha: baz
        queue: us-in-scraper
        region_code: us_in
        scraper_class: a_different_scraper
        scraper_package: us_in
        timezone: America/Indiana/Indianapolis
      us_ca:
        agency_name: Corrections
        agency_type: jail
        base_url: test
        names_file: us_ca_names.csv
        params:
          foo: bar
          sha: baz
        queue: us-ca-scraper
        region_code: us_ca
        scraper_class: a_different_scraper_2
        scraper_package: us_ca
        timezone: America/Los_Angeles
    """

FULL_MANIFEST = {
    'regions': {
        'us_ny': {
            'agency_name': 'Department of Corrections and '
                           'Community Supervision',
            'agency_type': 'prison',
            'base_url': 'http://nysdoccslookup.doccs.ny.gov',
            'names_file': 'us_ny_names.csv',
            'queue': 'us-ny-scraper',
            'region_code': 'us_ny',
            'scraper_package': 'us_ny',
            'timezone': 'America/New_York'
        },
        'us_in': {
            'agency_name': 'Department of Corrections',
            'agency_type': 'prison',
            'base_url': 'https://www.in.gov/apps/indcorrection/ofs/ofs',
            'names_file': 'us_in_names.csv',
            'params': {
                'foo': 'bar',
                'sha': 'baz'
            },
            'queue': 'us-in-scraper',
            'region_code': 'us_in',
            'scraper_class': 'a_different_scraper',
            'scraper_package': 'us_in',
            'timezone': 'America/Indiana/Indianapolis'
        },
        'us_ca': {
            'agency_name': 'Corrections',
            'agency_type': 'jail',
            'base_url': 'test',
            'names_file': 'us_ca_names.csv',
            'params': {
                'foo': 'bar',
                'sha': 'baz'
            },
            'queue': 'us-ca-scraper',
            'region_code': 'us_ca',
            'scraper_class': 'a_different_scraper_2',
            'scraper_package': 'us_ca',
            'timezone': 'America/Los_Angeles'
        }
    }
}


class TestRegions(TestCase):
    """Tests for regions.py."""

    def setup_method(self, _test_method):
        regions.MANIFEST = None

    def teardown_method(self, _test_method):
        regions.MANIFEST = None

    def test_get_full_manifest(self):
        manifest = with_manifest(regions.get_full_manifest)
        assert manifest == FULL_MANIFEST

    def test_get_region_manifest(self):
        manifest = with_manifest(regions.get_region_manifest, 'us_ny')
        assert manifest == FULL_MANIFEST['regions']['us_ny']

    def test_get_region_manifest_not_found(self):
        with pytest.raises(Exception) as exception:
            with patch("builtins.open",
                       mock_open(read_data=MANIFEST_CONTENTS)) \
                    as mock_file:
                regions.get_region_manifest('us_az')

        assert str(exception.value) == "Region 'us_az' not found in manifest."
        mock_file.assert_called_with('region_manifest.yaml', 'r')

    def test_get_supported_regions(self):
        supported_regions = with_manifest(regions.get_supported_regions)
        self.assertCountEqual(
            [region.region_code for region in supported_regions],
            ['us_ny', 'us_in', 'us_ca'])

    def test_get_supported_region_codes(self):
        supported_regions = with_manifest(regions.get_supported_region_codes)
        assert supported_regions == ['us_ny', 'us_in', 'us_ca']

    def test_get_supported_region_codes_timezone(self):
        supported_regions = with_manifest(
            regions.get_supported_region_codes, timezone='America/New_York')
        assert supported_regions == ['us_ny', 'us_in']

    def test_get_supported_region_codes_full_manifest(self):
        supported_regions = with_manifest(regions.get_supported_region_codes,
                                          full_manifest=True)
        assert supported_regions == FULL_MANIFEST['regions']

    def test_validate_region_code_valid(self):
        assert with_manifest(regions.validate_region_code, 'us_in')

    def test_validate_region_code_invalid(self):
        assert not with_manifest(regions.validate_region_code, 'us_az')

    def test_get_scraper_module(self):
        region = with_manifest(regions.Region, 'us_ny')
        module = region.get_scraper_module()
        assert module.__name__ == \
               'recidiviz.ingest.scrape.regions.us_ny'

    def test_get_scraper(self):
        region = with_manifest(regions.Region, 'us_ny')
        scraper = region.get_scraper()
        assert type(scraper).__name__ == 'UsNyScraper'

    def test_region_class(self):
        region = with_manifest(regions.Region, 'us_ny')
        assert region.get_scraper_module().__name__ == \
               'recidiviz.ingest.scrape.regions.us_ny'
        assert not region.params
        assert region.queue == 'us-ny-scraper'
        assert region.scraper_class == 'us_ny_scraper'
        assert region.names_file == 'us_ny_names.csv'

    def test_region_class_with_scraper_class(self):
        region = with_manifest(regions.Region, 'us_in')
        assert region.params == {'foo': 'bar', 'sha': 'baz'}
        assert region.queue == 'us-in-scraper'
        assert region.scraper_class == 'a_different_scraper'

def mock_manifest_open(filename, mode):
    if filename == 'region_manifest.yaml':
        file_object = mock_open(read_data=MANIFEST_CONTENTS).return_value
        file_object.__iter__.return_value = MANIFEST_CONTENTS.splitlines(True)
        return file_object
    return open(filename, mode)

def with_manifest(func, *args, **kwargs):
    with patch("recidiviz.utils.regions.open", new=mock_manifest_open):
        return func(*args, **kwargs)
