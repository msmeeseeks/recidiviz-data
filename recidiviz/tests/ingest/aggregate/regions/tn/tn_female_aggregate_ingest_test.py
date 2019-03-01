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
"""Tests for tn_female_aggregate_ingest.py."""
import datetime
from unittest import TestCase

import pandas as pd
from pandas.util.testing import assert_frame_equal
import pytest
from more_itertools import one
from sqlalchemy import func

import recidiviz.common.constants.enum_canonical_strings as enum_strings
from recidiviz import Session
from recidiviz.ingest.aggregate.regions.tn import tn_female_aggregate_ingest
from recidiviz.persistence.database import database
from recidiviz.persistence.database.schema import TnFacilityFemaleAggregate
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils import fakes

_REPORT_DATE = datetime.date(year=2019, month=1, day=31)

# Cache the parsed pdf between tests since it's expensive to compute
@pytest.fixture(scope="class")
def parsed_pdf(request):
    request.cls.parsed_pdf = tn_female_aggregate_ingest.parse(
        fixtures.as_filepath('JailFemaleJanuary2019.pdf'))


@pytest.mark.usefixtures("parsed_pdf")
class TestTnFemaleAggregateIngest(TestCase):
    """Test that tn_female_aggregate_ingest correctly parses the TN PDF."""

    def setup_method(self, _test_method):
        fakes.use_in_memory_sqlite_database()

    def testParse_ParsesHeadAndTail(self):
        result = self.parsed_pdf[TnFacilityFemaleAggregate]

        # Assert Head
        expected_head = pd.DataFrame({
            'facility_name': ['Anderson', 'Bedford'],
            'tdoc_backup_population': [20, 4],
            'pretrial_misdemeanor_population': [39, 19],
            'female_jail_population': [85, 46],
            'female_beds': [85, 36],
            'local_felons_population': [2, 0],
            'other_convicted_felons_population': [2, 0],
            'federal_and_other_population': [0, 0],
            'convicted_misdemeanor_population': [5, 20],
            'pretrial_felony_population': [17, 3],
            'fips': [47001, 47003],
            'report_date': 2 * [_REPORT_DATE],
            'report_granularity': 2 * [enum_strings.monthly_granularity]
        })
        assert_frame_equal(result.head(n=2), expected_head, check_names=False)

        # Assert Tail
        expected_tail = pd.DataFrame({
            'facility_name': ['Williamson', 'Wilson'],
            'tdoc_backup_population': [7, 11],
            'pretrial_misdemeanor_population': [9, 27],
            'female_jail_population': [68, 127],
            'female_beds': [93, 100],
            'local_felons_population': [2, 11],
            'other_convicted_felons_population': [1, 2],
            'federal_and_other_population': [0, 19],
            'convicted_misdemeanor_population': [29, 27],
            'pretrial_felony_population': [20, 30],
            'fips': [47187, 47189],
            'report_date': 2 * [_REPORT_DATE],
            'report_granularity': 2 * [enum_strings.monthly_granularity]
        }, index=[118, 119])
        assert_frame_equal(result.tail(n=2), expected_tail, check_names=False)

    def testWrite_CalculatesSum(self):
        # Act
        for table, df in self.parsed_pdf.items():
            database.write_df(table, df)

        # Assert
        query = Session().query(
            func.sum(TnFacilityFemaleAggregate.female_jail_population))
        result = one(one(query.all()))

        expected_sum_female_jail_population = 5987
        self.assertEqual(result, expected_sum_female_jail_population)
