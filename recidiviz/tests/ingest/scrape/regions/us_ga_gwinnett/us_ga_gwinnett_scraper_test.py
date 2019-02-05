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

"""Scraper tests for us_ga_gwinnett."""
import unittest

from recidiviz.ingest.scrape.constants import TaskType
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.scrape.task_params import Task
from recidiviz.ingest.scrape.regions.us_ga_gwinnett.us_ga_gwinnett_scraper \
    import UsGaGwinnettScraper
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils.base_scraper_test import BaseScraperTest

_PAGE = fixtures.as_dict('us_ga_gwinnett', 'AddMoreTasks.json')


class TestUsGaGwinnettScraper(BaseScraperTest, unittest.TestCase):
    """Tests for UsGaGwinnettScraper"""
    def _init_scraper_and_yaml(self):
        self.scraper = UsGaGwinnettScraper()

    def test_get_more_tasks(self):
        task = self.scraper.first_add_more_results_task()
        updated_json = task.json.copy()
        updated_json['RecordsLoaded'] = '10'
        expected_result = Task.evolve(task,
                                      task_type=TaskType.SCRAPE_DATA_AND_MORE,
                                      json=updated_json)

        self.validate_and_return_get_more_tasks(_PAGE, task, [expected_result])

    def test_populate_data(self):
        expected_info = IngestInfo()
        p1 = expected_info.create_person(person_id='Gxxxx',
                                         full_name='NAMEDSFJFDS\r',
                                         gender='\t\t\t\t\t\t\t\tMALE\r',
                                         race='\t\t\t\t\t\t\t\t\xa0 W\r',
                                         age='26',
                                         place_of_residence='sdsfhkjgsdfzj')
        b1 = p1.create_booking(booking_id='asdfg',
                               admission_date='01/11/1234 15:43:14',
                               custody_status='In Jail')
        c1 = b1.create_charge(statute='99-99999',
                              name='HOLD FOR COURT',
                              fee_dollars='$0.00',
                              case_number='000-0000 (GWINNETT COUNTY SHERIFF)')
        c1.create_bond(amount='NO BOND', bond_type='NO BOND')
        p2 = expected_info.create_person(person_id='sfdghjfdg',
                                         full_name='xsqdsfNAME\r',
                                         gender='\t\t\t\t\t\t\t\tFEMALE\r',
                                         race='\t\t\t\t\t\t\t\t\xa0 B\r',
                                         age='43',
                                         place_of_residence='aDSFZgjk')
        p2.create_booking(booking_id='asdfgfsdsfgs',
                          admission_date='01/25/2019 15:08:53',
                          custody_status='In Jail')

        with self.assertWarns(UserWarning):
            self.validate_and_return_populate_data(_PAGE, expected_info)
