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


"""Scraper implementation for the Zuercher vendor. This handles all Zuercher
specific navigation and data extraction. All counties that use Zuercher should
have their county-specific scrapers inherit from this class.

Vendor-specific notes:
    - The scraper is fully json based

Background scraping procedure:
    1. An init call fetches various mappings (cell blocks, race, sex, agencies)
    2. A request for each batch of people gets their details
"""

import abc
import os
import re
from datetime import datetime
from typing import List, Optional

import pytz

from recidiviz.common.constants.charge import ChargeClass, ChargeStatus
from recidiviz.ingest import constants
from recidiviz.ingest.base_scraper import BaseScraper
from recidiviz.ingest.extractor.json_data_extractor import JsonDataExtractor
from recidiviz.ingest.models.ingest_info import IngestInfo, Booking, Charge
from recidiviz.ingest.task_params import ScrapedData, Task

_BATCH_SIZE = 100

class ZuercherScraper(BaseScraper):
    """Scraper for counties using Zuercher."""
    yaml = os.path.join(os.path.dirname(__file__), 'zuercher.yaml')

    # Start of line keys
    CHARGE_KEY = 'Charge:'
    WARRANT_CHARGE_KEY = 'Warrant Charge:'
    WARRANT_KEY = 'Warrant:'
    OTHER_KEY = 'Other'
    ADDITIONAL_HOLD_KEY = 'Additional Hold'
    EXTRADITON_KEY = 'Extraditon'
    FELONY_PROBATION_KEY = 'Probation-F'
    PAROLE_KEY = 'Parole'

    # Middle of line keys
    ARREST_DATE_KEY = 'Arrest Date'
    BOND_KEY = 'Bond'
    SET_BY_KEY = 'Set By'

    # Suffixes
    COUNTS_SUFFIX = 'Counts'
    DEGREE_SUFFIX = 'Degree'

    def __init__(self, region_name):
        super(ZuercherScraper, self).__init__(region_name)

    @staticmethod
    @abc.abstractmethod
    def get_jurisdiction_name() -> str:
        """Returns the name of this jurisdiction as used in warrants.

        Note this is not the same as the agency name. Ex.:
        'Douglas County, GA' as opposed to 'Douglas County Sheriff's Office'
        """

    def get_initial_task(self) -> Task:
        return Task(
            task_type=constants.TaskType.INITIAL_AND_MORE,
            endpoint='/'.join([
                self.get_region().base_url,
                'api/portal/inmates/init'
            ]),
            custom={
                # Supply timezone info explicitly so the isoformat is correct.
                'search_time':  datetime.now(tz=pytz.utc).isoformat(),
            },
            response_type=constants.ResponseType.JSON,
        )

    def get_more_tasks(self, content, task: Task) -> List[Task]:
        start = 0
        if task.json:
            start = task.json['paging']['start'] + _BATCH_SIZE

        task_type = constants.TaskType.SCRAPE_DATA
        if self.is_initial_task(task.task_type) or \
                start + _BATCH_SIZE <= content['total_record_count']:
            task_type |= constants.TaskType.GET_MORE_TASKS

        return [Task(
            task_type=task_type,
            endpoint='/'.join([
                self.get_region().base_url,
                'api/portal/inmates/load'
            ]),
            json={
                "name": "",
                "race": "all",
                "sex": "all",
                "cell_block": "all",
                "held_for_agency": "any",
                "in_custody": task.custom['search_time'],
                "paging": {
                    "count": _BATCH_SIZE,
                    "start": start,
                },
                "sorting": {
                    "sort_by_column_tag": "name",
                    "sort_descending": False,
                },
            },
            custom={
                # Pass along the `search_time`
                'search_time': task.custom['search_time'],
            },
            response_type=constants.ResponseType.JSON,
        )]

    def populate_data(self, content, task: Task,
                      ingest_info: IngestInfo) -> Optional[ScrapedData]:
        content = self._preprocess_json(content)
        data_extractor = JsonDataExtractor(self.yaml)
        ingest_info = data_extractor.extract_and_populate_data(
            content, ingest_info)
        for person in ingest_info.people:
            for booking in person.bookings:
                for charge in booking.charges:
                    self._postprocess_charge(booking, charge)
        return ScrapedData(ingest_info=ingest_info, persist=True)

    @staticmethod
    def _preprocess_json(content):
        records = content['records']
        for record in records:
            hold_reasons_text = record.pop('hold_reasons')
            if hold_reasons_text:
                hrs = hold_reasons_text.split('<br />')
                record['charges'] = [hr.strip() for hr in hrs if hr]
        return content

    def _postprocess_charge(self, booking: Booking, charge: Charge):
        """Pull unstructured information out of charge name to fill charge"""
        if not charge.name:
            raise ValueError('Unexpected empty charge.')

        charge_text = charge.name
        charge.name = None

        # Split the charge on ';', skipping any within parentheses. Then
        # strip any whitespace and filter out any empty strings.
        charge_kvs = [text.strip()
                      for text in re.split(r';(?!.*\))', charge_text)
                      if text]

        # Sometimes a ';' ends up in the charge description. To handle this we
        # iterate backwards and if we don't recognize information we keep it
        # around and stick it in the charge description.
        suffix = ''
        for charge_kv in reversed(charge_kvs):
            suffix_added = False
            if (charge_kv.startswith(self.CHARGE_KEY) or
                    charge_kv.startswith(self.WARRANT_CHARGE_KEY)):
                if charge_kv.startswith(self.CHARGE_KEY):
                    charge_info = charge_kv[len(self.CHARGE_KEY):].strip()
                else:
                    charge_kv = charge_kv[len(self.WARRANT_CHARGE_KEY):].strip()
                    warrant_info, charge_info = charge_kv.split('(', maxsplit=1)
                    if 'issued by' in warrant_info:
                        jurisdiction = warrant_info[warrant_info.index(
                            'issued by') + len('issued by'):].strip()
                        if self.get_jurisdiction_name() not in jurisdiction:
                            booking.create_hold(jurisdiction_name=jurisdiction)
                    charge.charge_notes = warrant_info.strip()
                    charge_info = charge_info.rstrip(')')

                if suffix:
                    charge_info += suffix
                    suffix = ''

                # Pull out the statute, charge name, and other information
                charge.statute, charge_name, *rest = charge_info.split(' - ')

                # Parse anything in parentheses at the end of the charge name.
                charge_name = self._parse_parentheses(charge_name, charge)

                # Check if any suffix can be parsed
                charge_name_split = charge_name.rsplit('-', maxsplit=1)
                if len(charge_name_split) == 2 and charge_name_split[1]:
                    if charge_name_split[1].endswith(self.DEGREE_SUFFIX):
                        charge_name, degree = charge_name_split
                        charge.degree = degree[:-len(self.DEGREE_SUFFIX)]
                    elif ChargeClass.can_parse(charge_name_split[1],
                                               self.get_enum_overrides()):
                        charge_name, charge.charge_class = charge_name_split

                charge.name = charge_name

                for item in rest:
                    item = self._parse_parentheses(item, charge)
                    if item.endswith(self.COUNTS_SUFFIX):
                        charge.number_of_counts = \
                            item[:-len(self.COUNTS_SUFFIX)].strip()
                    elif ChargeClass.can_parse(item, self.get_enum_overrides()):
                        charge.charge_class = item
                    else:
                        raise ValueError('Unexpected charge info "{}" in '
                                         'charge: "{}"'.format(rest, charge_kv))

            elif charge_kv.startswith(self.WARRANT_KEY):
                if 'Unspecified' not in charge_kv:
                    raise ValueError(
                        'Unexpected warrant: "{}"'.format(charge_kv))
                booking.create_hold()
            elif charge_kv.startswith(self.ARREST_DATE_KEY):
                charge.offense_date = charge_kv[len(
                    self.ARREST_DATE_KEY):].strip()
            elif charge_kv.startswith(self.BOND_KEY):
                _, bond_text = charge_kv.split(' - ')
                bond_text_split = list(map(str.strip, bond_text.split(',')))
                bond_type = bond_amount = None
                if len(bond_text_split) == 1:
                    if bond_text_split[0].startswith('$'):
                        bond_amount = bond_text_split[0]
                    else:
                        bond_type = bond_text_split[0]
                elif len(bond_text_split) == 2:
                    bond_type, bond_amount = bond_text_split
                else:
                    raise ValueError('Unexpected bond: "{}"'.format(charge_kv))
                charge.create_bond(amount=bond_amount, bond_type=bond_type)
            elif charge_kv.startswith(self.SET_BY_KEY):
                set_by = charge_kv[len(self.SET_BY_KEY):].strip()
                if set_by.startswith('Judge'):
                    judge_name = set_by[len('Judge'):].strip()
                    if judge_name:
                        charge.judge_name = judge_name
            elif charge_kv.startswith(self.OTHER_KEY):
                # Skip 'Other' charges but ensure they are related to our county
                if self.get_region().agency_name not in charge_kv:
                    raise ValueError(
                        'Unexpected other charge: "{}"'.format(charge_kv))
                booking.prune()
            elif charge_kv.startswith(self.ADDITIONAL_HOLD_KEY):
                # Skip 'Additional' holds but ensure they are related to our
                # county
                if self.get_region().agency_name not in charge_kv:
                    raise ValueError(
                        'Unexpected additional hold: "{}"'.format(charge_kv))
                booking.prune()
            elif charge_kv.startswith(self.PAROLE_KEY):
                if self.get_region().agency_name not in charge_kv:
                    raise ValueError(
                        'Unexpected parole violation: "{}"'.format(charge_kv))
                charge.name = charge_kv
            elif charge_kv.startswith(self.EXTRADITON_KEY):
                if not charge_kv == 'Extraditon for extradition':
                    raise ValueError(
                        'Unexpected extraditon: "{}"'.format(charge_kv))
                charge.name = charge_kv
            elif charge_kv.startswith(self.FELONY_PROBATION_KEY):
                if 'Unspecified' not in charge_kv:
                    raise ValueError(
                        'Unexpected felony parole: "{}"'.format(charge_kv))
                charge.name = charge_kv
            else:
                suffix = '; ' + charge_kv + suffix
                suffix_added = True

            if suffix and not suffix_added:
                break

        if suffix:
            raise ValueError('Suffix "{}" was not used: "{}"'.format(
                suffix, charge_text))

    def _parse_parentheses(self, text, charge):
        text, *rest = map(str.strip, text.split('('))
        for item in rest:
            item = item.rstrip(')')
            if ChargeStatus.can_parse(item, self.get_enum_overrides()):
                charge.status = item
            elif ChargeClass.can_parse(item, self.get_enum_overrides()):
                charge.charge_class = item
        return text
