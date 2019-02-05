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
import html
import os
import re
from datetime import datetime
from typing import List, Optional, Set, Tuple

import pytz

from recidiviz.common.constants.bond import BondType
from recidiviz.common.constants.charge import ChargeClass, ChargeStatus
from recidiviz.ingest import constants
from recidiviz.ingest.base_scraper import BaseScraper
from recidiviz.ingest.extractor.json_data_extractor import JsonDataExtractor
from recidiviz.ingest.models.ingest_info import (Bond, Booking, Charge,
                                                 IngestInfo)
from recidiviz.ingest.task_params import ScrapedData, Task
from recidiviz.persistence.converter import converter_utils

_BATCH_SIZE = 100

class ZuercherScraper(BaseScraper):
    """Scraper for counties using Zuercher."""
    yaml = os.path.join(os.path.dirname(__file__), 'zuercher.yaml')

    # Start of line keys
    CHARGE_KEY = 'Charge:'
    COURT_ORDER_KEY = 'Court Order:'
    DRUG_COURT_SANCTION_KEY = 'Drug Court Sanction:'

    WARRANT_KEY = 'Warrant:'
    PROBATION_REVOCATION_KEY = 'Probation Revocation:'

    UNSPECIFIED_WARRANT_KEY = 'Unspecified Warrant:'
    FELONY_PROBATION_KEY = 'Probation-F'
    PAROLE_KEY = 'Parole'
    SENTENCED_KEY = 'Sentenced:'
    WEEKENDER_SENTENCE_KEY = 'Weekender Sentence:'
    BONDSMAN_OFF_BOND_KEY = 'Bondsman off Bond:'
    OTHER_KEY = 'Other'

    ADDITIONAL_HOLD_KEY = 'Additional Hold'
    HOLD_KEY = 'Hold for'
    EXTRADITION_KEY = 'Extraditon'  # Missing 'i' is purposeful
    BOARDER_KEY = 'Boarder'
    RE_BOOK_KEY = 'RE-BOOK'
    DRC_SANCTION_KEY = 'DRC Sanction'  # Day Reporting Center

    # Middle of line keys
    ARREST_DATE_KEY = 'Arrest Date'
    BOND_KEY = 'Bond'
    SET_BY_KEY = 'Set By'

    # Suffixes
    COUNTS_SUFFIX = 'Counts'
    DEGREE_SUFFIX = 'Degree'

    def __init__(self, region_name):
        super(ZuercherScraper, self).__init__(region_name)
        self.plain_charge_keys = {
            self.CHARGE_KEY, self.COURT_ORDER_KEY, self.DRUG_COURT_SANCTION_KEY,
        }
        self.warrant_with_charge_keys = {
            self.WARRANT_KEY, self.PROBATION_REVOCATION_KEY,
        }
        self.hold_keys = {
            self.BOARDER_KEY, self.ADDITIONAL_HOLD_KEY, self.HOLD_KEY,
            self.EXTRADITION_KEY, self.DRC_SANCTION_KEY, self.RE_BOOK_KEY,
        }

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
                booking.prune()
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

        charge_text = html.unescape(charge.name)
        charge.name = None

        # Split the charge on ';', strip any whitespace, and filter out any
        # empty strings.
        first_kv, *later_kvs = filter(None, map(str.strip,
                                                charge_text.split(';')))

        for kv in later_kvs:
            if _starts_with_key(kv, {self.ARREST_DATE_KEY}):
                charge.offense_date = _skip_key_prefix(kv,
                                                       {self.ARREST_DATE_KEY})
            elif _starts_with_key(kv, {self.BOND_KEY}):
                _parse_bond(kv, booking, charge)
            elif _starts_with_key(kv, {self.SET_BY_KEY}):
                set_by = _skip_key_prefix(kv, {self.SET_BY_KEY})
                if set_by.startswith('Judge'):
                    judge_name = set_by[len('Judge'):].strip()
                    if judge_name:
                        charge.judge_name = judge_name
            else:
                # If it doesn't match, then a ';' was in the charge description
                # so add it back to the first.
                first_kv += '; ' + kv

        if _starts_with_key(first_kv, self.plain_charge_keys):
            charge_info = _skip_key_prefix(first_kv, self.plain_charge_keys)
            self._parse_charge(charge_info, charge)
        elif _starts_with_key(first_kv, self.warrant_with_charge_keys):
            info = _skip_key_prefix(first_kv, self.warrant_with_charge_keys)
            warrant_info, [charge_info] = _paren_tokenize(info)

            _, jurisdiction = _split_by_substring(warrant_info, 'issued by')
            if (jurisdiction and
                    self.get_jurisdiction_name() not in jurisdiction):
                # If it is a different jurisdiction, get rid of the charge
                # info and just create a hold.
                _clear_charge(charge)
                booking.create_hold(jurisdiction_name=jurisdiction)
            else:
                self._parse_charge(charge_info, charge)
                charge.charge_notes = warrant_info.strip()
        elif _starts_with_key(first_kv, {self.SENTENCED_KEY}):
            charge_info = _skip_key_prefix(first_kv, {self.SENTENCED_KEY})
            self._parse_charge(charge_info, charge)
            charge.status = 'SENTENCED'
        elif _starts_with_key(first_kv, {self.BONDSMAN_OFF_BOND_KEY}):
            # Note: Must be placed above BOND_KEY check.
            # Skip these
            _clear_charge(charge)
        elif _starts_with_key(first_kv, {self.UNSPECIFIED_WARRANT_KEY}):
            if 'Unspecified' not in first_kv:
                raise ValueError(
                    'Unexpected warrant: "{}"'.format(first_kv))
            charge.name = first_kv
        elif _starts_with_key(first_kv, {self.WEEKENDER_SENTENCE_KEY}):
            info = _skip_key_prefix(first_kv, {self.WEEKENDER_SENTENCE_KEY})
            length, _relationship_type = map(str.strip, info.split(' - '))
            if length.startswith('Serving'):
                length = length[len('Serving'):].strip()

            # TODO(#441): Add sentence relationship once in `IngestInfo`
            charge.create_sentence(min_length=length, max_length=length)
        elif _starts_with_key(first_kv, {self.OTHER_KEY}):
            # Skip 'Other' charges but ensure they are related to our county
            if self.get_region().agency_name not in first_kv:
                raise ValueError(
                    'Unexpected other charge: "{}"'.format(first_kv))
            _clear_charge(charge)
        elif _starts_with_key(first_kv, self.hold_keys):
            hold_info = _skip_key_prefix(first_kv, self.hold_keys)
            _jurisdiction_type, jurisdiction_name = _split_by_substring(
                hold_info, 'for')
            # Only create a hold if it is for a different jurisdiction
            if (jurisdiction_name and
                    self.get_region().agency_name not in jurisdiction_name):
                booking.create_hold(jurisdiction_name=jurisdiction_name)
                _clear_charge(charge)
            else:
                charge.name = first_kv
        elif _starts_with_key(first_kv, {self.PAROLE_KEY}):
            if self.get_region().agency_name not in first_kv:
                raise ValueError(
                    'Unexpected parole violation: "{}"'.format(first_kv))
            charge.name = first_kv
        elif _starts_with_key(first_kv, {self.FELONY_PROBATION_KEY}):
            if 'Unspecified' not in first_kv:
                raise ValueError(
                    'Unexpected felony parole: "{}"'.format(first_kv))
            charge.name = first_kv
        else:
            # If we don't know what it is then just parse it as a charge.
            self._parse_charge(first_kv, charge)

    def _parse_charge(self, charge_info: str, charge: Charge):
        """Parse out the information from `charge_info` to fill `charge`."""
        # Pull out the statute, charge name, and other information.
        # First replace any ' - ' inside parens with '-', then split on
        # ' - '.
        charge_info = _replace_inside_parens(charge_info, ' - ', '-')
        charge_info_split = charge_info.split(' - ')
        if len(charge_info_split) == 1:
            charge_name, *rest = charge_info_split
        else:
            charge.statute, charge_name, *rest = charge_info_split

        # Parse anything in parentheses at the end of the charge name.
        charge_name, extra = \
            self._parse_parentheses(charge_name, charge)

        # Check if any suffix can be parsed
        charge_name_split = charge_name.rsplit('-', maxsplit=1)
        if len(charge_name_split) == 2 and charge_name_split[1]:
            if charge_name_split[1].endswith(self.DEGREE_SUFFIX):
                charge_name, degree = charge_name_split
                charge.degree = \
                    degree[:-len(self.DEGREE_SUFFIX)].strip()
            elif ChargeClass.can_parse(charge_name_split[1],
                                       self.get_enum_overrides()):
                charge_name, charge.charge_class = charge_name_split

        charge.name = charge_name

        for item in reversed(rest):
            item, dash_extra = self._parse_parentheses(item, charge)
            extra.extend(dash_extra)
            if item.endswith(self.COUNTS_SUFFIX):
                charge.number_of_counts = \
                    item[:-len(self.COUNTS_SUFFIX)].strip()
            elif ChargeClass.can_parse(item, self.get_enum_overrides()):
                charge.charge_class = item
            elif item.endswith(self.DEGREE_SUFFIX.upper()):
                charge.degree = item[:-len(self.DEGREE_SUFFIX)].strip()
            else:
                charge.name += ' - ' + item

        charge.name = ' '.join([charge.name] + extra)

    def _parse_parentheses(self, charge_text: str, charge: Charge) \
            -> Tuple[str, List[str]]:
        """Parse any information in parentheses and add to `charge`.

        If we are unable to parse a token into a charge specific item, we return
        it as a part of the `unused` list.
        """
        text, rest = _paren_tokenize(charge_text)
        unused = []
        for item in rest:
            if ChargeStatus.can_parse(item, self.get_enum_overrides()):
                charge.status = item
            elif ChargeClass.can_parse(item, self.get_enum_overrides()):
                charge.charge_class = item
            else:
                unused.append('({})'.format(item))
        return text, unused

    def get_enum_overrides(self):
        return {
            # Races
            'NOT SPECIFIED': None,

            # Charge Classes
            'MISD': ChargeClass.MISDEMEANOR,

            # Bond Types
            'CASH ONLY': BondType.CASH,
            'OTHER': None,
        }

def _starts_with_key(text: str, keys: Set[str]) -> bool:
    for key in keys:
        if re.match(key, text, re.I):
            return True
    return False

def _skip_key_prefix(text: str, keys: Set[str]) -> str:
    for key in keys:
        if re.match(key, text, re.I):
            return text[len(key):].strip()
    raise ValueError(
        'Text "{}" does not start with any key: {}'.format(text, keys))

def _clear_charge(charge):
    # Clear the charge
    charge.offense_date = None
    charge.judge_name = None
    charge.bond = None

def _parse_bond(text: str, booking: Booking, charge: Charge):
    """Parse out any bond related information from `text`."""
    _, bond_text = text.split(' - ')
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
        raise ValueError('Unexpected bond: "{}"'.format(text))

    if bond_type:
        if 'Blanket' in bond_type:
            # Make all the charges point to the single blanket bond.
            for other_charge in booking.charges:
                if other_charge.bond:
                    charge.bond = other_charge.bond
        elif 'Bound Over to Superior Court' in bond_type:
            charge.status = 'PRETRIAL'
            return
        elif 'Own Recognizance' in bond_type:
            return

    if not charge.bond:
        charge.bond = Bond()

    if bond_type:
        bond_type, _ = _paren_tokenize(bond_type)
        # 'Blanket' isn't a bond type so if that's all we have, skip it.
        if bond_type != 'Blanket':
            charge.bond.bond_type = bond_type
    if bond_amount and (not charge.bond.amount or \
                        not converter_utils.parse_dollars(charge.bond.amount)):
        charge.bond.amount = bond_amount

def _paren_tokenize(text: str) -> Tuple[str, List[str]]:
    """Pulls out any information in first level parentheses and between them.

    Example:
    'Murder (Felony) 1st degree (Guilty (CCH-310))'
    becomes
    'Murder', ['Felony', '1st degree', 'Guilty (CCH-310)']
    """
    text, *rest = text.split('(', maxsplit=1)
    tokens = []
    while rest:
        # find the closing bracket
        [next_text] = rest
        split_index = _find_matching_close_paren(next_text)
        token, next_text = next_text[:split_index], next_text[split_index + 1:]
        if token.strip():
            tokens.append(token.strip())
        token, *rest = next_text.split('(', maxsplit=1)
        if token.strip():
            tokens.append(token.strip())
    return text.strip(), tokens

def _find_matching_close_paren(text: str) -> int:
    num_unmatched_open = 1
    for i, c in enumerate(text):
        if c == '(':
            num_unmatched_open += 1
        if c == ')':
            num_unmatched_open -= 1
        if not num_unmatched_open:
            return i
    raise ValueError('No matching close paren in "{}"'.format(text))

def _split_by_substring(text: str, substring: str) \
        ->  Tuple[str, Optional[str]]:
    index = text.find(substring)
    if index == -1:
        return text, None
    return text[:index].strip(), text[index + len(substring):].strip()

def _replace_inside_parens(text: str, find: str, replace: str) -> str:
    for match in re.finditer(r'\(.*?\)', text):
        text = (text[:match.start()] +
                match[0].replace(find, replace) +
                text[match.end():])
    return text
