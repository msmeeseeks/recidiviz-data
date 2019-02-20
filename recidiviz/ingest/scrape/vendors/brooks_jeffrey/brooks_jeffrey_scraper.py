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

"""Scraper implementation for all websites using Brooks Jeffery marketing.
"""

import os
from copy import deepcopy
from typing import List, Optional, Tuple

import more_itertools

from recidiviz.common.constants.bond import BondStatus, BondType
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.ingest.scrape import constants, scraper_utils
from recidiviz.ingest.scrape.base_scraper import BaseScraper
from recidiviz.ingest.scrape.errors import ScraperError
from recidiviz.ingest.extractor.html_data_extractor import HtmlDataExtractor
from recidiviz.ingest.models.ingest_info import IngestInfo, Booking, Charge
from recidiviz.ingest.scrape.task_params import ScrapedData, Task


class BrooksJeffreyScraper(BaseScraper):
    """Scraper for counties using Brooks Jeffrey."""

    def __init__(self, region_name, mapping_filepath=None):
        if not mapping_filepath:
            mapping_filepath = os.path.join(
                os.path.dirname(__file__), 'brooks_jeffrey.yaml')
        self.mapping_filepath = mapping_filepath
        super(BrooksJeffreyScraper, self).__init__(region_name)

    def get_more_tasks(self, content, task: Task) -> List[Task]:
        content = deepcopy(content)
        content.make_links_absolute(self.get_region().base_url)

        params_list = []
        params_list.extend(_get_person_tasks(content))
        params_list.extend(_get_next_page_if_exists_tasks(content))
        return params_list

    def populate_data(self, content, task: Task, ingest_info: IngestInfo) \
            -> Optional[ScrapedData]:
        data_extractor = HtmlDataExtractor(self.mapping_filepath)
        content = _preprocess(content)
        ingest_info = data_extractor.extract_and_populate_data(
            content, ingest_info)
        booking = scraper_utils.one('booking', ingest_info)

        split_bonds, bond_status, charge_status, bond_type = \
            _parse_total_bond_if_necessary(booking)
        if any([split_bonds, bond_status, charge_status, bond_type]):
            booking.total_bond_amount = None

        booking.charges = _split_charges(booking, split_bonds, bond_status,
                                         charge_status, bond_type)

        return ScrapedData(ingest_info=ingest_info, persist=True)


def _preprocess(content):
    content = deepcopy(content)
    for cell in content.xpath('//*[contains(@class, "row")]'):
        cell.tag = 'tr'
    for cell in content.xpath('//*[contains(@class, "cell")]'):
        cell.tag = 'td'
    return content


def _get_next_page_if_exists_tasks(content) -> List[Task]:
    links = content.xpath('//a[@class="tbold"]')
    next_page_links = [link.xpath('./@href')[0] for link in links if
                       link.text_content() == ">>"]
    # There are multiple next page links on each roster page; however, they
    # are all equivalent, so choose the first one arbitrarily
    task_list = []
    if next_page_links:
        task_list.append(Task(
            task_type=constants.TaskType.GET_MORE_TASKS,
            endpoint=next_page_links[0],
        ))
    return task_list


def _get_person_tasks(content) -> List[Task]:
    links = content.xpath('//a[@class="text2"]')
    person_links = [link.xpath('./@href')[0] for link in links if
                    link.text_content() == "View Profile >>>"]
    task_list = []
    for person_link in person_links:
        task_list.append(Task(
            task_type=constants.TaskType.SCRAPE_DATA,
            endpoint=person_link,
        ))
    return task_list


# TODO(816): Replace with generic solution after issue is fixed.
def _parse_total_bond_if_necessary(
        booking: Booking) \
        -> Tuple[
            Optional[List[str]], Optional[BondStatus], Optional[ChargeStatus],
            Optional[BondType]]:
    """Looks at booking.total_bond_amount and, if necessary, parses it into a
    list of individual bond amounts or bond status."""
    charge_names = None
    bond_status = None
    charge_status = None
    bond_type = None
    if booking.total_bond_amount:
        normalized = booking.total_bond_amount.lower()
        split_bonds = _split(booking.total_bond_amount)
        if len(split_bonds) > 1:
            charge_names = split_bonds
        elif any([normalized.startswith(s) for s in
                  {'denied', 'no bond', 'none', 'parole'}]):
            bond_status = BondStatus.DENIED
        elif normalized.startswith('must see judge'):
            bond_status = BondStatus.PENDING
        elif normalized.startswith('sentenced'):
            charge_status = ChargeStatus.SENTENCED
        elif normalized.startswith('child sup'):
            bond_type = BondType.CASH
    return charge_names, bond_status, charge_status, bond_type


def _split_charges(
        booking: Booking, split_bond_amounts: Optional[List[str]],
        bond_status: Optional[BondStatus],
        charge_status: Optional[ChargeStatus],
        bond_type: Optional[BondType]) -> List[Charge]:
    """Splits the found charge into multiple charges and returns the split
    charges. Adds bond amounts and bond_statuses based on the provided
    values."""
    split_charges: List[Charge] = []
    if not booking.charges:
        return split_charges

    scraped_charge = more_itertools.one(booking.charges)
    split_charge_names = _split(scraped_charge.name)
    if split_bond_amounts and len(split_bond_amounts) != len(
            split_charge_names):
        raise ScraperError(
            'Mismatch, found {} individual bonds and {} charge_names'.format(
                len(split_bond_amounts), len(split_charge_names)))

    for idx, charge_name in enumerate(split_charge_names):
        split_charge = Charge(name=charge_name)
        if charge_status:
            split_charge.status = charge_status.value
        if split_bond_amounts or bond_status:
            bond = split_charge.create_bond()
            if split_bond_amounts:
                bond.amount = split_bond_amounts[idx]
            if bond_status:
                bond.status = bond_status.value
            if bond_type:
                bond.bond_type = bond_type.value
        split_charges.append(split_charge)

    return split_charges


def _split(repeated_object_str: str) -> List[str]:
    return list(filter(None, map(str.strip, repeated_object_str.split('\n'))))
