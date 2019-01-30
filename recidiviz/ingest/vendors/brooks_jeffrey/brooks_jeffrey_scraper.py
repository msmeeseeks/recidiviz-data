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
from typing import List, Optional, Tuple

import more_itertools

from recidiviz.common.constants.bond import BondStatus
from recidiviz.ingest import constants
from recidiviz.ingest.base_scraper import BaseScraper
from recidiviz.ingest.errors import ScraperError
from recidiviz.ingest.extractor.html_data_extractor import HtmlDataExtractor
from recidiviz.ingest.models.ingest_info import IngestInfo, Booking, Charge
from recidiviz.ingest.task_params import ScrapedData, Task


class BrooksJeffreyScraper(BaseScraper):
    """Scraper for counties using Brooks Jeffrey."""

    def __init__(self, region_name, mapping_filepath=None):
        if not mapping_filepath:
            mapping_filepath = os.path.join(
                os.path.dirname(__file__), 'brooks_jeffrey.yaml')
        self.mapping_filepath = mapping_filepath
        super(BrooksJeffreyScraper, self).__init__(region_name)

    def get_more_tasks(self, content, task: Task) -> List[Task]:
        content.make_links_absolute(self.get_region().base_url)

        params_list = []
        params_list.extend(_get_person_tasks(content))
        params_list.extend(_get_next_page_if_exists_tasks(content))
        return params_list

    def populate_data(self, content, task: Task, ingest_info: IngestInfo) \
            -> Optional[ScrapedData]:
        data_extractor = HtmlDataExtractor(self.mapping_filepath)
        ingest_info = data_extractor.extract_and_populate_data(
            content, ingest_info)
        person = more_itertools.one(ingest_info.people)
        booking = more_itertools.one(person.bookings)

        split_bonds, bond_status = _parse_total_bond_if_necessary(booking)
        if split_bonds or bond_status:
            booking.total_bond_amount = None

        booking.charges = _split_charges(booking, split_bonds, bond_status)

        return ScrapedData(ingest_info=ingest_info, persist=True)


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


def _parse_total_bond_if_necessary(booking: Booking) \
        -> Tuple[Optional[List[str]], Optional[BondStatus]]:
    """Looks at booking.total_bond_amount and, if necessary, parses it into a
    list of individual bond amounts or bond status."""
    if booking.total_bond_amount:
        if booking.total_bond_amount.startswith('No Bond Available'):
            return None, BondStatus.DENIED

        split_bonds = _split(booking.total_bond_amount)
        if len(split_bonds) > 1:
            return split_bonds, None
    return None, None


def _split_charges(
        booking: Booking, split_bond_amounts: Optional[List[str]],
        bond_status: Optional[BondStatus]) -> List[Charge]:
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
        if split_bond_amounts or bond_status:
            bond = split_charge.create_bond()
            if split_bond_amounts:
                bond.amount = split_bond_amounts[idx]
            if bond_status:
                bond.status = bond_status.value
        split_charges.append(split_charge)

    return split_charges


def _split(repeated_object_str: str) -> List[str]:
    return [s for s in repeated_object_str.split('\n') if s]
