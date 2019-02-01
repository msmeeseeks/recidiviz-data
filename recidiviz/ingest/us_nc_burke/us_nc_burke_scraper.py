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

"""Scraper implementation for us_nc_burke."""
import logging
from typing import Optional

from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.task_params import ScrapedData, Task
from recidiviz.ingest.vendors.superion.superion_scraper import SuperionScraper


class UsNcBurkeScraper(SuperionScraper):
    """Scraper implementation for us_nc_burke."""
    def __init__(self):
        super(UsNcBurkeScraper, self).__init__('us_nc_burke')

    def populate_data(self, content, task: Task,
                      ingest_info: IngestInfo) -> Optional[ScrapedData]:
        scraped_data = super(UsNcBurkeScraper, self).populate_data(
            content, task, ingest_info)

        ingest_info = scraped_data.ingest_info if scraped_data else ingest_info

        if len(ingest_info.people) != 1:
            logging.error("Did not find exactly one person, as expected")
            return None

        person = ingest_info.people[0]

        if len(person.bookings) != 1:
            logging.error("Did not find exactly one booking, as expected")
            return None

        booking = person.bookings[0]

        # Find the charges for the current booking
        current_case_numbers = []
        old_case_numbers = []
        for charge in booking.charges:
            if charge.bond and charge.bond.bond_type != 'N/A':
                current_case_numbers.append(charge.case_number)
            elif charge.case_number:
                old_case_numbers.append(charge.case_number)

        current_case_numbers = set(current_case_numbers)
        old_case_numbers = set(old_case_numbers) - current_case_numbers

        # Split the booking into one current and potentially
        # several old bookings.
        person.bookings = []

        current_booking = person.create_booking(custody_status='IN CUSTODY')
        for current_case_number in current_case_numbers:
            for charge in booking.charges:
                if charge.case_number == current_case_number:
                    current_booking.charges.append(charge)

        for old_case_number in old_case_numbers:
            old_booking = person.create_booking(custody_status='RELEASED')
            for charge in booking.charges:
                if charge.case_number == old_case_number:
                    old_booking.charges.append(charge)

        # Create one booking for all pre-2000 bookings, which don't
        # have case numbers.
        pre_2000_booking = person.create_booking(custody_status='RELEASED')
        for charge in booking.charges:
            if not charge.case_number:
                pre_2000_booking.charges.append(charge)

        return scraped_data

    def get_enum_overrides(self):
        return {
            'SUSPENDED SENTENCE OR SUPERVISED PRO': ChargeStatus.SENTENCED,
        }
