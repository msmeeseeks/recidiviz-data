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

"""Scraper implementation for us_fl_okeechobee."""
import os
from typing import List, Optional

from recidiviz.common.constants.booking import AdmissionReason
from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.common.constants.hold import HoldStatus
from recidiviz.common.constants.person import Race, Ethnicity
from recidiviz.ingest.extractor.csv_data_extractor import CsvDataExtractor
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.scrape.base_scraper import BaseScraper
from recidiviz.ingest.scrape import constants
from recidiviz.ingest.scrape.task_params import ScrapedData, Task

_WARRANTS = (
    'CAPIAS', 'BENCH WARRANT', 'OUT OF COUNTY WARRANT', 'PAROLE WARRANT',
    'WARRANT'
)
_HOLDS = ('EXTRADITION', 'HOLD FOR ANOTHER AGENCY')

class UsFlOkeechobeeScraper(BaseScraper):
    """Scraper implementation for us_fl_okeechobee."""

    def __init__(self, mapping_filepath=None):
        if not mapping_filepath:
            mapping_filepath = os.path.join(
                os.path.dirname(__file__), 'us_fl_okeechobee.yaml')
        self.mapping_filepath = mapping_filepath

        super(UsFlOkeechobeeScraper, self).__init__('us_fl_okeechobee')

    def populate_data(self, content, task: Task,
                      ingest_info: IngestInfo) -> Optional[ScrapedData]:
        data_extractor = CsvDataExtractor(self.mapping_filepath)
        ingest_info = data_extractor.extract_and_populate_data(content,
                                                               ingest_info)

        for person in ingest_info.people:
            self._postprocess_race_and_ethniticy(person)
            for booking in person.bookings:
                self._postprocess_admission_reason(booking)

        return ScrapedData(ingest_info, persist=True)

    def _postprocess_race_and_ethniticy(self, person):
        """Split demographic information into race and ethnicity fields."""
        if 'NON HISP' in person.race.replace('-', ' '):
            person.ethnicity = Ethnicity.NOT_HISPANIC.value

    def _postprocess_admission_reason(self, booking):
        """Possibly convert booking.admission_reason into other things."""
        reason = booking.admission_reason
        if reason == 'BOND REVOCATION':
            # Convert to a bond status.
            charge = booking.get_recent_charge() or booking.create_charge()
            bond = charge.get_recent_bond() or charge.create_bond()
            bond.status = reason
            booking.admission_reason = None
            booking.prune()
        elif reason in ('SENTENCED', 'WEEKENDER') + _WARRANTS:
            # Convert to a charge status.
            charge = booking.get_recent_charge() or booking.create_charge()
            charge.status = reason
            booking.admission_reason = None
            booking.prune()
        elif reason in _HOLDS:
            # Convert to a hold status.
            hold = booking.get_recent_hold() or booking.create_hold()
            hold.status = reason
            if reason == 'EXTRADITION':
                hold.jurisdiction_name = reason
            booking.admission_reason = None
            booking.prune()

    def get_more_tasks(self, content, task: Task) -> List[Task]:
        return [Task(
            endpoint=self.get_region().base_url,
            task_type=constants.TaskType.SCRAPE_DATA,
            response_type=constants.ResponseType.TEXT,
        )]

    def get_enum_overrides(self):
        overrides_builder = EnumOverrides.Builder()

        # Person overrides
        # Note: They also list HISPANIC, but this doesn't need an override.
        demo_mapping = {
            'N-BLACK, NON-HISP': Race.BLACK,
            'N-INDIAN/ALASKAN NAT, NON-HISP':
                Race.AMERICAN_INDIAN_ALASKAN_NATIVE,
            'N-WHITE, NON HISPANIC': Race.WHITE,
        }
        for demo, race in demo_mapping.items():
            overrides_builder.add(demo, race)

        # Booking overrides
        overrides_builder.add('PC VIOLATION OF PROBATION',
                              AdmissionReason.PROBATION_VIOLATION)
        overrides_builder.add('VIOLATION OF PROBATION',
                              AdmissionReason.PROBATION_VIOLATION)
        overrides_builder.add('TRANSPORT ORDER',
                              AdmissionReason.TRANSFER)

        # Charge overrides
        for warrant in _WARRANTS:
            overrides_builder.add(warrant, ChargeStatus.PRETRIAL)
        overrides_builder.add('WEEKENDER', ChargeStatus.SENTENCED)

        # Holds overrides
        for hold in _HOLDS:
            overrides_builder.add(hold, HoldStatus.ACTIVE)

        # No clear mapping
        overrides_builder.ignore('PROBABLE CAUSE')
        overrides_builder.ignore('MENTAL HEALTH COURT')

        return overrides_builder.build()
