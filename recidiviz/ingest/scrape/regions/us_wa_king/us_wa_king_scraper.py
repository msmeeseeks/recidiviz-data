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

"""Scraper implementation for us_wa_king."""
import json
import os
from typing import List, Optional

from recidiviz.common.constants.booking import CustodyStatus, ReleaseReason
from recidiviz.common.constants.charge import ChargeStatus, CourtType
from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.ingest.extractor.json_data_extractor import JsonDataExtractor
from recidiviz.ingest.models.ingest_info import IngestInfo
from recidiviz.ingest.scrape.base_scraper import BaseScraper
from recidiviz.ingest.scrape.constants import ResponseType, TaskType
from recidiviz.ingest.scrape.task_params import ScrapedData, Task

HEADERS = {
    'Content-Type': 'application/json; charset=utf-8'
}

class UsWaKingScraper(BaseScraper):
    """Scraper implementation for us_wa_king."""

    def __init__(self):
        super(UsWaKingScraper, self).__init__('us_wa_king')

    def get_initial_task(self) -> Task:
        return Task(
            task_type=TaskType.INITIAL_AND_MORE,
            endpoint='/'.join([self.get_region().base_url,
                               'GetEveryoneInCustody']),
            response_type=ResponseType.JSON,
            headers=HEADERS,
        )

    def get_more_tasks(self, content, task: Task) -> List[Task]:
        content = json.loads(content['d'])
        return [
            Task(
                task_type=TaskType.SCRAPE_DATA,
                endpoint='/'.join([self.get_region().base_url,
                                   'SearchByCCN']),
                response_type=ResponseType.JSON,
                headers=HEADERS,
                params={
                    'ccn': '"{}"'.format(person['CCN']),
                    'isSearch': 'false',
                },
                custom={
                    'person': person,
                },
            )
            for person in content["People"]
        ]

    def populate_data(self, content, task: Task,
                      ingest_info: IngestInfo) -> Optional[ScrapedData]:
        if not task.params:
            raise ValueError('Unexpected task in us_wa_king: {}'.format(task))

        content = json.loads(content['d'])
        data_extractor = JsonDataExtractor(os.path.join(
            os.path.dirname(__file__), 'individual_person_keys.yaml'))
        ingest_info = data_extractor.extract_and_populate_data(
            content, ingest_info)
        if ingest_info.people:
            [person] = ingest_info.people
            person.person_id = task.custom['person']['CCN']

            # This assumes that the most recent booking is the first booking
            [booking, *_] = person.bookings

            # The facilities on the list page can be different than those on the
            # individual level pages
            list_facility = task.custom['person']['CurrentCustodyFacility']
            if booking.facility != list_facility:
                booking.facility = '{} ({})'.format(
                    booking.facility, list_facility)

            for booking in person.bookings:
                for charge in booking.charges:
                    if charge.charging_entity:
                        charge.name = '{} (NCIC Code: {})'.format(
                            charge.name, charge.charging_entity)
                        charge.charging_entity = None
                    if charge.charge_notes:
                        charge.charge_notes = 'Charge No. {}'.format(
                            charge.charge_notes)
                    if booking.release_date and not booking.release_reason:
                        # TODO(1088): map this in enum overrides instead. Right
                        # now the last charge status wins, so we might end up
                        # with one that isn't actually a release reason.
                        booking.release_reason = charge.status
        else:
            # The detailed record doesn't exist for a few people, so instead we
            # fallback to the info we received from the main page.
            data_extractor = JsonDataExtractor(os.path.join(
                os.path.dirname(__file__), 'list_person_keys.yaml'))
            ingest_info = data_extractor.extract_and_populate_data(
                task.custom['person'], ingest_info)
        return ScrapedData(ingest_info=ingest_info, persist=True)

    def get_enum_overrides(self):
        ob = EnumOverrides.Builder()
        ob.add('TRUE', CustodyStatus.IN_CUSTODY)

        # Release Reason
        ob.add('ABSCONDED', ReleaseReason.ESCAPE)
        ob.add('BAIL', ReleaseReason.BOND)
        ob.add('IR INVESTIGATION RELEASE NO CHARGE',
               ReleaseReason.CASE_DISMISSED)
        ob.add('PR PERSONAL RECOGNIZANCE', ReleaseReason.OWN_RECOGNIZANCE)
        ob.add('REINSTATEMENT TO PROBATION', ReleaseReason.PROBATION)
        # 'State Intermediate Punishment' (at least in PA, assuming in WA also)
        ob.add('SIP SYSTEM RELEASE', ReleaseReason.PAROLE)
        ob.add('TRANSFER OF CUSTODY', ReleaseReason.TRANSFER)
        ob.ignore('CONDITIONAL RELEASE', ReleaseReason)
        ob.ignore('DRUG COURT', ReleaseReason)
        ob.ignore('INVESTIGATED AND CHARGED', ReleaseReason)
        ob.ignore('RC CHARGE REDUCED', ReleaseReason)
        ob.ignore('REVOCATION OF PAROLE', ReleaseReason)

        # Charge Status
        ob.add('BAIL', ChargeStatus.PRETRIAL)
        ob.add('BOND', ChargeStatus.PRETRIAL)
        ob.add('CASE DISMISSED', ChargeStatus.DROPPED)
        ob.add('DRUG COURT', ChargeStatus.PRETRIAL)
        ob.add('INVESTIGATED AND CHARGED', ChargeStatus.PRETRIAL)
        ob.add('IR INVESTIGATION RELEASE NO CHARGE', ChargeStatus.DROPPED)
        ob.add('REINSTATEMENT TO PROBATION', ChargeStatus.SENTENCED)
        ob.add('SENTENCE EXPIRATION', ChargeStatus.COMPLETED_SENTENCE)
        ob.add('SIP SYSTEM RELEASE', ChargeStatus.SENTENCED)
        # TODO(1088): map this to ReleaseReason.ESCAPE
        ob.ignore('ABSCONDED', ChargeStatus)
        ob.ignore('CONDITIONAL RELEASE', ChargeStatus)
        ob.ignore('ERROR', ChargeStatus)
        # TODO(1088): map this to ReleaseReason.ESCAPE
        ob.ignore('ESCAPE', ChargeStatus)
        ob.ignore('PR PERSONAL RECOGNIZANCE', ChargeStatus)
        ob.ignore('RC CHARGE REDUCED', ChargeStatus)
        # TODO(1088): map this to charge class
        ob.ignore('REVOCATION OF PAROLE', ChargeStatus)
        ob.ignore('TRANSFER OF CUSTODY', ChargeStatus)

        # Court Type
        ob.add(lambda text: 'DIST' in text, CourtType.DISTRICT)
        ob.add(lambda text: text.startswith('KCDC'), CourtType.DISTRICT)
        ob.add(lambda text: 'MUNI' in text, CourtType.CIVIL)
        ob.add(lambda text: 'SUPERIOR' in text, CourtType.SUPERIOR)
        ob.ignore('WASHINGTON STATE DEPT OF CORRECTIONS', CourtType)

        return ob.build()
