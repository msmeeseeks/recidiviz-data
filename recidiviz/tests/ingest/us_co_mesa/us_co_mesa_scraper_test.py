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
"""Scraper tests for us_co_mesa."""

from lxml import html
from recidiviz.ingest.models.ingest_info import IngestInfo, _Person, _Booking, \
    _Charge, _Bond
from recidiviz.ingest.us_co_mesa.us_co_mesa_scraper import UsCoMesaScraper
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils.base_scraper_test import BaseScraperTest

_DETAILS_PAGE_HTML = html.fromstring(
    fixtures.as_string('us_co_mesa', 'details.html'))


class TestScraperDetailsPage(BaseScraperTest):

    def _init_scraper_and_yaml(self):
        self.scraper = UsCoMesaScraper()
        self.yaml = None

    def test_parse(self):

        expected = IngestInfo(people=[_Person(
            given_names="FIRST MIDDLE",
            surname="LAST",
            birthdate="1/1/1970",
            bookings=[
                _Booking(
                    booking_id="2018-0000XXXX",
                    total_bond_amount="$9051",
                    charges=[
                        _Charge(
                            name="FAILURE TO APPEAR WARRANT",
                            statute="16-2-110",
                            charge_class="M",
                            bond=_Bond(
                                bond_id="2018-00000001",
                                amount="$25",
                                bond_type="(CASH) CASH",
                            ),
                        ), _Charge(
                            name="FAILURE TO APPEAR WARRANT",
                            statute="16-2-110",
                            charge_class="F",
                            bond=_Bond(
                                bond_id="2018-00000002",
                                amount="$3013",
                                bond_type="(CASH) CASH",
                            ),
                        ), _Charge(
                            name="FAILURE TO APPEAR WARRANT",
                            statute="16-2-110",
                            charge_class="F",
                            bond=_Bond(
                                bond_id="2018-00000003",
                                amount="$3013",
                                bond_type="(CASH) CASH",
                            ),
                        ), _Charge(
                            name="ASSAULT - 2ND DEG - ON A POLICE OFFICER / FIRE FIGHTER",
                            statute="18-3-203(1)(c)",
                            charge_class="F4",
                            bond=_Bond(
                                bond_id="2018-00000004",
                                amount="$3000",
                                bond_type="(CS) CASH/SURETY",
                            ),
                        ), _Charge(
                            name="CRIMINAL MISCHIEF $300 OR MORE, LESS THAN $750",
                            statute="18-4-501(4)(b)",
                            charge_class="M2",
                            bond=_Bond(
                                bond_id="2018-00000004",
                                amount="$3000",
                                bond_type="(CS) CASH/SURETY",
                            ),
                        ), _Charge(
                            name="RESISTING ARREST",
                            statute="18-8-103(4)",
                            charge_class="M2",
                            bond=_Bond(
                                bond_id="2018-00000004",
                                amount="$3000",
                                bond_type="(CS) CASH/SURETY",
                            ),
                        ), _Charge(
                            name="OBSTRUCTING PEACE OFFICER, FIREFIGHTER, ER MEDICAL SRVC PROVIDER, RESCUE SPC, VOLUNTEER",
                            statute="18-8-104(4)",
                            charge_class="M2",
                            bond=_Bond(
                                bond_id="2018-00000004",
                                amount="$3000",
                                bond_type="(CS) CASH/SURETY",
                            ),
                        ), _Charge(
                            name="RODE BICYCLE IN CARELESS MANNER",
                            statute="42-4-1402(1)#A",
                            bond=_Bond(
                                bond_id="2018-00000004",
                                amount="$3000",
                                bond_type="(CS) CASH/SURETY",
                            ),
                        ), _Charge(
                            name="POSSESSION OF DRUG PARAPHERNALIA",
                            statute="18-18-428",
                            charge_class="DPO",
                            bond=_Bond(
                                bond_id="2018-00000004",
                                amount="$3000",
                                bond_type="(CS) CASH/SURETY",
                            ),
                        ),
                    ]
                )
            ]
        ), ])

        self.validate_and_return_populate_data(
            _DETAILS_PAGE_HTML, {}, expected, IngestInfo())

