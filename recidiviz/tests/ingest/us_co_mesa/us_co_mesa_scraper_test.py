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

import unittest
from lxml import html
from recidiviz.ingest.models.ingest_info import IngestInfo, Person, Booking, \
    Charge, Bond
from recidiviz.ingest.us_co_mesa.us_co_mesa_scraper import UsCoMesaScraper
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils.base_scraper_test import BaseScraperTest

_DETAILS_TYPICAL_HTML = html.fromstring(
    fixtures.as_string('us_co_mesa', 'details_typical.html'))
_DETAILS_EDGE_HTML = html.fromstring(
    fixtures.as_string('us_co_mesa', 'details_edge.html'))


class TestScraperDetailsPage(BaseScraperTest, unittest.TestCase):

    def _init_scraper_and_yaml(self):
        self.scraper = UsCoMesaScraper()
        self.yaml = None

    def test_parse_typical(self):
        expected_info = IngestInfo(people=[Person(
            full_name="FIRST MIDDLE LAST",
            birthdate="1/1/1970",
            bookings=[
                Booking(
                    booking_id="2018-0000XXXX",
                    charges=[
                        Charge(
                            name="FAILURE TO APPEAR WARRANT",
                            statute="16-2-110",
                            charge_class="M",
                            bond=Bond(
                                bond_id="2018-00000001",
                                amount="$25",
                                bond_type="(CASH) CASH",
                            ),
                        ), Charge(
                            name="FAILURE TO APPEAR WARRANT",
                            statute="16-2-110",
                            charge_class="F",
                            bond=Bond(
                                bond_id="2018-00000002",
                                amount="$3013",
                                bond_type="(CASH) CASH",
                            ),
                        ), Charge(
                            name="FAILURE TO APPEAR WARRANT",
                            statute="16-2-110",
                            charge_class="F",
                            bond=Bond(
                                bond_id="2018-00000003",
                                amount="$3013",
                                bond_type="(CASH) CASH",
                            ),
                        ), Charge(
                            name="ASSAULT - 2ND DEG - ON A POLICE OFFICER / FIRE FIGHTER",
                            statute="18-3-203(1)(c)",
                            charge_class="F",
                            level="4",
                            bond=Bond(
                                bond_id="2018-00000004",
                                amount="$3000",
                                bond_type="(CS) CASH/SURETY",
                            ),
                        ), Charge(
                            name="CRIMINAL MISCHIEF $300 OR MORE, LESS THAN $750",
                            statute="18-4-501(4)(b)",
                            charge_class="M",
                            level="2",
                            bond=Bond(
                                bond_id="2018-00000004",
                                amount="$3000",
                                bond_type="(CS) CASH/SURETY",
                            ),
                        ), Charge(
                            name="RESISTING ARREST",
                            statute="18-8-103(4)",
                            charge_class="M",
                            level="2",
                            bond=Bond(
                                bond_id="2018-00000004",
                                amount="$3000",
                                bond_type="(CS) CASH/SURETY",
                            ),
                        ), Charge(
                            name="OBSTRUCTING PEACE OFFICER, FIREFIGHTER, ER MEDICAL SRVC PROVIDER, RESCUE SPC, VOLUNTEER",
                            statute="18-8-104(4)",
                            charge_class="M",
                            level="2",
                            bond=Bond(
                                bond_id="2018-00000004",
                                amount="$3000",
                                bond_type="(CS) CASH/SURETY",
                            ),
                        ), Charge(
                            name="RODE BICYCLE IN CARELESS MANNER",
                            statute="42-4-1402(1)#A",
                            bond=Bond(
                                bond_id="2018-00000004",
                                amount="$3000",
                                bond_type="(CS) CASH/SURETY",
                            ),
                        ), Charge(
                            name="POSSESSION OF DRUG PARAPHERNALIA",
                            statute="18-18-428",
                            charge_class="PO",
                            bond=Bond(
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
            _DETAILS_TYPICAL_HTML, expected_info)

    def test_parse_edge(self):
        expected_info = IngestInfo(people=[Person(
            full_name="FOO BAR",
            birthdate="12/31/1991",
            bookings=[
                Booking(
                    booking_id="2018-12345678",
                    charges=[
                        Charge(
                            name="FUGITIVE OTHER JURISDICTION WARRANT",
                            statute="16-19-123#A",
                            bond=Bond(
                                bond_id="2018-00",
                                amount="$2000",
                                bond_type="(CS) CASH/SURETY",
                            ),
                        ), Charge(
                            name="3RD DEGREE BURGLARY",
                            statute="18-4-204(2)",
                            charge_class="F",
                            level="5",
                            bond=Bond(
                                bond_id="2018-01",
                                amount="$2000",
                                bond_type="(PR) PERSONAL RECOGNIZANCE",
                            ),
                        ), Charge(
                            name="FORGERY, 2ND DEGREE",
                            statute="18-5-103",
                            charge_class="F",
                            level="5",
                            bond=Bond(
                                bond_id="2018-02",
                                amount="$5000",
                                bond_type="(CASH) CASH",
                            ),
                        ), Charge(
                            name="1ST DEGREE CRIMINAL TRESPASS OF DWELLING",
                            statute="18-4-502",
                            charge_class="F",
                            level="5",
                            bond=Bond(
                                bond_id="2018-03",
                                amount="$3000",
                                bond_type="(CASH) CASH",
                            ),
                        ), Charge(
                            name="MARIJUANA - OPEN CONTAINER/USE OR CONSUME IN MOTOR VEHICLE",
                            statute="42-4-1305.5",
                            charge_class="TI",
                            level="A",
                            bond=Bond(
                                bond_id="2018-04",
                                amount="$1000",
                                bond_type="(CASH) CASH",
                            ),
                        ), Charge(
                            name="2ND DEGREE CRIMINAL TRESPASS ENCLOSED/FENCED AREA",
                            statute="18-4-503 (1)(a)",
                            bond=Bond(
                                bond_id="2018-05",
                                amount="$25",
                                bond_type="(CASH) CASH",
                            ),
                        ), Charge(
                            name="DIST/MAN/POSS W/INTENT TO DIST > 4G OF SCH III/IV",
                            statute="18-18-405",
                            charge_class="F",
                            level="3",
                            bond=Bond(
                                bond_id="2018-06",
                                amount="$10000",
                                bond_type="(CS) CASH/SURETY",
                            ),
                        ), Charge(
                            name="DROVE (MOTOR/OFF-HIGHWAY) VEHICLE WHEN LICENSE UNDER RESTRAINT (REVOKED)",
                            statute="42-2-138(1)(a)#B",
                            bond=Bond(
                                bond_id="2018-07",
                                amount="$10000",
                                bond_type="(CASH) CASH",
                            ),
                        ), Charge(
                            name="PAWNBROKER-FALSE INFO BY SELLER",
                            statute="29-11.9-103 (1)",
                            bond=Bond(
                                bond_id="2018-08",
                                amount="$3000",
                                bond_type="(CASH) CASH",
                            ),
                        ), Charge(
                            name="THEFT-LESS THAN $50 - FROM BUILDING",
                            statute="18-4-401(1)(2)(b)",
                            charge_class="PO",
                            level="1",
                            bond=Bond(
                                bond_id="2018-09",
                                amount="$5000",
                                bond_type="(CS) CASH/SURETY",
                            ),
                        ),
                    ]
                )
            ]
        ), ])

        self.validate_and_return_populate_data(
            _DETAILS_EDGE_HTML, expected_info)
