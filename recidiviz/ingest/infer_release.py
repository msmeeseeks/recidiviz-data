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

"""Exposes API to infer release of people."""
import httplib
import logging
from datetime import datetime

from flask import Blueprint, request

from recidiviz.ingest import ingest_utils
from recidiviz.ingest import sessions
from recidiviz.utils.auth import authenticate_request
from recidiviz.utils.params import get_values
from recidiviz.persistence import persistence

infer_release_blueprint = Blueprint('infer_release', __name__)


@infer_release_blueprint.route('/release')
@authenticate_request
def infer_release():
    logging.info("TERINPW: in infer_release")
    regions = ingest_utils.validate_regions(get_values("region", request.args))
    logging.info("TERINPW: found_regions " + str(regions))

    if not regions:
        logging.error("No valid regions found in request")
        return 'No valid regions found in request', httplib.BAD_REQUEST

    for region in regions:
        logging.info('TERINPW: beginning to infer release for region: ' +
                     region)
        session = sessions.get_most_recent_completed_session(region)
        if session:
            logging.info("TERINPW: completed session end time = " + str(
                session.end))
            persistence.infer_release_on_open_bookings(region, session.begin)
        else:
            logging.info("TERINPW: no completed sessions found!!!")
            logging.info("TERINPW: all sessions for region = " + str(
                sessions.get_open_sessions(region, open_only=False)))
    d = datetime.strptime("2018-12-20", '%Y-%m-%d')
    persistence.infer_release_on_open_bookings('us_pa_greene', d)

    return '', httplib.OK
