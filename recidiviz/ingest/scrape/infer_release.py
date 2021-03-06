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

from http import HTTPStatus
import logging

from flask import Blueprint

from recidiviz.common.constants.booking import CustodyStatus
from recidiviz.ingest.scrape import sessions
from recidiviz.utils.auth import authenticate_request
from recidiviz.persistence import persistence
from recidiviz.utils.regions import Region, RemovedFromWebsite, \
    get_supported_regions

infer_release_blueprint = Blueprint('infer_release', __name__)


@infer_release_blueprint.route('/release')
@authenticate_request
def infer_release():
    regions = get_supported_regions()

    for region in regions:
        if region.agency_type != 'jail':
            continue

        session = sessions.get_most_recent_completed_session(region.region_code)
        if session:
            logging.info(
                'Got most recent completed session for %s with start time %s',
                region.region_code, session.start)
            persistence.infer_release_on_open_bookings(
                region.region_code, session.start, _get_custody_status(region))
    return '', HTTPStatus.OK


def _get_custody_status(region: Region):
    removed_from_website = region.removed_from_website
    if removed_from_website == RemovedFromWebsite.RELEASED:
        return CustodyStatus.INFERRED_RELEASE
    if removed_from_website == RemovedFromWebsite.UNKNOWN_SIGNIFICANCE:
        return CustodyStatus.UNKNOWN_REMOVED_FROM_SOURCE
    raise ValueError(
        "RemovedFromWebsite value {} not mapped to a ReleaseReason".format(
            removed_from_website))
