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

"""us_vt snapshot-specific functionality
"""

from google.appengine.ext import ndb
from recidiviz.models.snapshot import Snapshot


class UsVtSnapshot(Snapshot):
    """A subclass of Snapshot that adds Vermont-specific fields

    Datastore model for the Vermont-specific fields in snapshots of person
    information. Snapshots are generated each time the scraper runs and
    finds a property of a Person or Record entity has changed in the
    corrections system. Only that attribute (and any others which have changed
    since the last scrape) is stored in the snapshot.

    UsVtSnapshot entities are stored as children of their
    respective record entities. There may be multiple snapshots per
    record.

    Attributes:
        (see us_vt_record.py for attributes)
        (see models.snapshot for inherited attributes)

    """
    status = ndb.StringProperty()
    release_date = ndb.DateProperty()
    earliest_release_date = ndb.DateProperty()
    community_supervision_agency = ndb.StringProperty()
    parole_officer = ndb.StringProperty()
    case_worker = ndb.StringProperty()
