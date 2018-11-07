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

"""Record model for the Archonix vendor.
"""

from google.appengine.ext import ndb
from recidiviz.models.record import Record

class ArchonixRecord(Record):
    """A subclass of Record that adds Archonix specific fields

    Attributes:
		reference_id: (string) The reference id of the record, this is different
			from booking number which we map to record_id
    """
    reference_id = ndb.StringProperty()
