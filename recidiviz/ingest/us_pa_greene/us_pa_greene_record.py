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

"""Record model for PA Greene County Jail.
"""

from google.appengine.ext import ndb
from recidiviz.models.record import Offense
from recidiviz.models.record import Record


class UsPaGreeneOffense(Offense):
    """UsPaGreene-specific model to describe a particular crime of conviction

    Attributes:
        case_number: (string) Case number of this offense.
        bond_amount: (float) Dollar amount of bond on this charge, if any.

    """
    case_number = ndb.StringProperty()
    bond_amount = ndb.FloatProperty()

class UsPaGreeneRecord(Record):
    """A subclass of Record that adds PA Greene specific fields

    Datastore model for a specific person incarcerated in
    Greene County, PA. This extends the Person class.

    Attributes:
		reference_id: (string) The reference id of the record, this is different
			from booking number which we map to record_id
        committed_by: (string) The entity that committed the person.

    """
    reference_id = ndb.StringProperty()
    committed_by = ndb.StringProperty()
 