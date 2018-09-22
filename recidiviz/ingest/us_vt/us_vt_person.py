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

"""Person model for Vermont.
"""


from google.appengine.ext import ndb
from recidiviz.models.inmate import Inmate


class UsVtPerson(Inmate):
    """A subclass of Inmate that adds Vermont specific fields

    Datastore model for a specific person incarcerated in
    Vermont. This extends the Inmate class.

    Attributes:

        us_vt_person_id: (string) Same as person_id, but used as
            key for this entity type to force uniqueness / prevent
            collisions within the us_vt records (see
            models.inmate for inherited attributes)

    """
    us_vt_person_id = ndb.StringProperty()
