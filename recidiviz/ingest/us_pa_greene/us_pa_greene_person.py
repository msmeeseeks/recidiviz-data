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

"""Person model for PA Greene Country Jail.
"""


from google.appengine.ext import ndb
from recidiviz.models.person import Person


class UsPaGreenePerson(Person):
    """A subclass of Person that adds Greene County, PA specific fields

    Datastore model for a specific person incarcerated in
    Vermont. This extends the Person class.

    Attributes:

        us_pa_greene_person_id: (string) Same as person_id, but used as
            key for this entity type to force uniqueness / prevent
            collisions within the us_pa_greene records (see
            models.person for inherited attributes)
        us_pa_greene_inmate_id: (string) The internal db id that is used to
            construct the URL to go straight to a persons page.  We store this
            as well even though it is un-exposed in their UI.

    """
    us_pa_greene_person_id = ndb.StringProperty()
    us_pa_greene_inmate_id = ndb.StringProperty()
