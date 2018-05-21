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

"""us_vt record-specific functionality.
"""


from google.appengine.ext import ndb
from recidiviz.models.record import Offense


class UsVtOffense(Offense):
    """UsVt-specific model to describe a particular crime of conviction

    TODO #124 promote non-Vermont specific fields to Charge,
    Conviction, and Arrest when #124 is closed.

    Attributes:
        status: (string) The legal status of the charge in court.
        case_number: (string) Case number of this offense.
        bond_amount: (float) Dollar amount of bond on this charge, if any.
        bond_type: (string) How the bond must be secured.
        number_of_counts: (integer) Counts in this offense.
        arresting_agency: (string) Name of the agency that arrested this person
            on this charge.
        court_type: (string) Type of court that will hear/heard this case.
        warrant_number: (string) The warrant number for this charge, if any.
        court_time: (date) Date of the next court appearance.
        control_number: (string) Unsure what this is.
        crime_type: (string) Type of crime.
        arrest_code: (string) Identifier of the arrest event.
        modifier: (string) Unsure what this is.
        arrest_date: (date) Date of arrest.

    """
    status = ndb.StringProperty()
    case_number = ndb.StringProperty()
    bond_amount = ndb.FloatProperty()
    bond_type = ndb.StringProperty()
    number_of_counts = ndb.IntegerProperty()
    arresting_agency = ndb.StringProperty()
    court_type = ndb.StringProperty()
    warrant_number = ndb.StringProperty()
    court_time = ndb.DateProperty()
    control_number = ndb.StringProperty()
    crime_type = ndb.StringProperty()
    arrest_code = ndb.StringProperty()
    modifier = ndb.StringProperty()
    arrest_date = ndb.DateProperty()
