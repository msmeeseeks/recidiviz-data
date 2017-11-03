# Copyright 2017 Andrew Corp <andrew@andrewland.co> 
# All rights reserved.


from google.appengine.ext import ndb
from inmate import Inmate


"""
UsNyInmate

Datastore model for a snapshot of a specific person in the prison system. This extends 
the general Inmate, which has the following fields:

See Inmate for pre-populated fields. UsNyInmate extends 
these by adding the following:
    - us_ny_inmate_id: (string) Same as inmate_id, but used as key for this entity type
        to force uniqueness / prevent collisions within the us_ny records

Note the duplicated record ID - this allows us to use this field as a key,
forcing uniqueness within the UsNy record space without forcing it across
all regions (as there may be record ID collisions between states).

"""
class UsNyInmate(Inmate):
    us_ny_inmate_id = ndb.StringProperty()