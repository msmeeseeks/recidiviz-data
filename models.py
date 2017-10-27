# Copyright 2017 Andrew Corp <andrew@andrewland.co> 
# All rights reserved.

from google.appengine.ext import ndb
from google.appengine.ext.ndb import polymodel

"""
InmateListing

Datastore model for a snapshot of a particular inmate listing. For some scrapers, 
a listing is for a specific human being. For others, a listing is for a record
of a particular sentence being served. In both cases, we don't want multiple 
copies of the same inmate or record, so we update them when re-scraped.

Individual region scrapers are expected to create their own subclasses which
inherit these common properties, then add more if their region has more 
available. See us_ny_scraper.py for an example.  

Fields:
    - record_id: The identifier the state site uses for this listing
    - record_id_is_fuzzy: Whether we generated this ID / it's not x-sess stable
    - given_names: First and middle names, if provided
    - last_name: Last name, if provided
    - birthday: Birthdate or 1/1/<year of birth based on given age>
    - birthday_is_fuzzy: Whether the state gives the inmate's age, not DOB
    - added_date: Python datetime object of first time we added this record
    - added_date: Python datetime object of last time we updated this record
    - region: The region code for the scraper that captured this
    - release_date: Projected or actual release date of inmate in listing
    - sex: Sex of inmate in listing, if provided ("Male" or "Female")
    - race:  Race of inmate in the listing, for now string provided by region


Analytics tooling will use record_id + region to dedup if available, so it's
important to update the listing based on that field, rather than just write a 
new one. If none are in the UI, try to find them in the request parameters that
got to the listing page. If none can be found, generate a random 10-digit 
alphanumeric ID and set record_id_is_fuzzy to True. This will help in 
analytics logic.


Separate records should be stored for facility snapshots (for which a unique 
snapshot is generated with each scrape, to catch facility transfers).
"""
class InmateListing(polymodel.PolyModel):
    record_id = ndb.StringProperty()
    record_id_is_fuzzy = ndb.BooleanProperty()
    given_names = ndb.StringProperty()
    last_name = ndb.StringProperty()
    birthday = ndb.DateProperty()
    birthday_is_fuzzy = ndb.BooleanProperty()
    added_date = ndb.DateTimeProperty(auto_now_add=True)
    region = ndb.StringProperty()
    last_update = ndb.DateProperty(auto_now=True)
    sex = ndb.StringProperty()
    race = ndb.StringProperty()


"""
RecordEntry

Datastore model for historical violation records (e.g. 
ROBB. WPN-NOT DEADLY, 04/21/1995, sentence: 20mo). Multiple records may map
to one inmate.

Individual region scrapers are expected to create their own subclasses which
inherit these common properties, then add more if their region has more 
available. See us_ny_scraper.py for an example.

Fields:
    - offense: JSON'ified dict. {'crime_string': 'class', ...} Contains
        state-provided string describing the crimes and (if available) 
        class of crimes. 
    - record_id: The identifier the state site uses for this crime
    - min_sentence: (json encoded dict) Minimum sentence to be served {Life: 
        bool, years: int, months: int, days: int} - Life is always False.
    - max_sentence: (json encoded dict) Maximum sentence to be served {Life: 
        bool, years: int, months: int, days: int} - Life is whether it's a life
        sentence.
    - custody_date: Date the inmate's sentence started, if available
    - offense_date: Date the offense was committed, if available
    - is_released: Whether the inmate was released (at least, for this crime)
    - associated_snapshot: The InmateSnapshot this record was scraped with

Analytics tooling will use record_id for dedup, so it's important to update 
records based on that key, rather than attempting to store them each time. 

associated_snapshot is also a key field to include, as this enables dedup when
no record ID is given, and race/age analytics by tying to inmate data.
"""
class RecordEntry(polymodel.PolyModel):
    offense = ndb.JsonProperty()
    record_id = ndb.StringProperty()
    min_sentence_length = ndb.JsonProperty()
    max_sentence_length = ndb.JsonProperty()
    custody_date = ndb.DateProperty()
    offense_date = ndb.DateProperty()
    is_released = ndb.BooleanProperty()
    associated_listing = ndb.KeyProperty(kind=InmateListing)


"""
InmateFacilitySnapshot

Datastore model for the facility an inmate was located in at the time of a 
particular scraping. An inmate should have at least one of these records for
each scraping performed.

Individual region scrapers are expected to create their own subclasses which
inherit these common properties, then add more if their region has more 
available. See us_ny_scraper.py for an example.

Fields:
    - snapshot_date: Python datetime for time of snapshot
    - associated_listing: Listing this data came from
    - facility: State-provided facility name

Analytics tooling will use record_id for dedup, so it's important to update 
records based on that key, rather than attempting to store them each time. 

associated_snapshot is also a key field to include, as this enables dedup when
no record ID is given, and race/age analytics by tying to inmate data.
"""
class InmateFacilitySnapshot(polymodel.PolyModel):
    snapshot_date = ndb.DateTimeProperty(auto_now_add=True)
    facility = ndb.StringProperty()
    associated_listing = ndb.KeyProperty(kind=InmateListing)
    associated_record = ndb.KeyProperty(kind=RecordEntry)