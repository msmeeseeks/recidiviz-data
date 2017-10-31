# Copyright 2017 Andrew Corp <andrew@andrewland.co> 
# All rights reserved.

from google.appengine.ext import ndb
from google.appengine.ext.ndb import polymodel

"""
Offense

Datastore model for a specific crime that led to an incarceration event. Note
that many incarceration events result from multiple crimes (e.g. larceny AND
evading arrest, etc.) so this field repeats in a Record.

Fields:
    - crime_description: String scraped from prison site describing the crime
    - crime_class: String scraped from prison site describing class of crime
"""
class Offense(ndb.Model):
    crime_description = ndb.StringProperty()
    crime_class = ndb.StringProperty()


"""
SentenceDuration

Entry describing a duration of time for a sentence (could be minimum duration,
could be maximum - this is used for both for a particular Record).

Fields:
    - life_sentence: Whether or not this is a life sentence
    - years: Number of years in addition to the above for sentence
    - months: Number of months in addition to the above for sentence
    - days: Number of days in addition to the above for sentence
"""
class SentenceDuration(ndb.Model):
    life_sentence = ndb.BooleanProperty()
    years = ndb.IntegerProperty()
    months = ndb.IntegerProperty()
    days = ndb.IntegerProperty()


"""
Inmate

Datastore model for a snapshot of a particular inmate listing. This is intended
to be a 1:1 mapping to human beings in the prison system. We don't want 
multiple copies of the same inmate, so we update them when re-scraped.

Individual region scrapers are expected to create their own subclasses which
inherit these common properties, then add more if their region has more 
available. See us_ny_scraper.py for an example.  

Fields:
    - inmate_id: The identifier the state site uses for this listing
    - inmate_id_is_fuzzy: Whether we generated this ID/it's not x-scrape stable
    - given_names: First and middle names (space separated), if provided
    - last_name: Last name, if provided
    - birthday: Birth date, if available
    - age: Age, if birth date is not available.
    - region: The region code for the scraper that captured this
    - sex: Sex of inmate in listing, if provided ("Male" or "Female")
        >> This is due to classification in the systems scraped - if they 
        shift to a less binary approach, we can improve this <<
    - race:  Race of inmate in the listing, for now string provided by region
    - created_on: Python datetime object of first time we added this record
    - updated_on: Python datetime object of last time we updated this record

Analytics tooling may use inmate_id + region to dedup if available, so it's
important to update the listing based on that field, rather than just write a 
new one. If none can be found, generate a random 10-digit 
alphanumeric ID and set record_id_is_fuzzy to True. This will help in 
analytics logic.

For any inmate listing, there will be 1+ Records and 1+ InmateFacilitySnapshots.
"""
class Inmate(polymodel.PolyModel):
    inmate_id = ndb.StringProperty()
    inmate_id_is_fuzzy = ndb.BooleanProperty()
    given_names = ndb.StringProperty()
    last_name = ndb.StringProperty()
    birthday = ndb.DateProperty()
    age = ndb.IntegerProperty()
    region = ndb.StringProperty()
    sex = ndb.StringProperty()
    race = ndb.StringProperty()
    created_on = ndb.DateTimeProperty(auto_now_add=True)
    updated_on = ndb.DateProperty(auto_now=True)


"""
Record

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
    - associated_listing: The InmateSnapshot this record was scraped with

Analytics tooling will use record_id for dedup, so it's important to update 
records based on that key, rather than attempting to store them each time. 

associated_listing is also a key field to include, as this enables dedup when
no record ID is given, and race/age analytics by tying to inmate data.
"""
class Record(polymodel.PolyModel):
    offense = ndb.StructuredProperty(Offense, repeated=True)
    record_id = ndb.StringProperty()
    min_sentence_length = ndb.StructuredProperty(SentenceDuration, repeated=False)
    max_sentence_length = ndb.StructuredProperty(SentenceDuration, repeated=False)
    custody_date = ndb.DateProperty()
    offense_date = ndb.DateProperty()
    is_released = ndb.BooleanProperty()
    associated_listing = ndb.KeyProperty(kind=Inmate)


"""
InmateFacilitySnapshot

Datastore model for the facility an inmate was located in at the time of a 
particular scraping. An inmate should have at least one entry for this from
time of creation, but it will only be updated when the value changes. In
very rare cases, the scraper might not find a value, in which case the
'facility' will be empty (but the snapshot still collected.)

Individual region scrapers are expected to create their own subclasses which
inherit these common properties, then add more if their region has more 
available. See us_ny_scraper.py for an example.

Fields:
    - snapshot_date: Python datetime for time of snapshot
    - facility: State-provided facility name
    - associated_listing: Inmate this data is about
    - associated_record: Record that this is from
"""
class InmateFacilitySnapshot(polymodel.PolyModel):
    snapshot_date = ndb.DateTimeProperty(auto_now_add=True)
    facility = ndb.StringProperty()
    associated_listing = ndb.KeyProperty(kind=Inmate)
    associated_record = ndb.KeyProperty(kind=Record)