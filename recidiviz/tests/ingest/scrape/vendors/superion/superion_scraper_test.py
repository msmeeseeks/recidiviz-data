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
"""Scraper tests for the Superion vendor.

Any region scraper test class that inherits from SuperionScraperTest must
implement the following:

    def _get_scraper(self):
        return RegionScraper()
"""

import abc
from lxml import html
from recidiviz.ingest.scrape import constants

from recidiviz.ingest.models.ingest_info import IngestInfo, Person, \
    Booking, Charge, Bond
from recidiviz.ingest.scrape.task_params import Task
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils.base_scraper_test import BaseScraperTest

_NUM_PEOPLE_JSON = html.fromstring(
    fixtures.as_string('vendors/superion', 'num_people.json'))
_FRONT_HTML = html.fromstring(
    fixtures.as_string('vendors/superion', 'front_page.html'))
_PERSON_HTML = html.fromstring(
    fixtures.as_string('vendors/superion', 'person_page.html'))


class SuperionScraperTest(BaseScraperTest):
    """Scraper tests for the Superion vendor scraper."""

    @abc.abstractmethod
    def _get_scraper(self):
        """Gets a child superion scraper child object.
        """

    def _init_scraper_and_yaml(self):
        # The scraper to be tested. Required.
        self.scraper = self._get_scraper()
        self.yaml = None

    def test_get_num_people(self):
        expected_result = [Task(
            # pylint:disable=protected-access
            endpoint=self.scraper._session_endpoint,
            task_type=constants.TaskType.GET_MORE_TASKS,
            custom={
                'num_people': 916,
            }
        )]

        task = Task(
            task_type=constants.TaskType.INITIAL_AND_MORE,
            endpoint='',
        )

        self.validate_and_return_get_more_tasks(_NUM_PEOPLE_JSON, task,
                                                expected_result)

    def test_get_session_vars(self):

        # pylint:disable=line-too-long
        expected_result = [Task(
            task_type=constants.TaskType.SCRAPE_DATA,
            endpoint='http://p2c.guilfordcountysheriff.com/jailinmates.aspx',
            post_data={
                '__EVENTVALIDATION': None,
                '__VIEWSTATE': '/wEPDwUKMTMyNTg1OTUwMQ9kFgJmD2QWAmYPZBYGZg9kFgICBQ9kFgRmDxYCHgRUZXh0BUU8c2NyaXB0IHNyYz0ianMvanF1ZXJ5LTEuOC4zLm1pbi5qcyIgdHlwZT0idGV4dC9qYXZhc2NyaXB0Ij48L3NjcmlwdD5kAgEPFgIfAAX/ATxzY3JpcHQgc3JjPSJqcXVpLzEuMTEuNC9qcXVlcnktdWktMS4xMS40LmN1c3RvbS5taW4uanMiIHR5cGU9InRleHQvamF2YXNjcmlwdCI+PC9zY3JpcHQ+PGxpbmsgaHJlZj0ianF1aS8xLjExLjQvYmxhY2stdGllL2pxdWVyeS11aS5jc3MiIHJlbD0iU3R5bGVzaGVldCIgY2xhc3M9InVpLXRoZW1lIiAvPjxsaW5rIGhyZWY9ImpxdWkvMS4xMS40L2JsYWNrLXRpZS90aGVtZS5jc3MiIHJlbD0iU3R5bGVzaGVldCIgY2xhc3M9InVpLXRoZW1lIiAvPmQCAQ9kFgQCAw8WAh4FYWxpZ24FBmNlbnRlchYGAgEPFgIeA3NyYwUXfi9pbWFnZXMvQWdlbmN5TmFtZS5naWZkAgMPFgIeCGRpc2FibGVkZBYCZg9kFgRmD2QWAgIBDw8WAh8ABcALICA8YSBjbGFzcz0iTWVudVRleHQgcDJjLW5vd3JhcCIgaHJlZj0iLi9tYWluLmFzcHgiPkhPTUU8L2E+PHNwYW4gY2xhc3M9Ik1lbnVUZXh0Ij4mbmJzcDsmbmJzcDsmbmJzcDsmbmJzcDs8L3NwYW4+ICA8YSBjbGFzcz0iTWVudVRleHQgcDJjLW5vd3JhcCIgaHJlZj0iLi9zdW1tYXJ5LmFzcHgiPkVWRU5UIFNFQVJDSDwvYT48c3BhbiBjbGFzcz0iTWVudVRleHQiPiZuYnNwOyZuYnNwOyZuYnNwOyZuYnNwOzwvc3Bhbj4gIDxhIGNsYXNzPSJNZW51VGV4dCBwMmMtbm93cmFwIiBocmVmPSIuL2NvbW11bml0eWNhbGVuZGFyLmFzcHgiPkNPTU1VTklUWSBDQUxFTkRBUjwvYT48c3BhbiBjbGFzcz0iTWVudVRleHQiPiZuYnNwOyZuYnNwOyZuYnNwOyZuYnNwOzwvc3Bhbj4gIDxhIGNsYXNzPSJNZW51VGV4dCBwMmMtbm93cmFwIiBocmVmPSIuL21vc3R3YW50ZWQuYXNweCI+TU9TVCBXQU5URUQ8L2E+PHNwYW4gY2xhc3M9Ik1lbnVUZXh0Ij4mbmJzcDsmbmJzcDsmbmJzcDsmbmJzcDs8L3NwYW4+ICA8YSBjbGFzcz0iTWVudVRleHQgcDJjLW5vd3JhcCIgaHJlZj0iLi9kYWlseWJ1bGxldGluLmFzcHgiPkRBSUxZIEJVTExFVElOPC9hPjxzcGFuIGNsYXNzPSJNZW51VGV4dCI+Jm5ic3A7Jm5ic3A7Jm5ic3A7Jm5ic3A7PC9zcGFuPiAgPGEgY2xhc3M9Ik1lbnVUZXh0IHAyYy1ub3dyYXAiIGhyZWY9Ii4vYXJyZXN0cy5hc3B4Ij5BUlJFU1RTPC9hPjxzcGFuIGNsYXNzPSJNZW51VGV4dCI+Jm5ic3A7Jm5ic3A7Jm5ic3A7Jm5ic3A7PC9zcGFuPiAgPGEgY2xhc3M9Ik1lbnVUZXh0IHAyYy1ub3dyYXAiIGhyZWY9Ii4vY29udGFjdC5hc3B4Ij5DT05UQUNUIFVTPC9hPjxzcGFuIGNsYXNzPSJNZW51VGV4dCI+Jm5ic3A7Jm5ic3A7Jm5ic3A7Jm5ic3A7PC9zcGFuPiAgPGEgY2xhc3M9Ik1lbnVUZXh0IHAyYy1ub3dyYXAiIGhyZWY9Ii4vZmFxLmFzcHgiPkZBUTwvYT48c3BhbiBjbGFzcz0iTWVudVRleHQiPiZuYnNwOyZuYnNwOyZuYnNwOyZuYnNwOzwvc3Bhbj4gIDxhIGNsYXNzPSJNZW51VGV4dCBwMmMtbm93cmFwIiBocmVmPSIuL3dhbnRlZGxpc3QuYXNweCI+V0FOVEVEIExJU1Q8L2E+PHNwYW4gY2xhc3M9Ik1lbnVUZXh0Ij4mbmJzcDsmbmJzcDsmbmJzcDsmbmJzcDs8L3NwYW4+ICA8YSBjbGFzcz0iTWVudVRleHQgcDJjLW5vd3JhcCIgaHJlZj0iLi9qYWlsaW5tYXRlcy5hc3B4Ij5JTk1BVEUgSU5RVUlSWTwvYT48c3BhbiBjbGFzcz0iTWVudVRleHQiPiZuYnNwOyZuYnNwOyZuYnNwOyZuYnNwOzwvc3Bhbj4gIDxhIGNsYXNzPSJNZW51VGV4dCBwMmMtbm93cmFwIiBocmVmPSIuL3NleG9mZmVuZGVycy5hc3B4Ij5TRVggT0ZGRU5ERVJTPC9hPjxzcGFuIGNsYXNzPSJNZW51VGV4dCI+Jm5ic3A7Jm5ic3A7Jm5ic3A7Jm5ic3A7PC9zcGFuPiAgPGEgY2xhc3M9Ik1lbnVUZXh0IHAyYy1ub3dyYXAiIGhyZWY9Ii4vU2V4T2ZmZW5kZXJTZWFyY2guYXNweCI+U0VYIE9GRkVOREVSIFNFQVJDSDwvYT5kZAIBD2QWAgIBD2QWAmYPEA8WBh4NRGF0YVRleHRGaWVsZAUEdGV4dB4ORGF0YVZhbHVlRmllbGQFBGxpbmseC18hRGF0YUJvdW5kZ2QQFQ0QLSBRdWljayBMaW5rcyAtIARIb21lDEV2ZW50IFNlYXJjaBJDb21tdW5pdHkgQ2FsZW5kYXILTW9zdCBXYW50ZWQHQXJyZXN0cw5EYWlseSBCdWxsZXRpbgpDb250YWN0IFVzA0ZBUQtXYW50ZWQgTGlzdA5Jbm1hdGUgSW5xdWlyeQ1TZXggT2ZmZW5kZXJzE1NleCBPZmZlbmRlciBTZWFyY2gVDQN+LzALfi9tYWluLmFzcHgOfi9zdW1tYXJ5LmFzcHgYfi9jb21tdW5pdHljYWxlbmRhci5hc3B4EX4vbW9zdHdhbnRlZC5hc3B4Dn4vYXJyZXN0cy5hc3B4FH4vZGFpbHlidWxsZXRpbi5hc3B4Dn4vY29udGFjdC5hc3B4Cn4vZmFxLmFzcHgRfi93YW50ZWRsaXN0LmFzcHgSfi9qYWlsaW5tYXRlcy5hc3B4E34vc2V4b2ZmZW5kZXJzLmFzcHgYfi9TZXhPZmZlbmRlclNlYXJjaC5hc3B4FCsDDWdnZ2dnZ2dnZ2dnZ2cWAWZkAgcPZBYCZg9kFgICAw8PFgIfAAUqVGhlcmUgYXJlIGN1cnJlbnRseSBubyBpdGVtcyBpbiB5b3VyIGNhcnQuZGQCBw9kFgYCAQ9kFgYCAQ9kFgJmD2QWBGYPD2QWBB4Fc3R5bGUFGVRFWFQtVFJBTlNGT1JNOnVwcGVyY2FzZTseCG9uY2hhbmdlBTFpZihWYWxpZGF0ZUNhc2VOdW1iZXIodGhpcyk9PWZhbHNlKSByZXR1cm4gZmFsc2U7ZAICD2QWAgIBDw9kFgQfBwUZVEVYVC1UUkFOU0ZPUk06dXBwZXJjYXNlOx8IBTFpZihWYWxpZGF0ZUNhc2VOdW1iZXIodGhpcyk9PWZhbHNlKSByZXR1cm4gZmFsc2U7ZAIDD2QWAmYPZBYCAgsPFgIeBnR5cGVObwUBM2QCBQ9kFgJmD2QWAgILDxYCHwkFATRkAgUPZBYCAgMPPCsACwEADxYIHghEYXRhS2V5cxYAHgtfIUl0ZW1Db3VudAIKHglQYWdlQ291bnQCXB4VXyFEYXRhU291cmNlSXRlbUNvdW50ApQHZBYCZg9kFhQCAg9kFgpmDw8WAh8ABQY2NzQ4NDRkZAIBDw8WAh8ABRtBQkFURSwgUk9NQU4gVkVETyAoVyAvTS8zOClkZAICDw8WAh8ABQ5JREVOVElUWSBUSEVGVGRkAgMPDxYCHwAFCjA5LzIxLzIwMThkZAIEDw8WAh8ABR5HdWlsZm9yZCBDb3VudHkgU2hlcmlmZiBPZmZpY2VkZAIDD2QWCmYPDxYCHwAFBjY4NDE2MGRkAgEPDxYCHwAFIUFCRVJOQVRIWSwgS0FZQ0VFIExFSUdIIChXIC9GLzI4KWRkAgIPDxYCHwAFGUZUQSwgU0VDT05EIE9SIFNVQlNFUVVFTlRkZAIDDw8WAh8ABQoxMi8yOC8yMDE4ZGQCBA8PFgIfAAUeR3VpbGZvcmQgQ291bnR5IFNoZXJpZmYgT2ZmaWNlZGQCBA9kFgpmDw8WAh8ABQY2ODM2MjVkZAIBDw8WAh8ABR5BREFNUywgVEFCSVRIQSBTVEFSUiAoVyAvRi8yMClkZAICDw8WAh8ABRFPVVQgT0YgQ09VTlRZIE9GQWRkAgMPDxYCHwAFCjEyLzE3LzIwMThkZAIEDw8WAh8ABR5HdWlsZm9yZCBDb3VudHkgU2hlcmlmZiBPZmZpY2VkZAIFD2QWCmYPDxYCHwAFBjY4NDcxMGRkAgEPDxYCHwAFH0FHVUlMQVIsIEpFU1VTIEFMRU1BTiAoVyAvTS81NylkZAICDw8WAh8ABRFBU1NBVUxUIE9OIEZFTUFMRWRkAgMPDxYCHwAFCjAxLzA4LzIwMTlkZAIEDw8WAh8ABR5HdWlsZm9yZCBDb3VudHkgU2hlcmlmZiBPZmZpY2VkZAIGD2QWCmYPDxYCHwAFBjY3MzgwOWRkAgEPDxYCHwAFJUFHVUlMQVItVkVMQVNRVUVaLCBST0JFUlRPICAoVyAvTS8yNClkZAICDw8WAh8ABQtJTU1JR1JBVElPTmRkAgMPDxYCHwAFCjA4LzI5LzIwMThkZAIEDw8WAh8ABR5HdWlsZm9yZCBDb3VudHkgU2hlcmlmZiBPZmZpY2VkZAIHD2QWCmYPDxYCHwAFBjY4MzkxN2RkAgEPDxYCHwAFIEFMRFJJREdFLCBKT1NIVUEgU0hBTkUgKFcgL00vMjkpZGQCAg8PFgIfAAUVTEFSQ0VOWSAtIE1JU0RFTUVBTk9SZGQCAw8PFgIfAAUKMTIvMjgvMjAxOGRkAgQPDxYCHwAFHkd1aWxmb3JkIENvdW50eSBTaGVyaWZmIE9mZmljZWRkAggPZBYKZg8PFgIfAAUGNjU1NTcwZGQCAQ8PFgIfAAUiQUxFWEFOREVSLCBET01JTklRVUUgTk1OIChCIC9NLzQxKWRkAgIPDxYCHwAFCVRSVUUgQklMTGRkAgMPDxYCHwAFCjA1LzI1LzIwMThkZAIEDw8WAh8ABR5HdWlsZm9yZCBDb3VudHkgU2hlcmlmZiBPZmZpY2VkZAIJD2QWCmYPDxYCHwAFBjY4NDUzM2RkAgEPDxYCHwAFI0FMRVhBTkRFUiwgUk9CRVJUIERFUlJJQ0sgKEIgL00vMzkpZGQCAg8PFgIfAAUwUk9CQkVSWSBXSVRIIEZJUkVBUk1TIE9SIE9USEVSIERBTkdFUk9VUyBXRUFQT05TZGQCAw8PFgIfAAUKMDEvMDQvMjAxOWRkAgQPDxYCHwAFHkd1aWxmb3JkIENvdW50eSBTaGVyaWZmIE9mZmljZWRkAgoPZBYKZg8PFgIfAAUGNjgyOTEyZGQCAQ8PFgIfAAUaQUxHQlVSSSwgQU1NQVIgTSAoVyAvTS8yMClkZAICDw8WAh8ABSBEV0kgLSBEUklWRSBBRlRFUiBDT05TVU1JTkcgPCAyMWRkAgMPDxYCHwAFCjEyLzAxLzIwMThkZAIEDw8WAh8ABR5HdWlsZm9yZCBDb3VudHkgU2hlcmlmZiBPZmZpY2VkZAILD2QWCmYPDxYCHwAFBjY4NDMzNGRkAgEPDxYCHwAFHUFMTEVOLCBTVEVWRU4gTEVXSVMgKFcgL00vMzIpZGQCAg8PFgIfAAUhUFJPQkFUSU9OIFZJT0xBVElPTiAoTUlTREVNRUFOT1IpZGQCAw8PFgIfAAUKMDEvMDEvMjAxOWRkAgQPDxYCHwAFHkd1aWxmb3JkIENvdW50eSBTaGVyaWZmIE9mZmljZWRkAgkPZBYEAgEPZBYCZg9kFgICDA8WAh8JBQExZAIDD2QWAmYPZBYCAgMPFCsAAg8WBB8GZx8LAgtkZBYCZg9kFhYCAQ9kFgJmDxUCBGV2ZW4AZAICD2QWAmYPFQIDb2RkMTxiPkFSUkVTVDwvYj4tICA8aT4xKSBBc3NhdWx0IFdpdGggRGVhZGx5IC4uLjwvST5kAgMPZBYCZg8VAgRldmVuEiBhdCBMZWVzIENoYXBlbCBSZGQCBA9kFgJmDxUCA29kZABkAgUPZBYCZg8VAgRldmVuAGQCBg9kFgJmDxUCA29kZDk8Yj5UUkFGRklDIENJVEFUSU9OPC9iPi08ST5EaXNwbGF5IEV4cGlyZWQgUmVnaXN0ci4uLjwvST5kAgcPZBYCZg8VAgRldmVuLCBhdCBBbGFtYW5jZSBDaHVyY2ggUmQvd2lsZXkgTGV3aXMgLyAyNCBab25lZAIID2QWAmYPFQIDb2RkAGQCCQ9kFgJmDxUCBGV2ZW4AZAIKD2QWAmYPFQIDb2RkAGQCCw9kFgJmDxUCBGV2ZW4AZAICDxYCHwBlZBgCBR5fX0NvbnRyb2xzUmVxdWlyZVBvc3RCYWNrS2V5X18WAQVDY3RsMDAkTWFzdGVyUGFnZSRtYWluQ29udGVudCRSaWdodENvbHVtbkNvbnRlbnQkY3RsMDIkQkltYWdlQnV0dG9uMQU+Y3RsMDAkTWFzdGVyUGFnZSRtYWluQ29udGVudCRSaWdodENvbHVtbkNvbnRlbnQkY3RsMDEkbHZFdmVudHMPFCsADmRkZGRkZGQ8KwALAAILZGRkZgL/////D2QVKAz6Ks2fvQ9B+2WWf89EAkuJ5/qrEw3g7hz/IGwzHg==',
                '__VIEWSTATEGENERATOR': None,
                'ctl00$MasterPage$mainContent$CenterColumnContent$btnInmateDetail': '',
                'ctl00$MasterPage$mainContent$CenterColumnContent$hfRecordIndex': 0
            },
        )]

        task = Task(
            task_type=constants.TaskType.GET_MORE_TASKS,
            endpoint='',
            custom={
                'num_people': 1,
            }
        )

        self.validate_and_return_get_more_tasks(_FRONT_HTML, task,
                                                expected_result)

    def test_populate_data(self):
        expected_info = IngestInfo(people=[
            Person(
                full_name='NAME, NOPE',
                gender='MALE',
                age='30',
                race='WHITE',
                bookings=[
                    Booking(admission_date='9/13/2018',
                            charges=[
                                Charge(name='HABITUAL LARCENY',
                                       status='PRE-TRIAL',
                                       case_number='18CR84223',
                                       next_court_date='1/25/2019',
                                       bond=Bond(amount='2,500.00',
                                                 bond_type='SECURED BOND ')),
                                Charge(name='HABITUAL LARCENY',
                                       status='PRE-TRIAL',
                                       case_number='DAVIDSON',
                                       bond=Bond(amount='10,000.00',
                                                 bond_type='SECURED BOND ')),
                                Charge(name='IDENTITY THEFT',
                                       status='PRE-TRIAL',
                                       case_number='NEW HANOVER',
                                       bond=Bond(amount='1,000.00',
                                                 bond_type='SECURED BOND ')),
                                Charge(name=('RESIST  DELAY  OBSTRUCT PUBLIC'
                                             ' OFFICER'),
                                       status='PRE-TRIAL',
                                       case_number='NEW HANOVER',
                                       bond=Bond(amount='0.00',
                                                 bond_type='NONE SET ')),
                                Charge(name='HABITUAL LARCENY',
                                       status='PRE-TRIAL',
                                       case_number='DAVIDSON',
                                       bond=Bond(amount='0.00',
                                                 bond_type='NONE SET ')),
                                Charge(name='TRESPASS (SECOND DEGREE)',
                                       status='PRE-TRIAL',
                                       case_number='18CR84223',
                                       bond=Bond(amount='0.00',
                                                 bond_type='NONE SET ')),
                                Charge(name='NO OPERATOR LICENSE',
                                       status='PRE-TRIAL',
                                       case_number='NEW HANOVER',
                                       bond=Bond(amount='0.00',
                                                 bond_type='NONE SET ')),
                                Charge(name='HABITUAL LARCENY',
                                       status='PRE-TRIAL',
                                       case_number='DAVIDSON',
                                       bond=Bond(amount='0.00',
                                                 bond_type='NONE SET ')),
                                Charge(name=('OBTAINING PROPERTY BY FALSE'
                                             ' PRETENSES'),
                                       status='PRE-TRIAL',
                                       case_number='DAVIDSON',
                                       bond=Bond(amount='0.00',
                                                 bond_type='NONE SET ')),
                                Charge(name=('OBTAINING PROPERTY BY FALSE'
                                             ' PRETENSES'),
                                       status='PRE-TRIAL',
                                       case_number='DAVIDSON',
                                       bond=Bond(amount='0.00',
                                                 bond_type='NONE SET '))])])
        ])

        self.validate_and_return_populate_data(_PERSON_HTML, expected_info)
