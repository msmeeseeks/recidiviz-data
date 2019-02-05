# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Scraper tests for us_nc_buncombe."""

import unittest

from lxml import html
from recidiviz.ingest import constants

from recidiviz.ingest.models.ingest_info import IngestInfo, Bond
from recidiviz.ingest.task_params import Task
from recidiviz.ingest.scrape.regions.us_nc_buncombe.us_nc_buncombe_scraper \
    import UsNcBuncombeScraper
from recidiviz.tests.ingest import fixtures
from recidiviz.tests.utils.base_scraper_test import BaseScraperTest

_NUM_PEOPLE_JSON = html.fromstring(
    fixtures.as_string('us_nc_buncombe', 'num_people.json'))
_FRONT_HTML = html.fromstring(
    fixtures.as_string('us_nc_buncombe', 'front_page.html'))
_PERSON_HTML = html.fromstring(
    fixtures.as_string('us_nc_buncombe', 'person_page.html'))


class SuperionScraperTest(BaseScraperTest, unittest.TestCase):
    """Scraper tests for the Superion vendor scraper."""

    def _init_scraper_and_yaml(self):
        self.scraper = UsNcBuncombeScraper()
        self.yaml = None

    def test_get_num_people(self):
        expected_result = [Task(
            endpoint='https://bcsdp2c.buncombecounty.org/jailinmates.aspx',
            task_type=constants.TaskType.GET_MORE_TASKS,
            custom={
                'num_people': 529,
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
            endpoint='https://bcsdp2c.buncombecounty.org/jailinmates.aspx',
            post_data={
                '__EVENTVALIDATION':
                    '/wEdAAiIk1QI4BA15QjMKqi3Fx+DkAqVf2uSoUyhTPEtGbe49fMQJzTGSXkUT8HDoXA9ryXRuBQVQRHRHlbE8dp2SkOM8i+2OxBn+Rx5BkdfTvnMyov/AJXZWHt87wkUOu19ByOhVDt6+zmfau/SSzzGhFzpZAciyiGLfwhI808BGO85qmzyTGKnJQgyGQXlOKosW6GnoRReS6GJvkb1LcyyKmQO',
                '__VIEWSTATE':
                    '/wEPDwUKMTUwNjQxNDY5MQ9kFgJmD2QWAmYPZBYGAgEPZBYEAgUPZBYEZg8WAh4EVGV4dAVFPHNjcmlwdCBzcmM9ImpzL2pxdWVyeS0xLjguMy5taW4uanMiIHR5cGU9InRleHQvamF2YXNjcmlwdCI+PC9zY3JpcHQ+ZAIBDxYCHwAF/wI8bGluayB0eXBlPSJ0ZXh0L2NzcyIgaHJlZj0iY3NzL3N1cGVyZmlzaC5jc3MiIG1lZGlhPSJzY3JlZW4iIHJlbD0ic3R5bGVzaGVldCIgLz48c2NyaXB0IHR5cGU9InRleHQvamF2YXNjcmlwdCIgc3JjPSdqcy9ob3ZlckludGVudC5qcyc+PC9zY3JpcHQ+IDxzY3JpcHQgdHlwZT0idGV4dC9qYXZhc2NyaXB0IiBzcmM9J2pzL3N1cGVyZmlzaC5qcyc+PC9zY3JpcHQ+IDxzY3JpcHQgc3JjPSJqcXVpLzEuMTEuNC9qcXVlcnktdWktMS4xMS40LmN1c3RvbS5taW4uanMiIHR5cGU9InRleHQvamF2YXNjcmlwdCI+PC9zY3JpcHQ+PGxpbmsgaHJlZj0ianF1aS8xLjExLjQvbWludC1jaG9jL2pxdWVyeS11aS5jc3MiIHJlbD0iU3R5bGVzaGVldCIgY2xhc3M9InVpLXRoZW1lIiAvPmQCCA8WAh8ABfEBPHNjcmlwdCBsYW5ndWFnZT0iamF2YXNjcmlwdCIgdHlwZT0idGV4dC9qYXZhc2NyaXB0Ij4kKGRvY3VtZW50KS5yZWFkeShmdW5jdGlvbigpIHskKCd1bC5zZi1tZW51Jykuc3VwZXJmaXNoKHtkZWxheTogICAgICAgMTAwMCxhbmltYXRpb246ICAge29wYWNpdHk6J3Nob3cnLGhlaWdodDonc2hvdyd9LHNwZWVkOiAgJ2Zhc3QnLGF1dG9BcnJvd3M6ICBmYWxzZSwgZHJvcFNoYWRvd3M6IGZhbHNlIH0pO30pOzwvc2NyaXB0PmQCAw9kFgQCAw8WAh4FYWxpZ24FBGxlZnQWCAIBDxYCHgNzcmMFF34vaW1hZ2VzL0FnZW5jeU5hbWUuZ2lmZAIDDxYEHghkaXNhYmxlZAUIZGlzYWJsZWQeB1Zpc2libGVoFgJmD2QWAgIBD2QWAgIBD2QWAmYPEA8WBh4NRGF0YVRleHRGaWVsZAUEdGV4dB4ORGF0YVZhbHVlRmllbGQFBGxpbmseC18hRGF0YUJvdW5kZ2QQFQ4ESG9tZQxFdmVudCBTZWFyY2gKQ3JpbWUgVGlwcwtNb3N0IFdhbnRlZA9NaXNzaW5nIFBlcnNvbnMORGFpbHkgQnVsbGV0aW4OSW5tYXRlIElucXVpcnkPSW5tYXRlIERlcG9zaXRzHElubWF0ZSBWaXNpdGF0aW9uIFNjaGVkdWxpbmcRSW5tYXRlIFBob25lIFRpbWUUT2ZmaWNlciBDb21tZW5kYXRpb24MQ2xvc2VkIENhbGxzA0ZBUQpDb250YWN0IFVzFQ4Lfi9tYWluLmFzcHgOfi9zdW1tYXJ5LmFzcHgbfi9jdXN0b20vQ3JpbWVTdG9wcGVycy5hc3B4EX4vbW9zdHdhbnRlZC5hc3B4Dn4vbWlzc2luZy5hc3B4FH4vZGFpbHlidWxsZXRpbi5hc3B4En4vamFpbGlubWF0ZXMuYXNweBx+L2N1c3RvbS9Jbm1hdGVEZXBvc2l0cy5hc3B4Hn4vY3VzdG9tL0lubWF0ZVZpc2l0YXRpb24uYXNweBl+L2N1c3RvbS9Jbm1hdGVQaG9uZS5hc3B4Gn4vb2ZmaWNlcmNvbW1lbmRhdGlvbi5hc3B4F34vY2FkL2NhbGxzbmFwc2hvdC5hc3B4Cn4vZmFxLmFzcHgOfi9jb250YWN0LmFzcHgUKwMOZ2dnZ2dnZ2dnZ2dnZ2cWAWZkAgUPZBYCZg9kFgJmD2QWAgIBDxYCHwAF0CE8dWwgY2xhc3M9InNmLW1lbnUgdWktd2lkZ2V0IiBzdHlsZT0iei1pbmRleDo5OTkiPjxsaSBjbGFzcz0iIiBzdHlsZT0iIj48YSBzdHlsZT0iIiBjbGFzcz0idWktY29ybmVyLWFsbCB1aS1zdGF0ZS1kZWZhdWx0IiBocmVmPSIuL21haW4uYXNweCI+SG9tZTwvYT48L2xpPjxsaSBjbGFzcz0iIiBzdHlsZT0iIj48YSBzdHlsZT0iIiBjbGFzcz0idWktY29ybmVyLWFsbCB1aS1zdGF0ZS1kZWZhdWx0IiBocmVmPSIuL3N1bW1hcnkuYXNweCI+RXZlbnQgU2VhcmNoPC9hPjwvbGk+PGxpIGNsYXNzPSIiIHN0eWxlPSIiPjxhIHN0eWxlPSIiIGNsYXNzPSJ1aS1jb3JuZXItYWxsIHVpLXN0YXRlLWRlZmF1bHQiIGhyZWY9Ii4vY3VzdG9tL0NyaW1lU3RvcHBlcnMuYXNweCI+Q3JpbWUgVGlwczwvYT48L2xpPjxsaSBjbGFzcz0iIiBzdHlsZT0iIj48YSBzdHlsZT0iIiBjbGFzcz0idWktY29ybmVyLWFsbCB1aS1zdGF0ZS1kZWZhdWx0IiBocmVmPSIuL21vc3R3YW50ZWQuYXNweCI+TW9zdCBXYW50ZWQ8L2E+PC9saT48bGkgY2xhc3M9IiIgc3R5bGU9IiI+PGEgc3R5bGU9IiIgY2xhc3M9InVpLWNvcm5lci1hbGwgdWktc3RhdGUtZGVmYXVsdCIgaHJlZj0iLi9taXNzaW5nLmFzcHgiPk1pc3NpbmcgUGVyc29uczwvYT48L2xpPjxsaSBjbGFzcz0iIiBzdHlsZT0iIj48YSBzdHlsZT0iIiBjbGFzcz0idWktY29ybmVyLWFsbCB1aS1zdGF0ZS1kZWZhdWx0IiBocmVmPSIuL2RhaWx5YnVsbGV0aW4uYXNweCI+RGFpbHkgQnVsbGV0aW48L2E+PC9saT48bGkgY2xhc3M9IiIgc3R5bGU9IiI+PGEgc3R5bGU9IiIgY2xhc3M9InVpLWNvcm5lci1hbGwgdWktc3RhdGUtZGVmYXVsdCIgaHJlZj0iLi9qYWlsaW5tYXRlcy5hc3B4Ij5Jbm1hdGUgSW5xdWlyeTwvYT48L2xpPjxsaSBjbGFzcz0iIiBzdHlsZT0iIj48YSBzdHlsZT0iIiBjbGFzcz0idWktY29ybmVyLWFsbCB1aS1zdGF0ZS1kZWZhdWx0IiBocmVmPSIuL2N1c3RvbS9Jbm1hdGVEZXBvc2l0cy5hc3B4Ij5Jbm1hdGUgRGVwb3NpdHM8L2E+PC9saT48bGkgY2xhc3M9IiIgc3R5bGU9IiI+PGEgc3R5bGU9IiIgY2xhc3M9InVpLWNvcm5lci1hbGwgdWktc3RhdGUtZGVmYXVsdCIgaHJlZj0iLi9jdXN0b20vSW5tYXRlVmlzaXRhdGlvbi5hc3B4Ij5Jbm1hdGUgVmlzaXRhdGlvbiBTY2hlZHVsaW5nPC9hPjwvbGk+PGxpIGNsYXNzPSIiIHN0eWxlPSIiPjxhIHN0eWxlPSIiIGNsYXNzPSJ1aS1jb3JuZXItYWxsIHVpLXN0YXRlLWRlZmF1bHQiIGhyZWY9Ii4vY3VzdG9tL0lubWF0ZVBob25lLmFzcHgiPklubWF0ZSBQaG9uZSBUaW1lPC9hPjwvbGk+PGxpIGNsYXNzPSIiIHN0eWxlPSIiPjxhIHN0eWxlPSIiIGNsYXNzPSJ1aS1jb3JuZXItYWxsIHVpLXN0YXRlLWRlZmF1bHQiIGhyZWY9Ii4vb2ZmaWNlcmNvbW1lbmRhdGlvbi5hc3B4Ij5PZmZpY2VyIENvbW1lbmRhdGlvbjwvYT48L2xpPjxsaSBjbGFzcz0iIiBzdHlsZT0iIj48YSBzdHlsZT0iIiBjbGFzcz0idWktY29ybmVyLWFsbCB1aS1zdGF0ZS1kZWZhdWx0IiBocmVmPSIuL2NhZC9jYWxsc25hcHNob3QuYXNweCI+Q2xvc2VkIENhbGxzPC9hPjwvbGk+PGxpIGNsYXNzPSIiIHN0eWxlPSIiPjxhIHN0eWxlPSIiIGNsYXNzPSJ1aS1jb3JuZXItYWxsIHVpLXN0YXRlLWRlZmF1bHQiIGhyZWY9Ii4vZmFxLmFzcHgiPkZhcTwvYT48L2xpPjxsaSBjbGFzcz0iIiBzdHlsZT0iIj48YSBzdHlsZT0iIiBjbGFzcz0idWktY29ybmVyLWFsbCB1aS1zdGF0ZS1kZWZhdWx0IiBocmVmPSIuL2NvbnRhY3QuYXNweCI+Q29udGFjdCBVczwvYT48L2xpPjxsaSBjbGFzcz0iIiBzdHlsZT0iei1pbmRleDo5OTsgcG9zaXRpb246cmVsYXRpdmU7Ij48YSBjbGFzcz0idWktY29ybmVyLWFsbCB1aS1zdGF0ZS1kZWZhdWx0IiBocmVmPSIjIj5RdWljayBMaW5rczwvYT48dWwgc3R5bGU9InRleHQtYWxpZ246bGVmdDt3aWR0aDoyMDBweDt0b3A6IDI1cHg7IGRpc3BsYXk6bm9uZTsiPjxsaSBzdHlsZT0id2lkdGg6MjAwcHgiPjxhIHN0eWxlPSJmb250LXNpemU6LjdlbTsiIGNsYXNzPSJ1aS1jb3JuZXItYWxsIHVpLXN0YXRlLWhvdmVyIHVpLXN0YXRlLWRlZmF1bHQiIGhyZWY9Ii4vbWFpbi5hc3B4Ij5Ib21lPC9hPjwvbGk+PGxpIHN0eWxlPSJ3aWR0aDoyMDBweCI+PGEgc3R5bGU9ImZvbnQtc2l6ZTouN2VtOyIgY2xhc3M9InVpLWNvcm5lci1hbGwgdWktc3RhdGUtaG92ZXIgdWktc3RhdGUtZGVmYXVsdCIgaHJlZj0iLi9zdW1tYXJ5LmFzcHgiPkV2ZW50IFNlYXJjaDwvYT48L2xpPjxsaSBzdHlsZT0id2lkdGg6MjAwcHgiPjxhIHN0eWxlPSJmb250LXNpemU6LjdlbTsiIGNsYXNzPSJ1aS1jb3JuZXItYWxsIHVpLXN0YXRlLWhvdmVyIHVpLXN0YXRlLWRlZmF1bHQiIGhyZWY9Ii4vY3VzdG9tL0NyaW1lU3RvcHBlcnMuYXNweCI+Q3JpbWUgVGlwczwvYT48L2xpPjxsaSBzdHlsZT0id2lkdGg6MjAwcHgiPjxhIHN0eWxlPSJmb250LXNpemU6LjdlbTsiIGNsYXNzPSJ1aS1jb3JuZXItYWxsIHVpLXN0YXRlLWhvdmVyIHVpLXN0YXRlLWRlZmF1bHQiIGhyZWY9Ii4vY29tbXVuaXR5Y2FsZW5kYXIuYXNweCI+Q29tbXVuaXR5IENhbGVuZGFyPC9hPjwvbGk+PGxpIHN0eWxlPSJ3aWR0aDoyMDBweCI+PGEgc3R5bGU9ImZvbnQtc2l6ZTouN2VtOyIgY2xhc3M9InVpLWNvcm5lci1hbGwgdWktc3RhdGUtaG92ZXIgdWktc3RhdGUtZGVmYXVsdCIgaHJlZj0iLi9tb3N0d2FudGVkLmFzcHgiPk1vc3QgV2FudGVkPC9hPjwvbGk+PGxpIHN0eWxlPSJ3aWR0aDoyMDBweCI+PGEgc3R5bGU9ImZvbnQtc2l6ZTouN2VtOyIgY2xhc3M9InVpLWNvcm5lci1hbGwgdWktc3RhdGUtaG92ZXIgdWktc3RhdGUtZGVmYXVsdCIgaHJlZj0iLi9taXNzaW5nLmFzcHgiPk1pc3NpbmcgUGVyc29uczwvYT48L2xpPjxsaSBzdHlsZT0id2lkdGg6MjAwcHgiPjxhIHN0eWxlPSJmb250LXNpemU6LjdlbTsiIGNsYXNzPSJ1aS1jb3JuZXItYWxsIHVpLXN0YXRlLWhvdmVyIHVpLXN0YXRlLWRlZmF1bHQiIGhyZWY9Ii4vZGFpbHlidWxsZXRpbi5hc3B4Ij5EYWlseSBCdWxsZXRpbjwvYT48L2xpPjxsaSBzdHlsZT0id2lkdGg6MjAwcHgiPjxhIHN0eWxlPSJmb250LXNpemU6LjdlbTsiIGNsYXNzPSJ1aS1jb3JuZXItYWxsIHVpLXN0YXRlLWhvdmVyIHVpLXN0YXRlLWRlZmF1bHQiIGhyZWY9Ii4vamFpbGlubWF0ZXMuYXNweCI+SW5tYXRlIElucXVpcnk8L2E+PC9saT48bGkgc3R5bGU9IndpZHRoOjIwMHB4Ij48YSBzdHlsZT0iZm9udC1zaXplOi43ZW07IiBjbGFzcz0idWktY29ybmVyLWFsbCB1aS1zdGF0ZS1ob3ZlciB1aS1zdGF0ZS1kZWZhdWx0IiBocmVmPSIuL2N1c3RvbS9Jbm1hdGVEZXBvc2l0cy5hc3B4Ij5Jbm1hdGUgRGVwb3NpdHM8L2E+PC9saT48bGkgc3R5bGU9IndpZHRoOjIwMHB4Ij48YSBzdHlsZT0iZm9udC1zaXplOi43ZW07IiBjbGFzcz0idWktY29ybmVyLWFsbCB1aS1zdGF0ZS1ob3ZlciB1aS1zdGF0ZS1kZWZhdWx0IiBocmVmPSIuL2N1c3RvbS9Jbm1hdGVWaXNpdGF0aW9uLmFzcHgiPklubWF0ZSBWaXNpdGF0aW9uIFNjaGVkdWxpbmc8L2E+PC9saT48bGkgc3R5bGU9IndpZHRoOjIwMHB4Ij48YSBzdHlsZT0iZm9udC1zaXplOi43ZW07IiBjbGFzcz0idWktY29ybmVyLWFsbCB1aS1zdGF0ZS1ob3ZlciB1aS1zdGF0ZS1kZWZhdWx0IiBocmVmPSIuL2N1c3RvbS9Jbm1hdGVQaG9uZS5hc3B4Ij5Jbm1hdGUgUGhvbmUgVGltZTwvYT48L2xpPjxsaSBzdHlsZT0id2lkdGg6MjAwcHgiPjxhIHN0eWxlPSJmb250LXNpemU6LjdlbTsiIGNsYXNzPSJ1aS1jb3JuZXItYWxsIHVpLXN0YXRlLWhvdmVyIHVpLXN0YXRlLWRlZmF1bHQiIGhyZWY9Ii4vb2ZmaWNlcmNvbW1lbmRhdGlvbi5hc3B4Ij5PZmZpY2VyIENvbW1lbmRhdGlvbjwvYT48L2xpPjxsaSBzdHlsZT0id2lkdGg6MjAwcHgiPjxhIHN0eWxlPSJmb250LXNpemU6LjdlbTsiIGNsYXNzPSJ1aS1jb3JuZXItYWxsIHVpLXN0YXRlLWhvdmVyIHVpLXN0YXRlLWRlZmF1bHQiIGhyZWY9Ii4vY2FkL2NhbGxzbmFwc2hvdC5hc3B4Ij5DbG9zZWQgQ2FsbHM8L2E+PC9saT48bGkgc3R5bGU9IndpZHRoOjIwMHB4Ij48YSBzdHlsZT0iZm9udC1zaXplOi43ZW07IiBjbGFzcz0idWktY29ybmVyLWFsbCB1aS1zdGF0ZS1ob3ZlciB1aS1zdGF0ZS1kZWZhdWx0IiBocmVmPSIuL2ZhcS5hc3B4Ij5GYXE8L2E+PC9saT48bGkgc3R5bGU9IndpZHRoOjIwMHB4Ij48YSBzdHlsZT0iZm9udC1zaXplOi43ZW07IiBjbGFzcz0idWktY29ybmVyLWFsbCB1aS1zdGF0ZS1ob3ZlciB1aS1zdGF0ZS1kZWZhdWx0IiBocmVmPSIuL2NvbnRhY3QuYXNweCI+Q29udGFjdCBVczwvYT48L2xpPjwvdWw+PC9saT48L3VsPmQCBw9kFgJmD2QWAgIDDw8WAh8ABSpUaGVyZSBhcmUgY3VycmVudGx5IG5vIGl0ZW1zIGluIHlvdXIgY2FydC5kZAIHD2QWBgIBD2QWBAIBD2QWAmYPZBYEZg9kFgICAg8PFgIfAAXqATxhIGhyZWY9aHR0cDovL3d3dy5idW5jb21iZWNvdW50eS5vcmcvZ292ZXJuaW5nL2RlcHRzL3NoZXJpZmYvPlJldHVybiB0byBCQ1NPIEhvbWVwYWdlPC9hPjxicj48YSBocmVmPWh0dHA6Ly93d3cuamFpbGF0bS5jb20+SW5tYXRlIERlcG9zaXRzPC9hPjxicj48YSBocmVmPWh0dHBzOi8vdmlzaXRhdGlvbi5idW5jb21iZWNvdW50eS5vcmcvYXBwPklubWF0ZSBWaXNpdGF0aW9uIFNjaGVkdWxpbmc8L2E+PGJyPmRkAgIPPCsACwBkAgMPZBYCZg9kFgRmD2QWAgIBDw9kFgQeBXN0eWxlBRlURVhULVRSQU5TRk9STTp1cHBlcmNhc2U7HghvbmNoYW5nZQUxaWYoVmFsaWRhdGVDYXNlTnVtYmVyKHRoaXMpPT1mYWxzZSkgcmV0dXJuIGZhbHNlO2QCAQ9kFgICAQ8PZBYEHwgFGVRFWFQtVFJBTlNGT1JNOnVw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',
                '__VIEWSTATEGENERATOR': '5B295459',
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
        expected_info = IngestInfo()

        person = expected_info.create_person(
            full_name='SIMPSON, BART',
            gender='MALE',
            age='21',
            race='BLACK')

        booking = person.create_booking(admission_date='9/9/2018')

        booking.create_charge(name='MISDEMEANOR CHILD ABUSE',
                              charge_class='MISDEMEANOR',
                              status='PRE-TRIAL',
                              case_number='18CR 089385',
                              bond=Bond(amount='5,000.00',
                                        bond_type='SECURED '))
        booking.create_charge(name='FELONY PROBATION VIOLATION',
                              charge_class='FELONY',
                              status='PRE-TRIAL',
                              case_number='17CRS093084',
                              bond=Bond(amount='5,000.00',
                                        bond_type='SECURED '))
        booking.create_charge(name='MAINTN VEH/DWELL/PLACE CS (F)',
                              status='PRE-TRIAL',
                              case_number='18CRS000669',
                              bond=Bond(amount='10,000.00',
                                        bond_type='SECURED '))
        booking.create_charge(name='FELONY POSSESSION SCH II CS',
                              charge_class='FELONY',
                              status='PRE-TRIAL',
                              case_number='18CR 089383',
                              bond=Bond(amount='0.00',
                                        bond_type='INCLUDED W OTHER '))

        booking.create_charge(name='PWISD COCAINE',
                              status='PRE-TRIAL',
                              case_number='18CR 089384',
                              bond=Bond(amount='0.00',
                                        bond_type='INCLUDED W OTHER '))
        booking.create_charge(name='POSSESS DRUG PARAPHERNALIA',
                              status='PRE-TRIAL',
                              case_number='18CR 089385',
                              bond=Bond(amount='0.00',
                                        bond_type='INCLUDED W OTHER '))

        self.validate_and_return_populate_data(_PERSON_HTML, expected_info)
