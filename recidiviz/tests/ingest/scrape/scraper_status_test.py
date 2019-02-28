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

"""Tests for scraper_status.py"""
from unittest import TestCase

from flask import Flask
from mock import create_autospec, patch

from recidiviz.ingest.scrape import scraper_status
from recidiviz.utils.regions import Region

app = Flask(__name__)
app.register_blueprint(scraper_status.scraper_status)
app.config['TESTING'] = True

class TestScraperStatus(TestCase):
    """Tests for requests to the Scraper Status API."""

    # noinspection PyAttributeOutsideInit
    def setup_method(self, _test_method):
        self.client = app.test_client()

    @patch("recidiviz.ingest.scrape.ingest_utils.validate_regions")
    @patch("recidiviz.utils.regions.get_region")
    def test_check_for_finished_scrapers(
            self, mock_region, mock_validate_regions):
        region_code = 'us_ny'

        fake_region = create_autospec(Region)
        fake_region.region_code = region_code
        fake_region.get_queue_name.return_value = 'queue'
        mock_region.return_value = fake_region
        mock_validate_regions.return_value = [region_code]
        # mock queues

        request_args = {'region': 'all'}
        headers = {'X-Appengine-Cron': "test-cron"}
        response = self.client.get('/check_finished',
                                   query_string=request_args,
                                   headers=headers)
        assert response.status_code == 200

        mock_validate_regions.assert_called_with(['all'])
        mock_region.assert_called_with('us_ny')

        # assert logs?
