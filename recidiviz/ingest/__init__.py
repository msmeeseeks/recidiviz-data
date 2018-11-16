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

"""The ingest portion of the Recidiviz data platform.

This includes infrastructure, logic, and models for ingesting, validating,
normalizing, and storing records ingested from various criminal justice data
sources.
"""


import recidiviz.ingest.docket
import recidiviz.ingest.extractor.data_extractor
import recidiviz.ingest.models
import recidiviz.ingest.scraper_control
import recidiviz.ingest.sessions
import recidiviz.ingest.tracker
import recidiviz.ingest.us_co_mesa
import recidiviz.ingest.us_mt_gallatin
import recidiviz.ingest.us_ny
import recidiviz.ingest.us_pa_dauphin
import recidiviz.ingest.us_pa_greene
import recidiviz.ingest.us_vt
import recidiviz.ingest.worker
