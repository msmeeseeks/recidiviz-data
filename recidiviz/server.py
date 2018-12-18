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

"""Entrypoint for the application."""

from flask import Flask
import sqlalchemy

from recidiviz import Session
from recidiviz.ingest.infer_release import infer_release_blueprint
from recidiviz.ingest.scraper_control import scraper_control
from recidiviz.ingest.worker import worker
from recidiviz.persistence.database.schema import Base
from recidiviz.tests.utils.populate_test_db import test_populator
from recidiviz.utils import environment
from recidiviz.utils import secrets


# SQLAlchemy URL prefix declaring the database type and driver
# TODO(176): replace with postgres once we've migrated to Python 3
_URL_PREFIX = 'mysql+pymysql'


app = Flask(__name__)
app.register_blueprint(scraper_control, url_prefix='/scraper')
app.register_blueprint(worker, url_prefix='/scraper')
app.register_blueprint(infer_release_blueprint, url_prefix='/infer_release')
if not environment.in_prod():
    app.register_blueprint(test_populator, url_prefix='/test_populator')

# Create URL and connect to database
db_user = secrets.get_secret('sqlalchemy_db_user')
db_password = secrets.get_secret('sqlalchemy_db_password')
db_host = secrets.get_secret('sqlalchemy_db_host')
db_name = secrets.get_secret('sqlalchemy_db_name')
db_connection_name = secrets.get_secret('sqlalchemy_db_connection_name')

sqlalchemy_url = '{url_prefix}://{db_user}:{db_password}@{db_host}/'\
                 '{db_name}?unix_socket={db_connection_name}'.format(
                     url_prefix=_URL_PREFIX,
                     db_user=db_user,
                     db_password=db_password,
                     db_host=db_host,
                     db_name=db_name,
                     db_connection_name=db_connection_name)
engine = sqlalchemy.create_engine(sqlalchemy_url)
Base.metadata.create_all(engine)
Session.configure(bind=engine)
