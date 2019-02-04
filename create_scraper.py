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

"""Usage: python create_scraper.py <county> <state> <agency_type>
  - agency_type: one of 'jail', 'prison', 'unified'

Creates __init__.py, region_name_scraper.py, and region_name.yaml files in
recidiviz/ingest/region_name, and updates queue.yaml and region_manifest.yaml.
Also accepts the following optional arguments:
  - agency: the name of the agency
  - names_file: a file with a names list for this scraper
  - timezone: the timezone, e.g. America/New York
  - url: the initial url of the roster
  - vendor: create a vendor scraper. Available vendors:
    - `jailtracker`

If the flag -tests_only is set, will only create test files.
"""
import argparse
import os
from datetime import datetime
from string import Template
from typing import Optional

import us


def populate_file(template_path, target_path, subs):
    with open(template_path) as template:
        contents = Template(template.read()).substitute(subs)

    with open(target_path, 'w') as target:
        target.write(contents)


def create_scraper_files(subs, vendor: Optional[str]):
    """Creates __init__.py, region_name_scraper.py, and region_name.yaml files
    in recidiviz/ingest/region_name
    """
    def create_scraper(template):
        target = os.path.join(target_dir, subs['region'] + '_scraper.py')
        populate_file(template, target, subs)

    def create_yaml(template):
        target = os.path.join(target_dir, subs['region'] + '.yaml')
        populate_file(template, target, subs)

    ingest_dir = os.path.join(os.path.dirname(__file__), 'recidiviz/ingest/')
    if not os.path.exists(ingest_dir):
        raise OSError('Couldn\'t find directory recidiviz/ingest. Run this ' +
                      'script from the top level recidiviz-data directory.')
    template_dir = os.path.join(ingest_dir, 'scraper_template')
    target_dir = os.path.join(ingest_dir, subs['region'])
    if os.path.exists(target_dir):
        raise OSError('directory %s already exists' % target_dir)
    os.mkdir(target_dir)

    init_template = os.path.join(template_dir, '__init__.txt')
    init_target = os.path.join(target_dir, '__init__.py')
    populate_file(init_template, init_target, subs)

    if vendor:
        template_dir = os.path.join(template_dir, vendor)
    scraper_template = os.path.join(template_dir, 'region_scraper.txt')
    create_scraper(scraper_template)

    if not vendor:
        yaml_template = os.path.join(template_dir, 'region.txt')
        create_yaml(yaml_template)


def create_test_files(subs, vendor: Optional[str]):
    def create_test(template):
        test_target_file_name = subs['region'] + '_scraper_test.py'
        test_target = os.path.join(target_test_dir, test_target_file_name)
        populate_file(template, test_target, subs)
    ingest_dir = os.path.join(os.path.dirname(__file__), 'recidiviz/ingest/')
    test_dir = os.path.join(os.path.dirname(__file__),
                            'recidiviz/tests/ingest/')
    if not os.path.exists(ingest_dir):
        raise OSError('Couldn\'t find directory recidiviz/tests/ingest. Run ' +
                      'this script from the top level recidiviz-data ' +
                      'directory.')
    target_test_dir = os.path.join(test_dir, subs['region'])
    if os.path.exists(target_test_dir):
        raise OSError('directory %s already exists' % target_test_dir)
    os.mkdir(target_test_dir)

    template_dir = os.path.join(ingest_dir, 'scraper_template')
    if vendor:
        template_dir = os.path.join(template_dir, vendor)
    test_template = os.path.join(template_dir, 'region_scraper_test.txt')
    create_test(test_template)


def append_to_config_files(subs):
    """Updates queue.yaml and region_manifest.yaml with the new region.
    """
    top_level_path = os.path.dirname(__file__)
    queue_text = """
# $region_dashes-scraper - $county County, $state
- name: $region_dashes-scraper
  mode: push
  rate: 5/m
  bucket_size: 2
  max_concurrent_requests: 3
  retry_parameters:
    min_backoff_seconds: 5
    max_backoff_seconds: 300
    task_age_limit: 3d
    task_retry_limit: 5
"""
    with open(os.path.join(top_level_path, 'queue.yaml'), 'a') as queue_file:
        queue_file.write(Template(queue_text).safe_substitute(subs))

    region_text = """  $region:
    agency_name: $agency
    region_code: $region
    agency_type: $agency_type
    queue: $region_dashes-scraper
    base_url: $url
    scraper_package: $region
    timezone: $timezone
"""

    # only include `names_file` if it is provided
    if 'names_file' in subs:
        region_text += "    names_file: $names_file\n"

    manifest_path = os.path.join(top_level_path, 'region_manifest.yaml')
    with open(manifest_path, 'a') as region_file:
        region_file.write(Template(region_text).safe_substitute(subs))

    cron_text = """
  - description: Start $state_abbr $county scraper every day at 9pm
    url: /scraper/start?region=$region&scrape_type=background
    schedule: every day 21:00
    timezone: $timezone
    retry_parameters:
      min_backoff_seconds: 2.5
      max_doublings: 5
      job_age_limit: 9h

  - description: Stop $state_abbr $county scraper every day at 9am
    url: /scraper/stop?region=$region&scrape_type=background
    schedule: every day 09:00
    timezone: $timezone
    retry_parameters:
      min_backoff_seconds: 2.5
      max_doublings: 5
      job_age_limit: 9h
"""

    cron_path = os.path.join(top_level_path, 'cron.yaml')
    with open(cron_path, 'a') as cron_file:
        cron_file.write(Template(cron_text).safe_substitute(subs))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('county')
    parser.add_argument('state')
    parser.add_argument('agency_type')
    optional_args = [
        'agency',
        'names_file',
        'timezone',
        'url']
    for optional_arg in optional_args:
        parser.add_argument('--' + optional_arg)
    parser.add_argument('--vendor', required=False,
                        help='Create a vendor scraper.',
                        choices=['jailtracker'])
    parser.add_argument('-tests_only', required=False, action='store_true',
                        help='If set, only create test files.')
    args = parser.parse_args()

    state = us.states.lookup(args.state)
    if state is None:
        raise ValueError('Couldn\'t parse state "%s"' % args.state)
    region = ('us', state.abbr.lower()) + tuple(args.county.lower().split())

    substitutions = {
        'class_name': ''.join(s.title() for s in region),
        'county': args.county.title(),
        'region': '_'.join(region),
        'region_dashes': '-'.join(region),
        'agency_type': args.agency_type,
        'state': state.name,
        'state_abbr': state.abbr,
        'timezone': args.timezone or state.capital_tz,
        'year': datetime.now().year,
    }

    for optional_arg in optional_args:
        arg_value = vars(args)[optional_arg]
        if arg_value is not None:
            substitutions[optional_arg] = arg_value

    if not args.tests_only:
        create_scraper_files(substitutions, args.vendor)
        append_to_config_files(substitutions)
    create_test_files(substitutions, args.vendor)
