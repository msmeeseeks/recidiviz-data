# Database

## Overview

The database is a vanilla SQL database with application-time temporal tables.

### Temporal tables

An application-time temporal table is a table that tracks the changes over time to an entity reflected in another table (the "master table"). E.g., `person_history` tracks the changes over time to a given person in the `person` table.

Each row in the temporal table (a "snapshot") contains the state of that entity for a certain period of time, along with `valid_from` and `valid_to` timestamp columns defining the period over which that state is valid. The most recent snapshot is always identical to the current state of the entity on the master table, and has a `valid_to` value of `NULL`.

These tables are designed to be queried both vertically (the complete state of the database, including all relationships, at a given historical point in time) and horizontally (the changes to one entity over time).

The valid period columns reflect the state of the entity in the **real world** (to our best approximation), **not** the state of the entity in the database. E.g., if historical data for a sentence that was imposed in 2005 are ingested into the system, the corresponding historical snapshot will be dated to 2005 (the time the event happened in the real world), not the time the data were recorded in the database.

As part of the temporal table design, no rows are ever deleted from the master tables. Any event corresponding to a delete is instead indicated by updating a status value. This makes it easier to keep track of all entities that might ever have been related to a given entity at any point in its history, even if some of those entities are no longer valid at some later date.

## Querying

Do not query `prod-data` directly. `prod-data` should only be queried to validate migrations, as detailed below. All other queries should be run against the BigQuery export of `prod-data`.

TODO(garciaz): find appropriate documentation from rasmi@ about querying BigQuery to link to here

## Migrations

### Migration warnings and constraints

- Do not make any changes to `schema.py` without also running a migration to update the database. All jobs will fail for any release in which `schema.py` does not match the database.

- A migration should consist of a single change to the schema (a single change meaning a single conceptual change, which may consist of multiple update statements). Do not group changes.

- Do not share foreign key columns between master and historical tables. Master table foreign key columns point to other master tables, and should have foreign key constraints. Historical table foreign key columns point to other historical tables, and cannot have foreign key constraints (because the IDs they reference are not unique on the historical table).

- For historical tables, the primary key exists only due to requirements of SQLAlchemy, and should never be referenced elsewhere in the schema.

- If adding a column of type String, the length must be specified (this keeps the schema portable, as certain SQL implementations require an explicit length for String columns).

- Do not explicitly set the column name argument for a column unless it is required to give a column the same name as a Python reserved keyword.

### Running migrations

All migrations should be run from the `prod-data-client` VM, which is accessed by `gcloud compute ssh prod-data-client`. Then follow the steps below according to the type of migration.

If it is your **first time logging in to the VM**, run `initial-setup` from your home directory. This will set up the git repository, pip environment, and SSL certificates you will need to run migrations.

NOTE: All commands below assume you are running in your pip environment. To launch it, run `pipenv shell` from the top-level package of the git repository.

#### Generating the migration

##### Adding a value to an enum

1. Run `readonly-prod-psql` and get the current set of values for the enum using `SELECT enum_range(NULL::<enum_name>);`.

2. Run `recidiviz/tools/create_enum_migration.py` according to the instructions in its file docstring.

3. Update the generated migration according to the included comments using the enum values from the query.

##### Adding or dropping a column or table

1. Update `schema.py`.

2. Run `generate-auto-migration -m <migration name>`. (Note: This does not detect changes to enum values, which is why enum value migrations require the separate process outlined above.)

3. Check that the generated migration looks right.

##### Changing the type or format of existing data

1. Run `generate-empty-migration -m <migration name>`.

2. Follow the example in [change\_aggregate\_datetime\_to\_date](https://github.com/Recidiviz/recidiviz-data/blob/master/recidiviz/persistence/database/migrations/versions/997ed5aca81f_change_aggregate_datetime_to_date.py) for how to apply a transformation to existing data.

#### Applying the migration

TODO(garciaz): work with rasmi@ and arian487@ on how this should be timed with creating a new release

1. Send a PR containing the migration for review.

2. Incorporate any review changes. Do not merge the PR yet.

3. Check the value in the `alembic_version` table in both dev and prod and ensure it's the same in both. If it isn't, check "Troubleshooting Alembic version issues" below.

4. Apply the migration to dev by running `migrate-dev-to-head`. Run `dev-psql` to verify that the outcome of the migration was successful.

5. Merge the PR.

6. Apply the migration to prod by running `migrate-prod-to-head`. Run `readonly-prod-psql` to verify that the outcome of the migration was successful.

#### Migration troubleshooting

##### Re-using existing enums for new columns

If you generate a migration that adds a new column that should use an existing enum type, Alembic by default will attempt to re-create that existing type, which will cause an error during migration.

To avoid this, you need to update the migration to ensure Alembic knows to re-use the existing type:

Import the `postgresql` dialect, since the migration must reference a PostgreSQL enum type.

```python
from sqlalchemy.dialects import postgresql
```

Then in the `sa.Column` constructor for the relevant column, change `sa.Enum` to `postgresql.ENUM` and add `create_type=False` to the enum constructor, e.g.:

```python
sa.Column(
    'some_column',
    postgresql.ENUM('VALUE_A', 'VALUE_B', create_type=False, name='enum_name'))
```

##### Alembic version issues

Alembic automatically manages a table called `alembic_version`. This table contains a single row containing the hash ID of the most recent migration run against the database. When you attempt to autogenerate or run a migration, if alembic does not see a migration file corresponding to this hash in the `versions` folder, the attempted action will fail.

If the above process is always followed, the `alembic_version` values in both `dev-data` and `prod-data` should always match both each other and the latest migration file in `versions`. If they don't, there could be a number of reasons:

- A local manual migration was run against dev without being run against prod, presumably for testing purposes (this is fine). In this case, bring dev back into sync with prod via the steps in "Syncing dev via manual local migration" below.

- Someone ran the above workflow but didn't merge their PR before running against prod (this is mostly fine). In this case, just have them merge the PR, then pull the latest migration file locally.

- A checked-in migration was run against prod without being run against dev first (this is bad). In this case, do a manual check of prod via `readonly-prod-psql` to see if any issues were introduced by the migration. If not, bring dev up to date with prod via the steps in "Syncing dev via manual local migration" below.

- A local manual migration was run against prod without being checked in (this is very bad). Fixing this may require separately syncing both dev and prod with `schema.py` and then overwriting their `alembic_version` values, depending on what changes were made by the migration.

##### Syncing dev via manual local migration

This process can be used to get `dev-data` back into sync with `prod-data` and the `versions` folder if it gets out of sync.

1. Manually overwrite the `alembic_version` value in `dev-data` to match `prod-data` and the `versions` folder.

2. Autogenerate a migration using dev (rather than prod) as the reference with `set-alembic-dev-env && alembic -c recidiviz/persistence/database/alembic.ini revision --autogenerate -m <migration name>`.

3. Run `migrate-dev-to-head` to apply the local migration.

4. Delete the migration file.

5. Manually overwrite the `alembic_version` value again, to undo the hash update caused by running the local migration.

## Restoring backups

Cloud SQL automatically saves backups of all database instances. To restore the database to an earlier state in the case of a major error, visit the "Backups" tab for the `prod-data` instance on GCP, select a backup, and select "Restore". This will revert the database to its state at the time of that backup.

## Manual intervention

In the (extremely rare) case where manual operations need to be performed directly on `prod-data` (which you shouldn't do), you can run the below command (but please don't) on `prod-data-client` to access the database with full permissions:

```bash
psql "sslmode=verify-ca sslrootcert=$SERVER_CA_PATH sslcert=$CLIENT_CERT_PATH \
sslkey=$CLIENT_KEY_PATH hostaddr=$PROD_DATA_IP user=$PROD_DATA_MIGRATION_USER \
dbname=$PROD_DATA_DB_NAME"
```

The `dev-psql` command always gives full permissions on `dev-data`, so manual operations there can be done freely.

## Setting up the VM

If a new `prod-data-client` VM needs to be created, follow the below process:

Create a Compute Engine VM in GCP running the latest version of Ubuntu. (Wait some time after creation before trying to SSH in; various setup processes will still hold needed locks.)

SSH in using `gcloud compute ssh <VM name>`

Run the following commands:

```bash
# Add repository for python3.7, which is currently not provided by Ubuntu
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt update
sudo apt upgrade
# Should include all requirements installed by the project Dockerfile, plus
# postgresql-client
sudo apt install locales git libxml2-dev libxslt1-dev python3.7-dev \
    python3-pip default-jre postgresql-client
sudo pip3 install pipenv
```

Through the GCP console for the `prod-data` database, create a set of SSL certs for the VM to use. Whitelist the IP of the VM to connect for both `dev-data` and `prod-data`.

Save `server-ca.pem` and `client-cert.pem` in `/etc/ssl/certs`, and save `client-key.pem` in `/etc/ssl/private`.

Create `/usr/local/sbin/initial-setup` with the below content, and `chmod` it to be executable.

```bash
# Clone the repo and set up pipenv
git clone https://github.com/Recidiviz/recidiviz-data && \
cd recidiviz-data && pipenv sync --dev

# Make local copies of certs owned by user (to avoid issues with needing
# sudo to access shared certs)
mkdir ~/prod_data_certs/ && cp /etc/ssl/certs/server-ca.pem \
~/prod_data_certs/server-ca.pem && cp /etc/ssl/certs/client-cert.pem \
~/prod_data_certs/client-cert.pem && sudo cp /etc/ssl/private/client-key.pem \
~/prod_data_certs/client-key.pem && sudo chmod 0600 \
~/prod_data_certs/client-key.pem && sudo chown $USER: \
~/prod_data_certs/client-key.pem
```

Create `/etc/profile.d/.env` with the below content:

```bash
# These should match the ENV values set in the Dockerfile
export LC_ALL=en_US.UTF-8
export LC_CTYPE=en_US.UTF-8
export LANG=en_US.UTF-8
export TZ=America/New_York

export SERVER_CA_PATH=~/prod_data_certs/server-ca.pem
export CLIENT_CERT_PATH=~/prod_data_certs/client-cert.pem
export CLIENT_KEY_PATH=~/prod_data_certs/client-key.pem

export DEV_DATA_IP=<dev-data IP>
export DEV_DATA_PASSWORD=<dev-data password>
export DEV_DATA_USER=<dev-data username>
export DEV_DATA_DB_NAME=<dev-data database name>

export PROD_DATA_IP=<prod-data IP>
export PROD_DATA_PASSWORD=<prod-data password>
export PROD_DATA_READONLY_USER=<prod-data readonly username>
export PROD_DATA_MIGRATION_USER=<prod-data migration username>
export PROD_DATA_DB_NAME=<prod-data database name>
```

Create `/etc/profile.d/prod-data-aliases.sh` with the below content:

```bash
source /etc/profile.d/.env

alias dev-psql="psql \"sslmode=disable hostaddr=$DEV_DATA_IP \
user=$DEV_DATA_USER dbname=$DEV_DATA_DB_NAME\""

alias readonly-prod-psql="psql \"sslmode=verify-ca \
sslrootcert=$SERVER_CA_PATH sslcert=$CLIENT_CERT_PATH \
sslkey=$CLIENT_KEY_PATH hostaddr=$PROD_DATA_IP \
user=$PROD_DATA_READONLY_USER dbname=$PROD_DATA_DB_NAME\""

alias set-alembic-dev-env="export SQLALCHEMY_DB_NAME=$DEV_DATA_DB_NAME \
&& export SQLALCHEMY_DB_HOST=$DEV_DATA_IP \
&& export SQLALCHEMY_DB_PASSWORD=$DEV_DATA_PASSWORD \
&& export SQLALCHEMY_DB_USER=$DEV_DATA_USER \
&& export SQLALCHEMY_USE_SSL=0"

alias set-alembic-prod-env="export SQLALCHEMY_DB_NAME=$PROD_DATA_DB_NAME \
&& export SQLALCHEMY_DB_HOST=$PROD_DATA_IP \
&& export SQLALCHEMY_DB_PASSWORD=$PROD_DATA_PASSWORD \
&& export SQLALCHEMY_DB_USER=$PROD_DATA_MIGRATION_USER \
&& export SQLALCHEMY_USE_SSL=1 \
&& export SQLALCHEMY_SSL_CERT_PATH=$CLIENT_CERT_PATH \
&& export SQLALCHEMY_SSL_KEY_PATH=$CLIENT_KEY_PATH"

alias generate-empty-migration="alembic -c \
recidiviz/persistence/database/alembic.ini revision"

alias generate-auto-migration="set-alembic-prod-env && alembic -c \
recidiviz/persistence/database/alembic.ini revision --autogenerate"

alias migrate-dev-to-head="set-alembic-dev-env && alembic -c \
recidiviz/persistence/database/alembic.ini upgrade head"

function migrate-prod-to-head {
  read -p "Are you sure? (y to continue, any other key to exit): " response
  if [[ $response == "y" ]]
  then
    set-alembic-prod-env && alembic -c \
        recidiviz/persistence/database/alembic.ini upgrade head
  else
    echo "Cancelled"
  fi
}
```

At the end of `/etc/bash.bashrc`, add the below line. This makes aliases available in both interactive and login shells.

```bash
source /etc/profile.d/prod-data-aliases.sh
```

