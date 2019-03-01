"""migrate_frontslash_enums

Revision ID: c82f37920d63
Revises: ade09190b367
Create Date: 2019-03-01 11:25:44.311952

"""

# Hackity hack to get around the fact that alembic runs this file as a
# top-level module rather than a child of the recidiviz module
import sys
import os
module_path = os.path.abspath(__file__)
# Walk up directories to reach main package
while not module_path.split('/')[-1] == 'recidiviz':
    if module_path == '/':
        raise RuntimeError('Top-level recidiviz package not found')
    module_path = os.path.dirname(module_path)
# Must insert parent directory of main package
sys.path.insert(0, os.path.dirname(module_path))

from alembic import op
import sqlalchemy as sa

from recidiviz.persistence.database.schema import Person

# revision identifiers, used by Alembic.
revision = 'c82f37920d63'
down_revision = 'ade09190b367'
branch_labels = None
depends_on = None

interim_values = ['AMERICAN_INDIAN/ALASKAN_NATIVE', 'AMERICAN_INDIAN_ALASKAN_NATIVE', 'ASIAN', 'BLACK', 'EXTERNAL_UNKNOWN', 'NATIVE_HAWAIIAN/PACIFIC_ISLANDER', 'NATIVE_HAWAIIAN_PACIFIC_ISLANDER', 'OTHER', 'WHITE'] 
final_values = ['AMERICAN_INDIAN_ALASKAN_NATIVE', 'ASIAN', 'BLACK', 'EXTERNAL_UNKNOWN', 'NATIVE_HAWAIIAN_PACIFIC_ISLANDER', 'OTHER', 'WHITE'] 


def upgrade():
    # Update enum to have both old and new values, so we can convert
    # between them
    op.execute('ALTER TYPE race RENAME TO race_old;')
    sa.Enum(*interim_values, name='race').create(bind=op.get_bind())
    op.alter_column('person', column_name='race',
                    type_=sa.Enum(*interim_values, name='race'),
                    postgresql_using='race::text::race')
    op.alter_column('person_history', column_name='race',
                    type_=sa.Enum(*interim_values, name='race'),
                    postgresql_using='race::text::race')
    op.execute('DROP TYPE race_old;')

    connection = op.get_bind()
    for person in connection.execute(Person.select()):
        if person.race == 'AMERICAN_INDIAN/ALASKAN_NATIVE':
            connection.execute(
                'UPDATE person SET race = \'AMERICAN_INDIAN_ALASKAN_NATIVE\''
                'WHERE person_id = {};'.format(person.person_id))
        if person.race == 'NATIVE_HAWAIIAN/PACIFIC_ISLANDER':
            connection.execute(
                'UPDATE person SET race = '
                '\'NATIVE_HAWAIIAN_PACIFIC_ISLANDER\' WHERE '
                'person_id = {};'.format(person.person_id))

    # Remove old values and only leave new values
    op.execute('ALTER TYPE race RENAME TO race_old;')
    sa.Enum(*final_values, name='race').create(bind=op.get_bind())
    op.alter_column('person', column_name='race',
                    type_=sa.Enum(*final_values, name='race'),
                    postgresql_using='race::text::race')
    op.alter_column('person_history', column_name='race',
                    type_=sa.Enum(*final_values, name='race'),
                    postgresql_using='race::text::race')
    op.execute('DROP TYPE race_old;')


def downgrade():
    raise NotImplementedError()
