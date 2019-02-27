"""booking_id_foreign_key_deferred

Revision ID: cfc3a453f53f
Revises: 70cd29324a51
Create Date: 2019-02-27 15:41:01.558106

"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'cfc3a453f53f'
down_revision = '70cd29324a51'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint('bond_booking_id_fkey', 'bond', type_='foreignkey')
    op.create_foreign_key(None, 'bond', 'booking', ['booking_id'], ['booking_id'], initially='DEFERRED', deferrable=True)
    op.drop_constraint('sentence_booking_id_fkey', 'sentence', type_='foreignkey')
    op.create_foreign_key(None, 'sentence', 'booking', ['booking_id'], ['booking_id'], initially='DEFERRED', deferrable=True)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(None, 'sentence', type_='foreignkey')
    op.create_foreign_key('sentence_booking_id_fkey', 'sentence', 'booking', ['booking_id'], ['booking_id'])
    op.drop_constraint(None, 'bond', type_='foreignkey')
    op.create_foreign_key('bond_booking_id_fkey', 'bond', 'booking', ['booking_id'], ['booking_id'])
    # ### end Alembic commands ###
