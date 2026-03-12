"""Normalize resource group names to lowercase

Revision ID: 0005
Revises: 0004
Create Date: 2026-03-12 00:00:00.000000

"""
from alembic import op

revision = '0005'
down_revision = '0004'
branch_labels = None
depends_on = None


def upgrade():
    # Re-point fact FK from duplicates to canonical (lowest id per lower(resource_id))
    op.execute("""
        UPDATE fact_billing_line fl
        SET resource_group_fk = canonical.id
        FROM (
            SELECT lower(resource_id) AS key,
                   min(id) AS id
            FROM dim_resource_group
            GROUP BY lower(resource_id)
        ) canonical
        JOIN dim_resource_group dup
          ON lower(dup.resource_id) = canonical.key AND dup.id != canonical.id
        WHERE fl.resource_group_fk = dup.id
    """)

    # Delete duplicate rows, keeping the one with the lowest id
    op.execute("""
        DELETE FROM dim_resource_group
        WHERE id NOT IN (
            SELECT min(id)
            FROM dim_resource_group
            GROUP BY lower(resource_id)
        )
    """)

    # Lowercase resource_id and resource_group_name on all remaining rows
    op.execute("""
        UPDATE dim_resource_group
        SET resource_id         = lower(resource_id),
            resource_group_name = lower(resource_group_name)
    """)


def downgrade():
    pass  # Data loss on downgrade is acceptable; no-op
