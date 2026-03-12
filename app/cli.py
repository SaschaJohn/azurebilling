import os
import click
from datetime import datetime
from flask.cli import with_appcontext
from sqlalchemy import update

from app.extensions import db
from app.models.import_batch import ImportBatch
from app.services.csv_importer import import_csv


@click.command('import-file')
@click.argument('filepath', type=click.Path(exists=True))
@with_appcontext
def import_file_cmd(filepath):
    """Import an Azure billing CSV from a local file path."""
    filename = os.path.basename(filepath)
    click.echo(f'Starting import: {filename}')

    batch = ImportBatch(filename=filename, status='processing', started_at=datetime.utcnow())
    db.session.add(batch)
    db.session.commit()
    batch_id = batch.id

    row_count = 0
    skipped_count = 0
    try:
        def progress(n):
            click.echo(f'  {n} rows processed...')

        with open(filepath, 'rb') as f:
            row_count, skipped_count = import_csv(f, batch_id, db.session, progress_cb=progress)
        db.session.execute(
            update(ImportBatch).where(ImportBatch.id == batch_id).values(
                status='success', row_count=row_count,
                skipped_count=skipped_count, finished_at=datetime.utcnow(),
            )
        )
        db.session.commit()
        click.echo(f'Done: {row_count} inserted, {skipped_count} skipped/duplicate.')
    except Exception as exc:
        db.session.rollback()
        db.session.execute(
            update(ImportBatch).where(ImportBatch.id == batch_id).values(
                status='error', error_msg=str(exc)[:1000],
                row_count=row_count, skipped_count=skipped_count,
                finished_at=datetime.utcnow(),
            )
        )
        db.session.commit()
        click.echo(f'Import failed: {exc}', err=True)
        raise SystemExit(1)
