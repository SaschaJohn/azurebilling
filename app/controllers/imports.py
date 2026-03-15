from datetime import datetime

from flask import Blueprint, flash, redirect, render_template, request, url_for
from sqlalchemy import select, update

from app.extensions import db
from app.models.import_batch import ImportBatch
from app.services.csv_importer import import_csv

bp = Blueprint('imports', __name__, url_prefix='/imports')


@bp.route('/')
def index():
    page = request.args.get('page', 1, type=int)
    pagination = db.paginate(
        select(ImportBatch).where(ImportBatch.deleted_at.is_(None)).order_by(ImportBatch.started_at.desc()),
        page=page,
        per_page=20,
        error_out=False,
    )
    return render_template('imports/index.html', pagination=pagination)


@bp.route('/upload')
def upload_form():
    return render_template('imports/upload.html')


@bp.route('/', methods=['POST'])
def upload():
    file = request.files.get('file')
    if not file or not file.filename:
        flash('No file selected.', 'danger')
        return redirect(url_for('imports.upload_form'))

    batch = ImportBatch(
        filename=file.filename,
        status='processing',
        started_at=datetime.utcnow(),
    )
    db.session.add(batch)
    db.session.commit()
    batch_id = batch.id

    row_count = 0
    skipped_count = 0
    try:
        row_count, skipped_count = import_csv(file.stream, batch_id, db.session)
        db.session.execute(
            update(ImportBatch)
            .where(ImportBatch.id == batch_id)
            .values(
                status='success',
                row_count=row_count,
                skipped_count=skipped_count,
                finished_at=datetime.utcnow(),
            )
        )
        db.session.commit()
        flash(f'Import complete: {row_count} rows imported, {skipped_count} skipped.', 'success')
    except Exception as exc:
        db.session.rollback()
        db.session.execute(
            update(ImportBatch)
            .where(ImportBatch.id == batch_id)
            .values(
                status='error',
                error_msg=str(exc)[:1000],
                row_count=row_count,
                skipped_count=skipped_count,
                finished_at=datetime.utcnow(),
            )
        )
        db.session.commit()
        flash(f'Import failed: {exc}', 'danger')

    return redirect(url_for('imports.index'))


@bp.route('/<uuid:batch_id>/delete', methods=['POST'])
def delete_batch(batch_id):
    batch = db.session.get(ImportBatch, batch_id)
    if batch and batch.deleted_at is None:
        db.session.execute(
            update(ImportBatch)
            .where(ImportBatch.id == batch_id)
            .values(deleted_at=datetime.utcnow())
        )
        db.session.commit()
        flash(f'Eintrag "{batch.filename}" gelöscht (Daten bleiben erhalten).', 'success')
    return redirect(url_for('imports.index'))


@bp.route('/azure')
def azure_list():
    from app.services.azure_storage import list_csv_blobs
    import pathlib
    try:
        blobs = list_csv_blobs()
        error = None
    except Exception as exc:
        blobs = []
        error = str(exc)

    imported = {
        row[0]
        for row in db.session.query(ImportBatch.filename)
        .filter(ImportBatch.status == 'success', ImportBatch.deleted_at.is_(None))
        .all()
    }
    for blob in blobs:
        blob['imported'] = pathlib.Path(blob['name']).name in imported

    return render_template('imports/azure.html', blobs=blobs, error=error)


@bp.route('/azure', methods=['POST'])
def azure_import():
    from app.services.azure_storage import download_blob
    blob_name = request.form.get('blob_name', '').strip()
    if not blob_name:
        flash('Kein Blob-Name angegeben.', 'danger')
        return redirect(url_for('imports.azure_list'))

    try:
        local_path = download_blob(blob_name)
    except Exception as exc:
        flash(f'Download fehlgeschlagen: {exc}', 'danger')
        return redirect(url_for('imports.azure_list'))

    batch = ImportBatch(
        filename=local_path.name,
        status='processing',
        started_at=datetime.utcnow(),
    )
    db.session.add(batch)
    db.session.commit()
    batch_id = batch.id

    row_count = skipped_count = 0
    try:
        with open(local_path, 'rb') as f:
            row_count, skipped_count = import_csv(f, batch_id, db.session)
        db.session.execute(
            update(ImportBatch)
            .where(ImportBatch.id == batch_id)
            .values(
                status='success',
                row_count=row_count,
                skipped_count=skipped_count,
                finished_at=datetime.utcnow(),
            )
        )
        db.session.commit()
        flash(f'Import abgeschlossen: {row_count} Zeilen importiert, {skipped_count} übersprungen.', 'success')
    except Exception as exc:
        db.session.rollback()
        db.session.execute(
            update(ImportBatch)
            .where(ImportBatch.id == batch_id)
            .values(
                status='error',
                error_msg=str(exc)[:1000],
                row_count=row_count,
                skipped_count=skipped_count,
                finished_at=datetime.utcnow(),
            )
        )
        db.session.commit()
        flash(f'Import fehlgeschlagen: {exc}', 'danger')

    return redirect(url_for('imports.index'))
