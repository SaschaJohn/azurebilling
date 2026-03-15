import os
import pathlib

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from flask import current_app


def get_container_client():
    account = current_app.config['AZURE_STORAGE_ACCOUNT_NAME']
    container = current_app.config['AZURE_STORAGE_CONTAINER_NAME']
    url = f"https://{account}.blob.core.windows.net"
    client = BlobServiceClient(url, credential=DefaultAzureCredential())
    return client.get_container_client(container)


def list_csv_blobs():
    """Returns list of dicts: {name, size, last_modified}"""
    cc = get_container_client()
    blobs = []
    for blob in cc.list_blobs():
        name_lower = blob.name.lower()
        if name_lower.endswith('.csv') and not name_lower.startswith('cloudability') and not name_lower.startswith('mg'):
            blobs.append({
                'name': blob.name,
                'size': blob.size,
                'last_modified': blob.last_modified,
            })
    return sorted(blobs, key=lambda b: b['last_modified'], reverse=True)


def download_blob(blob_name):
    """Downloads blob to AZURE_BILLING_DOWNLOAD_DIR, returns local Path."""
    dest_dir = pathlib.Path(current_app.config['AZURE_BILLING_DOWNLOAD_DIR'])
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / pathlib.Path(blob_name).name
    cc = get_container_client()
    with open(dest_path, 'wb') as f:
        cc.download_blob(blob_name).readinto(f)
    return dest_path
