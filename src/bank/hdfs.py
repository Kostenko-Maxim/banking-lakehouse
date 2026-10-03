"""WebHDFS upload with bounded memory and immutable batch directories."""

import hashlib
import json
from urllib.parse import quote

import requests

BASE = "http://namenode:9870/webhdfs/v1"


def url(path, operation):
    return f"{BASE}{quote(path, safe='/')}?user.name=bank&op={operation}"


def upload(folder, manifest):
    destination = "/sources/" + manifest["batch_id"]
    response = requests.put(url(destination, "MKDIRS"), timeout=30)
    response.raise_for_status()
    for entry in manifest["files"] + [{"name": "manifest.json"}]:
        path = destination + "/" + entry["name"]
        existing = requests.get(url(path, "OPEN"), stream=True, timeout=30)
        if existing.status_code == 200:
            digest = hashlib.sha256()
            for part in existing.iter_content(1024 * 1024):
                digest.update(part)
            existing.close()
            local = hashlib.sha256()
            with (folder / entry["name"]).open("rb") as source:
                for part in iter(lambda: source.read(1024 * 1024), b""):
                    local.update(part)
            if local.digest() != digest.digest():
                raise ValueError("HDFS batch is immutable")
            continue
        if existing.status_code != 404:
            existing.raise_for_status()
        existing.close()
        temporary = path + ".upload"
        create = requests.put(url(temporary, "CREATE") + "&overwrite=true", allow_redirects=False, timeout=30)
        create.raise_for_status()
        if create.status_code != 307:
            raise RuntimeError("WebHDFS did not return DataNode redirect")
        with (folder / entry["name"]).open("rb") as stream:
            result = requests.put(create.headers["Location"], data=stream, timeout=300)
            result.raise_for_status()
        renamed = requests.put(url(temporary, "RENAME") + "&destination=" + quote(path, safe="/"), timeout=30)
        renamed.raise_for_status()
        if not renamed.json()["boolean"]:
            raise RuntimeError("Atomic source rename failed")
    return destination


def read_manifest(batch_id):
    response = requests.get(url(f"/sources/{batch_id}/manifest.json", "OPEN"), timeout=30)
    response.raise_for_status()
    return json.loads(response.content)
