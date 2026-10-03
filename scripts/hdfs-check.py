"""Real WebHDFS checksum/count verification, including repeated immutable upload."""

import argparse
import hashlib
import tempfile
from pathlib import Path

import requests

from bank.generator import generate
from bank.hdfs import read_manifest, upload, url
from bank.manifest import validate

parser = argparse.ArgumentParser()
parser.add_argument("--existing", action="store_true")
args = parser.parse_args()


def verify_files(destination, manifest):
    for entry in manifest["files"]:
        response = requests.get(url(destination + "/" + entry["name"], "OPEN"), timeout=30)
        response.raise_for_status()
        assert hashlib.sha256(response.content).hexdigest() == entry["sha256"]
        assert len(response.content.splitlines()) == entry["records"]


with tempfile.TemporaryDirectory() as temporary:
    folder = generate(Path(temporary), "2025-01-01", seed=987, count=20)
    manifest = validate(folder)
    if args.existing:
        assert read_manifest(manifest["batch_id"]) == manifest
        verify_files("/sources/" + manifest["batch_id"], manifest)
    destination = upload(folder, manifest)
    assert read_manifest(manifest["batch_id"]) == manifest
    assert upload(folder, manifest) == destination
    verify_files(destination, manifest)
    print("WebHDFS upload, checksums, counts and replay: passed")
