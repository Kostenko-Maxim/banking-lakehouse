from pathlib import Path

import yaml

from bank.config import ROOT
from bank.publisher import pinned


def test_interfaces_and_storage_are_local():
    compose = yaml.safe_load((ROOT / "compose.yaml").read_text())
    assert compose["networks"]["lake"]["internal"] is True
    assert (
        compose["networks"]["interfaces"]["driver_opts"]["com.docker.network.bridge.enable_ip_masquerade"]
        == "false"
    )
    for service in compose["services"].values():
        assert "latest" not in service.get("image", "")
        assert all(port.startswith("127.0.0.1:") for port in service.get("ports", []))
    assert {"postgres-data", "hdfs-name", "hdfs-data", "source-data"} <= set(compose["volumes"])


def test_all_mart_queries_use_pinned_snapshots():
    versions = {"transactions": 12345, "customer_history": 67890}
    for file in (ROOT / "sql" / "trino").glob("*.sql"):
        if file.name == "analytics.sql":
            continue
        sql = pinned(file.read_text(), versions)
        assert "FOR VERSION AS OF" in sql
        assert "status='POSTED'" in sql or file.name == "history.sql"


def test_scripts_have_unix_line_endings():
    for file in Path(ROOT).rglob("*.sh"):
        assert b"\r\n" not in file.read_bytes()


def test_rpc_addresses_survive_multiple_networks():
    import xml.etree.ElementTree as ET

    root = ET.fromstring((ROOT / "infra/hadoop/conf/hdfs-site.xml").read_text())
    properties = {p.findtext("name"): p.findtext("value") for p in root.findall("property")}
    assert properties["dfs.namenode.rpc-bind-host"] == "0.0.0.0"
    compose = yaml.safe_load((ROOT / "compose.yaml").read_text())
    master = compose["services"]["spark-master"]
    assert master["command"][-1] in master["networks"]["lake"]["aliases"]
