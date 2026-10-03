import os
import subprocess
from pathlib import Path
from xml.sax.saxutils import escape

properties = {
    "javax.jdo.option.ConnectionURL": "jdbc:postgresql://postgres:5432/metastore",
    "javax.jdo.option.ConnectionDriverName": "org.postgresql.Driver",
    "javax.jdo.option.ConnectionUserName": "bank",
    "javax.jdo.option.ConnectionPassword": os.environ["POSTGRES_PASSWORD"],
    "hive.metastore.warehouse.dir": "hdfs://namenode:8020/warehouse",
    "hive.metastore.schema.verification": "true",
    "datanucleus.schema.autoCreateAll": "false",
    "hive.metastore.event.db.notification.api.auth": "false",
}
Path("/opt/hive/conf/hive-site.xml").write_text(
    "<configuration>" + "".join(
        f"<property><name>{k}</name><value>{escape(v)}</value></property>" for k, v in properties.items()
    ) + "</configuration>"
)
if subprocess.run(["schematool", "-dbType", "postgres", "-info"]).returncode:
    subprocess.run(["schematool", "-dbType", "postgres", "-initSchema"], check=True)
os.execvp("hive", ["hive", "--service", "metastore", "-p", "9083"])
