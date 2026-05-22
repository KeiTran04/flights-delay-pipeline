#!/bin/bash
set -e

HIVE_CONF_DIR=/opt/hive/conf

echo '>>> Writing hive-site.xml with PostgreSQL config...'

cat > ${HIVE_CONF_DIR}/hive-site.xml << 'XMLEOF'
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:postgresql://postgres:5432/metastore_db</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>org.postgresql.Driver</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value>hive</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionPassword</name>
    <value>hivepassword</value>
  </property>
  <property>
    <name>datanucleus.autoCreateSchema</name>
    <value>true</value>
  </property>
  <property>
    <name>hive.metastore.uris</name>
    <value>thrift://0.0.0.0:9083</value>
  </property>
  <property>
    <name>hive.metastore.schema.verification</name>
    <value>false</value>
  </property>
  <property>
    <name>fs.s3a.endpoint</name>
    <value>http://minio:9000</value>
  </property>
  <property>
    <name>fs.s3a.access.key</name>
    <value>minioadmin</value>
  </property>
  <property>
    <name>fs.s3a.secret.key</name>
    <value>minioadminpassword</value>
  </property>
  <property>
    <name>fs.s3a.path.style.access</name>
    <value>true</value>
  </property>
  <property>
    <name>fs.s3a.impl</name>
    <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
  </property>
</configuration>
XMLEOF

echo '>>> Initializing Hive Metastore schema...'
/opt/hive/bin/schematool -dbType postgres -initSchema --verbose 2>&1 | tail -20

echo '>>> Starting Hive Metastore service on port 9083...'
exec /opt/hive/bin/hive --service metastore \
  --hiveconf hive.root.logger=INFO,console \
  --hiveconf hive.log.file=hive-metastore.log
