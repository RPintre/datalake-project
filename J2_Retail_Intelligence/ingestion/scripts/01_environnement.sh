#!/usr/bin/env bash
# Fiche d'environnement : releve des versions, chemins, configuration et etat des services (a executer dans hdfs-client).
export HADOOP_ROOT_LOGGER=ERROR,console
heure() { date '+%d/%m/%Y %H:%M:%S'; }
echo "== Heure de debut : $(heure) (heure de Paris)"
echo
echo "== 1. Systeme == [$(heure)]"
grep PRETTY_NAME /etc/os-release
uname -srm
echo "utilisateur : $(whoami)"
echo
echo "== 2. Java == [$(heure)]"
java -version 2>&1
echo "JAVA_HOME=${JAVA_HOME:-non defini} ; chemin reel de java : $(readlink -f "$(which java)")"
echo
echo "== 3. Python == [$(heure)]"
python3 --version
echo
echo "== 4. Hadoop == [$(heure)]"
hadoop version
echo "HADOOP_HOME=${HADOOP_HOME:-non defini}"
echo "HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-non defini}"
ls -1 "${HADOOP_CONF_DIR:-$HADOOP_HOME/etc/hadoop}"/*-site.xml
echo
echo "== 5. Configuration HDFS utilisee == [$(heure)]"
for k in fs.defaultFS dfs.namenode.rpc-address dfs.replication dfs.blocksize dfs.permissions.enabled dfs.namenode.name.dir dfs.datanode.data.dir; do
  echo "$k = $(hdfs getconf -confKey $k)"
done
echo
echo "== 6. Etat du NameNode et des DataNodes == [$(heure)]"
hdfs dfsadmin -safemode get
hdfs dfsadmin -report
echo
echo "== 7. Topologie == [$(heure)]"
hdfs dfsadmin -printTopology
echo; echo "== Heure de fin : $(heure)"
