#!/usr/bin/env bash
# Remise a zero du Data Lake de test avant de rejouer l'UC-03 : supprime /retail-lake dans HDFS (donnees de test uniquement).
# Le NameNode n'est PAS reformate. A executer dans hdfs-client.
export HADOOP_ROOT_LOGGER=ERROR,console
heure() { date '+%d/%m/%Y %H:%M:%S'; }
echo "== Remise a zero [$(heure)]"
echo "Avant :"
hdfs dfs -ls / 2>&1
if hdfs dfs -test -d /retail-lake; then
  hdfs dfs -rm -r -skipTrash /retail-lake
fi
echo "Apres :"
hdfs dfs -ls / 2>&1
echo "(repertoire racine vide si aucune ligne n'est listee)"
