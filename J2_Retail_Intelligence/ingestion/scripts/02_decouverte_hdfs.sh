#!/usr/bin/env bash
# Decouverte des commandes HDFS (etape 2) : etat, noeuds, listes, creation d'un repertoire, permissions, tailles.
export HADOOP_ROOT_LOGGER=ERROR,console
heure() { date '+%d/%m/%Y %H:%M:%S'; }
echo "== Heure de debut : $(heure) (heure de Paris)"
run() { echo; echo "[$(heure)] \$ $*"; "$@"; echo "(code retour : $?)"; }
echo "utilisateur du client : $(whoami)"
run hdfs dfs -ls /
run hdfs dfs -mkdir -p /retail-lake
run hdfs dfs -ls /
run hdfs dfs -ls /retail-lake
run hdfs dfs -ls -d / /retail-lake
run hdfs dfs -du -h /retail-lake
run hdfs dfs -df -h /
echo
echo "== Erreur volontaire et sans risque : lecture d'un repertoire qui n'existe pas =="
run hdfs dfs -ls /repertoire-inexistant
echo; echo "== Heure de fin : $(heure)"
