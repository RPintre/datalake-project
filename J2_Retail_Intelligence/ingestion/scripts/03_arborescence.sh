#!/usr/bin/env bash
# Creation de l'arborescence du Data Lake dans HDFS (etape 3).
export HADOOP_ROOT_LOGGER=ERROR,console
heure() { date '+%d/%m/%Y %H:%M:%S'; }
echo "== Heure de debut : $(heure) (heure de Paris)"
echo "== 1. Creation des zones == [$(heure)]"
for d in /retail-lake/raw/tpcxbb/initial /retail-lake/raw/tpcxbb/refresh /retail-lake/raw/nifi \
         /retail-lake/bronze/tpcxbb /retail-lake/silver /retail-lake/gold \
         /retail-lake/quality /retail-lake/logs/provenance /retail-lake/logs/ingestion; do
  hdfs dfs -mkdir -p "$d" && echo "cree : $d"
done
echo
echo "== 2. Permissions == [$(heure)]"
# Les zones sont lisibles par tous et modifiables par le proprietaire (hadoop).
hdfs dfs -chmod -R 755 /retail-lake
# Destination reservee au flux NiFi (UC-04) : proprietaire = identite sous laquelle NiFi ecrit dans HDFS.
hdfs dfs -chown nifi:supergroup /retail-lake/raw/nifi
hdfs dfs -chmod 775 /retail-lake/raw/nifi
echo
echo "== 3. Arborescence obtenue == [$(heure)]"
hdfs dfs -ls -R /retail-lake
echo; echo "== Heure de fin : $(heure)"
