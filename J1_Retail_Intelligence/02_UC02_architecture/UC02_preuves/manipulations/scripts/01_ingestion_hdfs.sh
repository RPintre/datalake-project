#!/usr/bin/env bash
# Ingestion (repli de NiFi) : creation de l'arborescence du Data Lake dans HDFS et depot des fichiers sources
# dans la zone Bronze, sans aucune transformation. A executer dans le conteneur hdfs-client.
set -euo pipefail

echo "== 1. Creation de l'arborescence du Data Lake dans HDFS =="
hdfs dfs -mkdir -p /datalake/bronze/data /datalake/bronze/data_refresh /datalake/silver /datalake/gold

echo "== 2. Depot des fichiers sources dans la zone Bronze (jeu initial) =="
for t in store_sales item date_dim; do
  hdfs dfs -mkdir -p /datalake/bronze/data/$t
  hdfs dfs -put -f /donnees/data/$t/$t.dat /datalake/bronze/data/$t/
done

echo "== 3. Depot des fichiers du jeu de rafraichissement (depot separe) =="
for t in store_sales item; do
  hdfs dfs -mkdir -p /datalake/bronze/data_refresh/$t
  hdfs dfs -put -f /donnees/data_refresh/$t/$t.dat /datalake/bronze/data_refresh/$t/
done

echo "== 4. Contenu de l'arborescence =="
hdfs dfs -ls -R /datalake

echo "== 5. Taille par table (zone Bronze) =="
hdfs dfs -du -h /datalake/bronze/data
hdfs dfs -du -h /datalake/bronze/data_refresh

echo "== 6. Controle de l'integrite des blocs (fsck) =="
hdfs fsck /datalake -files -blocks | tail -20

echo "== 7. Controle du nombre de lignes deposees =="
for t in store_sales item date_dim; do
  echo "$t (jeu initial) : $(hdfs dfs -cat /datalake/bronze/data/$t/$t.dat | wc -l) lignes dans HDFS, $(wc -l < /donnees/data/$t/$t.dat) lignes dans le fichier source"
done
