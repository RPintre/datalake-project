#!/usr/bin/env bash
# Verification du depot realise par le flux NiFi : presence, taille, proprietaire, somme de controle, dossier d'entree vide.
export HADOOP_ROOT_LOGGER=ERROR,console
heure() { date '+%d/%m/%Y %H:%M:%S'; }
echo "== Heure de debut : $(heure) (heure de Paris)"
echo "== 1. Destination HDFS du flux NiFi == [$(heure)]"
hdfs dfs -ls -h /retail-lake/raw/nifi
hdfs dfs -du -h /retail-lake/raw/nifi
echo
echo "== 2. Comparaison SHA-256 : depot NiFi, depot manuel (UC-03) et source == [$(heure)]"
printf '%-14s %-18s %-18s %-18s %s\n' fichier nifi manuel_uc03 source statut
for f in ship_mode income_band; do
  n=$(hdfs dfs -cat /retail-lake/raw/nifi/$f.dat | sha256sum | cut -c1-16)
  m=$(hdfs dfs -cat /retail-lake/raw/tpcxbb/initial/$f/$f.dat | sha256sum | cut -c1-16)
  s=$(sha256sum /donnees/data/$f/$f.dat | cut -c1-16)
  if [ "$n" = "$m" ] && [ "$m" = "$s" ]; then st=identiques; else st=DIFFERENTS; fi
  printf '%-14s %-18s %-18s %-18s %s\n' "$f.dat" "$n" "$m" "$s" "$st"
done
echo
echo "== 3. Dossier d'entree de NiFi (GetFile, Keep Source File = false) == [$(heure)]"
echo "fichiers restants : $(ls /data/nifi/input | wc -l)"
echo "== 4. Sources originales conservees == [$(heure)]"
ls -l /donnees/data/ship_mode/ship_mode.dat /donnees/data/income_band/income_band.dat
echo; echo "== Heure de fin : $(heure)"
