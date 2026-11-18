#!/usr/bin/env bash
# Etape 4 : inventaire prealable des fichiers TPCx-BB reellement disponibles, puis depot manuel (hdfs dfs -put) dans la zone Raw.
# Modes : inventaire | initial | refresh | contenu   (sans argument : les quatre a la suite)
# Les sources sont montees en lecture seule dans /donnees : aucune modification possible.
export HADOOP_ROOT_LOGGER=ERROR,console
heure() { date '+%d/%m/%Y %H:%M:%S'; }
SRC=/donnees
RAW=/retail-lake/raw/tpcxbb
CSV=/sorties/inventaire_fichiers.csv
mode=${1:-tout}
echo "== Heure de debut : $(heure) (heure de Paris)"

inventaire() {
  echo "== Inventaire prealable (avant tout depot) == [$(heure)]"
  echo "jeu;table;fichier;taille_octets;lignes;colonnes;sha256" > "$CSV"
  for jeu in data data_refresh; do
    while IFS= read -r f; do
      table=$(basename "$(dirname "$f")"); name=$(basename "$f")
      size=$(stat -c %s "$f"); lines=$(wc -l < "$f")
      case "$name" in *.csv) sep=',' ;; *) sep='|' ;; esac
      # Premiere ligne non vide (les fichiers d'audit commencent par une ligne vide), sans retour chariot.
      cols=$(tr -d '\r' < "$f" | awk 'NF{print; exit}' | awk -F"$sep" '{print NF}')
      sha=$(sha256sum "$f" | cut -d' ' -f1)
      echo "$jeu;$table;$name;$size;$lines;$cols;$sha" >> "$CSV"
    done < <(find "$SRC/$jeu" -type f | sort)
  done
  echo "fichiers inventories : $(($(wc -l < "$CSV") - 1)) ; inventaire ecrit dans $CSV"
  awk -F';' '{printf "%-13s %-22s %-30s %12s %9s %8s  %.16s\n", $1, $2, $3, $4, $5, $6, $7}' "$CSV"
  hdfs dfs -put -f "$CSV" /retail-lake/quality/inventaire_fichiers_sources.csv && echo "copie dans HDFS : /retail-lake/quality/inventaire_fichiers_sources.csv"
}

depot() {  # $1 = dossier source (data | data_refresh), $2 = destination (initial | refresh)
  echo "== Depot du jeu $2 dans la zone Raw (hdfs dfs -put, sans ecrasement) == [$(heure)]"
  while IFS= read -r f; do
    table=$(basename "$(dirname "$f")")
    hdfs dfs -mkdir -p "$RAW/$2/$table"
    if hdfs dfs -put "$f" "$RAW/$2/$table/"; then
      echo "[$(heure)] depose : $RAW/$2/$table/$(basename "$f")"
    else
      echo "[$(heure)] ECHEC : $f"
    fi
  done < <(find "$SRC/$1" -type f | sort)
  hdfs dfs -du -s -h "$RAW/$2"
}

contenu() {
  echo "== Contenu de la zone Raw apres le depot == [$(heure)]"
  hdfs dfs -ls -R "$RAW" | grep -v '^d' | awk '{printf "%s %-7s %-7s %12d octets  %s %s  %s\n", $1, $3, $4, $5, $6, $7, $8}'
  echo
  hdfs dfs -du -s -h "$RAW/initial" "$RAW/refresh"
}

case "$mode" in
  inventaire) inventaire ;;
  initial) depot data initial ;;
  refresh) depot data_refresh refresh ;;
  contenu) contenu ;;
  tout) inventaire; echo; depot data initial; echo; depot data_refresh refresh; echo; contenu ;;
  *) echo "mode inconnu : $mode"; exit 1 ;;
esac
echo; echo "== Heure de fin : $(heure)"
