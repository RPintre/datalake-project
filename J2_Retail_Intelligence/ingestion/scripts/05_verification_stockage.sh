#!/usr/bin/env bash
# Etape 5 : verification du stockage HDFS et journal de provenance du depot manuel.
# Modes : presence | tailles | sha256 | integrite   (sans argument : les quatre a la suite)
export HADOOP_ROOT_LOGGER=ERROR,console
heure() { date '+%d/%m/%Y %H:%M:%S'; }
SRC=/donnees
RAW=/retail-lake/raw/tpcxbb
TAILLES=/sorties/_tailles.tsv
JOURNAL=/sorties/provenance_depot_manuel.csv
mode=${1:-tout}
echo "== Heure de debut : $(heure) (heure de Paris)"

# Liste des fichiers du depot : "dossier_source:destination"
fichiers() { for j in data:initial data_refresh:refresh; do find "$SRC/${j%%:*}" -type f | sort | sed "s#^#${j##*:} #"; done; }

presence() {
  echo "== 1. Presence des repertoires attendus == [$(heure)]"
  for d in /retail-lake/raw/tpcxbb/initial /retail-lake/raw/tpcxbb/refresh /retail-lake/raw/nifi /retail-lake/bronze/tpcxbb \
           /retail-lake/silver /retail-lake/gold /retail-lake/quality /retail-lake/logs/provenance /retail-lake/logs/ingestion; do
    if hdfs dfs -test -d "$d"; then echo "present : $d"; else echo "ABSENT  : $d"; fi
  done
  echo
  echo "== 2. Permissions de la zone Raw == [$(heure)]"
  hdfs dfs -ls -d /retail-lake /retail-lake/raw /retail-lake/raw/tpcxbb /retail-lake/raw/tpcxbb/initial /retail-lake/raw/nifi
  hdfs dfs -ls "$RAW/initial/store_sales"
  echo
  echo "== 3. Lecture controlee : 2 premieres lignes de store_sales dans HDFS == [$(heure)]"
  hdfs dfs -cat "$RAW/initial/store_sales/store_sales.dat" 2>/dev/null | head -2
}

tailles() {
  echo "== 4. Comparaison source locale et fichier stocke : taille et nombre de lignes == [$(heure)]"
  : > "$TAILLES"
  ok=0; ko=0
  fichiers | while read -r dst_jeu f; do
    table=$(basename "$(dirname "$f")"); name=$(basename "$f"); dst="$RAW/$dst_jeu/$table/$name"
    s_size=$(stat -c %s "$f"); h_size=$(hdfs dfs -stat %b "$dst")
    s_lines=$(wc -l < "$f"); h_lines=$(hdfs dfs -cat "$dst" 2>/dev/null | wc -l)
    if [ "$s_size" = "$h_size" ] && [ "$s_lines" = "$h_lines" ]; then st=identique; else st=DIFFERENT; fi
    echo "$dst_jeu;$table;$name;$s_size;$h_size;$s_lines;$h_lines" >> "$TAILLES"
    printf '%s %-8s %-22s %-28s %12s %12s %10s %10s  %s\n' "$(date +%H:%M:%S)" "$dst_jeu" "$table" "$name" "$s_size" "$h_size" "$s_lines" "$h_lines" "$st"
  done
  echo "fichiers compares : $(wc -l < "$TAILLES") ; tailles ou lignes differentes : $(awk -F';' '$4!=$5 || $6!=$7' "$TAILLES" | wc -l)"
}

sha256() {
  echo "== 5. Somme de controle SHA-256 : source et contenu relu depuis HDFS == [$(heure)]"
  echo "horodatage;methode;source_native;jeu;table;fichier;taille_source;taille_hdfs;lignes_source;lignes_hdfs;sha256_source;sha256_hdfs;statut" > "$JOURNAL"
  ok=0; ko=0
  while IFS=';' read -r dst_jeu table name s_size h_size s_lines h_lines; do
    case "$dst_jeu" in initial) src_jeu=data ;; *) src_jeu=data_refresh ;; esac
    f="$SRC/$src_jeu/$table/$name"; dst="$RAW/$dst_jeu/$table/$name"
    s_sha=$(sha256sum "$f" | cut -d' ' -f1); h_sha=$(hdfs dfs -cat "$dst" 2>/dev/null | sha256sum | cut -d' ' -f1)
    if [ "$s_size" = "$h_size" ] && [ "$s_lines" = "$h_lines" ] && [ "$s_sha" = "$h_sha" ]; then st=identique; ok=$((ok+1)); else st=DIFFERENT; ko=$((ko+1)); fi
    echo "$(date '+%d/%m/%Y %H:%M:%S');hdfs dfs -put;kit TPCx-BB (fourni par le formateur);$dst_jeu;$table;$name;$s_size;$h_size;$s_lines;$h_lines;$s_sha;$h_sha;$st" >> "$JOURNAL"
    printf '%s %-8s %-22s %-28s %.16s %.16s  %s\n' "$(date +%H:%M:%S)" "$dst_jeu" "$table" "$name" "$s_sha" "$h_sha" "$st"
  done < "$TAILLES"
  echo "fichiers identiques : $ok ; differents : $ko"
  echo
  echo "== 6. Recuperation controlee (hdfs dfs -get) puis comparaison binaire avec la source == [$(heure)]"
  rm -f /tmp/store_sales_recupere.dat
  hdfs dfs -get "$RAW/initial/store_sales/store_sales.dat" /tmp/store_sales_recupere.dat
  if cmp -s /tmp/store_sales_recupere.dat "$SRC/data/store_sales/store_sales.dat"; then echo "cmp : fichier recupere identique a la source"; else echo "cmp : DIFFERENT"; fi
  rm -f /tmp/store_sales_recupere.dat
  echo
  echo "== 7. Somme de controle native HDFS (hdfs dfs -checksum) == [$(heure)]"
  hdfs dfs -checksum "$RAW/initial/store_sales/store_sales.dat"
  hdfs dfs -checksum "$RAW/initial/item/item.dat"
}

integrite() {
  echo "== 8. Sources originales conservees : somme de controle recalculee apres le depot == [$(heure)]"
  ok2=0; ko2=0
  while IFS=';' read -r jeu table fichier taille lignes colonnes sha; do
    [ "$jeu" = "jeu" ] && continue
    if [ "$(sha256sum "$SRC/$jeu/$table/$fichier" | cut -d' ' -f1)" = "$sha" ]; then ok2=$((ok2+1)); else ko2=$((ko2+1)); fi
  done < /sorties/inventaire_fichiers.csv
  echo "sources inchangees : $ok2 ; modifiees : $ko2"
  echo
  echo "== 9. Integrite des blocs (fsck) == [$(heure)]"
  hdfs fsck /retail-lake/raw -files -blocks 2>/dev/null | grep -E "web_clickstreams.dat|store_sales.dat"
  hdfs fsck /retail-lake 2>/dev/null | grep -E "Total size|Total files|Total blocks|Under-replicated|Missing|Corrupt|HEALTHY"
  echo
  echo "== 10. Journal de provenance du depot manuel == [$(heure)]"
  hdfs dfs -put -f "$JOURNAL" /retail-lake/logs/provenance/depot_manuel_uc03.csv
  hdfs dfs -ls -h /retail-lake/logs/provenance /retail-lake/quality
  echo "lignes du journal : $(($(wc -l < "$JOURNAL") - 1))"
}

case "$mode" in
  presence) presence ;;
  tailles) tailles ;;
  sha256) sha256 ;;
  integrite) integrite ;;
  tout) presence; echo; tailles; echo; sha256; echo; integrite ;;
  *) echo "mode inconnu : $mode"; exit 1 ;;
esac
echo; echo "== Heure de fin : $(heure)"
