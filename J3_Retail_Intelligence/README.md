# Retail Intelligence Platform : Jour 3

Projet fil rouge du Mastère Développement, Data & IA (IPSSI). Ce dossier contient les livrables, les scripts et les preuves du Jour 3 : UC-06 (transformation, qualité et Data Lakehouse). Il s'ajoute aux dossiers `J1_Retail_Intelligence` et `J2_Retail_Intelligence`. Ce README décrit l'organisation du dossier, la correspondance avec les livrables du TP, les versions observées, l'ordre d'exécution, les sorties produites, les contrôles disponibles et les limites connues. Toutes les heures des preuves sont en heure de Paris.

## 1. Contenu

```text
J3_Retail_Intelligence/
|-- README.md
|-- README_Jour3.pdf                    organisation, reproduction, limites
|-- UC06_Transformation_Qualite.pdf     livrables 1 a 9 + questions de recul
|-- preuves_uc06.pdf                    11 preuves d'execution reproduites
|-- notebooks/
|   `-- exploration_store_sales.ipynb   notebook d'exploration execute
|-- data/
|   `-- gold/                           jeux Gold (Parquet, quelques Ko)
|       |-- ventes_par_mois_magasin/
|       |-- ventes_par_mois_categorie/
|       `-- retours_par_motif/
|-- UC06_preuves/                       sorties brutes horodatees
|   |-- environnement/    01_environnement.txt
|   |-- sources/          02_inventaire.txt
|   |-- exploration/      03_exploration.txt, 03b_notebook.txt,
|   |                     notebook_exploration.html, notebook_exploration.png
|   |-- nettoyage/        04_nettoyage.txt
|   |-- transformations/  05_transformations.txt
|   |-- zones/            06_silver.txt, 07_gold.txt, 10_zones_hdfs.txt
|   |-- parquet/          08_parquet.txt
|   `-- qualite/          09_qualite.txt, 11_export_gold_rapport.txt,
|                         rapport_qualite_uc06.csv
`-- traitement/
    |-- docker-compose.yml              conteneur Spark (reseau HDFS du Jour 2)
    `-- scripts/
        |-- commun.py                   chemins, schemas, session Spark
        |-- nettoyage.py                regles de nettoyage R1 a R7
        |-- 01_environnement.py ... 09_qualite.py
        `-- exploration_store_sales.ipynb   notebook source (sans resultats)
```

## 2. Correspondance avec les livrables obligatoires du TP

| N° | Livrable du TP | Où le trouver |
|---|---|---|
| 1 | Fiche environnement | `UC06_Transformation_Qualite.pdf`, section 1 ; preuve 1 |
| 2 | Inventaire technique | `UC06_Transformation_Qualite.pdf`, section 2 ; preuve 2 |
| 3 | Notebook d'exploration | `notebooks/exploration_store_sales.ipynb` (exécuté, résultats enregistrés) et version HTML dans `UC06_preuves/exploration` ; script équivalent `03_exploration.py` ; section 3 ; preuves 3 et 4 |
| 4 | Règles de nettoyage | `UC06_Transformation_Qualite.pdf`, section 4 ; `traitement/scripts/nettoyage.py` ; preuve 5 |
| 5 | Scripts PySpark | `traitement/scripts/` ; section 5 ; preuve 6 |
| 6 | Données Silver | Section 6 ; preuves 7 et 11 (les fichiers Parquet restent dans HDFS : `/retail-lake/silver`) |
| 7 | Données Gold | Section 7 ; preuves 8 et 11 ; copie des trois jeux Parquet dans `data/gold` |
| 8 | Données Parquet | Section 8 ; preuve 9 ; fichiers Parquet de `data/gold` |
| 9 | Rapport qualité | Section 9 ; preuve 10 ; `UC06_preuves/qualite/rapport_qualite_uc06.csv` |
| 10 | README | Ce fichier |
| 11 | Preuves | `preuves_uc06.pdf` et dossier `UC06_preuves` |
| — | Questions de recul | `UC06_Transformation_Qualite.pdf`, section 10 |
| — | Tests et vérifications finales du TP (lecture, transformation, zones, Parquet, qualité) | `UC06_Transformation_Qualite.pdf`, section 11 |

## 3. Réalisé et non réalisé

| Élément | Statut |
|---|---|
| Spark 4.2.1 (mode local) lisant et écrivant dans le HDFS du Jour 2 | Exécuté |
| Inventaire et exploration de 6 sources TPCx-BB (2 tables de faits, 4 dimensions) | Exécutés (script et notebook Jupyter) |
| Règles de nettoyage R1 à R7 appliquées et mesurées | Exécutées |
| Jointures vérifiées (ventes × article, date, magasin ; retours × motif) | Exécutées |
| Zone Silver (8 jeux Parquet dont 2 quarantaines vides) | Exécutée et relue |
| Zone Gold (3 jeux Parquet) | Exécutée et relue |
| Comparaison CSV / Parquet | Exécutée (CSV et Parquet) ; JSON non disponible dans le jeu |
| Contrôles qualité (35 contrôles dont 3 d'information) | Exécutés, relus depuis Parquet |
| Données dégradées pour éprouver les contrôles | Non réalisé |
| Tables web, inventaire, clients, promotions | Non traitées |
| Format de table transactionnel (Delta, Iceberg, Hudi) | Non utilisé |

## 4. Environnement et versions

Spark / PySpark 4.2.1 (image `apache/spark:4.2.1-python3`), Python 3.10.12, Java 21.0.13, HDFS Hadoop 3.5.0 du Jour 2 (1 NameNode, 1 DataNode, réplication 1), Docker Desktop 29.9.3 sur Windows 10 Pro. Pour exécuter le notebook : `nbconvert` 7.17.2, `ipykernel` 7.3.0 et `nbformat` 5.11.1, installés dans le conteneur Spark. Détail dans `UC06_Transformation_Qualite.pdf`, section 1.

## 5. Reproduction

Prérequis : la pile HDFS du Jour 2 contenant les données dans la zone Raw (`J2_Retail_Intelligence`, étapes UC-03). Les commandes s'exécutent dans PowerShell, depuis le dossier `traitement`. Les scripts (`traitement/scripts`) s'exécutent dans le conteneur `spark`, qui rejoint le réseau Docker du Jour 2. Pour conserver une sortie, ajouter `| tee /sorties/<fichier>.txt` : le dossier `traitement\sorties` n'est pas versionné et les sorties de référence sont dans `UC06_preuves`.

Démarrage de l'environnement :

```powershell
docker compose -f ..\..\J2_Retail_Intelligence\ingestion\docker-compose.yml up -d namenode datanode hdfs-client
docker compose up -d spark
```

Chaque étape de traitement se lance avec la même commande, en changeant le nom du script :

```powershell
docker exec spark bash -c "cd /scripts && /opt/spark/bin/spark-submit --driver-memory 4g /scripts/<script>.py"
```

| Étape | Script | Ce qu'elle fait |
|---|---|---|
| 1 | `01_environnement.py` | Environnement : versions, session Spark, lecture, écriture de test |
| 2 | `02_inventaire.py` | Inventaire technique des 6 sources |
| 3 | `03_exploration.py` | Exploration PySpark |
| 4 | `04_nettoyage.py` | Règles de nettoyage et mesure de leur effet |
| 5 | `05_transformations.py` | Jointures vérifiées et agrégations (sans écriture) |
| 6 | `06_silver.py` | Écriture de la zone Silver |
| 7 | `07_gold.py` | Écriture de la zone Gold |
| 8 | `08_parquet.py` | Relecture Parquet et comparaison avec le CSV |
| 9 | `09_qualite.py` | Contrôles qualité et rapport |

Autres étapes :

| Étape | Commande | Ce qu'elle fait |
|---|---|---|
| Notebook | voir ci-dessous | Exécute `exploration_store_sales.ipynb` avec `jupyter nbconvert` (une installation préalable est nécessaire) |
| 10 | `docker exec hdfs-client hdfs dfs -ls -R /retail-lake/silver /retail-lake/gold /retail-lake/quality` puis `docker exec hdfs-client hdfs dfs -du -h /retail-lake/silver /retail-lake/gold /retail-lake/quality` | Relevé des zones Silver, Gold et qualité dans HDFS |
| 11 | `docker exec hdfs-client bash -c "mkdir -p /tmp/export && hdfs dfs -get /retail-lake/gold /tmp/export/gold && hdfs dfs -getmerge /retail-lake/quality/rapport_qualite_uc06/part-*.csv /tmp/export/rapport_qualite_uc06.csv"` puis `docker cp hdfs-client:/tmp/export .\sorties\export` | Export des jeux Gold et du rapport qualité vers le poste |

Exécution du notebook (installation des trois modules une seule fois dans le conteneur) :

```powershell
docker exec spark pip install nbconvert ipykernel nbformat
docker exec spark bash -c "cd /tmp && cp /scripts/exploration_store_sales.ipynb /sorties/notebook_exploration.ipynb && export SPARK_HOME=/opt/spark PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip PYSPARK_SUBMIT_ARGS='--driver-memory 4g pyspark-shell' && python3 -m nbconvert --to notebook --execute --inplace --ExecutePreprocessor.timeout=900 /sorties/notebook_exploration.ipynb"
```

Contrôles disponibles : l'étape 9 (`09_qualite.py`) exécute 35 contrôles (complétude, unicité, types, cohérence, volumes, intégrité référentielle, cohérence Gold / Silver) sur les fichiers Parquet relus et écrit le bilan dans `/retail-lake/quality/rapport_qualite_uc06` ; les étapes 4, 6 et 8 comportent aussi des contrôles de volume et de relecture.

Ordre : 1, 2, 3, 4, 5, 6, 7, 8, 9 (les étapes 6 et 7 doivent précéder la 8 et la 9, qui relisent Silver et Gold). Les scripts partagent `commun.py` (chemins, schémas, session) et `nettoyage.py` (règles). Paramètres : les chemins HDFS sont en tête de `commun.py` ; la mémoire du pilote (4 Go) est donnée par l'option `--driver-memory`. Arrêt : `docker compose stop` dans `traitement`, puis dans `J2_Retail_Intelligence\ingestion` ; ne jamais utiliser `down -v`, qui supprimerait les volumes HDFS.

## 6. Emplacements et sorties produites

| Élément | Chemin |
|---|---|
| Entrée (zone Raw) | `/retail-lake/raw/tpcxbb/initial` et `/retail-lake/raw/tpcxbb/refresh` |
| Silver (Parquet) | `/retail-lake/silver/{store_sales,store_returns,item,store,date_dim,reason}` et `/retail-lake/silver/_quarantaine/{store_sales,store_returns}` |
| Gold (Parquet) | `/retail-lake/gold/{ventes_par_mois_magasin,ventes_par_mois_categorie,retours_par_motif}` |
| Rapport qualité | `/retail-lake/quality/rapport_qualite_uc06` |
| Essai d'écriture | `/retail-lake/quality/test_ecriture` |

Les fichiers Silver (environ 57 Mo) restent dans HDFS et ne sont pas versionnés ; les trois jeux Gold, de quelques Ko, sont copiés dans `data/gold`.

## 7. Limites connues

Les données sont synthétiques (TPCx-BB) : profits majoritairement négatifs, prix à 0, libellés de motifs aléatoires ; aucun résultat n'est un résultat de benchmark officiel. Traitement mono-nœud (Spark en mode local), sans mesure de performance représentative. Les règles de nettoyage et les contrôles n'ont pas été éprouvés sur des données dégradées. Trois des 35 contrôles sont des contrôles d'information. Les sources non retenues (web, inventaire, clients) ne sont pas traitées.

## 8. Sécurité et confidentialité

Aucun mot de passe, identifiant ou jeton n'est stocké dans ces documents ni dans les scripts. Les fichiers du jeu (`retail_project`) et les volumes HDFS ne sont pas versionnés. Les interfaces web de HDFS sont limitées à `127.0.0.1` dans le `docker-compose.yml` du Jour 2.
