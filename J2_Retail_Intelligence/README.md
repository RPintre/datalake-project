# Retail Intelligence Platform : Jour 2

Projet fil rouge du Mastère Développement, Data & IA (IPSSI). Ce dossier contient les livrables, les scripts et les preuves du Jour 2 : UC-03 (Hadoop, HDFS et Data Lake) et UC-04 (ingestion avec Apache NiFi). Il s'ajoute au dossier `J1_Retail_Intelligence`. Ce README décrit l'organisation du dossier, la correspondance avec les livrables attendus et la procédure de reproduction (versions, commandes de démarrage, chemins HDFS, paramètres NiFi non sensibles, commandes d'arrêt). Toutes les heures des preuves sont en heure de Paris.

## 1. Contenu

```text
J2_Retail_Intelligence/
|-- README_Jour2.pdf
|-- UC03_Hadoop_HDFS.pdf
|-- UC03_preuves/
|   |-- preuves_uc03.pdf
|   |-- environnement/   (démarrage, relevé de l'environnement, capture de la vue générale)
|   |-- hdfs/            (découverte, vérifications, pannes du NameNode et leur correction)
|   `-- data_lake/       (arborescence, inventaire, dépôts, journal de provenance, captures)
|-- UC04_ingestion_NiFi.pdf
|-- UC04_preuves/
|   |-- preuves_uc04.pdf
|   |-- configuration/   (démarrage de NiFi, découverte, flux, configuration exportée, captures)
|   |-- execution/       (dépôts par le flux, vérification dans HDFS, flux enrichi testé)
|   |-- erreurs/         (PutHDFS absent de l'image, dossier inexistant, dépôt en double)
|   `-- provenance/      (événements de provenance, liste dans l'interface)
`-- ingestion/           (docker-compose.yml, scripts, configuration)
```

## 2. Correspondance avec les livrables

### 2.1 UC-03

| Livrable attendu | Où le trouver |
|---|---|
| Fiche d'environnement technique | `UC03_Hadoop_HDFS.pdf`, section 1 ; `UC04_ingestion_NiFi.pdf`, section 1 pour NiFi |
| Fiche de découverte Hadoop et HDFS | `UC03_Hadoop_HDFS.pdf`, section 2 |
| Commandes et résultats HDFS | `UC03_Hadoop_HDFS.pdf`, section 3 ; preuves 1 à 9 |
| Arborescence du Data Lake | `UC03_Hadoop_HDFS.pdf`, section 4 ; preuve 3 |
| Inventaire des fichiers TPCx-BB | `UC03_Hadoop_HDFS.pdf`, section 5 ; preuve 4 |
| Preuves du dépôt et des contrôles de stockage | `UC03_Hadoop_HDFS.pdf`, section 6 ; preuves 5 à 9 |
| Difficultés rencontrées et solutions | `UC03_Hadoop_HDFS.pdf`, section 7 ; preuves 10 et 11 |
| Questions de recul (1 à 6, 12 et 13) | `UC03_Hadoop_HDFS.pdf`, section 8 |

### 2.2 UC-04

| Livrable attendu | Où le trouver |
|---|---|
| Fiche de découverte de NiFi | `UC04_ingestion_NiFi.pdf`, sections 1 et 2 ; preuves 1 et 2 |
| Schéma du flux d'ingestion NiFi | `UC04_ingestion_NiFi.pdf`, sections 3.2 et 6.1 ; preuves 3 et 11 |
| Configuration NiFi (export ou captures) | `UC04_ingestion_NiFi.pdf`, sections 3.3 et 3.4 ; preuve 4 ; `UC04_preuves/configuration/flow_definition.json` |
| Preuves d'exécution, de dépôt, compteurs et files d'attente | `UC04_ingestion_NiFi.pdf`, section 4 ; preuves 5, 6, 12 et 14 |
| Tableau des erreurs et des corrections | `UC04_ingestion_NiFi.pdf`, section 5 ; preuves 7, 8 et 9 |
| Documentation de la provenance | `UC04_ingestion_NiFi.pdf`, section 7 ; preuves 10 et 13 |
| Flux enrichi (étape 12 du TP) | `UC04_ingestion_NiFi.pdf`, section 6 ; preuves 11 à 13 |
| Questions de recul (7 à 11 et 14) | `UC04_ingestion_NiFi.pdf`, section 8 |
| Script de repli | Sans objet : NiFi a été exécuté, le repli n'est pas utilisé |
| README de reproduction | Ce fichier |

## 3. Réalisé et conçu

| Élément | Statut |
|---|---|
| HDFS 3.5.0 (1 NameNode, 1 DataNode) | Exécuté |
| Arborescence `/retail-lake` et dépôt de 36 fichiers TPCx-BB dans la zone Raw | Exécuté et vérifié (taille, lignes, SHA-256) |
| Vérifications : présence, permissions, lecture, récupération, `fsck` | Exécutées |
| Pannes de démarrage du NameNode | Rencontrées lors de la mise en place, montrées par un essai dans un conteneur séparé, avec leur correction |
| Apache NiFi 2.12.0 : flux GetFile vers PutHDFS | Exécuté : 2 fichiers déposés, vérifiés par SHA-256 |
| Erreurs contrôlées : dossier inexistant, dépôt en double | Exécutées et corrigées |
| Flux enrichi (UpdateAttribute, RouteOnAttribute, LogAttribute) | Exécuté : 1 fichier reconnu et déposé, 1 fichier non reconnu et journalisé |
| Provenance NiFi | Relevée dans l'interface et par l'API (20 événements, 5 FlowFiles) |
| Zones Bronze, Silver et Gold | Créées, vides |
| Cluster multi-nœuds, réplication supérieure à 1 | Non réalisés : environnement mono-nœud |
| Parties complémentaire et avancée | Non réalisées |

## 4. Environnement et versions

Hadoop 3.5.0, NiFi 2.12.0 (avec les archives `nifi-hadoop-nar` et `nifi-hadoop-libraries-nar` 2.12.0), Java 17 (Hadoop) et 21 (NiFi), Python 3.12.3, Ubuntu 24.04 dans les conteneurs, Docker Desktop 29.9.3 sur Windows 10 Pro. Les écarts avec l'environnement de référence du TP sont décrits dans `UC03_Hadoop_HDFS.pdf`, section 1.5, et `UC04_ingestion_NiFi.pdf`, section 1.3.

## 5. Reproduction

Prérequis : Docker Desktop et les fichiers du jeu TPCx-BB (dossier `retail_project`, non versionné). Les commandes s'exécutent dans PowerShell, depuis le dossier `ingestion`. La variable `DATA_DIR` indique le dossier `retail_project` (par défaut `D:/code/datalake/retail_project`). Les scripts (dossier `scripts`) s'exécutent dans les conteneurs ; chacun affiche son heure de début et de fin. Pour conserver une sortie, ajouter `| tee /sorties/<fichier>.txt` : le dossier `ingestion\sorties` n'est pas versionné et les sorties de référence sont dans `UC03_preuves` et `UC04_preuves`.

### 5.1 UC-03

| Étape | Commande | Ce qu'elle fait |
|---|---|---|
| Démarrage | `docker compose up -d namenode datanode hdfs-client` | Démarre HDFS (NameNode, DataNode) et le conteneur client |
| 1 | `docker exec hdfs-client bash /scripts/01_environnement.sh` puis `docker exec namenode jps` et `docker exec datanode jps` | Relevé de l'environnement (versions, configuration, état, processus Java) |
| Pannes | `docker run --rm --network none --add-host namenode:127.0.0.1 --hostname namenode --env-file hadoop.env apache/hadoop:3.5.0 hdfs namenode` | Panne 1 : un NameNode d'essai, dans un conteneur neuf isolé (réseau coupé, aucun port publié), s'arrête faute de répertoire de données. Panne 2 : même commande avec un volume d'essai vide (`docker volume create namenode-essai-data`, droits donnés à `hadoop` par `docker run --rm -u root -v namenode-essai-data:/d apache/hadoop:3.5.0 chown hadoop:hadoop /d`, monté avec `-v namenode-essai-data:/tmp/hadoop-hadoop/dfs/name`), puis correction par `hdfs namenode -format -nonInteractive` sur ce volume d'essai seul ; supprimer ensuite le conteneur et le volume d'essai |
| 2 | `docker exec hdfs-client bash /scripts/02_decouverte_hdfs.sh` | Découverte des commandes HDFS |
| 3 | `docker exec hdfs-client bash /scripts/03_arborescence.sh` | Création de l'arborescence |
| 4 | `docker exec hdfs-client bash /scripts/04_inventaire_depot.sh <mode>` avec `inventaire`, `initial`, `refresh` puis `contenu` | Inventaire, dépôt du jeu initial, dépôt du rafraîchissement, contenu de la zone Raw |
| 5 | `docker exec hdfs-client bash /scripts/05_verification_stockage.sh <mode>` avec `presence`, `tailles`, `sha256` puis `integrite` | Présence et permissions, tailles et lignes, SHA-256, intégrité et journal |
| Captures | pages `http://localhost:9870/dfshealth.html#tab-overview` et `http://localhost:9870/explorer.html#/retail-lake` | Interface web de HDFS, captures d'écran faites avec Chrome |
| Remise à zéro | `docker exec hdfs-client bash /scripts/00_reinitialiser_uc03.sh` | Supprime `/retail-lake` (données de test) avant de rejouer |

### 5.2 UC-04

Les archives NiFi `nifi-hadoop-nar` et `nifi-hadoop-libraries-nar` 2.12.0 (absentes de l'image) se téléchargent une fois depuis Maven Central dans `nifi\extensions`, avec contrôle de la somme SHA-1 publiée à côté de chaque archive :

```powershell
New-Item -ItemType Directory -Force nifi\extensions
foreach ($a in "nifi-hadoop-nar", "nifi-hadoop-libraries-nar") {
  $base = "https://repo1.maven.org/maven2/org/apache/nifi/$a/2.12.0/$a-2.12.0"
  curl.exe -s -o "nifi\extensions\$a-2.12.0.nar" "$base.nar"
  (curl.exe -s "$base.nar.sha1").Trim(); (Get-FileHash "nifi\extensions\$a-2.12.0.nar" -Algorithm SHA1).Hash.ToLower()   # les deux valeurs doivent être identiques
}
```

Le flux se pilote avec `nifi_flux.py`, exécuté dans le conteneur `hdfs-client`. Les identifiants de NiFi sont générés au démarrage : `docker exec nifi grep -h "Generated" /opt/nifi/nifi-current/logs/nifi-app.log` les affiche, puis chaque commande est lancée avec `docker exec -e NIFI_USER=<identifiant> -e NIFI_PASS=<mot de passe> hdfs-client python3 /scripts/nifi_flux.py <commande>`. Ces valeurs ne doivent pas être enregistrées dans un fichier. Un fichier est déposé dans le dossier d'entrée du flux en copiant `retail_project\data\<table>\<table>.dat` dans `nifi_data\input`. L'abréviation `flux <commande>` ci-dessous désigne la commande `nifi_flux.py` complète.

| Étape | Opérations | Ce qu'elle fait |
|---|---|---|
| N0 | `docker compose rm -s -f nifi`, puis `$env:NIFI_RUN = Get-Date -Format "MMddHHmmss"` et `docker compose up -d --no-deps --force-recreate nifi` ; attendre que `curl.exe -k -s -o NUL -w "%{http_code}" https://localhost:8443/nifi-api/flow/about` réponde 401 | Démarre une nouvelle instance NiFi sur un état vierge |
| N1 | `flux decouverte` | Découverte de l'instance (version, processeurs disponibles) |
| N2 | `flux build` | Construit le flux GetFile vers PutHDFS |
| N3 | copier `ship_mode.dat` dans `nifi_data\input`, puis `flux start`, attendre 35 s, `flux status`, `flux stop`, et `docker exec hdfs-client hdfs dfs -ls -h /retail-lake/raw/nifi` | Premier dépôt par le flux |
| N4 | `flux set-input /data/nifi/inexistant`, relever l'état, puis rétablir avec `flux set-input /data/nifi/input` | Erreur contrôlée : dossier source inexistant |
| N5 | recopier `ship_mode.dat`, `flux start`, attendre 40 s, `flux status`, `flux stop`, relever la destination HDFS et `nifi-app.log` | Erreur contrôlée : même fichier déposé deux fois |
| N6 | copier `income_band.dat`, `flux start`, attendre 35 s, `flux stop`, puis `docker exec hdfs-client bash /scripts/06_verification_depot_nifi.sh` | Second dépôt et vérification dans HDFS |
| N7 | `flux provenance` | Provenance du flux simple |
| N12 | `flux enrichir`, puis copier `reason.dat` et `warehouse.dat`, `flux start`, attendre 40 s, `flux status`, `flux stop` | Construction et test du flux enrichi |
| N8 | `flux export` | Export de la configuration du flux |
| N7B | `flux provenance` | Provenance du flux enrichi |
| N13 | copier `warehouse.dat`, `flux start`, attendre 35 s, capturer le canvas pendant que le flux tourne, puis `flux stop` | Traitement d'un fichier et compteurs du canvas |
| N10 | `flux stop` | Arrêt du flux |
| Captures | interface de NiFi, `https://localhost:8443/nifi` : canvas, configuration des processeurs `GetFile_entree` et `PutHDFS_raw_nifi`, provenance | Captures d'écran faites avec Chrome |

L'interface de NiFi est sur `https://localhost:8443/nifi`.

Arrêt : `docker compose stop`. Ne jamais utiliser `docker compose down -v` : cette commande supprime les volumes HDFS. Chaque démarrage de NiFi (étape N0) crée de nouveaux volumes d'état (noms commençant par `nifi-`) ; les anciens peuvent être supprimés avec `docker volume rm` une fois l'UC-04 terminée.

## 6. Chemins HDFS et paramètres NiFi non sensibles

| Élément | Valeur |
|---|---|
| Racine du Data Lake | `/retail-lake` |
| Zone Raw | `/retail-lake/raw/tpcxbb/initial` et `/retail-lake/raw/tpcxbb/refresh` |
| Destination du flux NiFi | `/retail-lake/raw/nifi` |
| Contrôles et journaux | `/retail-lake/quality`, `/retail-lake/logs/provenance`, `/retail-lake/logs/ingestion` |
| GetFile | `Input Directory = /data/nifi/input`, `Keep Source File = false`, planification 10 s |
| PutHDFS | `Directory = /retail-lake/raw/nifi`, `Conflict Resolution Strategy = fail`, fichiers de configuration `/opt/hadoop-conf/core-site.xml` et `hdfs-site.xml` |
| RouteOnAttribute | Règle `tables_reference` : noms commençant par `reason`, `ship_mode` ou `income_band` |

## 7. Sécurité et confidentialité

Aucun mot de passe, identifiant ou jeton n'est stocké dans ces documents ni dans les scripts : les identifiants de NiFi sont générés au démarrage et lus dans les journaux au moment de l'exécution. Le dépôt ne contient pas les fichiers du jeu (dossier `retail_project` non versionné), les volumes HDFS ni les archives NiFi (76 Mo, téléchargées depuis Maven Central, section 5.2). Les interfaces web de HDFS et de NiFi sont limitées à `127.0.0.1` dans le `docker-compose.yml` livré.
