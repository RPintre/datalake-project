# J1 - Retail Intelligence

Projet fil rouge **Retail Intelligence Platform** (Mastère Dev, Data & IA, IPSSI) : Jour 1, besoin métier, exploration des données et choix d'architecture. Ce dossier contient les livrables des deux use cases obligatoires, **UC-01** et **UC-02**, sous forme de documents PDF et de preuves.

## 1. Contenu du dossier

```
J1_Retail_Intelligence/
├── README.md
├── 01_UC01_besoin_exploration/
│   ├── cadrage_metier.pdf
│   ├── questions_decisionnelles.pdf
│   ├── dictionnaire_KPI.pdf
│   ├── inventaire_sources.pdf
│   ├── qualification_donnees.pdf
│   ├── limites_hypotheses.pdf
│   ├── questions_de_recul.pdf
│   └── UC01_preuves/ (index + 19 preuves)
└── 02_UC02_architecture/
    ├── comparaison_architectures.pdf
    ├── architecture_cible.pdf
    ├── schema_flux.pdf
    ├── positionnement_composants.pdf
    ├── gouvernance.pdf
    ├── local_vers_azure.pdf
    ├── questions_de_recul.pdf
    └── UC02_preuves/manipulations/ (pile Docker, scripts, sorties, preuves HDFS et Spark)
```

### UC-01 : besoin métier et exploration des sources

| Document | Contenu |
|---|---|
| `cadrage_metier.pdf` | Contexte, problématique, 3 profils utilisateurs, questions métier, source de données |
| `questions_decisionnelles.pdf` | Matrice profils, questions, décisions, priorités et faisabilité |
| `dictionnaire_KPI.pdf` | 7 KPI avec formule, unité, périmètre, sources, données manquantes et limites |
| `inventaire_sources.pdf` | Inventaire des 23 tables du jeu initial et des 11 du rafraîchissement, avec annexe des 344 colonnes |
| `qualification_donnees.pdf` | Valeurs vides, types, doublons, cohérence des montants, 67 relations entre tables |
| `limites_hypotheses.pdf` | 10 limites, 10 hypothèses, couverture des besoins, fiabilité des KPI |
| `questions_de_recul.pdf` | Réponses aux 6 questions de recul |
| `UC01_preuves/` | Index des preuves, mesures et contrôles sur les données (voir section 5) |

Le dictionnaire des KPI, listé parmi les preuves du TP, est le fichier `dictionnaire_KPI.pdf` ci-dessus.

### UC-02 : choix et justification d'une architecture Data

| Document | Contenu |
|---|---|
| `comparaison_architectures.pdf` | Contraintes du projet, comparaison Data Warehouse, Data Lake et Lakehouse, choix retenu |
| `architecture_cible.pdf` | Architecture logique, rôle des couches, zones Bronze, Silver, Gold, data marts, sources retenues |
| `schema_flux.pdf` | 7 flux de données, traitement batch, contrôles et transformations de chaque flux |
| `positionnement_composants.pdf` | Matrice des composants selon leur statut (obligatoire, facultatif, étudié) et replis |
| `gouvernance.pdf` | Règles de provenance, qualité, accès, protection des données, suivi et coûts |
| `local_vers_azure.pdf` | Déclinaison locale et cible Azure (stockage, identités, secrets, coûts) |
| `questions_de_recul.pdf` | Réponses aux 7 questions de recul |
| `UC02_preuves/manipulations/01_preuves_manipulations.pdf` | Preuves de l'exécution de HDFS et de PySpark (Bronze, Silver, Gold) sur trois tables, avec contrôle croisé |

## 2. Ordre de lecture conseillé

1. `cadrage_metier.pdf`, `questions_decisionnelles.pdf`, `dictionnaire_KPI.pdf` : le besoin.
2. `inventaire_sources.pdf`, `qualification_donnees.pdf`, `limites_hypotheses.pdf` : les données réelles.
3. `comparaison_architectures.pdf`, `architecture_cible.pdf`, `schema_flux.pdf` : l'architecture.
4. `positionnement_composants.pdf`, `gouvernance.pdf`, `local_vers_azure.pdf` : les composants, la gouvernance et le cloud.

## 3. Les données

| Élément | Valeur |
|---|---|
| Dossier | `retail_project` (non versionné dans ce dépôt) |
| Jeu initial | `data` : 23 tables, 24 fichiers, environ 977 Mo |
| Jeu de rafraîchissement | `data_refresh` : 11 tables, 12 fichiers, environ 9 Mo |
| Format | Fichiers `.dat` séparés par une barre verticale, sans en-tête ; 2 fichiers CSV d'audit |
| Noms de colonnes | Spécification TPCx-BB v1.7.0 (scripts de création de tables) |
| Origine | Jeu synthétique TPCx-BB, utilisé pour ses données uniquement (le benchmark n'est pas exécuté) |
| Version | TPCx-BB, schéma identique à la spécification v1.7.0 (déduit) |
| Outil de génération | PDGF du kit TPCx-BB (déduit de la spécification) |
| Fournisseur | Formateur (énoncé du TP) ; date de mise à disposition non communiquée |

## 4. Conventions et statuts

Chaque information porte un statut de certitude :

- **Vérifié** : mesuré sur les fichiers ou lu dans la spécification.
- **Déduit** : interprétation justifiée.
- **Hypothèse** : proposition.
- **Non disponible** : absent des données.
- **Choix retenu** : décision du projet.

## 5. Preuves d'UC-01

Dans `01_UC01_besoin_exploration/UC01_preuves/` : schéma des besoins métier, arborescence et aperçu des données, comparaison des colonnes avec la spécification, tableau des colonnes et types, valeurs vides, cohérence des montants, clés candidates, doublons, période couverte, jeu de rafraîchissement, relations entre tables (clés étrangères, retours et ventes, clics et avis, tickets et commandes), recouvrement entre les deux jeux, rattachement des ventes aux promotions et clients des retours.

## 6. Réalisé et conçu

| Partie | Nature |
|---|---|
| UC-01 | Analyse réalisée : toutes les mesures portent sur les fichiers complets des deux jeux |
| UC-02 | Conception logique, plus une exécution locale de HDFS et de PySpark (conteneurs Docker, trois tables : `store_sales`, `item`, `date_dim`) |

Apache NiFi n'est pas installé : l'ingestion est faite par un script shell de repli. La cible Azure est une conception sur le papier, distincte d'un déploiement réel : aucun service Azure n'est utilisé.

## 7. Reproductibilité

- Les mesures d'UC-01 ont été faites avec des outils en ligne de commande standards (`awk`, `sort`, `cut`) sur les fichiers `.dat`, sans Python ni Spark.
- Les manipulations d'UC-02 se rejouent avec Docker Desktop : `docker compose up -d` dans `02_UC02_architecture/UC02_preuves/manipulations`, puis les scripts du dossier `scripts` (le dossier des données se règle avec la variable `DATA_DIR`).
- Les noms de colonnes sont extraits du PDF de spécification TPCx-BB avec `pdftotext`, puis rapprochés du nombre de colonnes de chaque fichier.
- Pour reproduire une mesure : relire les fichiers de `retail_project` avec le séparateur `|` et appliquer les formules décrites dans les fichiers de preuves.
- Les documents PDF ont été générés à partir de textes Markdown ; seuls les PDF et les preuves sont versionnés.

## 8. Sécurité et confidentialité

Le jeu est synthétique mais traité comme sensible (la table `customer` contient adresse électronique, identifiant de connexion et date de naissance). Le dépôt ne contient pas les fichiers du jeu (dossier `retail_project` non versionné), seulement des statistiques et quelques lignes d'exemple de tables de ventes, retours et stock ; aucun secret ni identifiant ; les règles d'accès et de protection sont décrites dans `02_UC02_architecture/gouvernance.pdf`.
