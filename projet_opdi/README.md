# Projet DAT535 — Trafic aérien européen (OPDI / EUROCONTROL)

Pipeline de data engineering en **architecture médaillon** (Bronze → Silver → Gold) avec PySpark, sur les données
ouvertes **OPDI v0.0.2** d'EUROCONTROL / OpenSky Network (liste des vols, événements de vol, mesures) et les
référentiels **OurAirports** (aéroports, pistes).

- **Question servie par le Gold :** quelle est l'activité de chaque aéroport européen, comment ses pistes sont
  utilisées et combien de temps durent les phases au sol, par jour et par heure (aéroports vitrines : LFBO, ENZV).
- **Périmètre :** 1er juin → 28 juillet 2026, ≈ 5,5 Go bruts, ≈ 209 millions de lignes.
- **Groupe 08 :** Thomas Canisares--Morère & Mathis Nancey.

## Organisation

```text
projet_opdi/
├── run_pipeline.py              # point d'entrée : python projet_opdi/run_pipeline.py --env dev --layer all
├── config/
│   ├── dev.toml                 # échantillon : 9 → 18 juin 2026, ~/opdi-data-dev
│   └── prod.toml                # périmètre complet : 1er juin → 28 juillet 2026, ~/opdi-data
├── src/opdi_pipeline/           # le code du pipeline (une couche = un module)
│   ├── config.py                #   lecture de la configuration (fichier < variables d'environnement < arguments)
│   ├── spark.py                 #   SparkSession (UTC, AQE, partitions de shuffle)
│   ├── ingestion.py             #   téléchargement, manifeste, fichiers republiés    -> raw/
│   ├── bronze.py                #   contrats de schéma, traçabilité, réconciliation  -> bronze/
│   ├── silver.py                #   nettoyage, quarantaine, doublons, enrichissement -> silver/, quarantine/
│   │                            #   (API DataFrame)
│   ├── arrivals.py              #   efficacité arrivées (anneau 100 NM, profils de descente, attentes)
│   ├── gold.py + sql/*.sql      #   activité des aéroports en Spark SQL, validation, publication -> gold/
│   ├── ml.py                    #   modèle GBT temps additionnel, scénarios, typologie KMeans -> gold/, models/
│   ├── schemas.py               #   contrats de schéma : sources (raw), tables Silver et Gold publiées
│   ├── quality.py               #   synthèse du run : contrôles qualité, volumes, durées
│   ├── performance.py           #   mesures de performance (notebook 03)
│   ├── common.py, cli.py        #   utilitaires, ligne de commande
├── notebooks/                   # vitrine pour le rapport et la démo (appellent le package)
│   ├── 01_data_understanding.ipynb   # profilage des tables Bronze
│   ├── 02_pipeline_walkthrough.ipynb # le pipeline couche par couche, avec les preuves de chaque couche
│   ├── 03_performance.ipynb          # goulots d'étranglement et optimisations mesurées
│   ├── 04_gold_dashboard.ipynb       # tableau de bord : le consommateur du Gold (graphiques LFBO, ENZV...)
│   └── 05_arrival_efficiency.ipynb   # étude de l'efficacité des arrivées et machine learning (8 visualisations)
├── tests/                       # pytest (voir « Tests »)
├── requirements-test.txt, pytest.ini
└── scripts/                     # scripts d'exploration du début du projet
```

## Données (sur la VM, jamais dans Git)

```text
~/opdi-data/                 # prod (même organisation pour ~/opdi-data-dev)
├── raw/                     # fichiers tels que téléchargés + manifest.csv + schema_report.csv
├── bronze/                  # colonnes d'origine + traçabilité ligne par ligne (partitionné par période source)
│                            # + _state.json (empreinte SHA-256 du fichier source de chaque période)
├── silver/                  # flights, flight_events, airports, runways, arrivals, descent_level_offs
├── quarantine/              # lignes rejetées, avec leur motif (_quarantine_reason)
├── gold/                    # airport_daily_ops, airport_hourly_ops, runway_usage, airport_ground_times,
│                            # arrival_reference, airport_arrival_daily, airport_arrival_hourly, holding_hotspots,
│                            # ml_feature_importance, ml_scenarios, airport_typology,
│                            # run_status (historique des publications), _SUCCESS (Gold prêt)
├── models/<run id>/         # modèle prédictif Spark MLlib entraîné (arrival_gbt)
├── reports/<run id>/        # rapport JSON de chaque couche d'un run (ingest, bronze, silver, gold, ml, summary)
├── logs/<run id>/           # logs de chaque couche (runs GitHub Actions)
└── pipeline_summary.json    # synthèse du dernier run (contrôles qualité, volumes, quarantaine, durées)
```

## Exécuter

```bash
source ~/spark-env/bin/activate
cd ~/group08                                         # clone du dépôt sur la VM

python projet_opdi/run_pipeline.py --env dev                      # tout le pipeline sur l'échantillon
python projet_opdi/run_pipeline.py --env prod --layer silver      # une seule couche (entrées déjà sur disque)
python projet_opdi/run_pipeline.py --env prod --layer gold,summary
python projet_opdi/run_pipeline.py --env prod --start 2026-06-01 --end 2026-06-11   # autre période
python projet_opdi/run_pipeline.py --env prod --full-refresh      # reconstruit toutes les périodes du Bronze
python projet_opdi/run_pipeline.py --env dev --layer explore     # à la demande : exploration de la phase d'arrivée
python projet_opdi/run_pipeline.py --help
```

- Couches : `ingest`, `bronze`, `silver`, `gold`, `ml`, `summary` (ou `all`, la valeur par défaut). Elles communiquent
  uniquement par les tables écrites sur disque : chacune peut être relancée seule.
- Le run s'arrête avec le code 1 si un contrôle qualité échoue. Les couches suivantes ne tournent pas, et le Gold
  n'est pas publié. Les contrôles :
  - Bronze : fichiers sources conformes au contrat de schéma (un changement de schéma chez EUROCONTROL arrête le
    run avant le Silver), réconciliation raw → Bronze ;
  - Silver : bilan des lignes, intégrité référentielle, tables publiées conformes à leur contrat ;
  - Gold : validation (réconciliation avec le Silver, cohérence, plages de valeurs), contrats de schéma.
- Bronze incrémental : `bronze/_state.json` garde l'empreinte SHA-256 du fichier source de chaque période. Un run
  ne réécrit que les fichiers nouveaux ou republiés, chacun remplaçant sa propre partition (écrasement dynamique
  des partitions) ; les autres périodes ne sont pas relues. La réconciliation, elle, vérifie toujours tous les
  fichiers du périmètre. L'état n'est enregistré qu'après une réconciliation réussie ; après un échec il est
  supprimé et le run suivant reconstruit tout. `--full-refresh` (ou `OPDI_FULL_REFRESH=true`) force la
  reconstruction complète, par exemple après une modification du code du Bronze. Le rapport `bronze.json` liste
  les fichiers reconstruits (`files_built`) et le nombre de fichiers déjà à jour (`files_up_to_date`).
- Fichiers republiés : un fichier déjà présent est retéléchargé si la source en publie une nouvelle version
  (taille ou `Last-Modified` différents). Le manifeste l'indique (`republished`), et le Bronze reconstruit
  seulement la période concernée, avec la traçabilité de la nouvelle version. Les référentiels OurAirports, mis
  à jour quotidiennement, sont ainsi rafraîchis à chaque changement.
- Exploration (`--layer explore`, jamais incluse dans `all`) : vérifie sur les tables publiées les hypothèses de
  l'étude de l'efficacité des arrivées (types et unités de `measurements`, jalons de descente et leur couverture
  par aéroport, appariement des paliers après le début de descente, signature des circuits d'attente) et écrit
  `reports/<run id>/explore.json`. Dans GitHub Actions : *Run workflow* sur le workflow dev, case `explore`.
- Paramètres : `config/<env>.toml`. Les variables d'environnement `OPDI_START`, `OPDI_END`, `OPDI_DATA_ROOT`,
  `OPDI_RUN_ID`, `OPDI_FULL_REFRESH` (et `OPDI_BASE_URL`, `OPDI_AIRPORTS_URL`, `OPDI_RUNWAYS_URL` pour les tests) les remplacent, et
  les arguments de la ligne de commande remplacent les variables.
- Notebooks : ouvrir `notebooks/` dans Jupyter sur la VM. Ils lisent par défaut les rapports du dernier run
  (`RUN_PIPELINE = False`), ou relancent le pipeline (`True`).

## Tests

```bash
cd projet_opdi
pip install -r requirements-test.txt     # sur un poste avec Java 17 (pas sur la VM : Spark vient de /opt/spark)
python -m pytest -v                      # ~2 minutes
python -m pytest -v -m "not slow"        # sans les tests de bout en bout
```

| Fichier | Ce qui est testé |
|---|---|
| `test_config.py` | séparation dev / prod, ordre de priorité des paramètres, option `--full-refresh`, paramètres invalides |
| `test_ingestion.py` | noms des fichiers OPDI (tranches de 10 jours alignées sur le 1er janvier 2022), téléchargement, SHA-256, fichiers republiés |
| `test_schemas.py` | contrats de schéma ; un fichier source au schéma de 2025 (ids `decimal`, colonne en plus) est détecté |
| `test_silver_rules.py` | nettoyage (valeurs manquantes, codes OACI, périmètre) et chaque règle de quarantaine |
| `test_arrival_rules.py` | géométrie d'arrivée (gisement, 8 secteurs, haversine, interpolation d'entrée 100 NM, détection d'attentes) |
| `test_ml.py` | découpage temporel sans fuite, entraînement GBTRegressor, baseline naïve, clustering KMeans et contrats de schéma ML |
| `test_notebooks.py` | le code des 5 notebooks vitrine reste valide |
| `test_pipeline_smoke.py` | **tout le pipeline, couche par couche via la ligne de commande**, sur des données synthétiques servies par un faux serveur EUROCONTROL. On y a placé des défauts connus : doublons, dates illisibles, chronologie inversée, aéroports vides ou `"nan"`, événements orphelins, pistes traversées. Le test vérifie les chiffres exacts de chaque couche, les contrats de schéma des tables publiées, qu'un second run donne les mêmes tables sans rien reconstruire, qu'un fichier republié ne reconstruit que sa période, et qu'un contrôle qualité en échec arrête le run |

## CI/CD (GitHub Actions)

| Workflow | Déclencheur | Jobs |
|---|---|---|
| [`dat535-dev.yml`](../.github/workflows/dat535-dev.yml) | push sur `dev`, pull request vers `dev` ou `main`, **chaque nuit** (02:17 UTC), manuel (case `explore` : exploration seule) | **tests** (runner GitHub) → **pipeline-dev** (runner de la VM, environnement `dev`, échantillon dans `~/opdi-data-dev`) |
| [`dat535-prod.yml`](../.github/workflows/dat535-prod.yml) | push sur `main` (merge), **le 1er de chaque mois**, manuel | **tests** → **pipeline-prod** (runner de la VM, environnement `main` : **approbation manuelle**, périmètre complet dans `~/opdi-data`) |
| [`labs.yml`](../.github/workflows/labs.yml) | manuel | pipelines des labs 2 et 3 |

- **Cycle de développement :** branche de fonctionnalité → pull request vers `dev` (tests + run dev) → relecture →
  merge → pull request `dev` → `main` → merge → run prod après approbation.
- **Runs planifiés :** GitHub exécute les déclencheurs `schedule` avec le workflow et le code de la branche par
  défaut (`main`) : le run de nuit vérifie chaque jour la source avec la version livrée, et grâce au Bronze
  incrémental il ne reconstruit que les fichiers republiés. Le run prod mensuel attend l'approbation, comme les
  autres runs prod. Le lancement manuel propose `start`, `end` et `full_refresh`.
- **Une étape par couche** (ingest, bronze, silver, gold, summary) : un échec désigne directement la couche
  fautive. La synthèse est toujours produite, même après un échec.
- **Visibilité :** chaque run affiche un résumé (contrôles qualité, lignes par table, motifs de quarantaine, étapes
  les plus lentes) et publie en artefacts les logs de chaque couche, les rapports JSON, `pipeline_summary.json`,
  le manifeste, le rapport de schémas et les tables Gold (quelques Mo, analysables sans accès à la VM).
- **Sécurité des runs :** un nouveau push sur `dev` annule le run dev en cours ; deux runs prod ne tournent jamais
  en même temps. Le marqueur `gold/_SUCCESS` est supprimé au début du Gold et n'est recréé que si la validation
  passe.

Le runner auto-hébergé exécute un job à la fois. Évitez de garder une grosse session Jupyter ouverte sur la VM
pendant un run de la CI : la mémoire est partagée.

## Étude de l'efficacité des arrivées (ASMA & Profils Verticaux)

### Problématique
> *« Combien de temps et de carburant les vols perdent-ils avant d'atterrir à Paris-CDG (LFPG), Toulouse-Blagnac (LFBO) et Stavanger (ENZV) — avec Londres-Heathrow (EGLL) et Francfort (EDDF) comme références — quand, pourquoi, et que pourrait-on améliorer ? »*

Pour répondre à cette question sans biaiser les résultats avec des heures de données incomplètes (pertes récepteur ADS-B ou coupures de flux), le pipeline met en œuvre deux familles d'indicateurs standardisés EUROCONTROL / PRU :

### Indicateur A — Temps de transit additionnel en phase d'arrivée (type ASMA)
- **Anneau d'entrée à 100 NM :** Le franchissement du rayon de 100 NM (185,2 km) autour de l'aérodrome de destination est calculé par **interpolation linéaire** entre le dernier jalon en croisière hors de l'anneau et le premier jalon détecté à l'intérieur.
- **Temps de transit réel :** Durée écoulée entre l'entrée dans l'anneau de 100 NM et le franchissement du seuil de piste (`landing` ou `entry-runway` le plus proche).
- **Temps de référence non contraint (*unimpeded reference time*) :** 20ème percentile des temps de transit observés en conditions fluides, calculé par classe de distance du vol, secteur cardinal d'entrée (N, NE, E, SE, S, SW, W, NW) et piste d'atterrissage.
  - *Hiérarchie de repli automatique :* `(piste, classe, secteur)` $\to$ `(classe, secteur)` $\to$ aéroport global.
- **Temps additionnel :** $\max(0, \text{temps réel} - \text{temps de référence})$.

### Indicateur B — Efficacité verticale de descente (CDO) & Attentes
- **Fenêtre de descente :** Délimitée à 200 NM de la destination à partir du début effectif de descente (première décroissance d'altitude sous l'altitude de croisière $\pm 300\text{ ft}$).
- **Détection et fusion des paliers horizontaux :** Consolidation automatique des segments horizontaux successifs espacés de moins de 60 secondes avec une variation d'altitude $\le 200\text{ ft}$.
- **Exclusion stricte des phases au sol :** Tous les paliers situés à moins de $1000\text{ ft}$ au-dessus de l'élévation de la plateforme sont éliminés pour éviter de comptabiliser le roulage ou le stationnement.
- **Ventilation par bandes d'altitude :** Analyse des paliers selon 4 tranches critiques : `<FL70`, `FL70-100`, `FL100-200`, `>=FL200`.
- **Détection des circuits d'attente (*holdings*) :** Identification géométrique des hippodromes sur critères stricts : durée cumulée $\ge 4\text{ min}$ et ratio $\frac{\text{distance parcourue}}{\text{déplacement direct}} \ge 1{,}5$.
- **Modèle de carburant et émissions CO₂ :**
  - Surconsommation en palier basse/moyenne altitude : évaluée à **$25\text{ kg}/\text{min}$**.
  - Facteur d'émission standardisé : **$3{,}16\text{ kg de CO}_2 / \text{kg de carburant}$**.

### Régularité temporelle & Heures incomplètes
- Détection systématique des heures dont le trafic européen total est strictement inférieur à 50% de la médiane européenne pour l'heure correspondante (coupures d'ingestion ou pannes de capteurs).
- Ces heures sont signalées (`is_complete_hour = false`), exclues des agrégations journalières et proscrites pour l'entraînement des modèles prédictifs.

### Modélisation Machine Learning, Scénarios & Typologie (Couche `ml`)
- **Modèle explicatif `GBTRegressor` (temps additionnel) :**
  - Entraîné uniquement sur les arrivées mesurées (`is_transit_plausible`) lors des **heures complètes**.
  - Variables explicatives : demande horaire locale (`arrivals`), demande de l'heure précédente (lagged), aéroport (`dest_icao`), heure (`arrival_hour`), jour de la semaine (`day_of_week`), secteur d'entrée (`entry_sector`), piste d'atterrissage (`arrival_runway`), classe d'appareil (`aircraft_class`), compagnie aérienne (`operator_icao` - top 30 sinon `OTHER`), nombre de paliers de descente (`level_off_count`).
  - **Découpage temporel strict :** Entraînement sur tout le périmètre sauf les 14 derniers jours (3 derniers jours en dev), test sur les derniers jours (garantissant l'absence totale de fuite d'information).
  - **Contrôle qualité bloquant pour publication :** Le modèle n'est publié que s'il bat la référence naïve (moyenne par aéroport et heure sur le jeu d'entraînement) en MAE.
  - Sauvegarde du modèle : versionné par run dans `<data_root>/models/<run_id>/arrival_gbt`.
- **Importance des variables (`gold/ml_feature_importance`) :** Poids relatif de chaque variable calculé à partir de `featureImportances` et classé par rang décroissant.
- **Scénarios de régulation de flux (`gold/ml_scenarios`) :**
  - Simulation d'un plafonnement de la demande horaire au 90ème percentile sur les aéroports focus, avec redistribution sur les heures creuses adjacentes.
  - Estimation des minutes totales gagnées par jour (`saving_min_per_day`).
- **Typologie des aéroports (`gold/airport_typology`) :**
  - Profilage multidimensionnel par aéroport (trafic journalier, temps additionnel moyen et p90, minutes de palier, % d'attentes, variabilité temporelle).
  - Normalisation `StandardScaler` et clustering non supervisé `KMeans` avec sélection optimale du nombre de clusters $k$ par score silhouette (`ClusteringEvaluator`).
  - Attribution de libellés sémantiques aux profils d'exploitation (*« Congested Hub with Regular Holdings »*, *« High-Density Continuous Flow »*, *« Fluid Regional Airport »*...).

### Mise en œuvre et performance
- `arrivals.py` lit les tables Silver **déjà écrites** (et `bronze/measurements`), jamais les DataFrames paresseux du
  run Silver : sinon Spark recalcule toute la chaîne Silver (lecture du Bronze, standardisation, déduplication de
  ~86 M d'événements) pour chaque écriture, ce qui a saturé la mémoire du driver au run prod-5
  (`GC overhead limit exceeded`).
- Seules les colonnes utiles sont lues, la carte `info` uniquement pour les événements de piste, et seuls les
  événements à moins de 300 NM de la destination sont gardés ; ce sous-ensemble, réutilisé par plusieurs étapes, est
  mis en cache sur disque. Des mesures, seule la distance parcourue est jointe (pas d'agrégation des 119 M lignes).
- `silver/descent_level_offs` est écrite d'abord, puis relue pour construire `silver/arrivals`.

### Limites méthodologiques
1. **Couverture des pistes :** L'assignation des pistes d'atterrissage dans les données ouvertes dépend des géométries OpenStreetMap et des événements au sol disponibles. La hiérarchie de repli garantit qu'aucun vol n'est privé de référence même si la piste est manquante.
2. **Absence d'horaires programmés :** OPDI ne fournissant pas les horaires prévus de grille (créneaux IATA / OAG), le temps de transit réel et la référence unimpeded au 20ème percentile constituent la référence relative objective.
3. **Données météorologiques externes :** Les conditions de vent, visibilité ou orages ne sont pas directement synchronisées dans la source OPDI brute.

## Sources

- OPDI : <https://www.opdi.aero> (données, méthodologie). Téléchargement :
  `https://www.eurocontrol.int/performance/data/download/OPDI/v002/...`
- OurAirports : <https://davidmegginson.github.io/ourairports-data/>
