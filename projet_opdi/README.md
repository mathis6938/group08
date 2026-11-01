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
│   ├── gold.py + sql/*.sql      #   activité des aéroports en Spark SQL, validation, publication -> gold/
│   ├── schemas.py               #   contrats de schéma : sources (raw), tables Silver et Gold publiées
│   ├── quality.py               #   synthèse du run : contrôles qualité, volumes, durées
│   ├── performance.py           #   mesures de performance (notebook 03)
│   ├── common.py, cli.py        #   utilitaires, ligne de commande
├── notebooks/                   # vitrine pour le rapport et la démo (appellent le package)
│   ├── 01_data_understanding.ipynb   # profilage des tables Bronze
│   ├── 02_pipeline_walkthrough.ipynb # le pipeline couche par couche, avec les preuves de chaque couche
│   ├── 03_performance.ipynb          # goulots d'étranglement et optimisations mesurées
│   └── 04_gold_dashboard.ipynb       # tableau de bord : le consommateur du Gold (graphiques LFBO, ENZV...)
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
├── silver/                  # flights, flight_events, airports, runways
├── quarantine/              # lignes rejetées, avec leur motif (_quarantine_reason)
├── gold/                    # airport_daily_ops, airport_hourly_ops, runway_usage, airport_ground_times,
│                            # run_status (historique des publications), _SUCCESS (Gold prêt)
├── reports/<run id>/        # rapport JSON de chaque couche d'un run (ingest, bronze, silver, gold, summary)
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

- Couches : `ingest`, `bronze`, `silver`, `gold`, `summary` (ou `all`, la valeur par défaut). Elles communiquent
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
| `test_notebooks.py` | le code des notebooks vitrine reste valide |
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

## Sources

- OPDI : <https://www.opdi.aero> (données, méthodologie). Téléchargement :
  `https://www.eurocontrol.int/performance/data/download/OPDI/v002/...`
- OurAirports : <https://davidmegginson.github.io/ourairports-data/>
