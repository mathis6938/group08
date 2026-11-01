# Projet DAT535 — Trafic aérien européen (OPDI / EUROCONTROL)

Pipeline de data engineering en **architecture médaillon** (Bronze → Silver → Gold) avec PySpark, sur les
données ouvertes **OPDI v0.0.2** d'EUROCONTROL / OpenSky Network (liste des vols, événements de vol, mesures)
et les référentiels **OurAirports** (aéroports, pistes).

- **Question servie par le Gold :** quelle est l'activité de chaque aéroport européen, comment ses pistes sont
  utilisées et combien de temps durent les phases au sol, par jour et par heure (aéroports vitrines : LFBO, ENZV).
- **Périmètre :** 1er juin → 28 juillet 2026, ≈ 5,5 Go bruts, ≈ 209 millions de lignes.
- **Groupe 08 :** Thomas Canisares--Morère & Mathis Nancey.

## Organisation

```text
projet_opdi/
├── src/opdi_pipeline/project_pipeline_en_construction.ipynb   # LE pipeline (parties 1 à 6)
├── run_project.py           # exécute le notebook sans interface (utilisé par GitHub Actions)
├── tests/                   # tests automatiques (pytest)
│   ├── test_ingestion.py        # noms des fichiers OPDI, téléchargement, empreinte SHA-256
│   ├── test_silver_rules.py     # règles de nettoyage et de qualité du Silver
│   ├── test_pipeline_smoke.py   # notebook complet sur des données synthétiques (faux serveur EUROCONTROL)
│   ├── synthetic_opdi.py        # générateur des données synthétiques, avec défauts connus
│   └── stubs/findspark.py       # remplace findspark dans les tests (PySpark installé par pip)
├── requirements-test.txt    # dépendances des tests (runner GitHub)
├── pytest.ini
└── scripts/                 # scripts d'exploration utilisés au début du projet
```

Le notebook est la **seule source du code** : on le lit, on l'exécute dans Jupyter sur la VM, et la CI
l'exécute tel quel. Les tests unitaires n'en copient pas le code : ils exécutent les cellules qui ne font que
définir des fonctions (cellules marquées du tag `pipeline-functions`) et testent ces fonctions.

## Données (sur la VM, jamais dans Git)

```text
~/opdi-data/            # prod : périmètre complet (aussi utilisé en interactif dans Jupyter)
├── raw/                # fichiers tels que téléchargés + manifest.csv + schema_report.csv
├── bronze/             # tables Parquet : colonnes d'origine + traçabilité ligne par ligne
├── silver/             # flights, flight_events, airports, runways
├── quarantine/         # lignes rejetées, avec leur motif (_quarantine_reason)
├── gold/               # airport_daily_ops, airport_hourly_ops, runway_usage, airport_ground_times, run_status
├── reports/run-<n>/    # notebook exécuté (.ipynb + .html) de chaque run GitHub Actions
└── pipeline_summary.json   # résultat du dernier run (contrôles qualité, volumes, motifs de quarantaine)

~/opdi-data-dev/        # dev : même organisation, une seule tranche de 10 jours (9 → 18 juin 2026)
```

## Exécuter

```bash
source ~/spark-env/bin/activate
cd ~/group08                                        # clone du dépôt sur la VM

# Interactif : ouvrir le notebook dans Jupyter et tout exécuter (valeurs par défaut = prod, ~/opdi-data)

# Sans interface, comme la CI :
python projet_opdi/run_project.py --env dev         # échantillon, ~/opdi-data-dev
python projet_opdi/run_project.py --env prod        # périmètre complet, ~/opdi-data (long : nohup ... &)
python projet_opdi/run_project.py --env prod --start 2026-06-01 --end 2026-06-11   # autre période
```

Les paramètres sont lus en partie 1.3 depuis des variables d'environnement : `OPDI_START`, `OPDI_END`
(exclu), `OPDI_DATA_ROOT`, et, pour les tests seulement, `OPDI_BASE_URL`, `OPDI_AIRPORTS_URL` et
`OPDI_RUNWAYS_URL`. Sans variable, ce sont les valeurs du projet.

Le run échoue (code de sortie 1) si une cellule échoue, notamment si un contrôle qualité échoue :
la réconciliation Bronze (2.3), le bilan des lignes et l'intégrité référentielle du Silver (3.8),
la validation du Gold (4.2, qui bloque la publication en 4.3) et la synthèse finale (partie 5).

## Tests

```bash
cd projet_opdi
pip install -r requirements-test.txt     # sur un poste avec Java 17 (pas sur la VM : Spark vient de /opt/spark)
python -m pytest -v                      # environ 1 minute
python -m pytest -v -m "not slow"        # sans le test de bout en bout
```

Le test de bout en bout sert des fichiers synthétiques via un serveur HTTP local, avec les vrais noms et
schémas OPDI. On y a placé des défauts connus : vols dupliqués ou hors période, dates illisibles, chronologie
inversée, aéroports vides ou `"nan"`, événements orphelins ou mal placés. Le test vérifie que le pipeline
retrouve exactement ces défauts et produit les bons chiffres Gold (mouvements, pistes, temps de roulage).

## CI/CD (GitHub Actions)

| Workflow | Déclencheur | Jobs |
|---|---|---|
| [`dat535-dev.yml`](../.github/workflows/dat535-dev.yml) | push sur `dev` (fichiers du projet), pull request vers `main`, manuel | **tests** (runner GitHub) → **pipeline-dev** (runner de la VM, environnement `dev`, échantillon dans `~/opdi-data-dev`) |
| [`dat535-prod.yml`](../.github/workflows/dat535-prod.yml) | push sur `main` (merge), **le 1er de chaque mois**, manuel | **tests** → **pipeline-prod** (runner de la VM, environnement `main` : **approbation manuelle**, périmètre complet dans `~/opdi-data`) |
| [`labs.yml`](../.github/workflows/labs.yml) | manuel | pipelines des labs 2 et 3 |

- **Séparation dev / prod :** branches (`dev` / `main`), environnements GitHub (`dev` sans protection,
  `main` avec relecteur obligatoire) et dossiers de données distincts. Un run dev raté ne touche jamais les
  données prod.
- **Contrôles automatiques :** les tests (ingestion, règles Silver, pipeline complet sur données synthétiques)
  tournent sur un runner hébergé par GitHub, avant tout run sur la VM. Si les tests échouent, rien n'est exécuté
  sur la VM.
- **Visibilité :** chaque run affiche un résumé (contrôles qualité, lignes par table, motifs de quarantaine)
  et publie en artefacts le notebook exécuté (HTML, avec toutes les sorties), `pipeline_summary.json`, le
  manifeste et le rapport de schémas.
- **Sécurité des runs :** un nouveau push sur `dev` annule le run dev en cours ; deux runs prod ne tournent
  jamais en même temps. Le marqueur `gold/_SUCCESS` est supprimé au début du Gold et n'est recréé que si la
  validation passe.

Le runner auto-hébergé exécute un job à la fois. Évitez de garder une grosse session Jupyter ouverte sur la VM
pendant un run de la CI (mémoire partagée).

## Sources

- OPDI : <https://www.opdi.aero> (données, méthodologie) — téléchargement :
  `https://www.eurocontrol.int/performance/data/download/OPDI/v002/...`
- OurAirports : <https://davidmegginson.github.io/ourairports-data/>
