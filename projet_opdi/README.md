# Projet DAT535 — Trafic aérien européen (OPDI / EUROCONTROL)

Pipeline de data engineering (architecture médaillon, PySpark) sur les données
ouvertes OPDI d'EUROCONTROL / OpenSky Network.

## Organisation du dépôt

```text
group08/projet_opdi/
├── README.md
├── .gitignore         # exclut les données, les clés et les fichiers temporaires
├── config/            # paramètres dev / prod (chemins, périodes à ingérer)
├── docs/              # fiche de définition du projet, schémas, rapport
├── notebooks/         # exploration et démonstrations (Jupyter)
├── scripts/           # scripts utilitaires (ex. vérification d'accès aux données)
├── src/               # code du pipeline (à venir)
└── tests/             # tests automatisés (à venir)
```

## Où sont les données ?

Les données ne sont **pas** dans ce dépôt. Elles sont téléchargées directement
depuis EUROCONTROL sur la VM, dans `~/opdi-data/` :

```text
~/opdi-data/
└── exploration/       # fichiers de test (vérification d'accès)
```

## Sources

- OPDI : https://www.opdi.aero/data
- Aéroports / pistes : OurAirports (https://davidmegginson.github.io/ourairports-data/)

## Vérifier l'accès aux données (sur la VM)

```bash
source ~/spark-env/bin/activate
cd <clone de group08>/projet_opdi
python scripts/check_opdi_access.py
```

Le dépôt `group08` (runner self-hosted, environnements `dev`/`main`, workflows)
est réutilisé tel quel ; les workflows du projet seront ajoutés quand le
pipeline existera.
