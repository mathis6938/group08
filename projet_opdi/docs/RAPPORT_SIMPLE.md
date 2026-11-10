# Pourquoi les avions brûlent-ils du kérosène inutilement avant d'atterrir ?
### Une analyse de 3,3 millions de vols européens

---

> **Ce rapport s'adresse à toute personne curieuse d'aviation et d'environnement.**
> Pas de formules, pas de jargon technique — juste des chiffres concrets et ce qu'ils révèlent.

---

## Le problème en une image

Imaginez que vous roulez vers chez vous en voiture. À 500 mètres de votre maison, un embouteillage vous oblige à tourner en rond dans votre quartier pendant dix minutes, moteur allumé, avant de pouvoir vous garer.

C'est exactement ce que vivent des milliers d'avions chaque jour au-dessus de l'Europe.

Avant d'atterrir, un avion doit descendre progressivement, réduire sa vitesse, et s'aligner sur la piste. Dans le meilleur des cas, cette descente est fluide et quasi continue — on parle de **descente continue optimisée** (CDO). Dans la réalité, les avions sont souvent contraints de **voler à palier** (en ligne droite horizontale) ou de **tourner en attente** (les fameux « holdings ») à cause de la saturation du trafic aérien.

Ces minutes perdues ne sont pas anodines : un avion qui vole à basse altitude brûle du carburant bien plus vite qu'en croisière. Chaque minute de vol "inutile" représente du kérosène brûlé, du CO₂ émis, et de l'argent dépensé.

---

## D'où viennent nos données ?

Notre analyse repose sur la base de données **OPDI** (Operational Performance Data Initiative), développée par EUROCONTROL. Elle regroupe les trajectoires réelles de vols commerciaux européens transmises par les radars au sol.

Nous avons analysé **3 304 717 vols** enregistrés sur une période de **58 jours** (du 1er juin au 28 juillet 2026), représentant plus de **86 millions d'événements de navigation** et **118 millions de mesures de position**.

Parmi ces vols, **1 870 000 trajectoires d'arrivée** ont été étudiées en détail sur cinq grands aéroports européens :

| Aéroport | Pays | Type de trafic |
|---|---|---|
| **Londres Heathrow (EGLL)** | Royaume-Uni | Hub international majeur |
| **Paris Charles-de-Gaulle (LFPG)** | France | Hub international majeur |
| **Francfort (EDDF)** | Allemagne | Hub international majeur |
| **Toulouse-Blagnac (LFBO)** | France | Aéroport régional/industriel |
| **Stavanger (ENZV)** | Norvège | Aéroport régional |

**Comment mesure-t-on le retard ?** Nous comparons le temps réel de transit de chaque avion à une référence de base : le temps que mettrait ce même type d'avion dans des conditions idéales et non saturées (le 20ᵉ percentile historique). La différence, c'est le **temps supplémentaire** — les minutes "perdues" avant l'atterrissage.

> ⚠️ **Note importante sur les estimations de carburant :** Les chiffres de consommation de kérosène sont calculés à partir d'un coefficient standard EUROCONTROL (12,7 kg de kérosène par minute de vol horizontal à basse altitude, moyenne flotte ECAC). Ce sont des **estimations d'ordre de grandeur**, pas des mesures directes pour chaque avion.

---

## Ce que les données révèlent

### 🔴 Heathrow : un aéroport à la limite de la saturation

Londres Heathrow est l'aéroport le plus chargé d'Europe. Et ça se voit dans les données.

**12,7 % des avions** qui arrivent à Heathrow passent par un circuit d'attente — ils tournent littéralement en rond dans les airs pendant que la piste se libère. Sur l'ensemble de notre période d'analyse, cela représente **32 802 minutes de circuits d'attente** rien qu'au-dessus de Heathrow.

En moyenne, chaque avion perd **4,9 minutes** avant d'atterrir. Et plus le trafic est dense, plus c'est catastrophique. Regardez ce que nos données montrent :

| Nombre d'arrivées par heure | Temps supplémentaire moyen |
|---|---|
| Moins de 20 | 0,1 à 0,2 minute |
| Entre 30 et 39 | 2,5 minutes |
| Entre 40 et 49 | **6,8 minutes** |

Autrement dit : quand Heathrow est saturé (ce qui arrive souvent), les avions perdent en moyenne **presque 7 minutes** chacun — uniquement à cause du surencombrement de la piste.

**L'estimation kérosène : environ 37 tonnes par jour** pour l'ensemble des arrivées — soit l'équivalent de plus de 100 tonnes de CO₂ par jour, uniquement liées à ces minutes d'attente.

Le verdict ? Heathrow fonctionne en permanence au bord de la rupture. Le moindre incident — une panne, un orage, un avion en retard — provoque une réaction en chaîne qui peut durer des heures.

---

### 🟠 Paris-CDG et Francfort : des descentes en escalier

Paris Charles-de-Gaulle et Francfort ont un profil différent de Heathrow. Leurs avions n'attendent pas autant en circuits (seulement 2,9 % de vols en holding), mais ils subissent un autre problème : **les descentes en paliers**.

Plutôt que de descendre progressivement et en continu (ce qui est optimal), les avions sont contraints de voler à altitude constante par paliers — comme descendre un escalier plutôt qu'une rampe. Chaque palier représente du carburant brûlé inutilement.

À **Paris-CDG**, nos indicateurs montrent :
- **3,1 minutes** de temps supplémentaire en moyenne par arrivée
- **4,6 minutes** de vol horizontal (palier) par arrivée
- **~30 tonnes de kérosène** brûlées en excès chaque jour

À **Francfort** :
- **2,7 minutes** de temps supplémentaire
- **4,1 minutes** de vol horizontal par arrivée
- **~34 tonnes de kérosène** par jour

Si CDG parvenait à éliminer ces paliers inutiles en adoptant une descente continue, l'économie pourrait atteindre **~11 000 tonnes de kérosène par an** — soit environ **34 760 tonnes de CO₂ évitées chaque année**, rien que pour cet aéroport.

---

### 🟢 Toulouse et Stavanger : des exemples à suivre

La bonne nouvelle, c'est que ce n'est pas une fatalité. Deux des aéroports de notre étude montrent qu'un trafic aérien géré efficacement peut avoir un impact bien plus faible.

**Toulouse-Blagnac (LFBO) :**
- Seulement **0,7 %** des vols en circuit d'attente
- **1,9 minute** de temps supplémentaire en moyenne
- **~2,2 tonnes de kérosène** par jour — 17 fois moins que Heathrow

**Stavanger (ENZV) :**
- **0,2 %** de vols en attente
- Des profils de descente quasi-idéaux
- Impact kérosène **négligeable**

La différence ? Ce n'est pas que les pilotes de Toulouse sont meilleurs. C'est que le volume de trafic est beaucoup plus faible, ce qui laisse aux contrôleurs aériens la latitude pour gérer chaque avion individuellement et optimiser chaque descente. La saturation, c'est l'ennemi de l'efficacité.

---

## Ce que l'intelligence artificielle nous apprend

En parallèle de l'analyse des trajectoires, nous avons entraîné un modèle d'intelligence artificielle (un algorithme de type "Gradient Boosting Tree") pour prédire le temps supplémentaire qu'un avion va perdre avant d'atterrir.

Résultat : notre modèle prédit ce temps avec une erreur moyenne de seulement **2,74 minutes** — bien meilleure qu'une prédiction "naïve" basée sur la seule moyenne historique (erreur de 2,84 minutes).

Mais le plus instructif, c'est ce que le modèle a appris : **quels facteurs expliquent vraiment les retards ?**

| Facteur | Importance dans la prédiction |
|---|---|
| **L'aéroport de destination** | 55,5 % |
| La catégorie de l'avion | 15,5 % |
| La piste d'atterrissage utilisée | 7,9 % |
| L'heure d'arrivée | 6,8 % |
| Le facteur de charge | 5,3 % |

**Plus de la moitié du retard s'explique simplement par... l'aéroport où l'avion atterrit.**

Autrement dit : peu importe le pilote, la compagnie, ou même le type d'avion — si vous atterrissez à Heathrow un lundi matin à l'heure de pointe, vous allez perdre du temps. C'est structurel.

---

## Peut-on faire mieux ?

Nous avons simulé ce qui se passerait si les gestionnaires du trafic aérien limitaient le nombre d'arrivées par heure à un niveau raisonnable — un "lissage de la demande" qui éviterait les pics de saturation.

Les résultats sont encourageants pour certains aéroports :

| Aéroport | Économie de temps estimée |
|---|---|
| **Francfort** | −7,0 min/jour de temps supplémentaire cumulé |
| **Toulouse** | −6,8 min/jour |
| **Paris-CDG** | −1,8 min/jour |
| Heathrow | Peu d'effet (déjà saturé structurellement) |

Heathrow est un cas particulier : il est tellement proche de sa capacité maximale que limiter les arrivées n'aide pas vraiment — les avions s'accumuleraient simplement plus tôt dans la journée. La seule vraie solution pour Heathrow serait structurelle : plus de pistes, ou moins de vols.

---

## En résumé : les grandes leçons de nos données

1. **Les minutes perdues avant l'atterrissage coûtent cher.** Chaque minute de vol horizontal à basse altitude consomme ~12,7 kg de kérosène. Sur des milliers de vols par jour, les chiffres deviennent considérables.

2. **Heathrow est le cas le plus critique.** 12,7 % de vols en attente, 32 802 minutes de circuits détectées, saturation extrême aux heures de pointe → ~37 tonnes de kérosène perdues par jour.

3. **CDG et Francfort perdent du carburant dans les descentes en palier.** Ce n'est pas la saturation de piste qui est le problème principal, mais la gestion verticale de la descente.

4. **Les aéroports moins chargés peuvent être quasi-optimaux.** Toulouse et Stavanger montrent que c'est possible — avec beaucoup moins de trafic.

5. **L'IA confirme ce que les données montrent.** La destination explique 55 % du retard : c'est la saturation structurelle des grands hubs, pas les comportements individuels, qui dégrade l'efficacité.

6. **L'amélioration est possible, mais elle a des limites.** Le lissage de la demande aide à Francfort et CDG. Pour Heathrow, seule une refonte structurelle changerait vraiment la donne.

---

## Quelques chiffres pour finir

- 🛫 **3 304 717 vols** analysés en 58 jours
- ⏱️ **47 417 circuits d'attente** détectés en Europe
- 🔁 **32 802 minutes** de circuits à Heathrow seul
- ⛽ **~37 tonnes** de kérosène perdues par jour à Heathrow
- ⛽ **~30 tonnes** perdues par jour à Paris-CDG
- 🌍 Si CDG adoptait la descente continue : **~34 760 tonnes de CO₂ évitées par an**
- 🤖 Précision du modèle IA : **MAE = 2,74 minutes**
- 🗺️ Facteur n°1 de retard : **l'aéroport de destination (55,5 %)**

---

*Données issues de la base OPDI EUROCONTROL — période d'analyse : 1er juin au 28 juillet 2026 (58 jours, run de production `prod-8-1`).*
*Coefficient de consommation kérosène : 12,7 kg/min (EUROCONTROL ECAC CDO study, 2018). CO₂ : facteur 3,16 kg CO₂/kg kérosène (ICAO).*
*Analyse réalisée par le Groupe 08 — DAT535 Data Engineering.*
