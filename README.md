# GlobalTech Data Platform

[![CI](https://github.com/BAYRYO/globaltech-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/BAYRYO/globaltech-data-platform/actions/workflows/ci.yml)
[![Platform](https://github.com/BAYRYO/globaltech-data-platform/actions/workflows/platform.yml/badge.svg)](https://github.com/BAYRYO/globaltech-data-platform/actions/workflows/platform.yml)

GlobalTech Industries, groupe industriel présent dans plusieurs pays, pilotait sa chaîne
logistique avec des fichiers Excel et des exports isolés, sans historique fiable. Ce dépôt est le
prototype de sa plateforme Big Data. La plateforme collecte les commandes, les produits, les
clients, les logs web et les événements des capteurs. Elle les nettoie, les historise, puis
calcule des indicateurs fiables : chiffre d'affaires au prix applicable à la date de commande,
top produits par pays, moyenne mobile, qualité des données et alertes des capteurs. Projet du
module *Big Data et traitement massif*, 3IL, 2026-2027.

## Architecture

Hadoop 3.3.6 (HDFS et YARN), Spark 3.5.3 et Python 3.11 tournent dans Docker. Un générateur à
graine fixe produit trois dépôts quotidiens, J1, J2 et J3, et une journée d'événements capteurs.
`land.sh` dépose chaque jour dans la zone `landing` de HDFS. Trois jobs Spark sur YARN font
ensuite passer le jour par trois couches :

- `bronze` garde les fichiers sans perte, en Parquet partitionné par date d'ingestion ;
- `silver` type les colonnes, écarte les lignes invalides avec leur motif, supprime les doublons,
  historise les produits (SCD type 2) et pseudonymise les données personnelles ;
- `gold` construit le modèle en étoile et neuf indicateurs.

Le job `streaming-sensors` agrège le flux des capteurs par minute et lève des alertes. Chaque
exécution d'un job ajoute une ligne à la table d'audit `/datalake/ops/audit`. Le schéma légendé
des services, des zones et des flux est dans [Architecture, vue d'ensemble](docs/architecture.md#vue-densemble).

## Structure du dépôt

```
conf/        configuration TOML commune, puis par environnement (local, cluster)
data/kit/    données du kit fourni, référentiel du générateur
docker/      image Hadoop + Spark, configuration du cluster, image des tests
docs/        architecture, ADR, dictionnaire, runbooks, incident, mesures
mapreduce/   mapper et reducer Hadoop Streaming du Lot 6
scripts/     cluster/ (exploitation), streaming/, mapreduce/, perf/, resilience/, test.sh
src/gtdp/    générateur, couches bronze, silver et gold, streaming, jobs et run.py
tests/       tests pytest en Spark local
```

## Prérequis

- Docker Engine 25 ou plus récent avec Docker Compose v2. Sous Windows et macOS, Docker Desktop
  avec au moins 14 Go de mémoire.
- Git et Bash. Sous Windows, lancez toutes les commandes depuis Git Bash.
- 5 Go de disque libre et les ports 9870, 8088, 18080 et 19888 libres sur `127.0.0.1`.
- Ni Java, ni Hadoop, ni Spark, ni Python sur le poste. Tout tourne dans des conteneurs.

## Démarrage rapide

Lancez les commandes depuis la racine du dépôt. Les durées viennent de la machine de référence
(16 CPU, 15 Gio pour Docker) et du profil de données `dev`. Ces étapes supposent un cluster neuf.
Sur un cluster qui a déjà traité J1 à J3, gardez le sel d'origine et suivez
[Rejouer un jour](docs/runbook/exploitation.md#rejouer-un-jour).

1. Clonez le dépôt.

	```bash
	git clone https://github.com/BAYRYO/globaltech-data-platform.git
	cd globaltech-data-platform
	```

2. Lancez le lint et les tests dans un conteneur. La suite a pris 36 min sur la machine de
   référence et de 33 à 57 min sur GitHub Actions le 27 novembre 2026. La dernière ligne de
   pytest compte les tests `passed`, sans `failed`.

	```bash
	scripts/test.sh
	```

3. Démarrez le cluster, créez l'arborescence HDFS et vérifiez le cluster. Le premier `up.sh`
   construit l'image en 6 min 52 s, plus si archive.apache.org est lent. Ensuite, les trois
   commandes prennent 95 s (15 s, 26 s et 54 s).

	```bash
	scripts/cluster/up.sh
	scripts/cluster/init-hdfs.sh
	scripts/cluster/smoke/run.sh
	```

	`up.sh` affiche `HDFS  3 live / 0 dead DataNodes` et `YARN  2 RUNNING / 2 NodeManagers`.
	`smoke/run.sh` se termine par `==> smoke OK`.

4. Générez les trois jours et la journée capteurs, puis vérifiez-les.

	```bash
	scripts/cluster/client.sh python3 -m gtdp.datagen generate --profile dev --seed 42 --out data/generated
	scripts/cluster/client.sh python3 -m gtdp.datagen verify --path data/generated/dev
	```

	La première commande affiche `192 files, 1124325 rows`, la seconde se termine par `OK`.

5. Choisissez un sel de test pour silver, qui pseudonymise les données personnelles. La commande
   l'écrit dans `.env`, que Git ignore.

	```bash
	echo "GTDP_PSEUDO_SALT=$(openssl rand -hex 16)" >> .env
	```

	Silver enregistre l'empreinte du premier sel et refuse ensuite tout autre sel. Un autre sel
	impose un silver reconstruit en entier ([changer le sel](docs/runbook/silver.md#changer-le-sel)).
	Gardez donc ce `.env`, et ne commitez jamais le sel réel.

6. Déposez et traitez J1, J2 et J3 dans l'ordre des dates, chaque jour par `bronze`, `silver` puis
   `gold`. Chacun des neuf jobs prend de 13 s à 77 s.

	```bash
	for d in 2026-01-01 2026-01-02 2026-01-03; do
		scripts/cluster/land.sh data/generated/dev/landing/$d $d &&
			scripts/cluster/submit.sh bronze --date $d &&
			scripts/cluster/submit.sh silver --date $d &&
			scripts/cluster/submit.sh gold --date $d || break
	done
	```

	Chaque job se termine par une ligne JSON `"message": "run ended"` avec `"status": "SUCCESS"`.

7. Vérifiez le résultat.

	```bash
	scripts/cluster/submit.sh src/gtdp/silver/report.py
	scripts/cluster/submit.sh src/gtdp/gold/report.py
	```

	Le rapport silver affiche `3 landed days, incomplete: []` et sort avec 0. Le rapport gold se
	termine par `gold PUBLISHED, 0 order dates mismatched, 0 checks failed`. La requête d'audit
	de [Traiter un jour](docs/runbook/exploitation.md#traiter-un-jour) montre une ligne `SUCCESS`
	par job et par jour, et vérifie que propres + rejets + doublons = lignes lues.

8. Démarrez le flux des capteurs dans un premier terminal. Le job tourne jusqu'à son arrêt.

	```bash
	GTDP_SPARK_SUBMIT_OPTS="--conf spark.executor.instances=1" scripts/cluster/submit.sh streaming-sensors
	```

	Alimentez-le depuis un second terminal, un fichier toutes les 3 s pendant 9 minutes.

	```bash
	scripts/cluster/client.sh python3 -m gtdp.datagen replay-sensors \
		--src data/generated/dev/sensors/2026-01-01 \
		--dst hdfs://namenode:8020/datalake/landing/sensors/incoming --interval 3
	```

	Une minute après la ligne `published 180 files, 11772 events`, arrêtez le flux. Le premier
	terminal rend la main avec le code 0. Lancez ensuite le rapport du flux.

	```bash
	scripts/streaming/stop.sh
	GTDP_SPARK_SUBMIT_OPTS="--conf spark.executor.instances=1" \
		scripts/cluster/submit.sh scripts/streaming/report.py /datalake
	```

	Sur un cluster neuf, le rapport compte 743 lignes d'agrégats, 10 alertes, 108 rejets et
	`duplicate keys 0` pour chaque table. La ligne d'audit porte `SUCCESS`, `in 11772` et
	`stop 'stop marker'`.

9. Comparez MapReduce et Spark sur le même calcul, le chiffre d'affaires par pays.

	```bash
	scripts/mapreduce/run_mr.sh /datalake/landing 2026-01-03 /datalake/ops/mr_vs_spark/mapreduce
	scripts/cluster/submit.sh revenue-by-country-spark --date 2026-01-03
	scripts/cluster/client.sh bash -c 'diff <(hdfs dfs -cat /datalake/ops/mr_vs_spark/mapreduce/part-*) \
		<(hdfs dfs -cat /datalake/ops/mr_vs_spark/spark/revenue/part-*) && echo identiques'
	```

	La dernière commande affiche `identiques`. `scripts/mapreduce/compare.sh --date 2026-01-03`
	mesure les deux moteurs, 5 exécutions chacun.

10. Arrêtez le cluster. `scripts/cluster/down.sh` garde les données HDFS dans les volumes Docker,
    `scripts/cluster/reset.sh` les efface sans confirmation.

## Documentation

| Document | Contenu |
| --- | --- |
| [Guide de démarrage et de la plateforme](docs/platform.md) | Relance en moins de 10 minutes, services et ports, budget mémoire, droits HDFS, soumission des jobs |
| [Runbook d'exploitation](docs/runbook/exploitation.md) | Démarrer, arrêter, vérifier la santé, traiter un jour, lancer chaque job, réparer les incidents |
| [Runbooks par couche](docs/runbook/README.md) | Bronze, silver, gold, flux capteurs, MapReduce, HDFS : lancer, rejouer, vérifier, dépanner |
| [Architecture](docs/architecture.md) | Schéma légendé, composants, arborescence HDFS, flux batch et streaming |
| [Contrat de données](docs/data-contract.md) | Sources J1, J2, J3, anomalies injectées, manifeste |
| [Modèle de données](docs/data-model.md) | Silver, schéma en étoile gold, indicateurs |
| [Dictionnaire de données](docs/data-dictionary/README.md) | Chaque colonne de chaque table : type, règle, exemple |
| [Décisions (ADR)](docs/adr/README.md) | Choix structurants et alternatives écartées |
| [Performance](docs/performance.md) | Naïf contre optimisé, déséquilibre de clé, cache, réplication 3 contre erasure coding |
| [MapReduce contre Spark](docs/mapreduce-vs-spark.md) | Même calcul sur les deux moteurs : résultats, durées, compteurs |
| [Incident du 27 novembre 2026](docs/incidents/2026-11-27-perte-datanode.md) | Perte d'un DataNode pendant l'écriture de gold |
| [Rapport technique](docs/rapport-technique.md) | Architecture, choix justifiés, résultats |
| [Pilotage](docs/project-management.md) | Rôles, jalons, Kanban Jira, définition de « terminé » |
| [Contribuer](CONTRIBUTING.md) | Branches, commits, PR, règles de code |
| [Backlog Jira](https://projet-bigdata-3il.atlassian.net/jira/software/projects/KAN/boards/1) | Projet KAN : stories, critères d'acceptation, jalons |

## Licence

[MIT](LICENSE)
