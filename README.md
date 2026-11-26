# GlobalTech Data Platform

[![CI](https://github.com/BAYRYO/globaltech-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/BAYRYO/globaltech-data-platform/actions/workflows/ci.yml)

Prototype de plateforme Big Data pour le pilotage logistique et industriel de GlobalTech
Industries : un data lake HDFS en trois couches (bronze, silver, gold), des pipelines Spark
exécutés sur YARN, un traitement streaming d'événements capteurs, une comparaison MapReduce
contre Spark et un banc de performance.

Projet du module *Big Data et traitement massif* (Hadoop, MapReduce, Spark), 3IL, 2026-2027.

## Documentation

| Document | Contenu |
| --- | --- |
| [Architecture](docs/architecture.md) | Couches, composants, arborescence HDFS, flux |
| [Contrat de données](docs/data-contract.md) | Sources J1/J2/J3, anomalies injectées, manifeste |
| [Modèle de données](docs/data-model.md) | Silver, schéma en étoile gold, indicateurs |
| [Décisions (ADR)](docs/adr/README.md) | Choix structurants et alternatives écartées |
| [Pilotage](docs/project-management.md) | Rôles, jalons, Kanban Jira, définition de « terminé » |
| [Contribuer](CONTRIBUTING.md) | Branches, commits, PR, règles de code |

Le backlog est suivi dans Jira : [projet KAN](https://projet-bigdata-3il.atlassian.net/jira/software/projects/KAN/boards/1).

## Démarrage rapide

Prérequis : Docker et Git. Sous Windows, lancer les scripts depuis Git Bash.

```bash
git clone https://github.com/BAYRYO/globaltech-data-platform.git
cd globaltech-data-platform
scripts/test.sh            # lint + tests unitaires (Spark local) dans un conteneur
```

Le démarrage du cluster et l'exécution du pipeline sont décrits dans le guide de la plateforme
(lot 1).

## Structure

```
conf/        configuration externalisée (TOML)
data/kit/    données du kit fourni (référentiel du générateur)
docker/      image Hadoop + Spark, configuration du cluster
docs/        architecture, ADR, dictionnaire, runbook, rapport
mapreduce/   job Hadoop Streaming
scripts/     exploitation du cluster et outillage
src/gtdp/    code Python (générateur, couches, streaming, jobs)
tests/       tests pytest
```

## Licence

[MIT](LICENSE)
