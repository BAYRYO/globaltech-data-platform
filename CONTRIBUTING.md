# Contribuer

## Flux de travail

1. Prendre une story dans Jira ([KAN](https://projet-bigdata-3il.atlassian.net/jira/software/projects/KAN/boards/1)) et la passer en « En cours ».
2. Créer une branche depuis `main` : `<type>/KAN-<n>-<sujet-court>`, par exemple
   `feat/KAN-21-bronze-ingestion`.
3. Commits petits et autonomes, au format [Conventional Commits](https://www.conventionalcommits.org/fr/) :
   `feat(bronze): write ingestion partitions with dynamic overwrite` et la clé Jira en pied de
   message (`Refs: KAN-21`).
4. Ouvrir une PR vers `main` au titre `type(scope): sujet`, avec le modèle fourni. Passer la
   story en « En cours de revue ».
5. Fusion après CI verte et revue. La story passe en « Terminé ».

Types : `feat`, `fix`, `perf`, `refactor`, `test`, `docs`, `build`, `ci`, `chore`.
Scopes : `platform`, `datagen`, `core`, `bronze`, `silver`, `gold`, `streaming`, `mapreduce`,
`perf`, `ops`, `docs`.

## Environnement

Prérequis : Docker (16 Go de RAM conseillés pour le cluster), Git, GNU Make (ou Git Bash sous
Windows).

```bash
make test        # lint + tests dans un conteneur Python 3.11 + Java 17, sans rien installer
make lint        # ruff
```

Développement local sans Docker : Python 3.11, Java 11 ou 17, puis
`pip install -r requirements-dev.txt && pip install -e .` et `pytest`.

Couverture : `scripts/test.sh --cov` (ou `pytest --cov`) affiche la couverture des lignes et des
branches du paquet `gtdp` ; la CI la publie dans le résumé du job de tests et joint `coverage.xml`.

## Règles de code

- Transformations Spark = fonctions pures `DataFrame -> DataFrame` dans `src/gtdp/<couche>/`.
  Les jobs (`src/gtdp/jobs/`) lisent la configuration, appellent ces fonctions, écrivent et
  tracent l'audit.
- Pas d'UDF Python quand une fonction native existe. Toute exception est justifiée par écrit
  dans la PR et dans le code.
- Aucun chemin, secret ou paramètre en dur : tout passe par `conf/` ou une variable
  d'environnement.
- Chaque règle métier a un test qui vérifie le comportement observable, cas limites compris.
- Pas de commentaire qui paraphrase le code. Un commentaire explique un *pourquoi* non évident.
