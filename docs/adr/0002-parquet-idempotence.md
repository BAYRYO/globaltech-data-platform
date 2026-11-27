# ADR-0002 : partitions nommées, écrites puis échangées sous verrou

**Statut** : acceptée

## Contexte

Le rejeu d'un même jour ne doit pas créer de doublons, et une exécution interrompue ne doit pas
laisser une table à moitié écrite. Delta Lake et Iceberg ne font pas partie du programme du
module. Le stockage reste HDFS + Parquet, sans transaction. Toutes les écritures et lectures de
tables du lake passent donc par `src/gtdp/io.py`.

## Décision

### Tables partitionnées : `write_partitions`

Toute écriture batch cible des partitions dérivées de l'entrée (`ingestion_date` en bronze et
silver, `order_date` touchées en gold). Le job **nomme** les partitions qu'il produit, par
exemple `{"ingestion_date": [D]}`, et `write_partitions(df, chemin, partitions)` procède en deux
temps :

1. **écriture complète** du DataFrame dans un répertoire de transit propre à l'exécution,
   `_<table>.staging-<uuid>`, frère caché de la table et partitionné comme elle ;
2. **échange** : une fois l'écriture réussie, chaque partition nommée est supprimée puis
   remplacée par le renommage de sa copie en transit.

Une partition nommée sans ligne devient un répertoire qui ne contient que `_SUCCESS`. Elle
remplace l'ancienne : rejouer un jour qui ne produit plus aucun rejet vide sa partition de
rejets. `read_table` la lit vide.

Une ligne hors des partitions nommées lève `ValueError` avant tout échange, et rien n'est publié.
Une écriture qui échoue (tâche en erreur, colonne manquante) laisse la table intacte. Le
répertoire de transit est supprimé dans tous les cas.

`drop_partitions` supprime des partitions nommées sous le même verrou. Silver s'en sert pour
retirer sa publication d'un jour, bronze pour vider la partition d'une source absente d'un dépôt.

### Tables non partitionnées : `replace_table`

`silver/customers` et `silver/products_scd2` sont réécrites en entier par
`replace_table(df, chemin)`. Le DataFrame est écrit complet dans `_<table>.tmp`, marqué
`_SUCCESS`. L'ancienne table passe en `_<table>.old`, la nouvelle prend sa place, puis
l'ancienne est supprimée. Le DataFrame peut lire la table qu'il remplace.

Si un arrêt brutal laisse la table absente, `replace_table`, `read_table` et `path_exists` la
remettent d'abord en place : `_<table>.tmp` s'il contient `_SUCCESS`, sinon `_<table>.old`. Un
lecteur laisse une table absente telle quelle tant qu'un verrou récent signale un échange en
cours.

### Verrou de table

Chaque écriture prend le verrou `_<table>.lock`, frère caché de la table, pour toute sa durée. Le
verrou est un fichier créé seulement s'il n'existe pas, ce que le NameNode décide de façon
atomique. Il contient l'identifiant de l'exécution, celui de l'application YARN et l'heure de
prise.

- Un verrou plus jeune que `spark.gtdp.io.staleAfterSeconds` (6 h par défaut) appartient à une
  exécution vivante. L'écriture échoue aussitôt avec `TableLockedError`, sans attente.
- Un verrou plus vieux a été laissé par une exécution morte. Il est repris, avec un
  avertissement dans le journal.
- Le détenteur du verrou supprime les répertoires `_<table>.staging-*` plus vieux que ce même
  délai, laissés par une exécution tuée.

Le puits du flux capteurs fait exception : au lieu d'échouer, il attend un verrou tenu
(`_write_waiting` de `gtdp.streaming.stream`). Il réessaie toutes les 2 s, pendant
`stale_after_s` (300 s, section `[streaming]`) plus 60 s de marge, le temps qu'un housekeeping
finisse la compaction d'un jour ou que le verrou d'un driver tué soit repris. Le job règle
`spark.gtdp.io.staleAfterSeconds` sur la même valeur ([ADR-0008](0008-streaming.md)).

### Renommages et signaux d'arrêt

Les renommages passent par `FileContext.rename` sans écrasement. Un renommage vers un chemin qui
existe échoue avec `FileExistsError`, alors que `FileSystem.rename` déplacerait la partition à
l'intérieur du répertoire existant.

SIGTERM et SIGINT sont différés pendant l'échange, la prise et la libération du verrou. Le signal
prend effet à la fin du bloc : une exécution arrêtée laisse chaque partition ancienne ou nouvelle,
jamais absente.

### Lecture : `read_table`

Les jobs lisent toute table du lake par `read_table(spark, chemin, schéma)`. Le schéma explicite
s'applique à chaque fichier : une colonne du schéma absente d'un fichier est lue nulle, une
colonne du fichier hors du schéma est écartée. Une table absente, ou sans fichier de données
(jamais écrite, ou dont chaque jour écrit était vide), rend un DataFrame vide de ce schéma au
lieu d'échouer.

### Pourquoi nommer les partitions

Le mode `partitionOverwriteMode=dynamic` de Spark ne suffit pas (mesuré lors de la revue du
Lot 0, Spark 3.5.3) :

- il ne remplace que les partitions qui reçoivent au moins une ligne : rejouer un jour qui ne
  produit plus aucun rejet laisserait l'ancienne partition de rejets en place ;
- posé seulement dans la session, il dépend de la configuration ambiante : en mode statique, le
  même `overwrite` efface sans erreur toutes les autres partitions.

En gold, le traitement de `D` recalcule chaque partition `order_date` présente dans silver(`D`) à
partir de toutes les lignes silver de ces dates : une commande retardataire ne fait pas
disparaître les commandes déjà publiées pour sa date.

### Table d'audit et arrêt d'une exécution

`run.py` ouvre une exécution auditée autour de chaque job (`gtdp.audit.audited_run`).
`<ops>/audit` est la seule table en ajout : chaque exécution écrit sa ligne dans son propre
répertoire `run_date=…/run_id=…`, ce qui évite le répertoire `_temporary` que Spark partage
entre jobs simultanés. La ligne porte un statut :

| Statut | Cause | Code de sortie de `run.py` |
| --- | --- | --- |
| `SUCCESS` | le job s'est terminé | 0 |
| `FAILED` | exception du job, message dans `error_message` (2 000 caractères au plus) | 1 |
| `STOPPED` | SIGTERM ou SIGINT reçu par le driver | 128 + numéro du signal, soit 143 ou 130 |

Sur `STOPPED`, les jobs Spark de l'exécution sont annulés jusqu'à l'écriture de la ligne. Un
second signal tue le processus aussitôt. Une erreur d'usage, une configuration invalide ou un
Spark qui ne démarre pas rendent 2, sans ligne d'audit. Un échec d'écriture de la ligne d'audit
est journalisé et ne remplace jamais l'erreur du job.

## Conséquences

- Rejouer un jour laisse exactement le même contenu qu'une exécution unique
  (`tests/core/test_io.py`, `tests/bronze/test_bronze_replay.py`,
  `tests/silver/test_silver_incremental.py`).
- Une exécution interrompue avant l'échange ne modifie rien. Arrêtée par signal pendant
  l'échange, elle le termine d'abord.
- Deux exécutions n'écrivent jamais la même table en même temps : la seconde échoue avec
  `TableLockedError` et doit être relancée après la première. Seul le flux capteurs attend.
- Limite connue : un `kill -9` entre la suppression et le renommage d'une partition laisse cette
  partition absente jusqu'au rejeu du jour. Le verrou de l'exécution tuée bloque ensuite la
  table pendant 6 h, sauf suppression à la main après avoir vérifié que l'application qu'il nomme
  est terminée. La procédure est dans le
  [runbook silver](../runbook/silver.md#réparer-un-jour-incomplet) et le
  [runbook bronze](../runbook/bronze.md#après-une-exécution-tuée).
- Un `spark.read.parquet` brut échoue sur une table sans fichier de données et ignore le schéma
  attendu : seul `read_table` est autorisé dans les jobs.

## Alternatives écartées

- `append` + déduplication en lecture : coût reporté sur chaque requête, doublons visibles.
- Delta Lake (`MERGE`) : hors programme, ajoute une dépendance et un format.
- `FileSystem.rename` : il déplace une partition dans une destination existante au lieu
  d'échouer, et imbrique la partition dans elle-même.
