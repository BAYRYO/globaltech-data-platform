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
silver, `order_date` dont le contenu change en gold). Le job **nomme** les partitions qu'il produit, par
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
lecteur laisse une table absente telle quelle tant que son verrou appartient à une exécution
vivante, jugée comme ci-dessous : c'est un échange en cours.

### Verrou de table

Chaque écriture prend le verrou `_<table>.lock`, frère caché de la table, pour toute sa durée. Le
verrou contient l'identifiant de l'exécution, celui de l'application YARN, l'heure de prise et un
jeton tiré au hasard : deux prises n'écrivent jamais le même contenu. Le contenu est d'abord écrit
dans `_<table>.lock.tmp-<id>`, puis renommé en `_<table>.lock` sans écrasement : le NameNode
refuse le renommage, de façon atomique, si le verrou existe. Un verrou visible nomme donc
toujours son application, même si son exécution meurt pendant l'écriture.

Quand le verrou existe déjà, l'écriture juge l'exécution qui le tient d'après son application :

| Application du verrou | Verrou |
| --- | --- |
| YARN la rapporte terminée : `FINISHED`, `FAILED` ou `KILLED` | repris aussitôt, quel que soit son âge, avec un avertissement |
| YARN la rapporte dans un autre état (`NEW`, `SUBMITTED`, `ACCEPTED`, `RUNNING`…) | tenu : `TableLockedError` aussitôt, sans attente, quel que soit son âge |
| pas d'état de YARN après trois essais : identifiant `local-…`, application inconnue, réponse illisible, ResourceManager injoignable | règle d'âge : repris s'il est plus vieux que `spark.gtdp.io.staleAfterSeconds` (6 h par défaut), tenu sinon ; le journal dit pourquoi |

L'état vient de l'API REST du ResourceManager, `GET http://<adresse>/ws/v1/cluster/apps/<id>`,
l'adresse de la configuration Hadoop (`yarn.resourcemanager.webapp.address`). Une requête qui
échoue (refus, délai de 3 s dépassé, réponse 5xx ou illisible) est répétée après 0,5 s puis 1 s,
soit 10,5 s au plus. Un 404 est une réponse, l'application est inconnue de YARN : il n'est pas
répété. Après le dernier essai, la règle d'âge s'applique. Une erreur ne libère donc jamais un
verrou plus jeune que `staleAfterSeconds`, mais le verrou plus vieux d'une application `RUNNING`
est repris si le ResourceManager reste injoignable. Une application `RUNNING` garde ses verrous
même pris plusieurs fois de suite (une table après l'autre, un micro-batch après l'autre), y
compris contre une autre exécution du même job.

`docker/conf/hadoop/yarn-site.xml` ne pose pas cette adresse : le driver prend la valeur par
défaut de Hadoop, `${yarn.resourcemanager.hostname}:8088` (`yarn-default.xml`), soit
`resourcemanager:8088`, vérifié sur le cluster depuis un driver en mode client. Sans adresse,
chaque verrou suit la règle d'âge et le journal le dit (`… is not set`).

Sur ce cluster, YARN rapporte terminée une application en mode client dès que son driver meurt :
l'ApplicationMaster voit la connexion du driver se fermer et se désinscrit (`FINISHED`,
`SUCCEEDED`). Mesuré en moins d'une demi-seconde après un `kill -9` du driver, et en moins de
2 s quand on tue son conteneur client (`docker kill`). Le délai
`yarn.am.liveness-monitor.expiry-interval-ms` (600 s) ne joue que si l'ApplicationMaster meurt
lui-même sans se désinscrire, par exemple avec son NodeManager : YARN lance alors une seconde
tentative (`yarn.resourcemanager.am.max-attempts` = 2) et l'application reste vivante. En mode
cluster, tuer `spark-submit` laisse le driver tourner dans l'ApplicationMaster : l'application
reste `RUNNING` et garde son verrou.

#### Suppression d'un verrou

Libération et reprise suppriment le verrou par la même voie. L'exécution qui va supprimer un
verrou dont elle a lu le contenu C crée d'abord la réclamation
`_<table>.lock.takeover-<empreinte de C>`, seulement si elle n'existe pas, comme un verrou. Puis
elle relit le verrou, le supprime s'il contient encore C, et supprime la réclamation si elle est
encore la sienne. Seule la détentrice de cette réclamation supprime un fichier qui contient C, et
aucun contenu ne revient : le verrou ne peut pas changer entre la relecture et la suppression.

- À la libération, C est le contenu de l'exécution. Si la réclamation existe déjà, un preneur
  reprend son verrou parce que la règle d'âge l'a jugée morte : elle le lui laisse, avec un
  avertissement (`leaving the lock … to the run that took it over`).
- À la reprise d'un verrou jugé mort, l'exécution qui crée la réclamation supprime le verrou puis
  crée le sien, comme une prise ordinaire. Les autres échouent avec `TableLockedError`.
- Une réclamation laissée par un preneur tué est un verrou comme un autre : elle est reprise de
  la même façon une fois morte.

Une libération qui ne peut pas lire son verrou le relit après 0,5 s puis 1 s. Si la lecture
échoue encore, le verrou reste. Une erreur du journal (`could not read the lock … to release
it`) donne alors l'application que YARN doit rapporter terminée, et la commande qui supprime le
verrou une fois qu'aucune exécution n'écrit la table.

Le détenteur du verrou supprime les répertoires `_<table>.staging-*`, les réclamations et les
copies `_<table>.lock.tmp-*` plus vieux que `spark.gtdp.io.staleAfterSeconds`, laissés par une
exécution tuée.

#### Garantie

Deux exécutions ne tiennent jamais le verrou d'une table en même temps, et aucune ne supprime le
verrou ou la réclamation d'une autre exécution vivante, tant que le jugement de mort est juste :
une application que YARN rapporte terminée n'écrit plus, et sans état de YARN aucune exécution
vivante ne garde un verrou ou une réclamation plus de `staleAfterSeconds`. La garantie ne tombe
que si le ResourceManager reste injoignable pendant qu'une exécution vivante garde un verrou ou
une réclamation plus de `staleAfterSeconds`. Le cas réaliste est un verrou, celui d'une exécution
plus longue que ce délai : elle et celle qui reprend son verrou écrivent alors la table en même
temps, aucune troisième n'entre, et la libération de la première laisse le verrou à la seconde.
Une réclamation suppose une exécution bloquée plus de `staleAfterSeconds` entre deux appels au
NameNode d'une même reprise : deux autres exécutions peuvent alors tenir le verrou ensemble.

Hors de cette garantie :

- un driver en mode client qui écrit encore après que YARN a rapporté son application `KILLED`
  ou `FAILED` (`yarn application -kill`, ApplicationMaster en échec) : les renommages de
  `write_partitions` et `replace_table` ne passent pas par Spark ;
- la seconde tentative d'une application en mode cluster : elle trouve son propre verrou sous le
  même identifiant, le juge tenu et échoue avec `TableLockedError` ; la soumission suivante le
  reprend une fois l'application terminée.

`tests/core/lock_model.py` explore tous les entrelacements des étapes (appel au NameNode,
fermeture d'un flux, renommage) de deux à quatre exécutions, en pilotant le vrai `_locked` sur
un système de fichiers en mémoire. Ses scénarios couvrent les deux régimes, YARN et règle d'âge :
preneurs d'un verrou mort, détenteur vivant qui dépasse `staleAfterSeconds` puis libère pendant
que d'autres prennent, réclamations mortes, exécution tuée à n'importe quelle étape. Dans chaque
état, il vérifie que deux exécutions ne sont pas dans le verrou ensemble, hors le détenteur que
la règle d'âge juge mort, et qu'aucune ne supprime le fichier d'une exécution vivante sans l'avoir
jugé mort. Une fois toutes finies, aucune n'a laissé de fichier, et une exécution lancée après une
mort a pris le verrou. `pytest` passe les scénarios les plus petits ;
`python tests/core/lock_model.py` les passe tous et donne le nombre d'états de chacun.
`tests/core/test_table_lock.py` rejoue sur le système de fichiers local les ordres trouvés par la
revue, et tue une exécution à chacune de ses étapes : la suivante prend le verrou aussitôt, sans
rien vieillir.

Le puits du flux capteurs fait exception : au lieu d'échouer, il attend un verrou tenu
(`_write_waiting` de `gtdp.streaming.stream`). Il réessaie toutes les 2 s, pendant
`stale_after_s` (300 s, section `[streaming]`) plus 60 s de marge, quel que soit le détenteur,
puis la requête échoue avec `TableLockedError`. Ce délai couvre la reprise du verrou d'un driver
tué ; une étape de housekeeping doit rester plus courte. Le job règle
`spark.gtdp.io.staleAfterSeconds` sur la même valeur ([ADR-0008](0008-streaming.md)).

### Renommages et signaux d'arrêt

Les renommages passent par `FileContext.rename` sans écrasement. Un renommage vers un chemin qui
existe échoue avec `FileExistsError`, alors que `FileSystem.rename` déplacerait la partition à
l'intérieur du répertoire existant.

SIGTERM et SIGINT sont différés pendant l'échange, la prise et la libération du verrou. Le signal
prend effet à la fin du bloc : une exécution arrêtée laisse chaque partition ancienne ou nouvelle,
jamais absente. Reçu pendant la prise, il prend effet une fois le verrou pris, puis le verrou est
libéré.

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

`build_spark` ne pose donc pas ce mode : aucune écriture n'en dépend.

En gold, chaque exécution recalcule `fact_sales` depuis toutes les commandes des jours silver
terminés, compare une empreinte par `order_date` avec la table publiée, réécrit les dates qui
diffèrent (`write_partitions`) et supprime celles qui n'ont plus de commande
(`drop_partitions`). Une commande retardataire, retirée ou déplacée par un rejeu silver ne laisse
donc ni perte ni fantôme. Tout est calculé et contrôlé avant la première écriture. Une seule
exécution gold tourne à la fois : `io.table_lock` sur le répertoire gold tient le verrou
`/datalake/_gold.lock` pendant toute l'exécution. `gold/_state.json` marque la publication
(`PUBLISHING`, puis `PUBLISHED`).

### Table d'audit et arrêt d'une exécution

`run.py` ouvre une exécution auditée autour de chaque job (`gtdp.audit.audited_run`).
`<ops>/audit` est la seule table en ajout : chaque exécution écrit sa ligne dans son propre
répertoire `run_date=…/run_id=…`, ce qui évite le répertoire `_temporary` que Spark partage
entre jobs simultanés. La ligne porte un statut :

| Statut | Cause | Code de sortie de `run.py` |
| --- | --- | --- |
| `SUCCESS` | le job s'est terminé | 0 |
| `FAILED` | exception du job, message dans `error_message` (2 000 caractères au plus) | 1, ou 2 pour une `ConfigError` levée par le job (section de configuration absente, `--date` manquant d'un job qui l'exige : `bronze`, `silver`, `gold`, `revenue-by-country-spark`) |
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
- Deux exécutions n'écrivent jamais la même table en même temps, dans les limites de la
  [garantie du verrou](#garantie) : la seconde échoue avec `TableLockedError` et doit être
  relancée après la première. Seul le flux capteurs attend.
- Limite connue : un `kill -9` entre la suppression et le renommage d'une partition laisse cette
  partition absente jusqu'au rejeu du jour. Le rejeu reprend aussitôt le verrou de l'exécution
  tuée, que YARN rapporte terminée. Sans réponse du ResourceManager, ce verrou bloque la table
  pendant 6 h, sauf suppression à la main après avoir vérifié que l'application qu'il nomme est
  terminée. La procédure est dans le
  [runbook silver](../runbook/silver.md#réparer-un-jour-incomplet) et le
  [runbook bronze](../runbook/bronze.md#après-une-exécution-tuée).
- Un `spark.read.parquet` brut échoue sur une table sans fichier de données et ignore le schéma
  attendu : seul `read_table` est autorisé dans les jobs.

## Alternatives écartées

- `append` + déduplication en lecture : coût reporté sur chaque requête, doublons visibles.
- Delta Lake (`MERGE`) : hors programme, ajoute une dépendance et un format.
- `FileSystem.rename` : il déplace une partition dans une destination existante au lieu
  d'échouer, et imbrique la partition dans elle-même.
