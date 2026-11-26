# ADR-0002 : Parquet partitionné et réécriture dynamique pour l'idempotence

**Statut** : acceptée

## Contexte

Le rejeu d'un même jour ne doit pas créer de doublons. Delta Lake et Iceberg ne font pas partie
du programme du module ; le stockage reste HDFS + Parquet.

## Décision

Toute écriture batch cible des partitions dérivées de l'entrée (`ingestion_date` en bronze et
silver, `order_date` touchées en gold). Le job **nomme** les partitions qu'il produit, et
`gtdp.io.write_partitions` procède en deux temps :

1. **écriture complète** du DataFrame dans un répertoire de transit propre à l'exécution
   (`_<table>.staging-<uuid>`), partitionné comme la cible ;
2. **échange** : une fois l'écriture réussie, chaque partition nommée est supprimée puis
   remplacée par renommage de sa version en transit. Une partition nommée sans ligne devient un
   répertoire vide marqué `_SUCCESS` : elle compte comme traitée (garde d'ordre de l'ADR-0005).

Une ligne hors des partitions nommées fait échouer l'écriture avant tout échange. Une écriture qui
échoue (tâche en erreur, colonne manquante, DataFrame qui relit la partition qu'il remplace)
laisse la partition publiée intacte.

Les tables non partitionnées et de petite taille (`products_scd2`, `customers`, dimensions,
indicateurs) passent par `replace_table` : écriture complète dans `_<table>.tmp`, ancienne version
mise de côté en `_<table>.old`, renommage de la nouvelle, suppression de l'ancienne. Si un arrêt
brutal laisse la table absente, la lecture ou l'écriture suivante remet en place la version en
transit complète (marquée `_SUCCESS`), sinon l'ancienne.

Les jobs lisent toute table par `gtdp.io.read_table(spark, chemin, schéma)` : une table absente ou
dont toutes les partitions sont vides rend un DataFrame vide du schéma attendu au lieu d'échouer.

Pourquoi nommer les partitions plutôt que compter sur le seul mode dynamique (mesuré lors de la
revue du Lot 0, Spark 3.5.3) :

- le mode dynamique ne remplace que les partitions qui reçoivent au moins une ligne : rejouer un
  jour qui ne produit plus aucun rejet laisserait l'ancienne partition de rejets en place ;
- posé seulement dans la session, il dépend de la configuration ambiante : en mode statique, le
  même `overwrite` efface sans erreur toutes les autres partitions.

En gold, le traitement de `D` recalcule chaque partition `order_date` présente dans silver(`D`) à
partir de toutes les lignes silver de ces dates : une commande retardataire ne fait pas
disparaître les commandes déjà publiées pour sa date.

## Conséquences

- Rejouer J2 trois fois laisse exactement le même contenu qu'une exécution unique (testé).
- Une exécution interrompue avant l'échange ne modifie rien ; interrompue pendant l'échange (quelques
  renommages sur le NameNode), elle laisse des partitions soit anciennes soit nouvelles, que la
  suivante remplace.
- Un `spark.read.parquet` brut échoue sur une table dont toutes les partitions sont vides : seul
  `read_table` est autorisé dans les jobs.
- La table d'audit, journal par nature, n'est jamais réécrite : chaque exécution ajoute son propre
  répertoire `run_date=…/run_id=…`, ce qui évite le répertoire `_temporary` partagé de Spark entre
  jobs simultanés.

## Alternatives écartées

- `append` + déduplication en lecture : coût reporté sur chaque requête, doublons visibles.
- Delta Lake (`MERGE`) : hors programme, ajoute une dépendance et un format.
