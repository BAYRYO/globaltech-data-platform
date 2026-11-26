# ADR-0002 : Parquet partitionné et réécriture dynamique pour l'idempotence

**Statut** : acceptée

## Contexte

Le rejeu d'un même jour ne doit pas créer de doublons. Delta Lake et Iceberg ne font pas partie
du programme du module ; le stockage reste HDFS + Parquet.

## Décision

Toute écriture batch cible des partitions dérivées de l'entrée (`ingestion_date` en bronze et
silver, `order_date` touchées en gold). Le job **nomme** les partitions qu'il produit ; l'utilitaire
d'écriture supprime ces répertoires de partition, puis écrit le DataFrame, avec
`partitionOverwriteMode=dynamic` posé sur l'écriture elle-même. Les tables non partitionnées et
de petite taille (`products_scd2`, `customers`, dimensions, indicateurs) sont recalculées en entier
dans un répertoire temporaire puis échangées par renommage HDFS (opération atomique sur le
NameNode).

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
- Une exécution interrompue laisse au pire une partition incomplète que la suivante remplace.
- La table d'audit, journal par nature, est la seule écriture en `append`.

## Alternatives écartées

- `append` + déduplication en lecture : coût reporté sur chaque requête, doublons visibles.
- Delta Lake (`MERGE`) : hors programme, ajoute une dépendance et un format.
