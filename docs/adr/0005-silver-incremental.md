# ADR-0005 : silver incrémental, publié par jour complet

**Statut** : acceptée

## Contexte

J2 et J3 renvoient des commandes déjà reçues (doublons inter-jours) et des commandes en retard.
La déduplication doit être incrémentale (ne pas relire tout l'historique à chaque jour) et le
rejeu de `D` idempotent. Un jour ne vaut que si ses quatre sources sont publiées et vérifiées :
gold et les jours suivants ne doivent jamais lire un jour à moitié publié.

## Décision

`silver/orders` et `silver/weblogs` sont partitionnées par `ingestion_date`, jour de première
arrivée de la ligne. `silver/customers` et `silver/products_scd2`, petites, sont reconstruites
depuis bronze jusqu'à `D` et échangées en entier ([ADR-0002](0002-parquet-idempotence.md)).

### Jours complets

`silver/_days/ingestion_date=D/` tient un marqueur `<source>.json` par source publiée et vérifiée
pour `D`, puis `_SUCCESS` quand les quatre sources ont le leur (`gtdp.silver.days`). Un jour sans
`_SUCCESS` est en cours, échoué, ou rendu incomplet par un rejeu.

### Traitement de `D`

Pour chaque source :

1. classer les lignes de bronze(`D`) : doublons exacts, règles de qualité, doublons de clé dans
   le dépôt ;
2. anti-jointure sur les clés publiées par les **jours complets antérieurs à `D`** (ceux qui ont
   leur `_SUCCESS`) : pour `orders` (`transaction_id`) et `weblogs` (`log_id`), une clé déjà
   publiée est un doublon inter-jours ;
3. contrôler avant l'écriture : `bronze(D) = propres + rejets + doublons`, clé unique (sur les
   jours complets et le jour pour une table partitionnée), invariants SCD2 de `products_scd2` ;
   un échec arrête la source sans rien écrire ;
4. réécrire la partition `D` seule, ou échanger la table reconstruite ;
5. relire ce qui a été écrit et refaire les contrôles ; un échec retire la publication de la
   source pour `D` ;
6. poser le marqueur de la source, puis `_SUCCESS` quand les quatre sources sont marquées.

Comme l'étape 2 ignore `D` et les jours suivants, rejouer `D` produit le même résultat. Le
test `tests/silver/test_silver_incremental.py` et la colonne `content_sum` du rapport silver le
vérifient ([runbook silver](../runbook/silver.md#rejouer-un-jour)).

### Garde d'ordre

Sauf `--force`, le job refuse `D` :

- si un jour antérieur à `D` présent en bronze n'est pas complet en silver (`DayOrderError`) :
  `D` se dédupliquerait contre un historique incomplet ;
- si un jour postérieur à `D` est complet (`LaterPartitionError`) : traiter J2 après J3 laisserait
  en J3 des clés que J2 publie aussi, chaque réconciliation journalière restant vraie.

Toute exécution sur `D` retire avant d'écrire les marqueurs des jours postérieurs : ils
deviennent incomplets. `--force D` passe donc la garde, puis les jours suivants se rejouent un
par un, dans l'ordre, la garde refusant tout autre ordre.

Il n'y a pas d'option `--cascade` qui rejouerait les jours suivants dans la même exécution.
Chaque jour rejoué garde sa propre ligne d'audit, que le rapport silver compare au manifeste de
ce jour. Une exécution qui couvrirait plusieurs jours mélangerait leurs comptes dans une seule
ligne.

### Dépendances entre sources

Les références d'une commande sont contrôlées contre les dimensions silver publiées pour `D`.
Sans `--source`, le job traite `customers` et `products`, puis `orders`, puis `weblogs`.
`--source orders` est refusé tant que `customers` et `products` ne sont pas publiées pour `D`.
Rejouer l'une d'elles retire le marqueur de `orders` pour `D`, qui est alors à refaire.

### Premier jour

Une table silver absente se lit comme une table vide (`read_table`), sans erreur
`PATH_NOT_FOUND`.

## Conséquences

- Coût de l'anti-jointure et des contrôles d'unicité : lecture de la seule colonne de clé des
  partitions des jours complets (Parquet colonnaire). Sur le cluster, les deux contrôles
  d'unicité de J2 prennent 8,2 s pour les 965 000 commandes de J1 (mesure du runbook silver).
- Un lecteur qui doit voir des jours entiers, comme gold, ne lit des tables partitionnées que les
  partitions de `complete_days(spark, silver)`.
- Gold se recalcule en entier depuis les jours complets et ne réécrit que les dates
  `order_date` dont le contenu change (voir ADR-0002).
- Corriger un jour passé passe par `--force` puis le rejeu des jours suivants. Entre les deux,
  `customers` et `products_scd2` reflètent l'état du jour forcé.

## Alternatives écartées

- Partition par `order_date` en silver : une commande en retard modifierait une partition déjà
  publiée, ce qui rend le rejeu non local.
- Présence d'une partition comme preuve de traitement : une exécution tuée, ou dont une autre
  source échoue, laisse des partitions d'un jour incomplet. Le marqueur n'est posé qu'après la
  vérification de la source relue.
- Option `--cascade` : voir « Garde d'ordre ».
