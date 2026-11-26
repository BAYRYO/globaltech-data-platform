# ADR-0005 : silver incrémental partitionné par date d'ingestion

**Statut** : acceptée

## Contexte

J2 et J3 renvoient des commandes déjà reçues (doublons inter-jours) et des commandes en retard.
La déduplication doit être incrémentale (ne pas relire tout l'historique à chaque jour) et le
rejeu de `D` idempotent.

## Décision

`silver/orders` est partitionné par `ingestion_date` (jour de première arrivée de la commande).
Pour traiter `D` :

1. valider bronze(`D`), dédupliquer à l'intérieur du dépôt ;
2. anti-jointure sur les clés présentes dans les partitions silver **strictement antérieures**
   à `D` : une clé déjà connue est un doublon inter-jours ;
3. réécrire la partition `D` seule.

Comme l'étape 2 ignore la partition `D` elle-même, rejouer `D` produit le même résultat.

## Conséquences

- Coût de l'anti-jointure : lecture de la seule colonne `transaction_id` des partitions
  antérieures (Parquet colonnaire).
- Gold repartitionne par `order_date` pour les requêtes métier.
- Rejouer un jour antérieur (`D-1`) après `D` impose de rejouer aussi les jours suivants ; le
  runbook le documente.

## Alternatives écartées

- Partition par `order_date` en silver : une commande en retard modifierait une partition déjà
  publiée, ce qui rend le rejeu non local.
