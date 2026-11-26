# ADR-0006 : 3 DataNodes, réplication 3, erasure coding XOR-2-1 comparé

**Statut** : acceptée

## Contexte

Le sujet impose au moins 2 DataNodes, une simulation de panne et une comparaison réplication 3
contre erasure coding. La machine de développement dispose de 16 Go de RAM pour tout le cluster.

## Décision

- 3 DataNodes : minimum pour la réplication 3 et pour la politique `XOR-2-1-1024k`
  (2 blocs de données + 1 de parité).
- Réplication par défaut 3 pour les couches du lake.
- La comparaison active `XOR-2-1-1024k` sur `/benchmarks/ec` et mesure le volume physique
  (`hdfs dfs -du`), la tolérance (1 panne contre 2) et le coût. `RS-6-3` (9 DataNodes) est
  calculé sur papier.

## Conséquences

- Surcoût de stockage : ×3 en réplication, ×1,5 en XOR-2-1.
- Avec 3 DataNodes, la perte d'un nœud laisse tous les blocs sous-répliqués (2/3) jusqu'à son
  retour : c'est le scénario de la fiche d'incident.

## Alternatives écartées

- 2 DataNodes : ni réplication 3 effective ni erasure coding possible.
- 5 DataNodes pour `RS-3-2` : dépasse la mémoire disponible avec YARN.
