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

- Détection d'un DataNode mort en 90 s : `dfs.namenode.heartbeat.recheck-interval` = 30 s et
  `dfs.heartbeat.interval` = 3 s, soit 2 × 30 + 10 × 3 = 90 s (valeurs par défaut : 2 × 300 + 10 × 3
  = 630 s, trop long pour une démonstration).

## Conséquences

- Surcoût de stockage : ×3 en réplication, ×1,5 en XOR-2-1 pour des fichiers d'au moins 2 Mio
  (cellule de 1 Mio). En dessous, la bande est incomplète : ×2,0 pour 150 Kio, ×1,67 pour 1,5 Mio
  (calcul de la revue du Lot 0). La comparaison porte donc sur des fichiers de 128 Mio et plus,
  et le surcoût des petits fichiers est rapporté à part.
- Avec 3 DataNodes, la perte d'un nœud laisse tous les blocs sous-répliqués (2/3) jusqu'à son
  retour : c'est le scénario de la fiche d'incident, analysé avec `hdfs fsck` avant, pendant et
  après la panne.

## Alternatives écartées

- 2 DataNodes : ni réplication 3 effective ni erasure coding possible.
- 5 DataNodes pour `RS-3-2` : Docker dispose de 15 444 Mio ; le budget mémoire du cluster
  (`docs/platform.md`) réserve l'essentiel aux conteneurs YARN, et deux DataNodes de plus (tas et
  surcoût JVM) retireraient environ 2 Gio aux exécuteurs Spark sans servir aucun autre livrable.
  `RS-3-2` et `RS-6-3` sont comparés par le calcul.
