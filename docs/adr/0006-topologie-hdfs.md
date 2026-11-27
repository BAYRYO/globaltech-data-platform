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

## Mise à jour du 2026-11-27

Trois phrases de cet ADR sont dépassées. La décision de garder 3 DataNodes ne change pas, mais
l'exercice de KAN-52 la remet en question.

- **Coût de 2 DataNodes de plus.** « environ 2 Gio retirés aux exécuteurs » a été écrit avant le
  [budget mémoire](../platform.md#budget-mémoire). Un DataNode a un plafond de 640 Mio : 2 de plus
  coûtent 1 280 Mio, et le pire cas du budget passe de 13 824 à 15 104 Mio sur les 15 444 Mio de
  Docker, soit 340 Mio de marge (calculé). Aucun conteneur YARN ne serait réduit. `RS-3-2` reste
  écarté pour cette marge et parce qu'aucun autre livrable n'en a besoin.
- **Blocs sous-répliqués « jusqu'à son retour ».** Depuis KAN-52, le DataNode de secours
  `datanode4` (profil `spare`, [ADR-0011](0011-exploitation-plateforme.md)) donne une cible aux
  répliques perdues une fois démarré à la main. Pendant l'exercice du 27 novembre 2026, les blocs
  sont restés 3 min 18 s sans cible, puis 69 min 50 s en re-réplication vers le secours. L'action 1
  de la [fiche d'incident](../incidents/2026-11-27-perte-datanode.md#actions-préventives) demande
  de garder 4 DataNodes en service, ou le secours prêt.
- **« `RS-3-2` et `RS-6-3` sont comparés par le calcul ».** Le calcul, à partir de la définition
  de chaque politique (blocs de parité divisés par blocs de données, un DataNode par bloc) :

  | Politique | Surcoût | Pannes tolérées par groupe | DataNodes nécessaires |
  | --- | ---: | ---: | ---: |
  | Réplication 3 | 200 % | 2 | 3 |
  | `XOR-2-1` | 50 % | 1 | 3 |
  | `RS-3-2` | 66,7 % | 2 | 5 |
  | `RS-6-3` | 50 % | 3 | 9 |
