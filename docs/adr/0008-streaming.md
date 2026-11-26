# ADR-0008 : sémantique du streaming capteurs

**Statut** : acceptée

## Contexte

Le Lot 5 exige des agrégats par fenêtre, un watermark, la déduplication, la reprise après arrêt
avec checkpoint, des alertes sur seuil et une écriture idempotente (« prouvée » au niveau
Excellent). La revue adverse du Lot 0 a mesuré sur Spark 3.5.3, avec un flux de test conforme au
contrat :

- en mode `append`, une fenêtre n'est écrite que lorsque le watermark la dépasse : avec un délai de
  10 min sur 10 min de flux, aucune fenêtre n'était écrite ; avec 5 min, 4 fenêtres sur 10 ;
- en mode `update`, chaque fenêtre est réécrite à chaque micro-batch (7 082 lignes pour 679 clés),
  ce qui rend un puits indexé par `batch_id` non idempotent au sens métier ;
- les deux premiers micro-batches passent avant que le watermark soit établi : des événements
  très en retard y sont agrégés ;
- un arrêt brutal après l'écriture du batch 3 puis une reprise sur checkpoint donnent exactement
  les mêmes lignes qu'une exécution sans arrêt.

## Décision

- Source : fichiers JSON Lines de `/datalake/landing/sensors/incoming`, schéma explicite,
  `maxFilesPerTrigger` pour un débit maîtrisé ; lignes illisibles ou sans valeur vers
  `streaming/rejects`.
- **Watermark de 2 minutes** sur `event_time` : le contrat place les retards ordinaires entre 10 et
  90 s (acceptés) et les très grands retards au-delà de 5 min (écartés), hors des 36 premiers
  fichiers pour que le watermark soit établi.
- **Déduplication** par `dropDuplicatesWithinWatermark("event_id")` (Spark ≥ 3.5) : l'état d'un
  identifiant est purgé avec le watermark, sans exiger que les doublons portent le même
  `event_time`.
- **Agrégats** : fenêtres fixes de 1 minute par machine et métrique, mode **`append`** : chaque
  fenêtre est écrite une seule fois, quand elle est définitive.
- **Alertes** : par événement, après déduplication, dès le micro-batch où il arrive (latence de
  quelques secondes, au lieu de fenêtre + watermark). Les seuils sont une table de configuration
  par métrique, pas des conditions dans le code.
- **Écriture idempotente** : `foreachBatch` écrit chaque sortie dans `…/batch_id=<n>/` en
  remplaçant ce répertoire. Spark rejoue un batch non validé avec le même `batch_id` après une
  reprise : la réécriture remplace le résultat partiel au lieu de le dupliquer. Preuve : arrêt
  brutal pendant l'écriture, reprise, comparaison avec une exécution sans arrêt.
- Les événements écartés par le watermark sont comptés par `numRowsDroppedByWatermark` (métriques
  de progression), hors des deux premiers micro-batches.

## Conséquences

- Le flux étant fini en démonstration, les fenêtres des 3 dernières minutes ne sont écrites que si
  des données plus récentes arrivent : la démonstration rejoue 15 minutes de flux pour en
  finaliser 12.
- Le checkpoint lie la requête à son plan : changer l'agrégation impose un nouveau répertoire de
  checkpoint (documenté dans le runbook).

## Alternatives écartées

- Mode `update` avec écrasement par clé (fenêtre, machine, métrique) : idempotent, mais réécrit
  chaque fenêtre à chaque batch et exige une fusion par clé que Parquet seul ne fournit pas.
- Alertes sur la fenêtre : latence de 3 minutes, inutile pour une alerte industrielle.
- `dropDuplicates(["event_id", "event_time"])` : un doublon réémis avec un autre horodatage
  passerait.
