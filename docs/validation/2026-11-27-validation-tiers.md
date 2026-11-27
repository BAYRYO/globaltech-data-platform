# Validation par un tiers du README et du runbook d'exploitation

Le 27 novembre 2026, un agent qui n'a écrit ni le [README](../../README.md) ni le
[runbook d'exploitation](../runbook/exploitation.md) a suivi le README de bout en bout sur un
cluster remis à zéro. Il a aussi chronométré une relance du cluster et exécuté quatre procédures
du runbook. Cette page garde ses mesures et ses constats, puis ce que la PR #26 a corrigé.

Elle sert de preuve à quatre critères :

- KAN-53 : un membre de l'équipe a suivi le README de bout en bout sans aide ;
- KAN-55 : une procédure a été exécutée par un membre qui ne l'a pas écrite ;
- grille, niveau « Excellent » : relance validée par un tiers en moins de 10 minutes ;
- grille, niveau « Excellent » : documentation utilisable par une équipe tierce sans aide.

## Conditions

| Élément | Valeur |
| --- | --- |
| Version suivie | branche de la PR #26 au commit `8a99fea`, dans un clone neuf |
| Date | 27 novembre 2026, de 16 h 07 à 17 h 38 UTC |
| Poste | machine de référence : Windows 11, Git Bash, Docker Desktop avec 16 CPU et 15 Gio |
| Cluster | accès exclusif au projet compose `gtdp` pendant toute la validation |
| Sel | sel de test créé par la commande du README dans le `.env` du clone, jamais affiché ni gardé |
| Fichiers suivis | aucun modifié (`git status --short` vide, hors `.env`, `data/generated/` et `dist/`, que Git ignore) |

Au départ, le cluster était sain : 9 services `(healthy)`, `HDFS  3 live / 0 dead DataNodes`,
`YARN  2 RUNNING / 2 NodeManagers`, aucune application YARN, aucun bloc sous-répliqué ou
manquant. Il portait le lac de travail de l'équipe, gold `PUBLISHED`.

## Verdicts

| Critère | Verdict | Preuve |
| --- | --- | --- |
| KAN-53, README suivi de bout en bout sans aide | Oui. Les 10 étapes sortent avec 0 et affichent le résultat annoncé. Aucun écart n'a été nécessaire pour avancer. | [Étapes du README](#étapes-du-readme) |
| KAN-55, procédure exécutée par un non-auteur | Oui. Quatre procédures atteignent l'état final décrit. Un contrôle intermédiaire du rejeu montre un écart que la page n'annonçait pas (M1). | [Procédures du runbook](#procédures-du-runbook) |
| Grille, relance validée par un tiers en moins de 10 min | Oui. 67 s avec reconstruction de l'image, 27 s sans. | [Chronométrages](#chronométrages) |
| Grille, documentation utilisable par une équipe tierce sans aide | Oui, avec 1 constat majeur et 9 mineurs, aucun bloquant. Chaque commande copiée telle quelle a fonctionné, et chaque nombre attendu a été retrouvé à l'unité. | [Constats et corrections](#constats-et-corrections) |

## Chronométrages

### Relance avec les données, image reconstruite

Commandes : `scripts/cluster/down.sh`, `scripts/cluster/up.sh --build`, puis les trois commandes
de [Vérifier la santé](../runbook/exploitation.md#vérifier-la-santé), répétées jusqu'à ce que
chaque contrôle de son tableau soit vert.

| Partie | Durée |
| --- | --- |
| `down.sh` | 5 s |
| `up.sh --build`, construction de l'image (cache Docker, sauf Spark téléchargé de nouveau en 18,6 s) | environ 31 s |
| `up.sh --build`, démarrage et attente des services sains | environ 24 s |
| Contrôle de santé, vert au premier essai | 7 s |
| **Total, jusqu'au premier contrôle entièrement vert** | **67 s** |

Après la relance, le lac était intact : `du` identique sur les six zones, même
`hdfs dfs -count /datalake/gold` (`403 513 53899715`), même exécution dans `gold/_state.json`.
`fsck /` répondait `Status: HEALTHY`, 3 DataNodes vivants, 0 mort, 2 NodeManagers `RUNNING`.

### Relance avec les données, sans reconstruction

Mesurée à l'étape 10 du README : `down.sh` 4 s, puis `up.sh` 23 s, soit **27 s**. Gold était
toujours `PUBLISHED`, avec la même exécution.

### Démarrage depuis zéro

| Commande | Mesuré | Annoncé avant la validation |
| --- | --- | --- |
| `reset.sh` | 4 s | 4 s |
| `up.sh` | 20 s | 15 s |
| `init-hdfs.sh` | 34 s | 26 s |
| `smoke/run.sh` | 64 s | 54 s |
| **`up.sh`, `init-hdfs.sh` et `smoke/run.sh`** | **118 s** (122 s avec `reset.sh`) | 95 s |

L'image était déjà construite. Sa première construction a pris 6 min 52 s lors d'une mesure
antérieure ([guide de la plateforme](../platform.md#prérequis)).

## Étapes du README

| Étape | Commande, telle qu'écrite | Code | Durée | Résultat attendu et constaté | Écart |
| --- | --- | --- | --- | --- | --- |
| 1 | `git clone`, `cd` | 0 | moins de 10 s | conforme | clone de la branche de la PR, pas de `main` |
| 2 | `scripts/test.sh` | 0 | 36 min 5 s | `All checks passed!`, `158 files already formatted`, dernière ligne `911 passed in 2156.45s (0:35:56)`, aucun `failed` | lancée en dernier, cluster inactif |
| 3 | `up.sh`, `init-hdfs.sh`, `smoke/run.sh` | 0, 0, 0 | 20 s, 34 s, 64 s | `HDFS  3 live / 0 dead DataNodes`, `YARN  2 RUNNING / 2 NodeManagers`, `==> smoke OK` | aucun |
| 4 | `client.sh python3 -m gtdp.datagen generate ...`, puis `verify ...` | 0, 0 | 9 s, 7 s | `192 files, 1124325 rows`, `verify` finit par `OK (7.6 s)` | aucun |
| 5 | commande du sel | 0 | moins de 1 s | `.env` créé, valeur de 32 caractères, sans retour chariot, `openssl` présent dans Git Bash | aucun |
| 6 | boucle `for d in ...` | 0 | 453 s | 9 fois `"run ended"` avec `"status": "SUCCESS"`. De 22 s à 61 s par `submit.sh`, `duration_s` de 9,05 s à 48,8 s | aucun |
| 7a | rapport silver | 0 | 29 s | `3 landed days, incomplete: []`, `108 measures, 0 mismatches` | aucun |
| 7b | rapport gold | 0 | 47 s | `gold PUBLISHED, 0 order dates mismatched, 0 checks failed` | aucun |
| 7c | requête d'audit de « Traiter un jour » | 0 | 7 s | 3 lignes `SUCCESS`, `adds_up` à `true`, pour J1 seul : la requête filtre `DATE'2026-01-01'` | requête relancée sans filtre de date pour vérifier J2 et J3 (m4) |
| 8 | `submit.sh streaming-sensors`, `replay-sensors ... --interval 3`, attente de 60 s, `stop.sh`, rapport du flux | 0, 0, 0, 0 | rejeu 542 s, arrêt 22 s, rapport 17 s | `published 180 files, 11772 events`. Rapport : 743 agrégats, 10 alertes, 108 rejets, `duplicate keys 0` pour chaque table. Audit `SUCCESS`, `in 11772`, `stop 'stop marker'` | les deux « terminaux » étaient deux tâches de fond d'un même script |
| 9 | `run_mr.sh ...`, `submit.sh revenue-by-country-spark --date 2026-01-03`, `diff` | 0, 0, 0 | 26 s, 19 s, 2 s | `identiques` | `compare.sh`, cité pour information, non lancé |
| 10 | `down.sh`, puis `up.sh` pour continuer | 0, 0 | 4 s, 23 s | 4 volumes gardés, `status.sh` répond `error: the cluster is not running: scripts/cluster/up.sh`. Après `up.sh`, gold `PUBLISHED`, même exécution | aucun |

Durées de bout en bout de chaque `submit.sh` à l'étape 6 :

| Jour | `bronze` | `silver` | `gold` |
| --- | --- | --- | --- |
| 2026-01-01 | 32 s | 52 s | 61 s |
| 2026-01-02 | 22 s | 47 s | 54 s |
| 2026-01-03 | 23 s | 55 s | 59 s |

### Résultats après J1 à J3

Les lignes d'audit sont celles du tableau
[« Lignes d'audit attendues »](../runbook/exploitation.md#traiter-un-jour) du runbook :

| Jour | `bronze` : entrée = sortie | `silver` : entrée | propres + rejets + doublons | `gold` : entrée = sortie |
| --- | --- | --- | --- | --- |
| 2026-01-01 | 1 106 403 | 1 106 403 | 1 070 797 + 25 518 + 10 088 | 965 000 |
| 2026-01-02 | 3 075 | 3 075 | 2 958 + 73 + 44 | 967 631 |
| 2026-01-03 | 3 075 | 3 075 | 2 958 + 73 + 44 | 970 262 |

La requête par source d'[Interroger l'audit](../runbook/README.md#interroger-laudit) donne
`adds_up = true` sur ses 12 lignes. Pour J1 : customers 5 075 = 5 000 + 0 + 75, orders
1 000 000 = 965 000 + 25 000 + 10 000, products 1 328 = 1 297 + 18 + 13, weblogs
100 000 = 99 500 + 500 + 0. Pour J2 et J3 : customers 44 = 42 + 0 + 2, orders
2 740 = 2 631 + 68 + 41, products 17 = 12 + 4 + 1, weblogs 274 = 273 + 1 + 0. Aucune ligne
d'audit n'est autre que `SUCCESS`.

Le rapport silver compte les rejets par motif, chacun `ok` contre le manifeste, l'audit et la
table. `P00042` a 2 versions : les commandes d'avant le 2025-09-25 08:19:03 prennent 504,06,
celles d'après 530,97. Le rapport gold affiche `pending []`, `withdrawn []`, 367 dates de
commande `ok`, silver 970 262 = `fact_sales` 970 262 et chaque invariant à `ok`.

## Procédures du runbook

### a. Rejouer un jour

Rejeu de J2 alors que J3 est déjà traité ([procédure](../runbook/exploitation.md#rejouer-un-jour)).

| Étape | Commande | Code | Durée | Résultat |
| --- | --- | --- | --- | --- |
| référence | rapport bronze | 0 | 30 s | exécution de J2 notée |
| 1 | `land.sh` J2 | non lancée | | la page la demande seulement si les fichiers ont changé, ce qui n'était pas le cas |
| 2 | `submit.sh bronze --date 2026-01-02` | 0 | 25 s | `SUCCESS`, 3 075 lignes lues et écrites |
| contrôle | `submit.sh silver --date 2026-01-02`, sans `--force` | 1 | 15 s | `LaterPartitionError: silver already holds the complete day 2026-01-03, later than 2026-01-02`, ligne d'audit `FAILED`, rien d'écrit |
| 2 | `submit.sh silver --date 2026-01-02 --force` | 0 | 46 s | `SUCCESS`, 3 075 = 2 958 + 73 + 44 |
| contrôle | rapport silver | 1 | 30 s | `incomplete: ['2026-01-03']`, comme annoncé |
| contrôle | rapport gold | 1 | 45 s | `withdrawn ['2026-01-03']` comme annoncé, mais aussi `revenue_eur (applicable price) 4632664461.76 vs 4632664974.40 MISMATCH` et `gold PUBLISHED, 0 order dates mismatched, 1 checks failed`, non annoncés (M1) |
| 2 | `submit.sh silver --date 2026-01-03` | 0 | 48 s | `SUCCESS` |
| 2 | `submit.sh gold --date 2026-01-03` | 0 | 52 s | `SUCCESS`, 970 262 lignes |
| vérifier | rapport bronze | 0 | 25 s | J2 porte la nouvelle exécution, `rows` et `hash_sum` identiques à la référence |
| vérifier | rapport silver | 0 | 28 s | `3 landed days, incomplete: []`, contenu identique octet pour octet à celui de l'étape 7 du README |
| vérifier | rapport gold | 0 | | `gold PUBLISHED, 0 order dates mismatched, 0 checks failed`, contenu des 15 tables identique à l'étape 7. Audit du gold rejoué : `rewritten 0, removed 0, unchanged 367` |

Résultat : réussi. Contenus silver et gold inchangés.

### b. Reprendre le flux capteurs après un arrêt brutal

Le jour capteurs de `/datalake` avait été entièrement lu à l'étape 8, et le rejeu saute les
fichiers déjà présents. Le validateur a donc fait tourner le flux sur une racine de test,
`/user/etl/validate-crash`, avec les surcharges `--set paths.*` de
`scripts/streaming/demo.sh` ([test de reprise](../runbook/streaming.md#test-de-reprise)). Il a supprimé cette
racine à la fin. L'arrêt brutal est celui que la page cite, un conteneur client tué :
`docker kill` du conteneur du driver une fois `aggregates/offsets/10` écrit.

| Étape | Commande | Code | Résultat |
| --- | --- | --- | --- |
| déclencheur | flux et rejeu `--interval 3` vers la racine de test, `docker kill` au micro-batch 10 | 137 | tué à 340 s. `offsets` 10 et `commits` 10 pour les trois requêtes, aucun verrou laissé |
| 1 | `client.sh yarn application -list` | 0 | `Total number of applications ...:0` |
| 2 | `submit.sh streaming-sensors`, avec les surcharges | 0 à la fin | `query starts` avec `last_planned_batch 10, last_committed_batch 10` pour `aggregates`, `alerts` et `rejects` |
| 3 | `submit.sh streaming-housekeeping`, à côté du flux | 0 | `SUCCESS` en 6,2 s |
| fin | rejeu terminé (`published 180 files, 11772 events`), 60 s d'attente, `stop.sh <racine>` | 0 | flux sorti avec 0, audit `SUCCESS`, `in 4613`, `stop 'stop marker'` |
| vérifier | `submit.sh scripts/streaming/report.py /datalake <racine>` | 0 | racine de test : 743 / 10 / 108, `duplicate keys 0` pour chaque table. Comparée à `/datalake` : 0 différence de ligne, de clé ou de valeur, `IDENTICAL`. L'application tuée est listée sans ligne d'audit, `batches 11 in 7159` (7 159 + 4 613 = 11 772) |
| vérifier | listage de `aggregates/commits` | 0 | dernier `commits` 18 = dernier `offsets` 18, mais seulement après un tri numérique : le listage brut finit par `8` et `9` (m6) |

Résultat : réussi. Aucune clé en double, comptes attendus.

### c. Arrêter proprement un batch

SIGTERM au processus Python d'un silver en cours
([codes de sortie](../runbook/exploitation.md#codes-de-sortie)).

| Étape | Commande | Code | Résultat |
| --- | --- | --- | --- |
| départ | `submit.sh silver --date 2026-01-03`, dernier jour complet | | |
| 1 | `docker ps --filter name=gtdp-client --format '{{.Names}}'` | 0 | un conteneur |
| 2 | `docker exec <conteneur> ps -eo pid,args` | 0 | `python3.11 /opt/project/src/gtdp/jobs/run.py silver ...`, à côté du lanceur `java` |
| 3 | `kill -TERM` du processus Python, 26 s après le départ | 0 | `submit.sh` sort avec **143** 3 s plus tard. Audit `STOPPED`, `stopped by SIGTERM`, `rows_in 2801`. Journal `"run stopped", "signal": "SIGTERM"` |
| vérifier | listage des verrous de « Verrou resté », étape 1 | 1 | `No such file or directory` pour les trois motifs : verrous libérés |
| vérifier | rapport silver | 1 | tables lisibles, contenu inchangé. J3 reste incomplet : `customers.json` et `products.json` publiés, `orders` et `weblogs` manquants, `incomplete: ['2026-01-03']` (m7) |
| rejeu | `submit.sh silver --date 2026-01-03` | 0 | `SUCCESS` en 47 s, 3 075 = 2 958 + 73 + 44. Rapport silver à 0, `incomplete: []`, contenu identique |

Résultat : réussi.

### d. Verrou resté

`kill -9` du driver gold pendant son écriture
([procédure](../runbook/exploitation.md#verrou-resté)).

| Étape | Commande | Code | Résultat |
| --- | --- | --- | --- |
| déclencheur | `submit.sh gold --date 2026-01-03`, `kill -9` du driver Python dès qu'un `gold/_*.lock` existe | 137 | ni `run ended`, ni ligne d'audit. `_state.json` à `PUBLISHING`, comme le dit le runbook gold |
| 1 | listage des verrous, puis `hdfs dfs -cat /datalake/_gold.lock` | 0 | `/datalake/_gold.lock` et `/datalake/gold/_kpi_revenue_daily.lock`. Le verrou porte `run_id`, `application_id`, `locked_at` et `token` |
| 2 | `yarn application -status application_1795797311961_0020` | 0 | `State : FINISHED`, `Final-State : SUCCEEDED`, le cas exact contre lequel la page met en garde |
| 3 | `submit.sh gold --date 2026-01-03` | 0 | `taking over the lock hdfs://namenode:8020/datalake/_gold.lock, left by a run that died (application_1795797311961_0020 is FINISHED)`, puis de même pour `_kpi_revenue_daily.lock`. `SUCCESS`, 970 262 lignes |
| vérifier | listage des verrous | 1 | `No such file or directory` pour les trois motifs, aucun `_*.staging-*` restant. Rapport gold à 0, `gold PUBLISHED, 0 order dates mismatched, 0 checks failed` |

Résultat : réussi. La relance a repris les verrous d'elle-même. La suppression à la main n'a pas
servi.

### Contrôle de santé contre les sorties réelles

Les trois commandes ont tourné telles qu'écrites, et chaque ligne du tableau était observable,
sauf une. `hdfs dfsadmin -report | sed -n "1,24p"` s'arrête dans le premier DataNode vivant, alors
qu'une section `Dead datanodes` viendrait après tous les nœuds vivants : la ligne « aucune section
`Dead datanodes` » ne se vérifiait pas avec cette commande (m5). L'exemple de sortie de
`status.sh` décrivait un lac plus ancien, avec les données de `/benchmarks`.

## Constats et corrections

Aucun bloquant. Les constats des deux revues statiques de la PR et leur traitement sont dans le
commentaire « Tour de correction » de la PR #26.

| Constat | Ce que la validation a montré | Correction dans la PR #26 |
| --- | --- | --- |
| M1, majeur | Entre le rejeu forcé de J2 et celui de J3, le rapport gold sort avec 1 : `revenue_eur (applicable price)` en `MISMATCH`, car `products_scd2` ne contient plus les versions de J3. Le runbook n'annonçait que `withdrawn`. | « Rejouer un jour » et le rapport du runbook gold annoncent ce `MISMATCH`, le code 1 et leur fin après silver J3 puis gold. |
| m1 | « Redémarrer » ne disait pas quand `--build` est nécessaire. L'image en service datait d'avant la configuration `docker/conf` commitée, et `down.sh` puis `up.sh` auraient donné un cluster sain sur cette image. | « Redémarrer » demande `up.sh --build` après une modification de `docker/`, `git pull` compris, et donne deux commandes pour comparer les dates. |
| m2 | Les durées depuis zéro étaient optimistes d'environ 25 % : 118 s mesurées pour 95 s annoncées. | Le README et le runbook donnent les durées mesurées ici et citent cette page. |
| m3 | « Chacun des neuf jobs prend de 13 s à 77 s » ne correspondait à aucune mesure. | Le README et le runbook donnent 22 s à 61 s de bout en bout et 9 s à 49 s de `duration_s`. |
| m4 | La requête d'audit lit J1 seulement, le README promettait une ligne par job et par jour. | Le README dit de changer la date pour J2 et J3. La requête se limite aussi à `bronze`, `silver` et `gold`. |
| m5 | La ligne « aucune section `Dead datanodes` » du contrôle de santé ne se vérifiait pas. | La commande filtre `hdfs dfsadmin -report` avec `grep` et montre `Live datanodes (3):` et, s'il y en a, `Dead datanodes (N):`. |
| m6 | Le listage des `commits` du flux est trié comme du texte et ne montre pas `offsets`. | Une commande affiche le dernier numéro de `offsets` et de `commits`, triés comme des nombres. |
| m7 | Après le code 143, le jour silver reste incomplet, ce que le tableau des codes ne disait pas. | Le tableau des codes dit qu'un jour silver arrêté reste incomplet et que gold le refuse jusqu'au rejeu. |
| m8 | Avec plusieurs clients en cours, `docker ps --format '{{.Names}}'` ne dit pas quel conteneur porte le batch. | La commande affiche aussi la commande de chaque conteneur. |
| m9 | Le runbook du flux lançait `python` sur le poste, contre le prérequis du README. | La commande passe par `scripts/cluster/client.sh python3`. |
| remarque | « Un driver tué laisse aussi un répertoire de staging » : aucun dans ce test. | « peut aussi laisser ». |

## État final

À 17 h 01 UTC, avant la suite de tests :

- 9 services sains, `HDFS  3 live / 0 dead DataNodes, safe mode off` (1 509 blocs par DataNode),
  `YARN  2 RUNNING / 2 NodeManagers`, aucun conteneur YARN ;
- `dfsadmin` : `Live datanodes (3)`, 0 bloc sous-répliqué ou manquant. `fsck /` :
  `Status: HEALTHY`, 0 bloc corrompu ;
- aucune application YARN, aucun conteneur client, aucun conteneur `datanode4`, aucun verrou ;
- J1 à J3 complets : `silver/_days` porte les trois jours, gold `PUBLISHED` sur
  `silver_days` J1, J2 et J3, rapport gold à `0 checks failed` ;
- flux arrêté par son marqueur, tables du flux à 743 / 10 / 108 sans clé en double, racine de
  test supprimée ;
- quotas loin de 0 : `/datalake/bronze` a encore 29,7 G sur 30 G.

La remise à zéro a effacé le lac précédent et les jeux de mesure de `/benchmarks/ec` et
`/benchmarks/rep3`. Les scripts de `scripts/perf/` les recréent.

## Contrôle complémentaire du pilote

Après la validation, le pilote a vérifié sur YARN que gold refuse un jour silver incomplet
(KAN-58), de 17 h 40 à 17 h 43 UTC, sur le lac laissé par le validateur, avec la même version.

| Étape | Commande | Code | Résultat |
| --- | --- | --- | --- |
| déclencheur | `submit.sh silver --date 2026-01-03`, SIGTERM au processus Python 27 s après le départ | 143 | audit `STOPPED`, `stopped by SIGTERM`. Seul `customers.json` est publié pour J3 |
| refus | `submit.sh gold --date 2026-01-02` | 1 | ligne d'audit gold `FAILED` après 3,7 s de `duration_s`, message ci-dessous. `gold/_state.json` reste `PUBLISHED`, avec l'exécution précédente |
| reprise | `submit.sh silver --date 2026-01-03`, puis `submit.sh gold --date 2026-01-03` | 0, 0 | silver `SUCCESS`, 3 075 = 2 958 + 73 + 44. Gold `SUCCESS`, 970 262 lignes |
| vérifier | rapport gold | 0 | `gold PUBLISHED, 0 order dates mismatched, 0 checks failed` |

Message du refus :

```
IncompleteSilverError: gold reads complete silver days only, but 2026-01-03 is not complete (published: customers; missing: orders, products, weblogs) and silver/customers holds rows of it and silver/customers was rebuilt for it and silver/products_scd2 holds rows of it and silver/products_scd2 was rebuilt for it. Run scripts/cluster/submit.sh silver --date 2026-01-03, then gold again
```

Le refus suit la règle de KAN-58 : gold ne lit que des jours silver complets et refuse de publier
quand `customers` ou `products_scd2` tient un jour incomplet. La reprise suit, telle qu'écrite, la
procédure [Gold refuse un jour silver incomplet](../runbook/exploitation.md#gold-refuse-un-jour-silver-incomplet)
du runbook, dont le message et les commandes reprennent ce cas.
