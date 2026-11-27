# Script de la soutenance

Ce script donne, pour chaque diapositive du [support](index.html), le rôle qui parle, le texte à
dire et sa durée cible. La soutenance dure 15 minutes, démonstration comprise, puis viennent les
questions individuelles. Chaque rôle présente les lots qu'il a tenus, comme le prévoit le
[pilotage](../project-management.md#rôles). La durée mesurée de chaque diapositive est dans la
[répétition chronométrée](repetition.md). La démonstration est décrite dans [demo.md](demo.md).

Chaque chiffre dit ou affiché vient d'un document du dépôt, cité dans la ligne « Sources » de sa
diapositive. Le support reprend ces lignes dans les notes de l'orateur : `scripts/demo/deck.py
notes` les recopie depuis ce fichier, et un test vérifie que les deux restent identiques.

## Répartition

| Rôle | Lots | Diapositives | Durée cible |
| --- | --- | --- | ---: |
| Responsable qualité et delivery | 0, 8, 9 | 1, 2, 11, 14, 15 | 2 min 55 s |
| Architecte plateforme | 1, 2, 7 (HDFS) | 3, 4, 10, démonstration étapes 2 et 6 | 2 min 50 s |
| Data engineer batch | 3, 4 | 5, 6, 13, démonstration étapes 3 et 4 | 3 min 15 s |
| Data engineer streaming et performance | 5, 6, 7 | 7, 8, 9, démonstration étapes 1 et 5 | 3 min 15 s |

Les durées cibles des rôles comptent leur texte pendant la démonstration, pas le temps où les
commandes tournent sans commentaire : la démonstration enregistrée dure 3 min 47 s, son texte
1 min 45 s.

Deux diapositives sortent volontairement de cette règle, car la grille demande aussi de répondre
sur une partie qui n'est pas la sienne, avec du recul sur les limites :

- la diapositive 10 fait présenter par l'architecte plateforme l'exercice de panne d'un DataNode,
  qui relève du Lot 8 du responsable qualité et delivery : la panne porte sur le HDFS que
  l'architecte a monté, et sa partie stockage relève du volet HDFS du Lot 7. Le responsable
  qualité et delivery répond aux questions sur la fiche d'incident ;
- la diapositive 13 fait parler le data engineer batch des limites de toute la plateforme, pas
  seulement des siennes.

## Diapositives

Le texte à dire est en citation. Les noms en gras dans la démonstration désignent le rôle qui
prend la parole et ne se disent pas.

### 1. GlobalTech Data Platform

- Rôle : responsable qualité et delivery
- Durée cible : 30 s
- Sources : docs/project-management.md (rôles et lots)

> Bonjour. Nous présentons GlobalTech Data Platform, un prototype de plateforme Big Data sur
> Hadoop et Spark. Nous sommes quatre, un par rôle du sujet. L'architecte plateforme présente le
> cluster, HDFS et la couche bronze. Le data engineer batch présente silver et gold. Le data
> engineer streaming et performance présente le flux capteurs, MapReduce contre Spark et les
> mesures de performance. Je suis responsable qualité et delivery. Après les quatre parties, nous
> ferons une démonstration sur le cluster.

### 2. Contexte et question

- Rôle : responsable qualité et delivery
- Durée cible : 45 s
- Sources : docs/rapport-technique.md section 2 ; docs/data-contract.md (profils de volume, commandes)

> GlobalTech pilote sa chaîne logistique avec des fichiers Excel et des exports isolés, sans
> historique fiable. Le sujet demande une plateforme qui collecte, nettoie, historise et expose
> des indicateurs fiables. Le kit fourni est propre. Notre générateur à graine fixe en tire trois
> dépôts, J1, J2 et J3, avec des anomalies volontaires et un manifeste qui les compte. Chaque
> compte attendu est donc exact et testable. J2 apporte des retardataires et des renvois de J1,
> J3 une nouvelle colonne. Le produit P00042 porte vingt pour cent des lignes synthétiques : c'est
> le déséquilibre de clé du sujet. La plateforme répond à neuf questions métier et surveille les
> capteurs.

### 3. Architecture

- Rôle : architecte plateforme
- Durée cible : 50 s
- Sources : docs/architecture.md (vue d'ensemble) ; docs/rapport-technique.md section 3 ; docs/adr/0001-image-hadoop-spark.md ; docs/adr/0006-topologie-hdfs.md ; docs/adr/0011-exploitation-plateforme.md ; docs/platform.md (budget mémoire)

> Le cluster tourne sous Docker Compose, à partir d'une seule image construite par le projet :
> Hadoop 3.3.6, Spark 3.5.3 et Python 3.11. HDFS a un NameNode et trois DataNodes, le minimum
> pour la réplication 3 et pour l'erasure coding. YARN a deux NodeManagers de 2 560 Mio, ce qui
> laisse le streaming tourner à côté d'un batch. Nous avons fixé la mémoire service par service.
> Au pire cas, avec deux drivers, le cluster demande 13 824 Mio sur les 15 444 dont dispose
> Docker. Le lac a une racine par couche, pour y poser droits et quotas. Les rejets gardent des
> données personnelles en clair : ils sont fermés aux analystes.

### 4. Bronze

- Rôle : architecte plateforme
- Durée cible : 45 s
- Sources : docs/rapport-technique.md section 4 ; docs/adr/0012-bronze-controle-manifeste.md ; docs/adr/0002-parquet-idempotence.md ; docs/adr/0007-evolution-schema.md ; PR #7 (tour de correction) ; PR #18 (verrous des exécutions terminées)

> Bronze garde la donnée brute, sans perte. Avant toute écriture, le job vérifie chaque fichier
> contre son manifeste : empreinte SHA-256, nombre de lignes, UTF-8 strict et colonnes
> obligatoires. Chaque fichier est lu avec son propre en-tête, tout en texte, donc la colonne
> ajoutée par J3 ne casse rien. Pour rejouer sans doublon, le job écrit dans un répertoire de
> transit, puis remplace chaque partition nommée par un renommage. Sur J1, 1 106 403 lignes lues,
> autant d'écrites. Rejouer J2 redonne les douze mêmes sommes de contrôle. Un verrou de table
> empêche deux écritures simultanées.

### 5. Silver

- Rôle : data engineer batch
- Durée cible : 55 s
- Sources : docs/rapport-technique.md section 5 ; docs/data-model.md (ordre des opérations et réconciliation) ; docs/adr/0005-silver-incremental.md ; docs/adr/0004-pseudonymisation.md ; PR #11 (preuves, tour de correction, vérification du pilote)

> Silver applique les règles de qualité en une seule passe, à partir d'un registre de règles par
> source. Chaque ligne bronze reçoit une seule disposition : propre, rejetée avec ses codes de
> motif, ou doublon. Avant d'écrire, le job vérifie que bronze égale propres plus rejets plus
> doublons. Sur J1, 1 106 403 lignes donnent 1 070 797 propres, 25 518 rejets et 10 088 doublons.
> Le rapport silver compare 108 mesures au manifeste, sans aucun écart. Un jour n'est publié que
> lorsque ses quatre sources le sont, ce qui rend le rejeu idempotent. Les emails et les adresses
> IP sont hachés avec un sel gardé hors du dépôt. Sans le sel, nous avons réidentifié zéro adresse
> IP sur 100 046.

### 6. Gold

- Rôle : data engineer batch
- Durée cible : 55 s
- Sources : docs/rapport-technique.md section 6 ; docs/adr/0009-gold-etoile-recalcul.md ; docs/data-model.md (gold, modèle en étoile) ; PR #11 (SCD2 de P00042) ; PR #15 (preuves, tour de correction) ; PR #22 (jours silver complets)

> Gold est un schéma en étoile : une table de faits, une ligne par ligne de commande, et les
> dimensions date, client et produit. La dimension produit est historisée en SCD type 2, et
> chaque commande prend le prix de la version valide à son heure. Le prix de P00042 passe de
> 504,06 à 530,97 euros le 25 septembre à 8 h 19. Une commande de 8 h 11 garde l'ancien prix, une
> de 8 h 19 et 9 secondes prend le nouveau. Gold calcule neuf indicateurs, dont un top 5 par pays
> et une moyenne mobile sur 7 jours. Chaque exécution recalcule tout depuis les jours silver
> complets et ne réécrit que les dates qui changent : un rejeu réécrit zéro date sur 367.

### 7. Streaming

- Rôle : data engineer streaming et performance
- Durée cible : 50 s
- Sources : docs/rapport-technique.md section 7 ; docs/adr/0008-streaming.md ; docs/runbook/streaming.md (vérifier, reprise après arrêt brutal) ; PR #9 (preuves, tour de correction)

> Le flux capteurs dépose un fichier JSON toutes les cinq secondes. Le job Structured Streaming
> agrège par fenêtre d'une minute, en mode append, avec un watermark de deux minutes. Les retards
> ordinaires passent, les retards de plus de cinq minutes sont écartés. Les doublons sont
> supprimés par identifiant d'événement. Chaque micro-batch écrit dans une partition à son
> numéro, qu'un rejeu remplace : l'écriture est idempotente. Sur la journée de démonstration,
> chaque compteur égale le manifeste : 11 772 événements lus, 108 rejets, 324 doublons, 108
> événements trop en retard et 10 alertes. Driver tué en plein micro-batch puis relancé, le
> résultat est identique à celui d'une exécution sans arrêt.

### 8. MapReduce contre Spark

- Rôle : data engineer streaming et performance
- Durée cible : 50 s
- Sources : docs/rapport-technique.md section 8 ; docs/mapreduce-vs-spark.md (preuve d'égalité, résultats de la campagne, taille et lisibilité du code) ; docs/perf-evidence/mr-vs-spark-campaign.md ; docs/adr/0010-moteur-et-reglages-spark.md

> Le Lot 6 calcule le chiffre d'affaires par pays, avec huit compteurs de lignes invalides, en
> Hadoop Streaming et en Spark. Les deux sorties sont identiques octet pour octet, et les
> compteurs égalent les manifestes. Nous avons fait 45 exécutions exclusives, cinq par variante
> et par volume. Sur environ 20 000 lignes, MapReduce est plus rapide : Spark paie un démarrage
> fixe. Sur 10 millions de lignes, aucune différence de durée n'est détectable. En MapReduce, il
> faut écrire à la main le tri, le combiner, le cache distribué et l'arithmétique entière. Nous
> gardons Spark pour cette plomberie, pas pour une vitesse que la mesure ne montre pas.

### 9. Performance

- Rôle : data engineer streaming et performance
- Durée cible : 55 s
- Sources : docs/rapport-technique.md section 9 (leviers classés par impact) ; docs/performance.md (protocole commun, naïf contre optimisé, déséquilibre de clé, cache, partitions) ; docs/perf-evidence/formats/report.md ; docs/perf-evidence/skew-4slots/report.md ; docs/perf-evidence/cache/report.md ; docs/perf-evidence/partitions/report.md

> Chaque levier est mesuré sur YARN, cinq exécutions par variante, en alternance. Une différence
> n'est retenue que si le test de Mann-Whitney donne p inférieur à 0,05. Parquet au lieu de CSV
> gagne 88 % sur la même requête. Diffuser la dimension produit gagne 48 % et supprime le
> shuffle. L'élagage par date gagne 43 % une fois la table listée, mais quand Spark liste ses 396
> dossiers par un job, la requête est 79 % plus lente. P00042 donne un rapport de 4,16 entre la
> tâche la plus longue et la médiane. AQE le ramène à 1,5, le salage entre 1,6 et 1,8, sans gain
> de durée retenu, et le salage ralentit de 20 %.

### 10. Stockage et panne d'un DataNode

- Rôle : architecte plateforme
- Durée cible : 55 s
- Sources : docs/rapport-technique.md sections 9 (réplication 3 contre erasure coding) et 11 ; docs/performance.md (réplication 3 contre erasure coding) ; docs/incidents/2026-11-27-perte-datanode.md ; docs/adr/0006-topologie-hdfs.md

> Sur le même fichier de 713 Mo, la réplication 3 coûte 200 % de volume en plus, l'erasure coding
> XOR-2-1 50 %, sans différence de lecture détectable. Mais XOR-2-1 ne tolère qu'une panne par
> groupe : le lac reste en réplication 3. Nous avons arrêté deux fois un DataNode pendant que
> gold écrivait. La première fois, le NameNode l'a déclaré mort entre 86 et 91 secondes après son
> dernier battement de cœur, pour 90 attendues, et fsck est resté sain : 5 568 blocs à deux
> répliques, zéro bloc manquant. La seconde, gold reconstruite pendant la panne est identique à
> la gold publiée. La limite est la topologie : avec trois DataNodes, les répliques perdues
> n'avaient aucune cible avant le démarrage manuel d'un secours.

### 11. Industrialisation et qualité

- Rôle : responsable qualité et delivery
- Durée cible : 50 s
- Sources : docs/rapport-technique.md sections 3 (relance du cluster) et 10 ; docs/validation/2026-11-27-validation-tiers.md (relance par un tiers, chronométrages) ; docs/project-management.md (revues, définition de « terminé ») ; docs/reviews/2026-11-26-revue-adverse-lot0.md ; PR #11 (revue de silver) ; PR #15 (revue de gold) ; PR #18 (revue des verrous) ; PR #25 (preuves sur YARN)

> Le code est découpé en transformations pures et en jobs minces, avec une configuration TOML,
> des journaux JSON et une table d'audit, une ligne par exécution. Au relevé du rapport, la suite
> comptait 911 tests pytest, tous verts, pour 91,2 % de couverture. La CI lance lint et tests à chaque PR, et démarre
> un cluster neuf quand la plateforme change. Chaque story Jira a sa branche et sa PR. Il n'y a
> pas eu de relecture humaine : chaque livrable passe deux revues adverses, et un constat n'est
> retenu que s'il est prouvé par une exécution. La revue de gold a trouvé un bloquant, le pays du
> client figé dans le fait. Enfin, un tiers a suivi le README de bout en bout : la relance avec
> les données gardées a pris 67 secondes.

### 12. Démonstration

- Rôle : data engineer streaming et performance, architecte plateforme, data engineer batch, dans l'ordre des étapes
- Durée cible : 3 min 50 s
- Sources : docs/soutenance/demo.md ; scripts/demo/soutenance.sh ; docs/runbook/README.md (interroger l'audit) ; docs/runbook/gold.md (vérifier gold) ; docs/runbook/streaming.md (test de reprise)

> **Data engineer streaming et performance.** Je lance d'abord le flux capteurs en arrière-plan,
> sur une racine de test : trente fichiers de cinq secondes de mesures. Le déclenchement passe de
> trente à dix secondes pour tenir dans la démonstration. Le script tuera le driver au micro-batch
> numéro 2, puis relancera le job.
>
> **Architecte plateforme.** Pendant ce temps, l'état du cluster : neuf services sains, trois
> DataNodes vivants, aucun mort, deux NodeManagers en marche.
>
> **Data engineer batch.** Gold est publiée pour les trois jours silver complets. La table
> d'audit redonne la réconciliation de chaque jour : pour J1, 1 106 403 lignes bronze égalent
> propres, rejets et doublons, comme sur la diapositive silver. Le rapport gold recalcule silver
> et compare : 970 262 commandes et le même chiffre d'affaires des deux côtés, zéro contrôle en
> échec. Puis le prix de P00042, qui change à 8 h 19, appliqué à chaque commande selon son heure.
>
> **Data engineer streaming et performance.** Le journal du flux montre le driver tué au
> micro-batch 2 : planifié, pas validé. Le job relancé reprend à son checkpoint et rejoue ce
> micro-batch. Le rapport compare avec l'exécution de référence, sans arrêt, préparée avant la
> soutenance sur les mêmes fichiers : identique, sans clé en double. L'échantillon est petit,
> 23 agrégats et 14 rejets : la reprise sur la journée entière est dans le runbook du flux.
>
> **Architecte plateforme.** Dans YARN, l'application tuée est terminée avec succès : en mode
> client, YARN ne voit pas la mort du driver. Le rapport la repère donc à l'absence de sa ligne
> d'audit.

### 13. Limites

- Rôle : data engineer batch
- Durée cible : 50 s
- Sources : docs/rapport-technique.md sections 1 et 12 ; docs/incidents/2026-11-27-perte-datanode.md (actions préventives) ; docs/adr/0002-parquet-idempotence.md (garantie)

> Voici nos limites, par risque. Trois DataNodes pour une réplication 3 : après une panne, il faut
> démarrer un secours à la main. Un seul NameNode, un seul ResourceManager, un seul rack, et
> aucune alerte : la panne a été détectée par notre script. Gold relit tout l'historique silver à
> chaque exécution, et un rejeu silver lancé pendant gold n'est pas détecté. Le verrou de table
> garde deux cas résiduels, si YARN ne répond plus ou si un driver tué écrit encore. Le rejet des
> dates d'avant 1900 n'est prouvé que par des tests locaux. L'authentification HDFS est simple.
> Enfin, toutes nos mesures viennent d'une seule machine de 16 cœurs.

### 14. Retour d'expérience

- Rôle : responsable qualité et delivery
- Durée cible : 45 s
- Sources : docs/rapport-technique.md sections 5, 8, 10 et 12 ; docs/data-contract.md ; docs/project-management.md (définition de « terminé »)

> Ce qui a marché. Des manifestes qui comptent chaque anomalie : chaque compte devient un test
> exact. Mesurer avant de décider : nous avons gardé Spark pour sa plomberie, et écarté le
> salage, plus lent. Des revues qui doivent prouver leurs constats : elles ont trouvé le pays figé
> dans le fait et le sel visible dans un plan d'exécution. Ce que nous ferions autrement. Quatre
> DataNodes dès le départ. Prouver chaque story sur YARN avant de la fusionner : deux ne l'ont pas
> été, et l'une n'y est prouvée que pour sa non-régression. Archiver toutes les revues dans le
> dépôt, mettre un seuil de couverture, et publier l'image dans un registre.

### 15. Questions

- Rôle : responsable qualité et delivery
- Durée cible : 5 s
- Sources : docs/rapport-technique.md

> Merci. Nous répondons à vos questions, chacun sur sa partie.

## Questions probables

Pour chaque rôle, les cinq questions qu'un jury posera le plus probablement sur sa partie, avec
une réponse courte et le document qui la tient.

### Architecte plateforme

1. **Pourquoi trois DataNodes, et pas deux ou cinq ?** Deux ne permettent ni la réplication 3 ni
   l'erasure coding. Deux de plus pour `RS-3-2` coûteraient 1 280 Mio : le pire cas passerait à
   15 104 Mio sur 15 444. L'exercice de panne montre qu'il en faudrait quatre, ou le secours prêt.
   Source : [ADR-0006](../adr/0006-topologie-hdfs.md), [rapport, section 12](../rapport-technique.md#risques-pour-les-données).
2. **Pourquoi déclarer un DataNode mort en 90 s, et pas en 630 s par défaut ?** 90 s, c'est
   2 × 30 s de recontrôle plus 10 × 3 s de battement de cœur. Le coût : `datanode3`, vivant, a été
   déclaré mort pendant que la machine saturait. Source : [ADR-0006](../adr/0006-topologie-hdfs.md),
   [fiche d'incident](../incidents/2026-11-27-perte-datanode.md#événement-antérieur-du-même-jour).
3. **Comment le rejeu de bronze évite-t-il les doublons ?** Le job nomme ses partitions, écrit
   dans un répertoire de transit, puis remplace chaque partition par un renommage. Le mode
   `dynamic` de Spark a été écarté : il ne remplace que les partitions qui reçoivent une ligne.
   Source : [ADR-0002](../adr/0002-parquet-idempotence.md#pourquoi-nommer-les-partitions).
4. **Une autre équipe peut-elle relancer le cluster ?** Un tiers a suivi le README de bout en
   bout, 10 étapes sur 10 en code 0, et relancé le cluster en 67 s avec les données gardées,
   122 s depuis zéro. Un tiers sans image dépend du débit de archive.apache.org. Source :
   [rapport, section 3](../rapport-technique.md#relance-du-cluster),
   [validation par un tiers](../validation/2026-11-27-validation-tiers.md).
5. **Pourquoi ne pas passer le lac en erasure coding ?** `XOR-2-1` tolère une panne au lieu de
   deux. Un fichier plus petit que la cellule de 1 Mio paie 100 % au lieu de 50 %, et les blocs du
   lac font 167 748 octets en moyenne. Sans quatrième nœud, aucune réparation n'est possible.
   Source : [rapport, section 9](../rapport-technique.md#réplication-3-contre-erasure-coding),
   [ADR-0006](../adr/0006-topologie-hdfs.md).

### Data engineer batch

1. **Comment prouvez-vous que propres + rejets + doublons = total ?** Silver le vérifie avant
   d'écrire, puis sur les données relues. Un échec avant l'écriture ne publie rien. Le rapport
   silver compare 108 mesures au manifeste, à l'audit et aux tables. Source :
   [ADR-0005](../adr/0005-silver-incremental.md), [rapport, section 5](../rapport-technique.md#réconciliation-de-j1-à-j3).
2. **Comment obtenez-vous le prix applicable ?** Une version produit ne s'ouvre que si un attribut
   historisé change. La version 1 vaut depuis le 1900-01-01. La commande rejoint la version telle
   que `valid_from <= event_time < valid_to`. Les invariants SCD2 ont 0 ligne en violation.
   Source : [modèle de données](../data-model.md#products_scd2).
3. **Pourquoi recalculer tout gold à chaque exécution ?** Une date dépend des versions des
   produits et des clients, qu'un jour silver peut changer pour des dates anciennes. La revue a
   mesuré 5 202 lignes de 27 clients au pays périmé sur la première version. Source :
   [ADR-0009](../adr/0009-gold-etoile-recalcul.md#alternatives-écartées).
4. **La pseudonymisation est-elle réversible ?** La donnée est pseudonymisée, pas anonymisée :
   avec le sel, la correspondance reste calculable. Le sel passe comme une donnée, parce que la
   revue a trouvé un `lit(sel)` en clair dans `explain`. Restent en clair `user_agent`, la requête
   de `path` et le préfixe IP. Source : [ADR-0004](../adr/0004-pseudonymisation.md).
5. **Que se passe-t-il si on rejoue J2 après J3 ?** Silver refuse (`LaterPartitionError`). Avec
   `--force`, puis le rejeu de J3, le contenu redevient identique. Gold refuse aussi un silver
   incomplet (KAN-58). Source : [rapport, section 5](../rapport-technique.md#autres-preuves),
   [section 6](../rapport-technique.md#6-couche-gold).

### Data engineer streaming et performance

1. **Pourquoi un watermark de deux minutes ?** La revue a mesuré qu'avec 10 minutes, en `append`,
   aucune fenêtre n'est écrite sur 10 minutes de flux. Le contrat place les retards ordinaires
   entre 10 et 90 s et les très grands au-delà de 5 minutes. Source :
   [ADR-0008](../adr/0008-streaming.md).
2. **Comment l'écriture du flux est-elle idempotente ?** Chaque micro-batch écrit dans des
   partitions `batch_id=<n>` remplacées. Un micro-batch planifié mais non validé est rejoué avec
   les mêmes fichiers, et son écriture remplace la précédente. Source :
   [runbook streaming](../runbook/streaming.md#reprise-après-arrêt-brutal).
3. **Spark n'est pas plus rapide : pourquoi le garder ?** Pour la plomberie distribuée qu'il
   fournit et pour son CPU par ligne : MapReduce en consomme 1,7 à 2,0 fois plus sur `full`
   (inféré). Source : [ADR-0010](../adr/0010-moteur-et-reglages-spark.md),
   [rapport, section 8](../rapport-technique.md#campagne-de-mesure).
4. **Comment avez-vous diagnostiqué le déséquilibre de clé ?** Par le rapport entre la durée de la
   tâche la plus longue et la médiane de l'étape de jointure : 4,16. La partition chaude reçoit
   31,0 Mo de shuffle contre 7,4 Mo en médiane. Source :
   [performance](../performance.md#déséquilibre-de-clé-kan-45).
5. **Le cache est-il justifié ?** Sur la lignée diffusée de gold, il perd 52 % à 3 réutilisations
   et ne se distingue pas à 6. Sur une lignée avec un shuffle, il gagne 26 % dès 3. Gold relit
   `fact_sales` au moins 10 fois, au-delà du seuil d'environ 7 relectures, qui est inféré. Source :
   [performance](../performance.md#cache).

### Responsable qualité et delivery

1. **Comment le projet a-t-il été réalisé ?** Par un pilote et des agents de développement : le
   pilote conçoit, découpe, relit et fusionne, et chaque story est confiée à un agent dans sa
   branche. Chaque rôle du sujet défend les lots qui lui sont attribués. Source :
   [pilotage](../project-management.md#rôles).
2. **Comment fonctionnent des revues sans relecteur humain ?** Deux relecteurs indépendants, sur
   deux modèles, chacun dans un rôle différent de celui de l'auteur. Un constat n'est retenu que
   s'il est prouvé par une exécution. Limite : la règle écarte les faux constats, pas les angles
   morts communs aux modèles. Source : [pilotage](../project-management.md#revues),
   [rapport, section 10](../rapport-technique.md#revues-adverses).
3. **Que teste la CI, et que ne teste-t-elle pas ?** ruff, shellcheck et pytest à chaque PR, un
   cluster neuf quand la plateforme change. Pas de seuil de couverture, et les preuves sur le
   cluster sont des exécutions publiées dans les PR, pas des tests automatiques. Source :
   [rapport, section 10](../rapport-technique.md#tests-et-ci).
4. **À quoi sert la table d'audit ?** Elle garde une ligne par exécution d'un job Spark qui
   démarre : statut, compteurs, durée, application YARN. Un driver tué n'en laisse pas. Source :
   [runbook, interroger l'audit](../runbook/README.md#interroger-laudit).
5. **Que dit `fsck` pendant la panne ?** Au premier exercice, `Status: HEALTHY`, 5 568 blocs
   sous-répliqués (100 %), réplication moyenne 2.0, 0 bloc manquant. Côté erasure coding, 3
   groupes sur 4 ont perdu une unité, reconstruite par XOR à la lecture. Source :
   [rapport, section 9](../rapport-technique.md#réplication-3-contre-erasure-coding),
   [fiche d'incident](../incidents/2026-11-27-perte-datanode.md).
