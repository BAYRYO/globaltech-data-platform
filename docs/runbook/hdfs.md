# Runbook HDFS

Cette page traite les incidents des DataNodes : perte d'un nœud, DataNode de secours,
décommission et retour à l'état nominal. Les commandes se lancent depuis la racine du dépôt, sous
Git Bash ou Linux, cluster démarré ([guide de la plateforme](../platform.md)). Les commandes
d'administration HDFS demandent `HADOOP_USER_NAME=hadoop`. Le
[rapport d'incident du 27 novembre 2026](../incidents/2026-11-27-perte-datanode.md) donne les
mesures citées ici.

## État nominal

Le cluster est nominal quand :

- `hdfs dfsadmin -report` affiche `Live datanodes (3)`, aucun `Dead datanodes` et, sous
  `Replicated Blocks` et `Erasure Coded Block Groups`, des compteurs de blocs sous-répliqués,
  manquants et corrompus à 0 ;
- `hdfs fsck /` répond `The filesystem under path '/' is HEALTHY` avec
  `Under-replicated blocks: 0`, `Missing blocks: 0` et `Corrupt blocks: 0` ;
- `scripts/cluster/status.sh` affiche `HDFS  3 live / 0 dead DataNodes`.

```bash
HADOOP_USER_NAME=hadoop scripts/cluster/client.sh bash -c \
	'hdfs dfsadmin -report | sed -n "1,24p"; hdfs fsck / | tail -n 40'
```

Un DataNode retiré par décommission, `datanode4` par exemple, reste listé sous
`Dead datanodes` jusqu'au prochain redémarrage du NameNode, et `status.sh` le montre `DEAD`. Ce
n'est pas une panne : le NameNode ne lui compte plus aucune réplique. Le cluster n'a pas de liste
d'inclusion (`dfs.hosts`), seule capable de l'effacer sans redémarrage : avec les adresses IP que
Docker réattribue, une liste d'inclusion refuserait un DataNode redémarré sous une nouvelle
adresse. L'étape [5.7](#5-retour-du-datanode-réparé-puis-retrait-du-secours) redémarre le
NameNode pour l'effacer.

## Perte d'un DataNode

### Symptômes

| Où | Ce que l'on voit | Quand |
| --- | --- | --- |
| `scripts/cluster/status.sh` | `HDFS  2 live / 1 dead DataNodes` et un nœud `DEAD` autre qu'un `datanode4` retiré | 90 s après le dernier battement de cœur |
| `hdfs dfsadmin -report` | `Dead datanodes (1)`, `Under replicated blocks` égal à presque tous les blocs | idem, puis au contrôle suivant du NameNode |
| Journal du NameNode | `BLOCK* removeDeadDatanode: lost heartbeat from <ip>:9866` | au premier contrôle (toutes les 30 s) après les 90 s |
| Journaux des exécuteurs et du driver Spark | `Exception in createBlockOutputStream`, `Excluding datanode` | dès l'arrêt, une fois par nouveau bloc écrit avant que le nœud soit déclaré mort |
| `docker ps -a` | le conteneur `gtdp-datanodeN-1` `Exited`, `unhealthy` ou absent | dès l'arrêt |

Le NameNode déclare un DataNode mort après `2 × dfs.namenode.heartbeat.recheck-interval +
10 × dfs.heartbeat.interval = 2 × 30 s + 10 × 3 s = 90 s` sans battement de cœur. Il retire
ses répliques de la carte des blocs au contrôle suivant, lancé toutes les 30 s, soit entre 90 s
et 125 s après l'arrêt. Mesuré le 27 novembre 2026 : nœud mort dans le rapport entre 86 s et
91 s après son dernier battement de cœur ; répliques retirées 91,5 s après l'arrêt au premier
exercice, 99,5 s au second.

Les jobs continuent. Les DataNodes et les NodeManagers sont des conteneurs distincts : la perte
d'un DataNode ne tue aucun conteneur YARN. Un client qui lit un bloc passe à une autre réplique.
Un client qui écrit un nouveau bloc avant que le nœud soit déclaré mort reçoit un pipeline qui le
contient (3 DataNodes pour une réplication 3) : sa connexion échoue, puis il redemande le bloc
sans lui et l'écrit sur les 2 autres. Chaque échec coûte de quelques millisecondes à 65 s selon
la réponse du réseau, ce qui ralentit le job sans le faire échouer.

### 1. Mesurer l'impact

1. Lisez l'état des DataNodes et des blocs.

	```bash
	HADOOP_USER_NAME=hadoop scripts/cluster/client.sh hdfs dfsadmin -report | sed -n '1,24p'
	```

	Résultat attendu : `Missing blocks: 0` et `Missing block groups: 0`. Les blocs sous-répliqués
	ont encore 2 répliques sur 3, les groupes en erasure coding 2 unités sur 3.

2. Vérifiez qu'aucun bloc ne manque.

	```bash
	HADOOP_USER_NAME=hadoop scripts/cluster/client.sh hdfs fsck / | tail -n 40
	```

	Résultat attendu : `Status: HEALTHY`, `Missing blocks: 0`, `Corrupt blocks: 0`. Si un bloc
	manque, arrêtez-vous : plusieurs DataNodes sont touchés. Redémarrez d'abord tous les
	DataNodes arrêtés (étape 3), puis relancez `fsck`.

### 2. Trouver la cause

```bash
docker ps -a --filter name=gtdp-datanode --format '{{.Names}} {{.Status}}'
docker logs --tail 200 gtdp-datanode1-1
```

| Trace | Cause probable |
| --- | --- |
| `SHUTDOWN_MSG` après `RECEIVED SIGNAL 15: SIGTERM` | arrêt du conteneur (`docker stop`, redémarrage de Docker) |
| `JvmPauseMonitor ... pause of approximately 20000ms` | machine hôte saturée : le DataNode n'a pas pu envoyer ses battements de cœur |
| `DisallowedDatanodeException ... hostname cannot be resolved` | le DNS de Docker ne résolvait pas encore l'adresse au réenregistrement : le DataNode s'arrête, Docker le relance (`restart: unless-stopped`), le second essai passe |
| `OutOfMemoryError` | tas de 384 Mio trop petit |

### Redémarrer le nœud ou démarrer le secours

Redémarrez d'abord le DataNode perdu (étape 3). Démarrez `datanode4` (étape 4) dans deux cas :

- le DataNode n'est pas `live` 5 minutes après la commande `docker start` ;
- la cause trouvée à l'étape 2 empêche un redémarrage rapide (disque, mémoire qui manque à
  chaque démarrage, conteneur ou volume perdu).

Un DataNode qui redémarre s'enregistre 2 s après `docker start` (deux mesures le 27 novembre
2026) : 5 minutes laissent passer un réenregistrement refusé puis relancé par Docker. Tant
que le nœud ou le secours n'est pas là, les blocs n'ont que 2 répliques.

### 3. Si le DataNode peut redémarrer

1. Redémarrez-le.

	```bash
	docker start gtdp-datanode1-1
	```

2. Attendez son retour : `status.sh` le montre `live` en moins d'une minute.

	```bash
	scripts/cluster/status.sh
	```

3. Vérifiez l'état nominal ([État nominal](#état-nominal)). Le DataNode envoie son rapport de
   blocs : ses anciennes répliques comptent à nouveau. Seuls les blocs écrits pendant la panne,
   sur 2 DataNodes, reçoivent une troisième réplique copiée : en 2 min 53 s après le retour du
   nœud au second exercice.

### 4. Si le DataNode ne revient pas : démarrer le DataNode de secours

Avec 3 DataNodes et une réplication 3, les répliques perdues n'ont pas de cible. `datanode4`
(profil compose `spare`) en donne une.

1. Démarrez `datanode4`, sans toucher aux autres services.

	```bash
	docker compose --profile spare up --detach --no-deps datanode4
	```

2. Vérifiez qu'il est vivant et qu'il reçoit des blocs : `datanode4` apparaît sous
   `Live datanodes` et son `Num of Blocks` augmente.

	```bash
	HADOOP_USER_NAME=hadoop scripts/cluster/client.sh hdfs dfsadmin -report | grep -A20 'Hostname: datanode4' | grep -E 'Hostname|Num of Blocks'
	```

3. Suivez la re-réplication jusqu'à `Under replicated blocks: 0` et
   `Low redundancy block groups: 0`.

	```bash
	HADOOP_USER_NAME=hadoop scripts/cluster/client.sh hdfs dfsadmin -report | sed -n '6,18p'
	```

	Durée mesurée le 27 novembre 2026 : 69 min 50 s pour 5 571 répliques et 3 unités d'erasure
	coding, soit 1,33 bloc par seconde. Le NameNode confie 2 copies à la fois à chaque DataNode
	source (`dfs.namenode.replication.max-streams`), au plus 4 au total
	(`dfs.namenode.replication.max-streams-hard-limit`), à chaque passage de 3 s. Les groupes en
	erasure coding passent en dernier, pas en premier (voir le rapport d'incident).

### 5. Retour du DataNode réparé, puis retrait du secours

1. Redémarrez le DataNode réparé.

	```bash
	docker start gtdp-datanode1-1
	```

	Chaque bloc a alors 4 répliques. Le NameNode en supprime une par bloc. Attendez
	`Pending deletion blocks: 0` dans `dfsadmin -report` : 32 s le 27 novembre 2026. Le
	NameNode supprime la réplique du DataNode qui annonce le moins d'espace libre, pas
	forcément celle du nœud revenu.

2. Décommissionnez `datanode4` : écrivez son nom dans le fichier d'exclusion du NameNode, puis
   faites relire ce fichier.

	```bash
	echo datanode4 | docker exec -i gtdp-namenode-1 sh -c \
		'cat > /opt/hadoop/etc/hadoop/dfs.exclude.new && mv -f /opt/hadoop/etc/hadoop/dfs.exclude.new /opt/hadoop/etc/hadoop/dfs.exclude'
	HADOOP_USER_NAME=hadoop scripts/cluster/client.sh hdfs dfsadmin -refreshNodes
	```

	Résultat attendu : `Refresh nodes successful`, puis `Decommission Status : Decommission in
	progress` pour `datanode4`. Le NameNode copie ailleurs chaque bloc qui n'a pas 3 répliques
	en dehors de `datanode4`.

3. Attendez `Decommission Status : Decommissioned`. Le 27 novembre 2026 : 47 min 22 s pour
   5 574 blocs, 2 blocs par seconde. La durée dépend du nombre de blocs dont `datanode4` porte
   une réplique sans équivalent ailleurs : ce jour-là, presque tous, parce que la suppression
   des répliques en trop de l'étape 1 avait vidé `datanode2`.

	```bash
	HADOOP_USER_NAME=hadoop scripts/cluster/client.sh hdfs dfsadmin -report | grep -A2 'Hostname: datanode4'
	```

4. Arrêtez `datanode4`.

	```bash
	docker stop gtdp-datanode4-1
	```

5. Attendez que `datanode4` passe sous `Dead datanodes`, 90 s après l'arrêt, toujours
   `Decommissioned`. Le rapport limité aux nœuds morts le montre :

	```bash
	HADOOP_USER_NAME=hadoop scripts/cluster/client.sh hdfs dfsadmin -report -dead | grep -E 'Dead datanodes|Hostname|Decommission Status'
	```

	Résultat attendu : `Dead datanodes (1):`, `Hostname: datanode4`,
	`Decommission Status : Decommissioned`. Videz alors le fichier d'exclusion et faites-le relire.

	```bash
	: | docker exec -i gtdp-namenode-1 sh -c \
		'cat > /opt/hadoop/etc/hadoop/dfs.exclude.new && mv -f /opt/hadoop/etc/hadoop/dfs.exclude.new /opt/hadoop/etc/hadoop/dfs.exclude'
	HADOOP_USER_NAME=hadoop scripts/cluster/client.sh hdfs dfsadmin -refreshNodes
	```

	Le NameNode garde en mémoire l'adresse IP résolue de chaque nom du fichier jusqu'au
	`-refreshNodes` suivant. Docker redonne l'adresse d'un conteneur arrêté au prochain
	conteneur démarré : le 27 novembre 2026, `datanode4` a reçu l'adresse que `datanode1` venait
	de libérer. Un DataNode redémarré qui recevrait l'adresse de `datanode4` serait décommissionné
	sans que personne ne l'ait demandé. Ne videz pas le fichier avant que `datanode4` soit mort :
	un nœud encore vu vivant et sorti du fichier redevient une cible, et le NameNode
	compterait ses répliques à nouveau.

6. Supprimez le conteneur et le volume de `datanode4`.

	```bash
	docker compose --profile spare rm --stop --force datanode4
	docker volume rm gtdp_datanode4
	```

	Le NameNode ne lui compte plus aucune réplique, mais son disque les garde toutes : 5 574
	fichiers de blocs et 2,8 Go après l'exercice. Un secours redémarré sur ce volume enverrait un
	rapport de 5 574 blocs déjà en trop, et un nouvel exercice mesurerait ce rapport au lieu de la
	re-réplication.

7. Redémarrez le NameNode pour qu'il ne liste plus `datanode4`. Avant : aucune application YARN
   et `fsck` sain. Le NameNode est indisponible une dizaine de secondes : aucun job ne doit
   tourner.

	```bash
	HADOOP_USER_NAME=hadoop scripts/cluster/client.sh bash -c \
		'yarn application -list 2>/dev/null | head -n 1; hdfs fsck / | grep Status'
	docker restart gtdp-namenode-1
	```

	Résultat attendu avant : `Total number of applications ...:0` et `Status: HEALTHY`. Après le
	redémarrage, attendez la sortie du mode sans échec (12 s le 27 novembre 2026), puis passez à
	l'étape 8.

	```bash
	HADOOP_USER_NAME=hadoop scripts/cluster/client.sh hdfs dfsadmin -safemode wait
	```

8. Vérifiez l'état nominal ([État nominal](#état-nominal)) : `Live datanodes (3)`, aucun
   `Dead datanodes`, et `HDFS  3 live / 0 dead DataNodes` dans `status.sh`.

### Rejouer la panne

`scripts/resilience/datanode-failure.sh` enchaîne les étapes 1 à 5.6 pendant un rejeu de gold et
écrit la chronologie dans `output/resilience/<horodatage UTC>/` :

```bash
scripts/resilience/datanode-failure.sh --victim datanode1 --date 2026-01-03
```

Le script demande le cluster pour lui seul : aucune application YARN en cours, gold `PUBLISHED`,
3 DataNodes vivants, fichier d'exclusion vide. Il a duré 2 h 05 min le 27 novembre 2026 : 70 min
de re-réplication et 47 min de décommission. La dernière ligne doit être
`==> resilience scenario OK`. Il ne redémarre pas le NameNode : faites l'étape 5.7 ensuite.

Le rejeu gold du 2026-01-03 ne réécrit pas `fact_sales` quand ses lignes n'ont pas changé. Pour
écrire toutes les tables pendant la panne, `scripts/resilience/gold-write-under-failure.sh`
reconstruit gold dans une zone vide hors de `/datalake`, arrête le DataNode au premier fichier de
`fact_sales`, le redémarre à la fin du job, compare l'empreinte à `/datalake/gold` et efface la
zone (7 min 15 s le 27 novembre 2026, sans DataNode de secours).

```bash
scripts/resilience/gold-write-under-failure.sh --victim datanode1 --date 2026-01-03
```

Après un arrêt anticipé (erreur, Ctrl-C), les deux scripts redémarrent le DataNode arrêté et
arrêtent le job en cours. `datanode-failure.sh` retire aussi `datanode4` s'il est arrêté. S'il
tourne encore, il le laisse en décommission : relancez le script, il termine d'abord ce retrait,
ou faites les étapes 5.3 à 5.6. Il refuse de démarrer dans ces cas :

| Message | Correction |
| --- | --- |
| `gold is not PUBLISHED` | un rejeu arrêté a laissé gold en `PUBLISHING` : `scripts/cluster/submit.sh gold --date 2026-01-03` |
| `the exclude file names ...` | une autre décommission est en cours : terminez-la (étapes 5.3 à 5.5 pour ce nœud) |
| `YARN applications are running` | attendez la fin des jobs, ou arrêtez celui qu'un arrêt anticipé aurait laissé (`yarn application -kill <id>`) |

## Charger la configuration du NameNode

Le NameNode lit `dfs.hosts.exclude` à son démarrage seulement. Sur un cluster créé avant ce
réglage, `dfsadmin -refreshNodes` n'a aucun effet. Reconstruisez l'image et recréez les
conteneurs : les données HDFS restent dans les volumes.

```bash
scripts/cluster/up.sh --build
```

Vérifiez ensuite la valeur chargée par le NameNode, pas celle du fichier :

```bash
scripts/cluster/client.sh python3 -c 'import urllib.request
print("dfs.hosts.exclude" in urllib.request.urlopen("http://namenode:9870/conf").read().decode())'
```

Résultat attendu : `True`.

## Dépannage

| Symptôme | Cause et correction |
| --- | --- |
| `datanode4` reste `Decommission in progress`. | Il n'y a pas 3 autres DataNodes vivants pour recevoir ses répliques. Redémarrez le DataNode réparé d'abord (étape 5.1). |
| `-refreshNodes` réussit mais `datanode4` reste `Normal`. | Le NameNode a démarré sans `dfs.hosts.exclude`. Voir [Charger la configuration du NameNode](#charger-la-configuration-du-namenode). |
| Un DataNode redémarré s'arrête aussitôt avec `DisallowedDatanodeException`. | Résolution DNS pas encore prête. Docker le relance tout seul : vérifiez qu'il est `live` une minute plus tard. |
| `datanode1` disparaît de `dfsadmin -report` au démarrage de `datanode4`. | `datanode4` a reçu l'adresse IP de `datanode1`, arrêté. Le NameNode efface l'entrée morte qui portait cette adresse (`BLOCK* registerDatanode: 172.22.0.4:9866` dans son journal). `datanode1` revient comme un nouveau nœud à son redémarrage. Lisez le champ `Hostname`, pas l'adresse IP. |
| `status.sh` montre `datanode4 DEAD` sur un cluster sain. | `datanode4` a été retiré et le NameNode n'a pas redémarré depuis. Étapes 5.6 et 5.7. |
