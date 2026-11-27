# Runbook HDFS

Cette page traite les incidents des DataNodes : perte d'un nœud, DataNode de secours,
décommission et retour à l'état nominal. Les commandes se lancent depuis la racine du dépôt, sous
Git Bash ou Linux, cluster démarré ([guide de la plateforme](../platform.md)). Les commandes
d'administration HDFS demandent `HADOOP_USER_NAME=hadoop`. Le
[rapport d'incident du 27 novembre 2026](../incidents/2026-11-27-perte-datanode.md) donne les
mesures citées ici.

## État nominal

Le cluster est nominal quand :

- `hdfs dfsadmin -report` affiche `Live datanodes (3)` et, sous `Replicated Blocks` et
  `Erasure Coded Block Groups`, des compteurs de blocs sous-répliqués, manquants et corrompus à 0 ;
- `hdfs fsck /` répond `The filesystem under path '/' is HEALTHY` avec
  `Under-replicated blocks: 0`, `Missing blocks: 0` et `Corrupt blocks: 0`.

```bash
HADOOP_USER_NAME=hadoop scripts/cluster/client.sh bash -c \
	'hdfs dfsadmin -report | sed -n "1,24p"; hdfs fsck / | tail -n 40'
```

Un DataNode retiré par décommission, `datanode4` par exemple, reste listé sous
`Dead datanodes` jusqu'au prochain redémarrage du NameNode. Ce n'est pas une panne : il ne porte
plus aucune réplique. Le cluster n'a pas de liste d'inclusion (`dfs.hosts`), seule capable de
l'effacer sans redémarrage : avec les adresses IP que Docker réattribue, une liste d'inclusion
refuserait un DataNode redémarré sous une nouvelle adresse.

## Perte d'un DataNode

### Symptômes

| Où | Ce que l'on voit | Quand |
| --- | --- | --- |
| `scripts/cluster/status.sh` | `HDFS  2 live / 1 dead DataNodes` et le nœud `DEAD` | 90 s après le dernier battement de cœur |
| `hdfs dfsadmin -report` | `Dead datanodes (1)`, `Under replicated blocks` égal à presque tous les blocs | idem, puis au contrôle suivant du NameNode |
| Journal du NameNode | `BLOCK* removeDeadDatanode: lost heartbeat from <ip>:9866` | au premier contrôle (toutes les 30 s) après les 90 s |
| Journaux des exécuteurs Spark | `Exception in createBlockOutputStream`, `Excluding datanode` | dès l'arrêt, pour les écritures en cours |
| `docker ps -a` | le conteneur `gtdp-datanodeN-1` `Exited`, `unhealthy` ou absent | dès l'arrêt |

Le NameNode déclare un DataNode mort après `2 × dfs.namenode.heartbeat.recheck-interval +
10 × dfs.heartbeat.interval = 2 × 30 s + 10 × 3 s = 90 s` sans battement de cœur. Il retire
ses répliques de la carte des blocs au contrôle suivant, lancé toutes les 30 s, soit entre 90 s
et 125 s après l'arrêt. Mesuré le 27 novembre 2026 : nœud mort dans le rapport entre 86 s et
91 s après son dernier battement de cœur, répliques retirées à 94 s.

Les jobs continuent. Les DataNodes et les NodeManagers sont des conteneurs distincts : la perte
d'un DataNode ne tue aucun conteneur YARN. Un client qui lit un bloc passe à une autre réplique,
un client qui écrit continue sur les DataNodes restants.

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
   sur 2 DataNodes, reçoivent une troisième réplique copiée.

### 4. Si le DataNode ne revient pas vite : démarrer le DataNode de secours

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

	Durée mesurée le 27 novembre 2026 : 69 min 50 s pour 5 552 blocs, soit 1,33 bloc par
	seconde. La limite est `dfs.namenode.replication.max-streams` : 2 copies en cours par
	DataNode source, renouvelées à chaque battement de cœur de 3 s. Les groupes en erasure
	coding passent en dernier, pas en premier (voir le rapport d'incident).

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
   `Decommissioned`. Videz alors le fichier d'exclusion et faites-le relire.

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

6. Vérifiez l'état nominal ([État nominal](#état-nominal)). `datanode4` reste listé sous
   `Dead datanodes`, avec `Decommission Status : Normal`, jusqu'au prochain redémarrage du
   NameNode. Il ne porte plus aucune réplique : ce n'est pas une panne.

### Rejouer la panne

`scripts/resilience/datanode-failure.sh` enchaîne les étapes 1 à 5 pendant un rejeu de gold et
écrit la chronologie dans `output/resilience/<horodatage UTC>/` :

```bash
scripts/resilience/datanode-failure.sh --victim datanode1 --date 2026-01-03
```

Le script demande le cluster pour lui seul : aucune application YARN en cours, gold `PUBLISHED`,
3 DataNodes vivants. Il a duré 2 h 05 min le 27 novembre 2026 : 70 min de re-réplication et 47 min de
décommission. La dernière
ligne doit être `==> resilience scenario OK`.

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
