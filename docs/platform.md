# Guide de la plateforme

Ce guide démarre et exploite le cluster du projet sur un poste de développement : HDFS
(1 NameNode, 3 DataNodes), YARN (1 ResourceManager, 2 NodeManagers), Spark History Server et
MapReduce JobHistory. Tous les services tournent dans Docker à partir d'une seule image
(voir [ADR-0001](adr/0001-image-hadoop-spark.md) et [ADR-0006](adr/0006-topologie-hdfs.md)).

## Prérequis

- Docker Engine 25 ou plus récent avec Docker Compose v2. Sous Windows et macOS, Docker
  Desktop avec au moins 14 Go de mémoire alloués (voir [Budget mémoire](#budget-mémoire)).
- Git et un shell Bash. Sous Windows, utiliser Git Bash.
- Environ 5 Go de disque libre pour l'image et les volumes HDFS.
- Ni Java, ni Hadoop, ni Spark, ni Python sur le poste. Tout s'exécute dans des conteneurs.

Le premier lancement construit l'image `gtdp/hadoop-spark:3.3.6-3.5.3` et télécharge environ
1,04 Go d'archives Apache depuis archive.apache.org : `hadoop-3.3.6.tar.gz` (730 Mo) et
`spark-3.5.3-bin-without-hadoop.tgz` (314 Mo). Ce premier build a pris 6 min 52 s sur la machine
de référence (16 CPU, 15 Gio pour Docker). La durée dépend surtout du débit d'archive.apache.org. Les builds
suivants réutilisent le cache Docker.

## Démarrage en moins de 10 minutes

Le chronométrage ci-dessous suppose l'image déjà construite.

1. Cloner le dépôt et entrer dans le dossier.

	```bash
	git clone https://github.com/BAYRYO/globaltech-data-platform.git
	cd globaltech-data-platform
	```

2. Démarrer le cluster. Le script attend que les 9 services soient sains et que HDFS accepte
   les écritures, puis affiche l'état.

	```bash
	scripts/cluster/up.sh
	```

	Vous devez voir `HDFS  3 live / 0 dead DataNodes` et `YARN  2 RUNNING / 2 NodeManagers`.

3. Créer l'arborescence HDFS, les droits, les quotas et l'archive des jars Spark.

	```bash
	scripts/cluster/init-hdfs.sh
	```

4. Vérifier le cluster de bout en bout : un job Spark sur YARN visible dans le Spark History
   Server, le job MapReduce `pi` visible dans JobHistory et un job Hadoop Streaming en Python.

	```bash
	scripts/cluster/smoke/run.sh
	```

	La dernière ligne doit être `==> smoke OK`.

Sur la machine de référence, avec des volumes vides et l'image construite, `up.sh` prend 15 s,
`init-hdfs.sh` 27 s et `smoke/run.sh` 60 s. Un deuxième `init-hdfs.sh` prend aussi 27 s et ne
change rien.

Avec `make`, les mêmes étapes sont `make cluster-up`, `make cluster-init` et `make cluster-smoke`.

## Services, ports et interfaces web

Les ports web sont publiés sur `127.0.0.1` uniquement. Les autres ports restent sur le réseau
Docker `gtdp`.

| Service compose | Rôle | Interface web | Ports internes |
| --- | --- | --- | --- |
| `namenode` | NameNode HDFS | http://localhost:9870 | 8020 (RPC), 9870 |
| `datanode1` à `datanode3` | DataNodes HDFS | non publiée | 9864, 9866, 9867 |
| `resourcemanager` | ResourceManager YARN | http://localhost:8088 | 8030 à 8033, 8088 |
| `nodemanager1`, `nodemanager2` | NodeManagers YARN | non publiée | 8042, 13562 (shuffle) |
| `spark-history` | Spark History Server | http://localhost:18080 | 18080 |
| `mr-history` | MapReduce JobHistory | http://localhost:19888 | 10020, 19888 |

Le nom du projet compose est fixé à `gtdp`. Toutes les copies et tous les worktrees du dépôt
pilotent donc le même cluster.

## Budget mémoire

Docker dispose de 15 444 Mio sur la machine de référence (`docker info`, MemTotal). Sans
réglage, chaque NodeManager Hadoop 3.3.6 annoncerait 8 192 Mio à YARN, soit 16 Gio pour les
deux avant tout démon. Chaque valeur est donc fixée explicitement.

| Service | Instances | Tas JVM | Conteneurs YARN | Plafond par instance | Total |
| --- | --- | --- | --- | --- | --- |
| `namenode` | 1 | 768 Mio | | 1 024 Mio | 1 024 Mio |
| `datanode` | 3 | 384 Mio | | 640 Mio | 1 920 Mio |
| `resourcemanager` | 1 | 768 Mio | | 1 024 Mio | 1 024 Mio |
| `nodemanager` | 2 | 512 Mio | 2 560 Mio | 3 328 Mio | 6 656 Mio |
| `mr-history` | 1 | 384 Mio | | 640 Mio | 640 Mio |
| `spark-history` | 1 | 512 Mio | | 768 Mio | 768 Mio |
| Cluster | | | | | 12 032 Mio (11,75 Gio) |
| Conteneur client (driver Spark) | 1 | 1 024 Mio | | 1 408 Mio | 13 440 Mio (13,1 Gio) |

Le plafond par instance vaut le tas plus 256 Mio de mémoire hors tas (métaspace, threads, code
JIT), plus la mémoire YARN pour un NodeManager. YARN tue un conteneur qui dépasse sa réservation
(contrôle de mémoire physique actif). `docker stats` mesure 2,6 Gio pour le cluster au repos et
4,9 Gio au pic de `smoke/run.sh`, client compris.

Tailles par défaut des applications, dans `docker/conf/spark/spark-defaults.conf` et
`docker/conf/hadoop/mapred-site.xml` :

| Application | Conteneur | Mémoire demandée | Réservation YARN |
| --- | --- | --- | --- |
| Spark | driver (mode client, hors YARN) | 1 Gio | |
| Spark | ApplicationMaster | 512 Mio + 384 Mio | 1 024 Mio |
| Spark | 2 exécuteurs, 2 cœurs chacun | 1 Gio + 384 Mio | 1 536 Mio chacun |
| MapReduce | ApplicationMaster | 768 Mio | 768 Mio |
| MapReduce | map ou reduce | 512 Mio | 512 Mio |

YARN arrondit chaque demande au multiple de 256 Mio supérieur. Une application Spark par défaut
réserve 4 096 Mio sur les 5 120 Mio du cluster. Pour faire tourner deux applications Spark en
même temps (le streaming et un batch, par exemple), réduire `spark.executor.instances` à 1 pour
chacune.

Pour changer la mémoire des NodeManagers, copier `.env.example` en `.env` et modifier
`YARN_NM_MEMORY_MB` et `YARN_NM_VCORES`, puis relancer `scripts/cluster/up.sh`. La CI utilise
2 048 Mio par NodeManager et des exécuteurs de 512 Mio.

## Exploitation au quotidien

Tous les scripts se lancent depuis la racine du dépôt, sous Linux comme sous Git Bash.

| Commande | Effet |
| --- | --- |
| `scripts/cluster/up.sh [options compose]` | Démarre le cluster et attend qu'il soit sain. `--build` reconstruit l'image après une modification de `docker/`. |
| `scripts/cluster/down.sh` | Arrête et supprime les conteneurs. Les données HDFS restent dans les volumes. |
| `scripts/cluster/reset.sh` | Supprime les conteneurs et les volumes, donc toutes les données HDFS. Affiche les volumes supprimés, sans demander de confirmation. |
| `scripts/cluster/status.sh` | Santé des services, DataNodes vivants et morts, NodeManagers, adresses web. |
| `scripts/cluster/init-hdfs.sh` | Crée l'arborescence, les propriétaires, les modes, les ACL, les quotas, l'erasure coding et l'archive Spark. Idempotent. |
| `scripts/cluster/client.sh [commande]` | Lance une commande dans un conteneur client éphémère. Sans commande, ouvre un shell. |
| `scripts/cluster/land.sh <dossier_dépôt> <date>` | Copie un dépôt du générateur dans la zone landing. |
| `scripts/cluster/submit.sh <job ou script.py> [args]` | Soumet une application PySpark à YARN. |
| `scripts/cluster/smoke/run.sh` | Vérification de bout en bout (Spark, MapReduce, Hadoop Streaming). |
| `scripts/cluster/smoke/hdfs-state.sh` | Affiche l'état géré par `init-hdfs.sh`, sans horodatage, pour comparer deux exécutions. |

### Conteneur client

`scripts/cluster/client.sh` lance l'image du cluster sur le réseau `gtdp`. Le conteneur monte la
copie du dépôt qui contient le script, quel que soit le dossier courant, dans `/opt/project`.
C'est aussi le dossier de travail. Il reçoit :

- `HADOOP_USER_NAME`, qui vaut `etl` par défaut ;
- `GTDP_CONF_DIR=/opt/project/conf` ;
- `PYTHONPATH=/opt/project/src` ;
- `GTDP_GIT_SHA`, le commit de la copie (`git rev-parse --short HEAD`) ;
- les variables du fichier `.env` s'il existe, au format `CLÉ=valeur` sans guillemets ;
- toutes les variables `GTDP_*` exportées dans le shell, qui l'emportent sur `.env`.

```bash
scripts/cluster/client.sh hdfs dfs -ls /datalake
HADOOP_USER_NAME=hadoop scripts/cluster/client.sh hdfs dfsadmin -report
scripts/cluster/client.sh yarn application -list -appStates ALL
scripts/cluster/client.sh                      # shell interactif
```

L'utilisateur `hadoop` est le super-utilisateur HDFS. L'authentification est simple. HDFS fait
confiance à `HADOOP_USER_NAME`. Les droits protègent contre les erreurs, pas contre un attaquant.

### Déposer un jour de données

```bash
scripts/cluster/land.sh data/generated/dev/landing/2026-01-01 2026-01-01
```

Le dossier de dépôt doit se trouver dans la copie du dépôt, car seul ce dossier est monté dans le
conteneur client. Pour chaque dossier source présent (`orders`, `customers`, `products`,
`weblogs`), le script remplace `/datalake/landing/<source>/ingestion_date=<date>/`.
`_MANIFEST.json` remplace `/datalake/landing/_manifests/ingestion_date=<date>/`. Les fichiers
passent d'abord par `/datalake/landing/_staging/`, puis un renommage les met en place. Relancer
la commande donne le même résultat, et un fichier retiré du dépôt disparaît de HDFS.

### Le secret de pseudonymisation

Les jobs `--env cluster` lisent le sel dans `GTDP_PSEUDO_SALT` et s'arrêtent avec le code 2 sans
lui ([ADR-0004](adr/0004-pseudonymisation.md)). Le mettre dans `.env`, ignoré par Git, ou
l'exporter dans le shell. Ne jamais le commiter.

```bash
echo 'GTDP_PSEUDO_SALT=<valeur fournie hors dépôt>' >> .env
```

## Soumettre un job

```bash
scripts/cluster/submit.sh bronze --date 2026-01-01
scripts/cluster/submit.sh scripts/cluster/smoke/spark_smoke.py
```

`submit.sh` construit `dist/gtdp.zip` à partir de `src/gtdp` (le paquet `gtdp/` à la racine de
l'archive), puis lance `spark-submit --master yarn --py-files dist/gtdp.zip` dans un conteneur
client.

- Un argument qui finit par `.py` est un script, soumis tel quel. Son chemin part de la racine du
  dépôt.
- Tout autre argument est un job enregistré. Le script exécute
  `src/gtdp/jobs/run.py <job> --env cluster [args]`.

| Variable | Effet |
| --- | --- |
| `GTDP_DEPLOY_MODE` | `client` par défaut, avec le driver dans le conteneur client. `cluster` place le driver dans un conteneur YARN. |
| `GTDP_SPARK_SUBMIT_OPTS` | Options `spark-submit` ajoutées avant l'application, séparées par des espaces. Une valeur ne peut pas contenir d'espace. |
| `HADOOP_USER_NAME` | Utilisateur qui soumet, `etl` par défaut. |

```bash
GTDP_SPARK_SUBMIT_OPTS="--conf spark.executor.instances=1 --conf spark.sql.shuffle.partitions=16" \
	scripts/cluster/submit.sh silver --date 2026-01-01
GTDP_DEPLOY_MODE=cluster scripts/cluster/submit.sh gold --date 2026-01-01
```

En mode `cluster`, le driver n'a pas accès à la copie du dépôt. `submit.sh` envoie donc `conf/`
sous forme d'archive (`--archives dist/conf.zip#conf`) et transmet chaque variable `GTDP_*` au
driver par `spark.yarn.appMasterEnv.*`. `spark.redaction.regex` masque le sel dans les
interfaces et les event logs.

Suivre et diagnostiquer une application :

```bash
scripts/cluster/client.sh yarn application -status <application_id>
scripts/cluster/client.sh yarn logs -applicationId <application_id> -log_files stdout,stderr
```

Les journaux des conteneurs sont agrégés dans `/app-logs` quelques secondes après la fin de
l'application. Les applications Spark terminées apparaissent dans le Spark History Server, qui
relit `hdfs:///spark-logs` toutes les 10 secondes.

### MapReduce et Hadoop Streaming

Le jar d'exemples est
`/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.6.jar`. Le jar Hadoop Streaming
est `/opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar`, aussi exposé par la variable
`HADOOP_STREAMING_JAR` dans l'image. Les NodeManagers exécutent `python3` (Python 3.11).

```bash
scripts/cluster/client.sh bash -c 'hadoop jar "$HADOOP_STREAMING_JAR" \
	-files scripts/cluster/smoke/wordcount_mapper.py,scripts/cluster/smoke/wordcount_reducer.py \
	-mapper "python3 wordcount_mapper.py" -reducer "python3 wordcount_reducer.py" \
	-input /user/etl/smoke/streaming/input.txt -output /user/etl/smoke/streaming/out2'
```

## Arborescence HDFS, droits et quotas

`scripts/cluster/init-hdfs.sh` porte la table qui fait foi. Le tableau ci-dessous la reprend.

| Chemin | Propriétaire | Mode | ACL | Quota d'espace |
| --- | --- | --- | --- | --- |
| `/datalake` | `etl:datalake` | `755` | | |
| `/datalake/landing` et ses sous-dossiers | `etl:datalake` | `770` | | 20 Go |
| `/datalake/bronze` | `etl:datalake` | `750` | | 30 Go |
| `/datalake/silver` | `etl:datalake` | `750` | `group:analysts:r-x`, par défaut aussi | 30 Go |
| `/datalake/silver/rejects` | `etl:datalake` | `750` | `group:analysts:---`, par défaut aussi | |
| `/datalake/gold` | `etl:datalake` | `755` | `group:analysts:r-x`, par défaut aussi | 10 Go |
| `/datalake/streaming`, `checkpoints/`, `rejects/` | `etl:datalake` | `750` | | 5 Go |
| `/datalake/ops` | `etl:datalake` | `750` | `group:ops:r-x`, par défaut aussi | 2 Go |
| `/spark-logs` | `spark:hadoop` | `1777` | | 5 Go |
| `/app-logs`, `/tmp` | `hadoop:hadoop` | `1777` | | |
| `/mr-history`, `/user`, `/apps`, `/apps/spark` | `hadoop:hadoop` | `755` | | |
| `/user/etl` | `etl:datalake` | `755` | | |
| `/benchmarks` | `etl:datalake` | `755` | | 20 Go |
| `/benchmarks/rep3` | `etl:datalake` | `755` | réplication forcée | |
| `/benchmarks/ec` | `etl:datalake` | `755` | erasure coding `XOR-2-1-1024k` | |

Les sous-dossiers de landing sont `orders`, `customers`, `products`, `weblogs`, `_manifests` et
`sensors/incoming`. Les sorties du streaming (`/datalake/gold/sensor_aggregates`,
`/datalake/gold/sensor_alerts`) sont créées par le job lui-même.

Choix de droits :

- **Silver.** L'ACL par défaut de `/datalake/silver` donne la lecture aux analystes sur chaque
  entité créée ensuite, même quand un job réécrit le dossier d'une entité. `rejects/` contient
  les lignes brutes rejetées, avec emails et adresses IP en clair. Une entrée explicite
  `group:analysts:---` l'exclut, et son ACL par défaut l'étend aux partitions. Un job ne doit
  jamais supprimer `/datalake/silver/rejects` lui-même.
- **Gold et ops.** L'ACL par défaut fait hériter la lecture aux tables et aux fichiers d'audit
  créés plus tard.
- **Quotas.** Un quota d'espace HDFS compte les octets répliqués. 30 Go en réplication 3 font
  donc 10 Go de données.
- **Utilisateurs.** Les comptes `hadoop`, `etl` (groupe `datalake`), `analyst` (groupe
  `analysts`) et `spark` (groupe `hadoop`), ainsi que le groupe `ops`, existent dans l'image. Le
  NameNode résout les groupes avec `id` (`ShellBasedUnixGroupsMapping`).

Vérifier les droits :

```bash
HADOOP_USER_NAME=hadoop scripts/cluster/client.sh hdfs dfs -getfacl /datalake/gold
HADOOP_USER_NAME=analyst scripts/cluster/client.sh hdfs dfs -put README.md /datalake/bronze/
# put: Permission denied: user=analyst, access=EXECUTE, inode="/datalake/bronze":etl:datalake:drwxr-x---
```

### Erasure coding

`/benchmarks/ec` utilise `XOR-2-1-1024k`. Chaque groupe de blocs a 2 cellules de données et
1 cellule de parité, sur 3 DataNodes distincts. Un fichier de 5 000 000 octets y occupe 7 902 848 octets, contre
15 000 000 dans `/benchmarks/rep3`. `hdfs ec` affiche un avertissement, car le cluster n'a qu'un
rack et la politique en demande 3 pour tolérer la perte d'un rack. La perte d'un DataNode reste
tolérée. `hdfs fsck` montre les 3 cellules sur 3 DataNodes différents.

`hdfs ec -getPolicy -path /benchmarks/rep3` répond `unspecified`. La politique de réplication
posée par `-replicate` n'a pas de nom d'erasure coding.

## Panne d'un DataNode

Le NameNode déclare un DataNode mort après
`2 × dfs.namenode.heartbeat.recheck-interval + 10 × dfs.heartbeat.interval`, soit
`2 × 30 s + 10 × 3 s = 90 s` (630 s avec les valeurs Hadoop par défaut). Les deux réglages sont
dans `docker/conf/hadoop/hdfs-site.xml`.

```bash
scripts/cluster/client.sh hdfs fsck / -files -blocks -locations
docker compose stop datanode2          # simule la panne
scripts/cluster/status.sh              # datanode2 passe DEAD après 90 s environ
docker compose start datanode2
```

Avec 3 DataNodes et une réplication 3, la perte d'un nœud laisse tous les blocs sous-répliqués
jusqu'à son retour. Une écriture en cours continue sur les 2 DataNodes restants
(`dfs.client.block.write.replace-datanode-on-failure.best-effort`).

## Dépannage

Les problèmes ci-dessous ont été rencontrés pendant la mise au point.

| Symptôme | Cause et correction |
| --- | --- |
| `spark-submit` n'affiche aucun journal, même pas l'identifiant d'application. | Spark sans Hadoop journalise par le slf4j 1.7 de Hadoop et le pont log4j 1 vers un log4j 2 non configuré, qui ne garde que les erreurs. `docker/conf/spark/log4j2.properties` configure log4j 2. Si vous remplacez `SPARK_CONF_DIR`, copiez ce fichier. |
| `load-spark-env.sh: line 68: ps: command not found` | Les scripts de Spark appellent `ps`. L'image installe `procps`. Reconstruire l'image avec `scripts/cluster/up.sh --build`. |
| En mode cluster, l'application échoue au démarrage et le NodeManager journalise `conf.zip, ARCHIVE ... No such file or directory`. | `conf/` était vide, donc `dist/conf.zip` aussi, et YARN ne sait pas extraire une archive vide. Le mode cluster suppose un `conf/` non vide. |
| `yarn logs` répond `Can not find any log file` pour une application dont le premier conteneur n'a jamais démarré. | Aucun journal à agréger. Lire le diagnostic avec `yarn application -status <id>`. |
| `hdfs ec` affiche `3 racks are required for the erasure coding policies: XOR-2-1-1024k`. | Avertissement attendu sur un cluster à un rack. Voir [Erasure coding](#erasure-coding). |
| `scripts/cluster/client.sh` répond `network 'gtdp' not found`. | Le cluster n'est pas démarré. Lancer `scripts/cluster/up.sh`. |
| Un job échoue avec `Permission denied: user=etl, access=WRITE, inode="/":hadoop:supergroup`. | `/user/etl` n'existe pas encore, donc Spark ne peut pas créer son dossier de staging. Lancer `scripts/cluster/init-hdfs.sh`. |

Commandes utiles :

```bash
docker compose ps                      # état et santé des services
docker compose logs --tail 100 namenode
docker inspect --format '{{json .State.Health}}' gtdp-namenode-1
```

Sous Git Bash dans la fenêtre mintty par défaut, `docker run -t` exige `winpty`.
`client.sh` l'ajoute tout seul quand un terminal interactif est détecté.

## Notes de conception

- **Une image, un rôle par service.** `gtdp-entrypoint <rôle>` lance le démon au premier plan
  (`namenode`, `datanode`, `resourcemanager`, `nodemanager`, `mr-history`, `spark-history`).
  Toute autre commande s'exécute telle quelle, ce qui fait de la même image le conteneur client.
  Le NameNode formate son volume au premier démarrage avec le cluster id `gtdp`.
- **Santé réelle.** Un DataNode n'est sain qu'une fois enregistré auprès du NameNode, un
  NodeManager qu'une fois `RUNNING` dans le ResourceManager (`docker/bin/healthcheck.py`). Les
  dépendances `service_healthy` démarrent donc chaque étage sur un étage complet. `up.sh` attend
  ensuite la sortie du safe mode.
- **Spark sans Hadoop.** Spark utilise les jars Hadoop 3.3.6 du cluster
  (`SPARK_DIST_CLASSPATH=$(hadoop classpath)`), donc une seule version de Hadoop partout.
  `init-hdfs.sh` téléverse une fois les jars Spark dans `/apps/spark/spark-jars-3.5.3.zip`
  (`spark.yarn.archive`). Le fichier est lisible par tous, donc chaque NodeManager le met en
  cache public une seule fois au lieu de recevoir 230 Mo par application.
- **Scripts portables.** Chaque script qui parle à HDFS ou YARN se relance lui-même dans un
  conteneur client. Le même code tourne sous Linux, sous Git Bash et dans l'image, sans Java
  sur le poste. `MSYS_NO_PATHCONV=1` et `pwd -W` donnent à Docker Desktop des chemins Windows.
- **Configuration par environnement.** `yarn-site.xml` lit la mémoire et les vcores des
  NodeManagers avec `${env.YARN_NM_MEMORY_MB:-2560}`, que compose remplit depuis `.env`.
- **History Server.** Le Spark History Server tourne sous l'utilisateur `spark`, propriétaire de
  `/spark-logs`. Il lit les event logs des autres utilisateurs par le groupe `hadoop`. Il crée
  `/spark-logs` à son démarrage s'il manque, car il refuse de démarrer sans ce dossier.
- **Ordonnanceur.** `capacity-scheduler.xml` porte `maximum-am-resource-percent` à 0,5. Avec la
  valeur par défaut de 0,1, une seule application tournerait à la fois sur ce petit cluster.
- **Staging MapReduce.** `yarn.app.mapreduce.am.staging-dir=/user` place le staging de chaque
  utilisateur dans son dossier personnel. Un dossier partagé sous `/tmp` appartiendrait au
  premier utilisateur qui soumet.
