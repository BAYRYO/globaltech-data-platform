# Démonstration de la soutenance

La démonstration montre, sur le vrai cluster, les preuves que le [support](index.html) annonce :
le cluster sain, gold publiée et réconciliée, puis le flux capteurs qui reprend sans doublon après
un arrêt brutal de son driver. Elle dure entre 3 et 4 minutes. Un seul script la déroule,
[`scripts/demo/soutenance.sh`](../../scripts/demo/soutenance.sh), et le texte de chaque rôle est
dans le [script](script.md#12-démonstration).

## Règles

- La démonstration lit `/datalake` sans rien y écrire. Elle écrit sous `/user/etl/soutenance`,
  que `clean` supprime à la fin, et chaque application Spark laisse son journal d'événements
  dans `/spark-logs`, comme tout job du cluster. Le script n'accepte qu'une racine sous
  `/user/<utilisateur>/`, sans `.` ni `..` : `clean` la supprime sans corbeille.
- Elle n'affiche ni `.env` ni l'environnement, et le sel de pseudonymisation n'apparaît nulle part :
  les jobs du flux et les rapports n'en ont pas besoin
  ([runbook du flux](../runbook/streaming.md#démarrer-le-job)).
- Elle ne relance ni n'arrête aucun service du cluster. Le seul conteneur tué est le client qui
  porte le driver du flux, par `scripts/streaming/demo.sh` lui-même.

## Ce qui est en direct, ce qui est préparé

| Étape | Rôle | Contenu | En direct ou préparé | Durée mesurée |
| ---: | --- | --- | --- | ---: |
| 1 | Data engineer streaming et performance | flux capteurs lancé en arrière-plan, driver tué au micro-batch 2 puis relancé | en direct | tourne pendant les étapes 2 à 4 ; l'étape 5 attend sa fin |
| 2 | Architecte plateforme | santé du cluster | en direct | 1,4 s |
| 3 | Data engineer batch | état de gold, réconciliation silver tirée de la table d'audit | en direct | 14,1 s |
| 4 | Data engineer batch | rapport gold : gold contre silver recalculé, prix applicable de `P00042` | en direct | 109,0 s, dont 88,8 s de calcul et deux pauses de 10 s |
| 5 | Data engineer streaming et performance | fin du flux, comparaison avec l'exécution de référence | en direct ; la référence est préparée avant | 100,2 s |
| 6 | Architecte plateforme | applications YARN de la démonstration | en direct | 2,0 s |
| | | **Total de l'exécution enregistrée** | | **227,0 s (3 min 47 s)** |

L'exécution de référence, le même flux sans arrêt, est préparée avant la soutenance : la
recalculer en direct doublerait la durée du flux.

## Avant la soutenance

1. Vérifier le cluster : `scripts/cluster/status.sh` doit montrer 9 services sains, 3 DataNodes
   vivants et 2 NodeManagers ([santé du cluster](../runbook/exploitation.md#vérifier-la-santé)). Gold
   doit être publiée pour J1 à J3 ([runbook gold](../runbook/gold.md)).
2. Avoir la journée de capteurs `dev` sous `data/generated/dev/sensors/2026-01-01`, sinon
   `python -m gtdp.datagen generate --profile dev`.
3. Préparer la référence :

   ```bash
   bash scripts/demo/soutenance.sh prepare
   ```

   La commande copie les 30 premiers fichiers de la journée, soit 150 s de mesures, sous
   `data/generated/soutenance/sensors`, rejoue ce flux sans arrêt sous
   `/user/etl/soutenance/ref`, puis vérifie qu'il a écrit des agrégats. Le rapport de
   l'exécution enregistrée montre cette référence : 23 lignes d'agrégats, 14 rejets
   (`MALFORMED_JSON` 4, `NULL_VALUE` 10), aucune alerte.
4. Répéter une fois `bash scripts/demo/soutenance.sh live`. Chaque exécution écrit sous une racine
   neuve, `/user/etl/soutenance/crash-<heure>`, et le rapport ignore l'historique de la référence :
   une répétition ne pollue pas la suivante.

## Déroulé

```bash
bash scripts/demo/soutenance.sh live
```

Le script annonce chaque étape par une ligne `== N/6`, affiche chaque commande précédée de `$`,
puis sa sortie. Les journaux complets vont dans le dossier `logs:` affiché en premier.

- Le rapport gold s'affiche en deux temps, chacun seul à l'écran pendant 10 s
  (`GTDP_DEMO_PAUSE_S`) : d'abord gold contre silver et la ligne finale, puis le prix applicable
  de `P00042`. Le flux tourne pendant ces pauses : elles n'allongent pas la démonstration.
- Une étape qui échoue affiche son code de sortie, et la démonstration continue : les secours
  ci-dessous partent de là. L'étape 6 ne liste que les applications de cette exécution, relevées
  dans ses journaux.
- Si la démonstration s'arrête avant la fin (Ctrl+C, erreur), le script arrête le flux en
  arrière-plan : `demo.sh`, puis les conteneurs clients démarrés depuis le début de `live`.

Sorties attendues, relevées dans l'exécution enregistrée le 27 novembre 2026 à 21 h 25 :

| Étape | Ce que l'écran doit montrer |
| ---: | --- |
| 2 | 9 services `(healthy)` ; `HDFS  3 live / 0 dead DataNodes, safe mode off` ; `YARN  2 RUNNING / 2 NodeManagers` |
| 3 | `_state.json` : `"status": "PUBLISHED"` et `"silver_days": ["2026-01-01", "2026-01-02", "2026-01-03"]` ; la requête d'audit : trois jours, `adds_up` à `true`, J1 `1106403` = `1070797` + `25518` + `10088` |
| 4 | `orders` `970262` des deux côtés, les 8 contrôles `ok`, les deux versions de `P00042` (504.06, puis 530.97 à partir du 2025-09-25 08:19:03), dernière ligne `gold PUBLISHED, 0 order dates mismatched, 0 checks failed` |
| 5 | `killed the driver container (...) at aggregates batch 2`, puis `last planned batch 2, last committed batch 1` pour les trois requêtes ; `job 2 started` ; `job stopped: "stop_reason": "idle"`. Le rapport : `duplicate keys 0` partout, trois lignes `row difference 0`, `IDENTICAL` ; la ligne d'audit du job 2 (`SUCCESS`, `in 1688`) et le job 1 listé sans ligne d'audit (`batches 2 in 258`) : 258 + 1 688 = 1 946 événements lus, autant que les 1 946 publiés |
| 6 | les quatre applications de cette exécution, `FINISHED SUCCEEDED` : deux `gtdp-streaming-sensors`, `gtdp-gold-report`, `gtdp-streaming-report` |

Le rapport gold tourne à côté du flux, avec un seul exécuteur (voir les écarts plus bas).

## Après la soutenance

```bash
bash scripts/demo/soutenance.sh clean
```

La commande supprime `/user/etl/soutenance`, références et exécutions comprises. Les fichiers
locaux de `data/generated/soutenance` sont ignorés par Git.

## Vidéo de secours

La vidéo [soutenance-demo.mp4](demo/soutenance-demo.mp4) montre l'exécution du
27 novembre 2026, enregistrée sur le cluster au format asciicast v2 dans
[soutenance-demo.cast](demo/soutenance-demo.cast). L'enregistreur,
[`scripts/demo/cast.py`](../../scripts/demo/cast.py), ne capte que ce que le script écrit sur sa
sortie standard et sa sortie d'erreur : ni le bureau, ni une autre fenêtre.

Pour enregistrer à nouveau, sous Windows avec Git Bash :

```bash
TMPDIR=/tmp python scripts/demo/cast.py record docs/soutenance/demo/soutenance-demo.cast \
  --title "Démonstration de la soutenance, enregistrée sur le cluster" \
  -- "$(cygpath -w "$(which bash)")" scripts/demo/soutenance.sh live
```

Pour produire la vidéo, avec ffmpeg :

```bash
python scripts/demo/cast.py render docs/soutenance/demo/soutenance-demo.cast \
  docs/soutenance/demo/soutenance-demo.mp4 --max-idle 20 --ffmpeg "$(command -v ffmpeg)"
```

Le rendu raccourcit à 20 s chaque attente d'au moins 21 s et l'annonce à l'écran
(« attente de N s raccourcie à 20 s »). Le bas de l'image affiche, seconde par seconde, la durée
réelle écoulée à côté du temps de la vidéo ; pendant une attente raccourcie, la durée réelle
avance plus vite. Trois attentes sont raccourcies : le calcul du rapport gold (89 s), la fin du
flux après sa dernière publication (60 s) et le rapport du flux (26 s). Les 227,0 s de
l'exécution donnent 115,3 s de vidéo, en 1280×720, pour 1,1 Mo. Le tableau des contrôles gold
reste seul à l'écran 10 s. 20 s laissent le temps de dire sur la vidéo le texte de la
démonstration, 105 s dans la [répétition](repetition.md).

## Écarts avec le job configuré

- Le déclenchement passe de 30 s à 10 s (`GTDP_STREAMING__TRIGGER_INTERVAL_S=10`) pour que le flux
  finisse pendant la démonstration. `demo.sh` arrête le job après 4 déclenchements sans données :
  40 s, plus que le temps de publier le premier fichier. Le reste est le job configuré.
- Toutes les soumissions de la démonstration prennent un seul exécuteur
  (`spark.executor.instances=1`), comme le flux : le rapport gold tourne à côté du flux.
- 30 fichiers sur 180 : en mode `append`, une fenêtre ne sort qu'une fois dépassée par le
  watermark, si bien que seules les deux premières fenêtres sortent, 23:58 et 23:59 de la veille,
  faites d'événements en retard. La comparaison porte donc sur un petit échantillon : 23 lignes
  d'agrégats tirées de 29 événements sur les 1 946 lus, 14 rejets et aucune alerte. Elle montre
  la reprise sans doublon, pas les agrégats d'une journée. Le test de reprise sur la journée
  entière, alertes comprises, est celui du
  [runbook du flux](../runbook/streaming.md#reprise-après-arrêt-brutal).
- Le rejeu publie un fichier toutes les 3 à 4 s au lieu de 1 s : chaque publication lance la JVM
  du client `hdfs`.

## Secours, étape par étape

| Étape | Ce qui peut échouer | Secours |
| ---: | --- | --- |
| 1 | `demo.sh` refuse de démarrer, par exemple sans fichiers préparés | lancer `prepare`, ou passer à la vidéo à l'étape 5 |
| 2 | un service n'est pas sain | le dire, ne pas relancer le cluster pendant la soutenance, montrer la vidéo |
| 3 | `pyspark` local échoue dans le client | montrer les mêmes chiffres dans le [rapport technique](../rapport-technique.md#5-couche-silver) et sur la diapositive 5 |
| 4 | YARN n'a pas la place de lancer le rapport | attendre la fin du flux et relancer `scripts/cluster/submit.sh src/gtdp/gold/report.py`, sinon la vidéo |
| 5 | la comparaison donne `DIFFERENT` | le dire tel quel, montrer la vidéo et le test sur la journée entière du runbook ; ne pas masquer l'écart |
| 6 | la liste YARN échoue | l'interface du ResourceManager, `http://localhost:8088` |
