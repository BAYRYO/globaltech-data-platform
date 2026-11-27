# ADR-0012 : bronze vérifie chaque fichier contre son manifeste avant toute écriture

**Statut** : acceptée

Cet ADR consigne après coup une décision prise pendant le Lot 2 (KAN-24 et KAN-25, #7). Elle
était décrite dans le [runbook bronze](../runbook/bronze.md#ce-que-fait-une-exécution) et le
[dictionnaire bronze](../data-dictionary/bronze.md), sans ADR.
[ADR-0002](0002-parquet-idempotence.md) garde l'écriture, le verrou et l'audit,
[ADR-0007](0007-evolution-schema.md) la lecture de chaque fichier par son propre en-tête.

## Contexte

Bronze garde les données brutes sans perte. Le générateur écrit avec chaque dépôt un manifeste
qui donne, pour chaque fichier, son SHA-256, son nombre de lignes et les anomalies injectées
([contrat de données](../data-contract.md)). Un fichier tronqué, remplacé ou mal encodé ne doit
pas entrer dans le lac sans signal : les comptes de silver se comparent au manifeste.

## Décision

- Bronze lit le manifeste du jour, que `land.sh` écrit en dernier. Sans manifeste, le jour n'est
  pas entièrement déposé et le job échoue.
- La partition landing de chaque source contient exactement les fichiers que le manifeste liste.
- Pour chaque fichier, le driver calcule le SHA-256, vérifie l'UTF-8 strict et compte les lignes
  de données. Le nombre de lignes du manifeste, celui du fichier et celui que lit le lecteur CSV
  doivent être égaux. L'en-tête nomme toutes les colonnes obligatoires de sa source.
- Au premier écart, le job échoue avant toute écriture (`LandingError`, `ReconciliationError`).
- Après l'écriture, il relit chaque partition et compare les lignes de chaque fichier aux lignes
  du landing.
- Chaque ligne reçoit `_ingested_at`, `_source_file`, `_run_id`, `_record_hash` (SHA-256 de la
  ligne brute) et `_corrupt_record`.

## Preuves qui ont justifié ces choix

Sur le cluster, J1, J2, J3 puis le rejeu de J2 : `manifest_rows` = `landing_lines` =
`rows_read` = `rows_written` pour les 16 fichiers des 4 exécutions, et les 12 `hash_sum` du rejeu
égaux à ceux du premier tour. Le rapport bronze ne trouve aucune ligne de bronze absente du
landing sur les 12 partitions
([#7, tour de correction](https://github.com/BAYRYO/globaltech-data-platform/pull/7#issuecomment-7202646257)).

## Conséquences

- Le driver lit une fois les octets de chaque fichier : le coût croît avec la taille du dépôt.
- Le lecteur CSV saute une ligne vide ou faite d'espaces : un tel fichier fait échouer le job sur
  le compte des lignes, et doit être corrigé en amont
  ([dépannage](../runbook/bronze.md#dépanner)).
- `_record_hash`, écrit en 64 caractères hexadécimaux, occupe 73,4 % des octets de
  `bronze/orders` ([constat](../performance.md#constat--_record_hash-en-bronze)).

## Alternatives écartées

- Compter les lignes sans vérifier le SHA-256 : un fichier remplacé par un autre de même taille
  en lignes passerait.
- Publier les fichiers sans contrôle et laisser silver signaler les écarts : bronze garderait
  des données qui ne sont pas celles du dépôt.
