# Revue adverse du Lot 0 (cadrage) — 2026-11-26

Périmètre : `docs/architecture.md`, `docs/data-contract.md`, `docs/data-model.md`, `docs/adr/`,
`docs/project-management.md`, `data/kit/` au commit `25f0556`.

| Relecteur | Modèle | Rôle tenu | Constats |
| --- | --- | --- | --- |
| A | Claude Fable 5.2 | data engineer batch + architecture | 2 bloquants, 5 majeurs, 6 mineurs |
| B | Claude Opus 5.5 | streaming et performance + plateforme | 1 bloquant, 5 majeurs, 7 mineurs |

Méthode : chaque constat est accompagné de la commande qui le prouve (profilage du kit, prototype
PySpark 3.5.3, calcul). Le pilote a relancé les sondes (profilage du kit, simulation du contrat,
trois sondes Spark batch, sonde streaming, calculs d'erasure coding et de chiffre d'affaires) : tous
les constats ci-dessous se reproduisent. Les sondes sont conservées hors dépôt.

## Constats et décisions

| # | Gravité | Constat (preuve) | Décision | Où |
| --- | --- | --- | --- | --- |
| A1 | bloquant | Gold partitionné par `order_date` + réécriture dynamique : une commande retardataire de J2 efface les commandes de J1 de la même date (4 lignes attendues, 1 obtenue) | gold(D) recalcule les dates touchées à partir de toutes les lignes silver de ces dates | ADR-0002, data-model |
| A2 | bloquant | Changement de schéma J3 sans règle : schéma fixe → toutes les lignes corrompues, colonne perdue sans `mergeSchema`, `revenue_eur` nul sur J1/J2 | schéma brut par en-tête de fichier, schéma canonique silver, `discount_pct` = 0 par défaut | ADR-0007 |
| A3 | majeur | Anomalies des référentiels : 4,96 % des commandes du kit pointent une fiche client/produit anomalisée, disposition non définie | table de disposition par code ; références contrôlées contre tous les identifiants vus en bronze | data-model, contrat |
| A4 | majeur | SCD2 : `CONFLICTING_VERSION` double une ligne de fait ; commande antérieure à la 1ʳᵉ version sans prix | groupe en conflit rejeté ; version 1 valide depuis 1900-01-01 ; unicités vérifiées | data-model, contrat |
| A5 | majeur | Prix facturé du kit indépendant du catalogue (corrélation −0,0003) | constat documenté ; prix synthétique = prix SCD2 × (1 − remise 0-10 %) ; indicateur en médiane | contrat, data-model |
| A6, B4, B12 | majeur | Kit « propre » contredit : 49 580 commandes avant inscription, 688 inscriptions après J1, régions `AA-AA` à `AA-AAAA` | incohérences connues, conservées et signalées ; format `^[A-Z]{2}-[A-Z]{2,4}$` | contrat, data-model |
| A7 | majeur | Doublons tirés de lignes invalides ou renvoyés au mauvais format en J3 (écart de 38 lignes au manifeste) | doublons tirés de lignes saines ; renvoi J3 au format J3 avec `discount_pct` vide | contrat |
| A8 | mineur | J3 traité avant J2 : clé en double ; premier jour : `PATH_NOT_FOUND` | refus si partition postérieure (option `--cascade`), unicité globale, table absente = vide | ADR-0005 |
| A9 | mineur | Réécriture dynamique : partition périmée conservée si le rejeu ne produit plus de ligne ; en statique, historique effacé | partitions nommées par le job et supprimées avant écriture, mode posé sur l'écriture | ADR-0002 |
| A10 | mineur | Nom de partition `_ingestion_date` contre `ingestion_date` | `ingestion_date` partout, rejets sous `rejects/<source>/ingestion_date=` | data-model |
| A11 | mineur | Sel de pseudonymisation lisible dans `explain` et l'event log | sel passé comme donnée, test sur le plan et l'event log | ADR-0004, data-model |
| A12, B11 | mineur | Profil `ci` contredit « 200 000 lignes du kit intactes » | invariant limité aux lignes retenues par le profil (10 000 premières en `ci`) | contrat |
| A13 | mineur | Champs absents du kit (produit et terme des logs, nom de produit) | tirages déterministes déclarés | contrat |
| B1 | bloquant | Streaming : en `append`, 0 ou 4 fenêtres sur 10 écrites selon le watermark ; en `update`, 10,4 copies par fenêtre | watermark 2 min, retards du contrat recalés (10-90 s / 5-15 min), `append`, alertes par événement, flux de 15 min | ADR-0008, contrat |
| B2 | majeur | Plan de performance absent ; le skew est masqué par la diffusion et l'AQE (49,7 contre 1,4) | banc défini : indicateur, version naïve, 6 leviers, deux méthodes de skew, trois `explain` | architecture |
| B3 | majeur | Calcul MapReduce/Spark non défini (écart de 2 à 4 % selon le prix retenu) | montant facturé des lignes `COMPLETED`, même entrée CSV, jointure côté map, règles d'invalidité communes | architecture |
| B5 | majeur | Pas de budget mémoire ; défauts Hadoop = 2 × 8 Gio annoncés | budget calculé dans le guide de la plateforme, valeurs explicites | architecture, plateforme |
| B6 | majeur | Détection d'un DataNode mort en 630 s par défaut, `fsck` absent | 90 s (recheck 30 s), `fsck` avant/pendant/après | ADR-0006 |
| B7 | mineur | Événements très en retard agrégés pendant les 2 premiers micro-batches | jamais dans les 36 premiers fichiers ; comptage par `numRowsDroppedByWatermark` | contrat, ADR-0008 |
| B8 | mineur | Taux capteurs incompatibles avec « au moins une fois par fichier » ; `SPIKE` non défini | règle par journée ; 60 événements de base par fichier ; `SPIKE` ≥ 2 min | contrat |
| B9 | mineur | Surcoût XOR-2-1 de ×1,5 faux sous 2 Mio | comparaison sur fichiers ≥ 128 Mio, petits fichiers à part | ADR-0006 |
| B10 | mineur | Téléchargement du premier build : 1,04 Go et non 800 Mo | chiffre corrigé | ADR-0001 |
| B13 | mineur | Agrégats capteurs hors de gold ; droits et quotas manquants pour `/datalake/streaming` et `/benchmarks` | agrégats et alertes en gold ; deux lignes ajoutées | architecture |

Aucun constat écarté.
