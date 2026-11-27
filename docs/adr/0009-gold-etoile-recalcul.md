# ADR-0009 : gold en étoile, recalculée depuis les jours silver complets

**Statut** : acceptée

Cet ADR consigne après coup des décisions prises pendant le Lot 4 (KAN-32 à KAN-36) et sa
correction KAN-58 (#22). Elles étaient décrites dans le [modèle de données](../data-model.md#gold--modèle-en-étoile),
le [runbook gold](../runbook/gold.md#ce-que-fait-une-exécution), [ADR-0002](0002-parquet-idempotence.md)
et [ADR-0005](0005-silver-incremental.md), sans ADR qui les rassemble.

## Contexte

Gold calcule les neuf indicateurs du sujet ([définitions](../data-model.md#indicateurs-gold)).
Silver grandit par jour d'ingestion. J2 et J3 apportent des commandes en retard sur des dates déjà
publiées. Les produits ont des versions datées (SCD type 2) et les fiches clients sont remplacées
par leur dernière version (SCD type 1). Un indicateur doit donner le même résultat quel que soit
l'ordre des exécutions, et un lecteur ne doit jamais voir une gold à moitié publiée.

## Décision

- **Schéma en étoile.** `fact_sales` (grain : une ligne de commande, `transaction_id` unique),
  `fact_web_events`, `dim_date`, `dim_customer` (SCD 1) et `dim_product` (une ligne par version
  SCD 2). Chaque commande rejoint la version de `dim_product` valide à son `event_time` :
  `revenue_eur` au prix catalogue applicable, `invoiced_amount_eur` au prix facturé, tous deux en
  `decimal(20,2)`.
- **Attributs du client hors du fait.** `fact_sales` ne copie aucun attribut du client. Les
  indicateurs par pays ou par segment joignent `dim_customer` à chaque calcul.
- **Recalcul complet depuis les jours complets.** Chaque exécution recalcule toutes les tables
  depuis les jours silver qui ont leur `_SUCCESS`. Elle compare une empreinte par `order_date` à
  `fact_sales` publiée, réécrit les dates qui diffèrent et supprime les dates sans commande
  ([ADR-0002](0002-parquet-idempotence.md#pourquoi-nommer-les-partitions)).
- **Jours complets pour les tables entières (KAN-58).** Gold lit `customers` et `products_scd2`,
  puis refuse de s'exécuter, avant toute écriture et avant de prendre son verrou, si une ligne lue
  porte un `ingestion_date` qui n'est pas un jour complet ou si le `_built_for.json` de la table
  nomme un tel jour (`IncompleteSilverError`,
  [ADR-0005](0005-silver-incremental.md#conséquences)).
- **Contrôles fermés.** Avant toute écriture, chaque commande a exactement une version, chaque
  table a une clé unique et aucune colonne obligatoire nulle, aucune référence n'est orpheline.
  Au premier écart, gold s'arrête sans rien écrire
  ([références et contrôles](../data-dictionary/gold.md#références-et-contrôles)).
- **Publication.** Le verrou de la couche gold couvre l'exécution de la fin du contrôle de silver
  à la fin. `gold/_state.json` vaut `PUBLISHING` pendant l'écriture, puis `PUBLISHED` après la
  relecture de `fact_sales`.

## Preuves qui ont justifié ces choix

- La première version de gold copiait le pays du client dans le fait. La revue de #15 a mesuré
  qu'après J3, 5 202 lignes de 27 clients portaient un pays périmé, alors que le rapport gold
  restait entièrement `ok`
  ([revue](https://github.com/BAYRYO/globaltech-data-platform/pull/15#issuecomment-7206133610)).
- Sur le lac `dev`, gold calculée jour par jour en alternance avec silver et gold construite une
  fois sur le silver final ont le même `content_sum` sur les 14 tables. Le rejeu de gold J1 après
  J3 ne réécrit aucune des 367 dates
  ([#15, tour de correction](https://github.com/BAYRYO/globaltech-data-platform/pull/15#issuecomment-7207818096)).
- La revue A de #21 a prouvé que gold publiait avec les dimensions d'un jour que silver n'avait
  pas fini : `customers` et `products` de J2 publiées, `orders` pas encore. Les 6 tests de ce cas
  échouaient avant la correction
  ([#22, preuves](https://github.com/BAYRYO/globaltech-data-platform/pull/22#issuecomment-7214321020)).
  La revue A de #22 a prouvé trois défauts de la première correction, qui se fiait à un
  enregistrement des jours plutôt qu'aux lignes lues : 13 tests échouaient sur cette version
  ([#22, tour de correction](https://github.com/BAYRYO/globaltech-data-platform/pull/22#issuecomment-7217275110)).

## Conséquences

- Chaque exécution relit tout l'historique silver terminé et tout `fact_sales` publié : son coût
  croît avec l'historique, pas avec le jour. C'est acceptable à environ un million de commandes
  par an ([runbook gold](../runbook/gold.md#ce-que-fait-une-exécution)).
- Le pays d'un indicateur est le pays actuel du client : un déménagement déplace tout son
  historique.
- Gold refuse une dimension reconstruite pour un jour incomplet même quand son contenu n'a pas
  changé (#22, « Tradeoffs »).
- Sur une table écrite avant #22, sans `_built_for.json`, une version produit renvoyée sur un jour
  incomplet passe jusqu'à la reconstruction suivante de la table.
- Un rejeu silver d'un jour complet lancé pendant une exécution gold n'est pas détecté : gold peut
  lire certaines tables avant la réécriture et d'autres après.
- Les règles de KAN-58 sont prouvées par les tests locaux sur les dépôts `ci`. Elles ont tourné
  sur YARN le 2026-11-27, après la fusion de #22, car le cluster était pris par l'exercice de
  KAN-52. Sur un silver complet, gold publie avec 0 contrôle en échec. Quand un jour silver est
  arrêté par SIGTERM après `customers`, gold refuse avant toute écriture, sur une
  `IncompleteSilverError` qui nomme la commande à lancer
  ([preuves sur YARN](https://github.com/BAYRYO/globaltech-data-platform/pull/25#issuecomment-7224839486)).

## Alternatives écartées

- Recalculer seulement les dates touchées par le jour : une date dépend aussi des versions des
  produits et des fiches clients, qu'un jour silver peut changer pour des dates anciennes. Chaque
  oubli de dépendance laisse gold en désaccord avec silver sans le signaler (constat des deux
  revues du Lot 4, [runbook gold](../runbook/gold.md#ce-que-fait-une-exécution)).
- Copier les attributs du client dans le fait : faux après un déménagement, mesuré par la revue
  de #15 ci-dessus.
- Contrôler un enregistrement des jours au lieu des lignes lues : faux sur un lac sans
  enregistrement et après une écriture échouée (revue A de #22 ci-dessus).
