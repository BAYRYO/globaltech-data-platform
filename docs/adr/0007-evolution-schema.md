# ADR-0007 : évolution de schéma (colonne `discount_pct` en J3)

**Statut** : acceptée

## Contexte

Le dépôt J3 ajoute une 9ᵉ colonne `discount_pct` aux commandes. La grille d'évaluation attend une
« gestion d'un changement de schéma ». La revue adverse du Lot 0 a montré, sur Spark 3.5.3, trois
échecs des lectures naïves :

- un schéma fixe à 8 colonnes appliqué au fichier J3 marque toutes ses lignes en
  `_corrupt_record` et perd `discount_pct` ; un schéma à 9 colonnes fait l'inverse sur J1 et J2 ;
- la lecture Parquet de bronze sans `mergeSchema` ne voit que le schéma d'une partition et perd la
  colonne ;
- la formule `quantity × prix × (1 − discount_pct)` rend `NULL` pour toutes les commandes de J1 et
  J2, où la colonne est absente.

## Décision

1. **Bronze** : chaque fichier est lu avec un schéma tout en `string` construit à partir de **son
   propre en-tête**, plus `_corrupt_record`. Une ligne n'est corrompue que si son nombre de champs
   diffère de l'en-tête de son fichier. Le schéma brut de chaque partition est celui du fichier.
   L'en-tête d'un fichier CSV doit nommer toutes les colonnes obligatoires de sa source
   (`Source.required` de `src/gtdp/schemas.py`, toutes sauf `discount_pct`) : sinon le fichier est
   refusé avant toute écriture.
2. **Silver** lit bronze avec le schéma canonique de la source, `bronze_schema(source)`, par
   `read_table` : les colonnes attendues en `string`, les colonnes techniques, puis
   `ingestion_date`. Une colonne attendue absente d'une partition est lue nulle. `discount_pct`
   vaut `0.000` quand elle est nulle ou vide, ce qui couvre J1, J2 et les doublons inter-jours
   renvoyés dans J3 avec un champ vide.
3. Une colonne inconnue (absente de `SOURCES`) est conservée en bronze, non lue par silver et
   signalée par source dans `metrics_json.unknown_columns` des lignes d'audit des jobs bronze et
   silver, par exemple `{"orders": ["coupon_code"]}`. Un ajout amont est ainsi visible sans
   casser le pipeline.
4. Test de non-régression : J1, J2 et J3 traités ensemble, `revenue_eur` non nul sur toutes les
   lignes et égal à `quantity × prix` quand `discount_pct` est absent
   (`tests/silver/test_silver_schema_change.py`).

## Conséquences

- J1 et J2 peuvent être rejoués après J3 sans changement de code.
- La liste des colonnes attendues (`SOURCES`) et les schémas silver vivent dans un seul module,
  `src/gtdp/schemas.py`.
- Silver ne lit pas les données avec `mergeSchema` : le schéma explicite de `read_table` donne
  les mêmes colonnes à chaque partition. `mergeSchema` sert seulement à lister les colonnes d'une
  partition bronze, pour signaler les colonnes inconnues.

## Alternatives écartées

- Un schéma par jour codé en dur : chaque évolution amont demanderait une livraison.
- `inferSchema` : types instables d'un jour à l'autre et lecture complète supplémentaire.
