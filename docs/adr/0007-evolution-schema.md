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
2. **Silver** lit bronze avec `mergeSchema=true`, puis projette un **schéma canonique** : chaque
   colonne attendue est sélectionnée si elle existe, sinon remplacée par sa valeur par défaut.
   `discount_pct` vaut `0.000` quand la colonne est absente ou vide, ce qui couvre aussi les
   doublons inter-jours renvoyés dans J3 avec un champ vide.
3. Une colonne inconnue (non prévue par le schéma canonique) est conservée en bronze, ignorée en
   silver et signalée dans l'audit (`metrics_json.unknown_columns`), pour qu'un ajout amont soit
   visible sans casser le pipeline.
4. Test de non-régression : J1, J2 et J3 traités ensemble, `revenue_eur` non nul sur toutes les
   lignes et égal à `quantity × prix` quand `discount_pct` est absent.

## Conséquences

- J1 et J2 peuvent être rejoués après J3 sans changement de code.
- Le schéma canonique est la seule définition des colonnes silver : il vit dans
  `src/gtdp/schemas.py`.

## Alternatives écartées

- Un schéma par jour codé en dur : chaque évolution amont demanderait une livraison.
- `inferSchema` : types instables d'un jour à l'autre et lecture complète supplémentaire.
