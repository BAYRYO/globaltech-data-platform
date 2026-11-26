# ADR-0004 : pseudonymisation par SHA-256 salé

**Statut** : acceptée

## Contexte

Les clients portent des données personnelles (nom, email, téléphone) et les logs web des
adresses IP. Le RGPD impose de minimiser ces données dès que l'analyse ne les requiert pas,
tout en gardant la capacité de compter des clients distincts et de joindre les sources.

## Décision

- `email` et IP sont remplacés par `sha2(concat(sel, valeur_normalisée), 256)` en silver.
- `full_name` et `phone` sont supprimés dès silver.
- Le sel vient de `GTDP_PSEUDO_SALT`, hors dépôt. Sans le sel, retrouver un email demande une
  attaque par dictionnaire sur un espace inconnu.
- Fonctions natives Spark uniquement (`sha2`, `concat`, `lower`, `trim`), aucune UDF.
- Le sel entre dans le calcul comme une **donnée** (DataFrame d'une ligne diffusé et joint), jamais
  comme un littéral : la revue du Lot 0 a montré qu'un `lit(sel)` apparaît en clair dans
  `explain` et deux fois dans l'event log servi par le History Server, même avec
  `spark.sql.redaction.string.regex`. Un test vérifie l'absence du sel dans le plan et l'event log.

## Conséquences

- La donnée est pseudonymisée et non anonymisée : avec le sel, la correspondance reste
  calculable. Le sel est donc une donnée sensible, à protéger comme un mot de passe.
- Changer le sel casse la continuité des `email_hash` : rotation planifiée avec recalcul.

## Alternatives écartées

- HMAC-SHA256 : préférable en théorie, mais absent des fonctions natives de Spark 3.5 ; l'écart
  de robustesse avec un sel secret de 32 octets ne justifie pas une UDF.
- Chiffrement réversible (AES) : nécessite une gestion de clés et ne sert aucun cas d'usage
  analytique.
