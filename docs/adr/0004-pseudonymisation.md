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

## Conséquences

- La donnée est pseudonymisée et non anonymisée : avec le sel, la correspondance reste
  calculable. Le sel est donc une donnée sensible, à protéger comme un mot de passe.
- Changer le sel casse la continuité des `email_hash` : rotation planifiée avec recalcul.

## Alternatives écartées

- HMAC-SHA256 : préférable en théorie, mais absent des fonctions natives de Spark 3.5 ; l'écart
  de robustesse avec un sel secret de 32 octets ne justifie pas une UDF.
- Chiffrement réversible (AES) : nécessite une gestion de clés et ne sert aucun cas d'usage
  analytique.
