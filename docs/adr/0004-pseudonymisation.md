# ADR-0004 : pseudonymisation par SHA-256 salé

**Statut** : acceptée

## Contexte

Les clients portent des données personnelles (nom, email, téléphone) et les logs web des
adresses IP. Le RGPD impose de minimiser ces données dès que l'analyse ne les requiert pas,
tout en gardant la capacité de compter des clients distincts et de joindre les sources.

## Décision

Règle : aucune colonne silver lisible par les analystes n'est une fonction non salée d'une valeur
personnelle ou d'une ligne qui en contient.

- `email`, l'adresse IP et la ligne de log sont remplacés par `sha2(concat(sel, valeur), 256)` :
  `email_hash` sur `lower(trim(email))`, nul pour un email manquant ou invalide ; `ip_hash` sur
  `lower(ip)` ; `log_id` sur la ligne brute entière. Sans sel, le hachage de la ligne,
  recalculé à partir des autres colonnes et des 256 adresses du `/24`, redonnait l'adresse.
- `full_name` et `phone` sont supprimés dès silver.
- `customers` et `orders` ne publient pas le `_record_hash` de bronze. Ce SHA-256 non salé de la
  ligne confirmerait une ligne devinée, nom, email et téléphone compris. `products_scd2` le
  garde : une ligne produit ne contient aucune donnée personnelle.
- `ip_prefix` garde le réseau `/24` d'une adresse IPv4 ou le `/48` d'une adresse IPv6, pour la
  géographie grossière.
- Les rejets `silver/rejects/<source>` gardent les lignes brutes, données personnelles comprises,
  pour l'analyse des anomalies. L'ACL `group:analysts:---` de `/datalake/silver/rejects`, héritée
  par chaque partition, les ferme aux analystes.
- Le sel vient de `GTDP_PSEUDO_SALT`, hors dépôt. Sans le sel, retrouver un email demande une
  attaque par dictionnaire sur un espace inconnu.
- Fonctions natives Spark uniquement (`sha2`, `concat`, `lower`, `trim`), aucune UDF.
- Le sel entre dans le calcul comme une **donnée** (DataFrame d'une ligne diffusé et joint), jamais
  comme un littéral : la revue du Lot 0 a montré qu'un `lit(sel)` apparaît en clair dans
  `explain` et deux fois dans l'event log servi par le History Server, même avec
  `spark.sql.redaction.string.regex`. Un test vérifie l'absence du sel dans le plan et l'event log.
- Sur le cluster, le job silver refuse avant toute écriture un sel vide, de moins de 16
  caractères ou égal au sel de développement du dépôt (`SaltError`).
- La première exécution silver écrit l'empreinte `sha256("gtdp-salt-fingerprint" ‖ sel)` dans
  `<ops>/pseudonymization/salt_fingerprint.json`. Une exécution avec un autre sel est refusée
  avant toute écriture : ses hachages ne correspondraient à rien de ce qui est déjà publié.

## Conséquences

- La donnée est pseudonymisée et non anonymisée : avec le sel, la correspondance reste
  calculable. Le sel est donc une donnée sensible, à protéger comme un mot de passe.
- L'empreinte ne contient pas le sel et ne permet pas de le retrouver. Elle empêche de mélanger
  deux sels dans une même table.
- Changer le sel change tous les `email_hash`, `ip_hash` et `log_id`. Une rotation impose de
  reconstruire silver en entier, puis gold
  ([runbook silver, « Changer le sel »](../runbook/silver.md#changer-le-sel)).
- Risque résiduel accepté, détaillé dans le
  [dictionnaire silver](../data-dictionary/silver.md#pseudonymisation) :
  - `user_agent` : empreinte de navigateur, réduite à trois appareils dans le jeu de données ;
  - la partie requête de `path` (`/search?q=…`) : termes de recherche en clair ;
  - `ip_prefix` : localise un visiteur à 256 adresses près en IPv4, sans permettre de tester les
    candidates faute de sel ;
  - les quasi-identifiants des clients (`region`, `segment`, `signup_date`, `updated_at`) :
    combinés, ils peuvent isoler un client, mais les indicateurs en ont besoin.

  Sur des données réelles, tronquer l'agent utilisateur et la requête de `path` serait la mesure
  suivante.

## Alternatives écartées

- HMAC-SHA256 : absent des fonctions natives de Spark 3.5. Son avantage sur `sha2(sel ‖ valeur)`
  est la résistance à l'extension de longueur, qui protège une authentification de message. Une
  pseudonymisation n'authentifie rien : l'écart ne justifie pas une UDF.
- Chiffrement réversible (AES) : nécessite une gestion de clés et ne sert aucun cas d'usage
  analytique.
