# Répétition chronométrée

Cette répétition vérifie que le [script](script.md) tient en 15 minutes, démonstration comprise.

## Méthode

- Le texte de chaque diapositive, tiré de `script.md`, est lu par la voix française de Windows
  `Microsoft Hortense Desktop` (System.Speech, fr-FR), à la vitesse par défaut (`Rate` 0). Chaque
  diapositive donne un fichier WAV, dont la durée est lue dans son en-tête.
- La démonstration compte pour la durée réelle de l'exécution enregistrée sur le cluster, le
  dernier événement de [soutenance-demo.cast](demo/soutenance-demo.cast) : son texte se dit
  pendant que les commandes tournent. L'outil vérifie que ce texte dure moins que l'exécution.
- Les fichiers WAV restent dans un dossier temporaire et ne sont pas versionnés.
- Si le total dépasse 15 minutes, c'est le script qui est raccourci, pas la vitesse de la voix.

Une voix de synthèse ne marque aucune pause entre deux diapositives et ne s'arrête pas pour
montrer un schéma. Un orateur réel ajoute ces secondes : la marge sous 15 minutes est là pour
elles.

## Reproduire

Sous Windows, depuis la racine du dépôt :

```bash
python scripts/demo/deck.py rehearse --demo-cast docs/soutenance/demo/soutenance-demo.cast
```

La commande réécrit le tableau ci-dessous et sort en erreur si le total dépasse 15 minutes.
`--voice` et `--rate` changent la voix et sa vitesse, `--out` garde les fichiers WAV dans un
dossier choisi.

## Résultat

<!-- rehearsal -->
| # | Diapositive | Rôle | Durée cible | Durée mesurée | Cumul |
| ---: | --- | --- | ---: | --- | ---: |
| 1 | GlobalTech Data Platform | responsable qualité et delivery | 30 s | 39 s | 39 s |
| 2 | Contexte et question | responsable qualité et delivery | 45 s | 49 s | 1 min 29 s |
| 3 | Architecture | architecte plateforme | 50 s | 51 s | 2 min 20 s |
| 4 | Bronze | architecte plateforme | 45 s | 44 s | 3 min 05 s |
| 5 | Silver | data engineer batch | 55 s | 55 s | 4 min 00 s |
| 6 | Gold | data engineer batch | 55 s | 51 s | 4 min 50 s |
| 7 | Streaming | data engineer streaming et performance | 50 s | 51 s | 5 min 41 s |
| 8 | MapReduce contre Spark | data engineer streaming et performance | 50 s | 45 s | 6 min 26 s |
| 9 | Performance | data engineer streaming et performance | 55 s | 54 s | 7 min 20 s |
| 10 | Stockage et panne d'un DataNode | architecte plateforme | 55 s | 55 s | 8 min 15 s |
| 11 | Industrialisation et qualité | responsable qualité et delivery | 50 s | 51 s | 9 min 06 s |
| 12 | Démonstration | data engineer streaming et performance, architecte plateforme, data engineer batch, dans l'ordre des étapes | 3 min 50 s | démonstration 227 s (texte dit en 105 s pendant la démonstration) | 12 min 53 s |
| 13 | Limites | data engineer batch | 50 s | 47 s | 13 min 40 s |
| 14 | Retour d'expérience | responsable qualité et delivery | 45 s | 43 s | 14 min 23 s |
| 15 | Questions | responsable qualité et delivery | 5 s | 6 s | 14 min 28 s |
| | **Total** | | | | **14 min 28 s** (868 s) |

| Rôle | Temps de parole hors démonstration |
| --- | ---: |
| responsable qualité et delivery | 3 min 08 s |
| architecte plateforme | 2 min 31 s |
| data engineer batch | 2 min 33 s |
| data engineer streaming et performance | 2 min 30 s |
<!-- /rehearsal -->
