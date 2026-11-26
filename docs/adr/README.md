# Décisions d'architecture (ADR)

Une décision structurante par fichier : contexte, décision, conséquences, alternatives écartées.
Une décision remplacée garde son fichier avec le statut « Remplacée par ADR-xxxx ».

| ADR | Décision | Statut |
| --- | --- | --- |
| [0001](0001-image-hadoop-spark.md) | Image unique Hadoop 3.3.6 + Spark 3.5.3 construite par le projet | Acceptée |
| [0002](0002-parquet-idempotence.md) | Partitions nommées, écrites puis échangées sous verrou ; audit en ajout | Acceptée |
| [0003](0003-configuration-toml.md) | Configuration TOML par environnement, surchargeable par variables d'environnement et `--set` | Acceptée |
| [0004](0004-pseudonymisation.md) | Pseudonymisation par SHA-256 salé, sel hors dépôt et contrôlé par son empreinte | Acceptée |
| [0005](0005-silver-incremental.md) | Silver incrémental par date d'ingestion, publié par jour complet, jours traités dans l'ordre | Acceptée |
| [0006](0006-topologie-hdfs.md) | 3 DataNodes, réplication 3 par défaut, erasure coding XOR-2-1 comparé | Acceptée |
| [0007](0007-evolution-schema.md) | Schéma brut par en-tête de fichier en bronze, schéma canonique en silver | Acceptée |
| [0008](0008-streaming.md) | Streaming : watermark 2 min, fenêtres 1 min en append, alertes par événement, écriture par `batch_id` | Acceptée |
