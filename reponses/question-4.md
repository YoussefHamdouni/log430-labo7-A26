# Labo 07 – Question 4

> Comment Kafka utilise-t-il son système de partitionnement pour atteindre des performances de lecture élevées ? Lisez [cette section](https://kafka.apache.org/24/documentation.html#intro_topics) de la documentation officielle à Kafka et résumez les points principaux.

## Résumé de la documentation

**1. Un topic est découpé en partitions, et chaque partition est un log en ajout seul.**
Chaque partition est une séquence de messages ordonnée et immuable, à laquelle on ajoute continuellement à la fin. Chaque message reçoit un numéro séquentiel, l'**offset**, qui l'identifie de façon unique dans sa partition. Écrire à la fin d'un fichier et le relire de façon séquentielle est l'opération la plus rapide qu'un disque puisse faire. Il n'y a ni index à mettre à jour ni lignes à modifier.

**2. Les performances ne dépendent pas de la quantité de données conservées.**
Kafka garde tous les messages publiés pendant la période de rétention configurée, qu'ils aient été lus ou non. Selon la documentation, les performances de Kafka sont effectivement constantes par rapport à la taille des données, donc garder des données longtemps ne pose pas de problème. C'est ce qui rend l'event sourcing possible : l'historique reste disponible sans ralentir les lectures.

**3. Le consommateur contrôle sa propre position de lecture.**
La seule métadonnée gardée pour chaque consommateur est son offset dans le log. C'est le consommateur qui le fait avancer, et il peut aussi revenir en arrière pour relire l'historique (par exemple `auto_offset_reset=earliest`) ou sauter à la fin. Lire un message ne le supprime pas et n'affecte pas les autres consommateurs. Les consommateurs sont donc très peu coûteux : on peut en ajouter sans impact sur le cluster ni sur les autres lecteurs.

**4. Les partitions répartissent la charge sur plusieurs serveurs.**
Un topic peut avoir plusieurs partitions, et chacune doit tenir sur un seul serveur. Un topic peut donc grossir au-delà de la capacité d'une seule machine. Les partitions sont distribuées entre les brokers du cluster et répliquées pour la tolérance aux pannes. Chaque partition a un broker **leader**, qui traite toutes les lectures et écritures, et des **followers** qui la répliquent. Comme chaque broker est leader pour certaines partitions et follower pour d'autres, la charge est bien équilibrée dans le cluster.

**5. La partition est l'unité de parallélisme des lectures.**
Dans un groupe de consommateurs, chaque partition est lue par **un seul** consommateur du groupe. Avec N partitions, jusqu'à N consommateurs lisent en parallèle, chacun sa part du topic, sans coordination entre eux. L'ordre des messages est garanti à l'intérieur d'une partition, pas entre les partitions. Si l'ordre total est requis, il faut un topic avec une seule partition, et donc un seul consommateur actif par groupe.

## Application dans notre labo

Notre topic `user-events` a une seule partition, sur un seul broker :

```
$ docker exec kafka kafka-topics --bootstrap-server kafka:9092 --describe --topic user-events
Topic: user-events  TopicId: y7bTY51hQPeB-x1L5HXkCQ  PartitionCount: 1  ReplicationFactor: 1  Configs: segment.bytes=214748364,retention.bytes=1073741824
    Topic: user-events  Partition: 0  Leader: 1  Replicas: 1  Isr: 1
```

L'ordre des événements d'un utilisateur est donc garanti (un `UserDeleted` arrive toujours après le `UserCreated` correspondant). En contrepartie, un seul consommateur par groupe peut lire. C'est pour cette raison que le consommateur historique de l'activité 6 utilise un `group_id` distinct : il lit toute la partition depuis l'offset 0 sans prendre la place du `UserEventConsumer`.

La configuration de rétention ajoutée au `docker-compose.yml` de Coolriel (activité 5) contrôle combien de temps et combien de données chaque partition conserve, et la taille des segments (fichiers) qui la composent :

```yml
kafka:
  environment:
    KAFKA_LOG_RETENTION_HOURS: 168 # Garde les messages 7 jours
    KAFKA_LOG_RETENTION_BYTES: 1073741824 # Taille des partitions : 1GB
    KAFKA_LOG_SEGMENT_BYTES: 214748364 # Taille des log segments : 200MB (parties d'une partition sur le disque)
```

Configuration effective du broker après le redémarrage :

```
log.retention.bytes=1073741824  synonyms={STATIC_BROKER_CONFIG:log.retention.bytes=1073741824, DEFAULT_CONFIG:log.retention.bytes=-1}
log.retention.hours=168         synonyms={STATIC_BROKER_CONFIG:log.retention.hours=168, DEFAULT_CONFIG:log.retention.hours=168}
log.segment.bytes=214748364     synonyms={STATIC_BROKER_CONFIG:log.segment.bytes=214748364, DEFAULT_CONFIG:log.segment.bytes=1073741824}
```

Après `docker compose restart kafka`, les 4 événements créés avant le redémarrage sont toujours lisibles depuis le début du topic :

```
$ docker exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic user-events --from-beginning --timeout-ms 5000
{"event": "UserCreated", "id": 6, ...}
{"event": "UserCreated", "id": 7, ...}
{"event": "UserCreated", "id": 8, ...}
{"event": "UserDeleted", "id": 7, ...}
```
