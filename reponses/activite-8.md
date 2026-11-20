# Labo 07 – Activité 8 : test de charge (création et suppression d'utilisateurs)

## Configuration

Mêmes paramètres qu'au labo 4, activité 5 : **150 utilisateurs, spawn rate 2/s, 2 minutes**, en local, directement sur le Store Manager (`http://store_manager:5000`). KrakenD ne route pas `DELETE /users` sur cette branche, et le test vise le Store Manager comme au labo 4.

Nouvelle classe `UserLifecycleUser` dans `locustfiles/locustfile.py` (dépôt `log430-labo5`, branche `labo7-a8-locust`) :

- `create_user` (poids 2) : `POST /users` avec un **courriel unique** (`locust-<uuid>@test.ca`), car la colonne `email` de la table `users` est `UNIQUE`. Chaque création publie un événement `UserCreated` dans Kafka.
- `delete_user` (poids 1) : `DELETE /users/<id>` sur un utilisateur que ce même utilisateur Locust a créé. On ne supprime jamais un utilisateur inexistant ou déjà supprimé.

```python
class UserLifecycleUser(HttpUser):
    wait_time = between(1, 2)

    def on_start(self):
        self.created_user_ids = []

    @task(2)
    def create_user(self):
        payload = {"name": "Locust User", "email": f"locust-{uuid.uuid4().hex}@test.ca"}
        with self.client.post("/users", json=payload, catch_response=True) as response:
            if response.status_code == 201:
                self.created_user_ids.append(response.json()["user_id"])
                ...

    @task(1)
    def delete_user(self):
        if not self.created_user_ids:
            return
        user_id = self.created_user_ids.pop(0)
        with self.client.delete(f"/users/{user_id}", name="/users/[id]", catch_response=True) as response:
            ...
```

Lancement (le nom de la classe sélectionne seulement ce test, sans la tâche de rate limiting du labo 5) :

```bash
docker compose run --rm -p 8090:8089 locust -f /mnt/locust/locustfile.py UserLifecycleUser --host=http://store_manager:5000
# interface : http://localhost:8090 (150 users, ramp 2, Advanced options > Run time 2m)
```

## Résultats

| Requête | Requêtes | Échecs | Médiane | p95 | Max | Débit |
|---|---|---|---|---|---|---|
| `POST /users` | 4 065 | 765 (18,8 %) | 390 ms | 2 000 ms | 2 800 ms | 34,0 req/s |
| `DELETE /users/[id]` | 1 877 | 380 (20,3 %) | 400 ms | 2 000 ms | 2 642 ms | 15,7 req/s |
| **Total** | **5 942** | **1 145 (19,3 %)** | **390 ms** | **2 000 ms** | **2 800 ms** | **49,7 req/s** |

Évolution (historique `--csv`, toutes les 10 s) :

| t (s) | Utilisateurs | req/s | échecs/s | p50 (ms) | p95 (ms) |
|---|---|---|---|---|---|
| 10 | 20 | 4,9 | 0 | 43 | 110 |
| 30 | 58 | 28,4 | 0 | 46 | 110 |
| 50 | 98 | 49,6 | 0 | 100 | 470 |
| **51** | **100** | – | **1er échec** (1 570 requêtes) | – | – |
| 70 | 138 | 68,4 | 9,0 | 390 | 700 |
| 90 | 150 | 50,9 | 16,2 | 1 900 | 2 500 |
| 120 | 150 | 74,2 | 24,1 | 500 | 1 100 |

Ressources (`docker stats`, toutes les 15 s) : `store_manager` jusqu'à 323 % CPU, MySQL jusqu'à 103 %, Kafka 7 à 19 % (avec des pointes ponctuelles), **Coolriel 0,2 à 7 % CPU et ~17 Mo**.

## Observations

1. **Jusqu'à ~100 utilisateurs, aucune erreur et des temps courts** (médiane 43 à 100 ms). La création d'un utilisateur est rapide, car le Store Manager publie l'événement dans Kafka sans attendre Coolriel (voir Q1).
2. **Point de rupture à ~100 utilisateurs (t = 51 s)**, beaucoup plus tard qu'au labo 5 pour la création de commandes (~36 à 45 utilisateurs). Une création d'utilisateur ne fait qu'un `INSERT` et un `send()` Kafka, alors qu'une commande lit les produits, écrit plusieurs lignes et appelle le service de paiement en HTTP synchrone.
3. **Toutes les erreurs sont des `500 – Too many connections` (MySQL 1040)**, pour la création comme pour la suppression :
   ```txt
   466  POST /users: Erreur : 500 - (mysql.connector.errors.DatabaseError) 1040 (HY000): Too many connections
   299  POST /users: Erreur : 500 - (mysql.connector.errors.OperationalError) 1040 (08004): Too many connections
   208  DELETE /users/[id]: Erreur : 500 - (mysql.connector.errors.DatabaseError) 1040 (HY000): Too many connections
   172  DELETE /users/[id]: Erreur : 500 - (mysql.connector.errors.OperationalError) 1040 (08004): Too many connections
   ```
   Le goulot est le même qu'aux labos 4 et 5 : `get_sqlalchemy_session()` crée un nouvel engine (et un nouveau pool de connexions) à chaque appel. Kafka n'y est pour rien. Cette fois, MySQL est revenu à la normale après le test (`Threads_connected = 7`, et un nouveau `POST /users` répond `201`).
4. **Coolriel suit la charge sans difficulté.** À la fin du test, le groupe `coolriel-group` n'a **aucun retard** (`LAG = 0`) : les 3 305 événements du topic `user-events` ont tous produit un courriel (3 305 fichiers dans `output/`), avec moins de 7 % de CPU :
   ```txt
   $ kafka-consumer-groups --describe --group coolriel-group
   GROUP           TOPIC        PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
   coolriel-group  user-events  0          3305            3305            0
   ```
   Même si Coolriel avait été plus lent, il n'aurait pas ralenti le Store Manager : les événements se seraient accumulés dans Kafka (le `LAG` aurait augmenté) et auraient été traités ensuite. C'est l'avantage du découplage asynchrone.
5. **La base de données reste cohérente** : 5 utilisateurs avant le test, + 3 300 créations réussies, − 1 497 suppressions réussies = **1 808 utilisateurs** après, ce qui correspond exactement au compte dans MySQL.
6. **Petite incohérence entre MySQL et Kafka** : 3 303 événements `UserCreated` ont été publiés pendant le test, alors que Locust ne compte que 3 300 créations réussies. Pour quelques requêtes, l'erreur 500 est arrivée **après** le `commit` et l'envoi à Kafka : le client a reçu une erreur, mais l'utilisateur existe et son courriel a été généré.
7. Sur cette branche, `delete_user` ne publie pas encore `UserDeleted` (c'est fait dans `labo7-a2-a3-users`). Les suppressions ne génèrent donc pas de courriel d'au revoir dans ce test.

## Pistes d'amélioration

- Un seul engine SQLAlchemy global avec un pool de taille fixe (`pool_size`, `max_overflow`) : c'est la vraie limite de ce test.
- Un serveur WSGI de production (gunicorn avec plusieurs workers) au lieu du serveur de développement Flask.
- Pour une garantie forte entre MySQL et Kafka, le patron Outbox (labo 8) : enregistrer l'événement dans la même transaction que l'utilisateur, puis le publier.
