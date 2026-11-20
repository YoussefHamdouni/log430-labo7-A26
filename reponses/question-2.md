# Labo 07 – Question 2

> Quelles méthodes avez-vous modifiées dans `src/orders/commands/write_user.py`? Illustrez avec des captures d'écran ou des extraits de code.

Les modifications ont été faites dans le dépôt `log430-labo5` (Store Manager), branche `labo7-a2-a3-users`. Dans `src/orders/commands/write_user.py`, nous avons modifié les deux méthodes : `add_user` et `delete_user`.

## 1. `add_user` : accepter et enregistrer `user_type_id`

La méthode reçoit un nouveau paramètre `user_type_id`. Elle valide qu'il correspond à l'un des trois types de la table `user_types`, l'enregistre dans la colonne `user_type_id`, et l'ajoute au message `UserCreated` envoyé à Kafka. Coolriel peut ainsi adapter le courriel selon le type d'utilisateur.

```python
def add_user(name: str, email: str, user_type_id: int = 1):
    """Insert user with items in MySQL"""
    if not name or not email:
        raise ValueError("Cannot create user. A user must have name and email.")
    if user_type_id not in (1, 2, 3):
        raise ValueError("Cannot create user. user_type_id must be 1 (Client), 2 (Employee) or 3 (Manager).")

    session = get_sqlalchemy_session()

    try:
        new_user = User(name=name, email=email, user_type_id=user_type_id)
        session.add(new_user)
        session.flush()
        session.commit()

        user_event_producer = UserEventProducer()
        user_event_producer.get_instance().send('user-events', value={'event': 'UserCreated',
                                           'id': new_user.id,
                                           'name': new_user.name,
                                           'email': new_user.email,
                                           'user_type_id': new_user.user_type_id,
                                           'datetime': str(datetime.datetime.now())})
        return new_user.id
```

## 2. `delete_user` : publier l'événement `UserDeleted`

Avant, la méthode supprimait l'utilisateur sans prévenir les autres services (`# TODO: envoyer un evenement UserDeleted à Kafka`). Maintenant, elle copie les données de l'utilisateur **avant** la suppression (l'objet SQLAlchemy n'est plus utilisable après le commit), puis publie un événement `UserDeleted` dans le topic `user-events` :

```python
def delete_user(user_id: int):
    """Delete user in MySQL"""
    session = get_sqlalchemy_session()
    try:
        user = session.query(User).filter(User.id == user_id).first()
        if user:
            # Copier les données avant la suppression, l'objet n'est plus utilisable après le commit
            deleted_user = {'event': 'UserDeleted',
                            'id': user.id,
                            'name': user.name,
                            'email': user.email,
                            'user_type_id': user.user_type_id}
            session.delete(user)
            session.commit()

            deleted_user['datetime'] = str(datetime.datetime.now())
            UserEventProducer().get_instance().send('user-events', value=deleted_user)
            return 1
        else:
            return 0
```

L'événement n'est publié qu'après le `commit`. Si la suppression échoue, aucun événement n'est envoyé et Coolriel ne génère pas de courriel d'au revoir pour un utilisateur qui existe encore.

## Modifications associées (hors `write_user.py`)

Pour que `add_user` puisse enregistrer le type, trois autres fichiers ont été adaptés :

- `db-init/init.sql` : nouvelle table `user_types` (Client, Employee, Manager) et colonne `user_type_id` dans `users`, reliée par une `FOREIGN KEY` :

```sql
CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(150) NOT NULL UNIQUE,
    user_type_id INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_type_id) REFERENCES user_types(id) ON DELETE RESTRICT
);
```

- `src/orders/models/user.py` : ajout de la colonne dans le modèle SQLAlchemy :

```python
user_type_id = Column(Integer, nullable=False)
```

- `src/orders/controllers/user_controller.py` : lecture du champ dans le corps de la requête (Client par défaut, pour ne pas briser les requêtes existantes) :

```python
user_type_id = payload.get('user_type_id', 1)  # Client par défaut
user_id = add_user(name, email, user_type_id)
```

## Vérification

Création d'un utilisateur de chaque type, d'un utilisateur avec un type invalide, puis suppression de l'utilisateur 7 :

```
$ curl -X POST localhost:5000/users -d '{"name":"Test Type1","email":"type1@test.ca","user_type_id":1}'
{"user_id":6}
$ curl -X POST localhost:5000/users -d '{"name":"Test Type2","email":"type2@test.ca","user_type_id":2}'
{"user_id":7}
$ curl -X POST localhost:5000/users -d '{"name":"Test Type3","email":"type3@test.ca","user_type_id":3}'
{"user_id":8}
$ curl -X POST localhost:5000/users -d '{"name":"Bad","email":"bad@test.ca","user_type_id":7}'
{"error":"Cannot create user. user_type_id must be 1 (Client), 2 (Employee) or 3 (Manager)."}
$ curl -X DELETE localhost:5000/users/7
{"deleted":true}
```

Contenu de la base de données après les tests :

```
id  name            email                   type
1   Ada Lovelace    alovelace@example.com   Client
2   Adele Goldberg  agoldberg@example.com   Client
3   Alan Turing     aturing@example.com     Client
6   Test Type1      type1@test.ca           Client
4   Jane Doe        jdoe@magasinducoin.ca   Employee
5   Da Boss         dboss@magasinducoin.ca  Manager
8   Test Type3      type3@test.ca           Manager
```

Messages présents dans le topic `user-events` :

```
{"event": "UserCreated", "id": 6, "name": "Test Type1", "email": "type1@test.ca", "user_type_id": 1, "datetime": "2026-11-20 18:48:26.192192"}
{"event": "UserCreated", "id": 7, "name": "Test Type2", "email": "type2@test.ca", "user_type_id": 2, "datetime": "2026-11-20 18:48:26.404183"}
{"event": "UserCreated", "id": 8, "name": "Test Type3", "email": "type3@test.ca", "user_type_id": 3, "datetime": "2026-11-20 18:48:26.492145"}
{"event": "UserDeleted", "id": 7, "name": "Test Type2", "email": "type2@test.ca", "user_type_id": 2, "datetime": "2026-11-20 18:48:26.643892"}
```

Logs de Coolriel, qui reçoit bien l'événement `UserDeleted` :

```
UserEventConsumer - DEBUG - Evenement : UserDeleted
Handler - DEBUG - {'event': 'UserDeleted', 'id': 7, 'name': 'Test Type2', 'email': 'type2@test.ca', 'user_type_id': 2, 'datetime': '2026-11-20 18:48:26.643892'}
```
