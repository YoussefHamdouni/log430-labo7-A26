"""
Kafka Historical User Event Consumer (Event Sourcing)
SPDX-License-Identifier: LGPL-3.0-or-later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""

import json
import os
from logger import Logger
from typing import Optional
from kafka import KafkaConsumer
from handlers.handler_registry import HandlerRegistry

class UserEventHistoryConsumer:
    """A consumer that starts reading Kafka events from the earliest point from a given topic"""
    
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        registry: HandlerRegistry,
        output_dir: str = "output",
        consumer_timeout_ms: int = 5000
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        # group_id distinct de celui de UserEventConsumer : sinon Kafka partagerait
        # la partition entre les deux consommateurs et chacun n'en lirait qu'une partie
        self.group_id = group_id
        self.registry = registry
        # earliest : lire la séquence depuis le premier message conservé par Kafka
        self.auto_offset_reset = "earliest"
        # s'arrêter 5 s après le dernier message, au lieu d'attendre indéfiniment
        self.consumer_timeout_ms = consumer_timeout_ms
        self.output_file = os.path.join(output_dir, "user_events_history.json")
        os.makedirs(output_dir, exist_ok=True)
        self.events = []
        self.consumer: Optional[KafkaConsumer] = None
        self.logger = Logger.get_instance("UserEventHistoryConsumer")
    
    def start(self) -> None:
        """Start consuming messages from Kafka"""
        self.logger.info(f"Démarrer un consommateur : {self.group_id}")
        
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset=self.auto_offset_reset,
                consumer_timeout_ms=self.consumer_timeout_ms,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                # Pas de commit des offsets : au prochain démarrage, le groupe n'a
                # aucune position enregistrée et relit donc tout l'historique
                enable_auto_commit=False
            )

            # La boucle se termine toute seule après consumer_timeout_ms sans nouveau message
            for message in self.consumer:
                event = message.value
                self.events.append(event)
                self.logger.debug(
                    f"Historique #{len(self.events)} : {event.get('event')} "
                    f"(partition {message.partition}, offset {message.offset})"
                )

            # Une seule écriture sur le disque, après la boucle (et non à chaque événement)
            with open(self.output_file, 'w', encoding='utf-8') as f:
                f.write(json.dumps(self.events, indent=2, ensure_ascii=False))

            self.logger.info(f"{len(self.events)} événement(s) historique(s) enregistré(s) dans {self.output_file}")
        except Exception as e:
            self.logger.error(f"Erreur: {e}", exc_info=True)
        finally:
            self.stop()

    def stop(self) -> None:
        """Stop the consumer gracefully"""
        if self.consumer:
            self.consumer.close()
            self.logger.info("Arrêter le consommateur!")
