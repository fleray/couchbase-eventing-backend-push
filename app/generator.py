"""Générateur de documents pour la démo.

Toutes les MIN_DELAY_SEC à MAX_DELAY_SEC secondes (aléatoire), crée un document
"agent_event" dans bucket.scope.collection. En plus, avec une certaine probabilité,
met à jour ou supprime un document existant afin de montrer tous les types d'événements.
"""

import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.exceptions import CouchbaseException, DocumentNotFoundException
from couchbase.options import ClusterOptions

HOST = os.environ.get("CB_HOST", "couchbase")
USERNAME = os.environ.get("CB_USERNAME", "Administrator")
PASSWORD = os.environ.get("CB_PASSWORD", "password")
BUCKET = os.environ.get("CB_BUCKET", "demo_event")
SCOPE = os.environ.get("CB_SCOPE", "app")
COLLECTION = os.environ.get("CB_COLLECTION", "agent_event")

MIN_DELAY_SEC = float(os.environ.get("MIN_DELAY_SEC", 1))
MAX_DELAY_SEC = float(os.environ.get("MAX_DELAY_SEC", 5))
UPDATE_PROBABILITY = float(os.environ.get("UPDATE_PROBABILITY", 0.3))
DELETE_PROBABILITY = float(os.environ.get("DELETE_PROBABILITY", 0.1))

AGENTS = ["alice", "bruno", "chloe", "david", "emma", "farid", "gaelle", "hugo"]
EVENT_TYPES = ["call_started", "chat_started", "email_received", "ticket_opened", "callback_requested"]
CHANNELS = {"call_started": "phone", "callback_requested": "phone", "chat_started": "chat",
            "email_received": "email", "ticket_opened": "web"}
QUEUES = ["support", "billing", "sales", "vip"]
STATUS_FLOW = ["new", "in_progress", "on_hold", "resolved"]

running = True


def stop(*_):
    global running
    running = False


signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def connect():
    """Connexion avec réessais : le cluster peut encore finir de démarrer."""
    for attempt in range(1, 31):
        try:
            cluster = Cluster(f"couchbase://{HOST}",
                              ClusterOptions(PasswordAuthenticator(USERNAME, PASSWORD)))
            cluster.wait_until_ready(timedelta(seconds=20))
            return cluster.bucket(BUCKET).scope(SCOPE).collection(COLLECTION)
        except CouchbaseException as e:
            print(f"Connexion à Couchbase impossible (tentative {attempt}) : {e}", flush=True)
            time.sleep(3)
    sys.exit("Couchbase injoignable, abandon.")


def new_event():
    event_type = random.choice(EVENT_TYPES)
    return {
        "type": "agent_event",
        "agent": random.choice(AGENTS),
        "event": event_type,
        "channel": CHANNELS[event_type],
        "queue": random.choice(QUEUES),
        "priority": random.choice(["low", "normal", "normal", "high"]),
        "status": "new",
        "created_at": now(),
    }


def main():
    collection = connect()
    print(f"Connecté à {BUCKET}.{SCOPE}.{COLLECTION}, génération toutes les "
          f"{MIN_DELAY_SEC:g}-{MAX_DELAY_SEC:g} s", flush=True)
    live_keys = []

    while running:
        try:
            key = f"agent_event::{uuid.uuid4().hex[:12]}"
            doc = new_event()
            collection.insert(key, doc)
            live_keys.append(key)
            print(f"+ {key} {doc['agent']} {doc['event']} ({doc['queue']})", flush=True)

            if live_keys and random.random() < UPDATE_PROBABILITY:
                key = random.choice(live_keys)
                current = collection.get(key).content_as[dict]
                idx = STATUS_FLOW.index(current.get("status", "new"))
                current["status"] = STATUS_FLOW[min(idx + 1, len(STATUS_FLOW) - 1)]
                current["updated_at"] = now()
                collection.replace(key, current)
                print(f"~ {key} -> {current['status']}", flush=True)

            if len(live_keys) > 3 and random.random() < DELETE_PROBABILITY:
                key = live_keys.pop(random.randrange(len(live_keys)))
                collection.remove(key)
                print(f"- {key}", flush=True)

        except DocumentNotFoundException:
            pass
        except CouchbaseException as e:
            print(f"Erreur Couchbase : {e}", flush=True)

        time.sleep(random.uniform(MIN_DELAY_SEC, MAX_DELAY_SEC))

    print("Arrêt du générateur.", flush=True)


if __name__ == "__main__":
    main()
