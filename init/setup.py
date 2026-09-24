"""Initialise Couchbase pour la démo, via l'API REST uniquement (stdlib Python).

1. Initialisation du cluster (services data, index, query, eventing)
2. Bucket, scope applicatif + collection, scope technique pour Eventing
3. Création et déploiement de la fonction Eventing (code lu depuis push_to_backend.js)

Idempotent : chaque étape vérifie l'existant, on peut relancer le conteneur sans risque.
"""

import base64
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

HOST = os.environ.get("CB_HOST", "couchbase")
USERNAME = os.environ.get("CB_USERNAME", "Administrator")
PASSWORD = os.environ.get("CB_PASSWORD", "password")
BUCKET = os.environ.get("CB_BUCKET", "demo_event")
SCOPE = os.environ.get("CB_SCOPE", "app")
COLLECTION = os.environ.get("CB_COLLECTION", "agent_event")

EVENTING_SCOPE = "eventing"
METADATA_COLLECTION = "metadata"
DLQ_COLLECTION = "dlq"

FUNCTION_NAME = "push_to_backend"
FUNCTION_FILE = os.environ.get("EVENTING_FUNCTION_FILE", "/eventing/push_to_backend.js")
BACKEND_URL = os.environ.get("BACKEND_URL", "http://backend:3000")
HOOK_TOKEN = os.environ.get("HOOK_TOKEN", "demo-secret")

MGMT = f"http://{HOST}:8091"
EVENTING = f"http://{HOST}:8096"


# --- HTTP ---------------------------------------------------------------------

def request(method, url, form=None, json_body=None, auth=True):
    """Retourne (status, body). Ne lève pas d'exception sur les codes HTTP d'erreur."""
    headers = {}
    data = None
    if form is not None:
        data = urllib.parse.urlencode(form).encode()
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    elif json_body is not None:
        data = json.dumps(json_body).encode()
        headers["Content-Type"] = "application/json"
    if auth:
        token = base64.b64encode(f"{USERNAME}:{PASSWORD}".encode()).decode()
        headers["Authorization"] = f"Basic {token}"

    req = urllib.request.Request(url, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as res:
            return res.status, res.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()
    except (urllib.error.URLError, ConnectionError, TimeoutError) as e:
        return 0, str(e)


def expect(label, status, body, ok=(200, 202)):
    if status not in ok:
        sys.exit(f"✗ {label} : HTTP {status} {body}")
    print(f"✓ {label}")


def retry(label, fn, attempts=60, delay=2):
    """Réessaie fn() -> (ok, detail) tant que le service n'est pas prêt."""
    detail = ""
    for _ in range(attempts):
        ok, detail = fn()
        if ok:
            print(f"✓ {label}")
            return
        time.sleep(delay)
    sys.exit(f"✗ {label} : {detail}")


# --- Étapes -------------------------------------------------------------------

def init_cluster():
    status, _ = request("GET", f"{MGMT}/pools/default")
    if status == 200:
        print("• Cluster déjà initialisé")
        return

    expect("Services data, index, query, eventing",
           *request("POST", f"{MGMT}/node/controller/setupServices",
                    form={"services": "kv,index,n1ql,eventing"}, auth=False))
    expect("Quotas mémoire",
           *request("POST", f"{MGMT}/pools/default",
                    form={"memoryQuota": 512, "indexMemoryQuota": 256, "eventingMemoryQuota": 256},
                    auth=False))
    expect("Stockage des index",
           *request("POST", f"{MGMT}/settings/indexes", form={"storageMode": "plasma"}, auth=False))
    expect("Identifiants administrateur",
           *request("POST", f"{MGMT}/settings/web",
                    form={"username": USERNAME, "password": PASSWORD, "port": "SAME"}, auth=False))


def create_bucket():
    status, _ = request("GET", f"{MGMT}/pools/default/buckets/{BUCKET}")
    if status == 200:
        print(f"• Bucket {BUCKET} déjà présent")
    else:
        expect(f"Bucket {BUCKET}",
               *request("POST", f"{MGMT}/pools/default/buckets",
                        form={"name": BUCKET, "bucketType": "couchbase", "ramQuota": 256,
                              "flushEnabled": 1}))

    def bucket_ready():
        status, body = request("GET", f"{MGMT}/pools/default/buckets/{BUCKET}")
        if status != 200:
            return False, body
        nodes = json.loads(body).get("nodes", [])
        return bool(nodes) and all(n.get("status") == "healthy" for n in nodes), "bucket en attente"

    retry(f"Bucket {BUCKET} opérationnel", bucket_ready)


def create_scope(scope):
    status, body = request("POST", f"{MGMT}/pools/default/buckets/{BUCKET}/scopes",
                           form={"name": scope})
    if status == 400 and "already exists" in body:
        print(f"• Scope {scope} déjà présent")
    else:
        expect(f"Scope {BUCKET}.{scope}", status, body)


def create_collection(scope, collection):
    status, body = request("POST",
                           f"{MGMT}/pools/default/buckets/{BUCKET}/scopes/{scope}/collections",
                           form={"name": collection})
    if status == 400 and "already exists" in body:
        print(f"• Collection {scope}.{collection} déjà présente")
    else:
        expect(f"Collection {BUCKET}.{scope}.{collection}", status, body)


def keyspace(scope, collection, **extra):
    return {"bucket_name": BUCKET, "scope_name": scope, "collection_name": collection, **extra}


def deploy_eventing_function():
    with open(FUNCTION_FILE, encoding="utf-8") as f:
        appcode = f.read()

    definition = {
        "appname": FUNCTION_NAME,
        "appcode": appcode,
        "function_scope": {"bucket": "*", "scope": "*"},
        "depcfg": {
            "source_bucket": BUCKET,
            "source_scope": SCOPE,
            "source_collection": COLLECTION,
            "metadata_bucket": BUCKET,
            "metadata_scope": EVENTING_SCOPE,
            "metadata_collection": METADATA_COLLECTION,
            "buckets": [
                {"alias": "src", "access": "r", **keyspace(SCOPE, COLLECTION)},
                {"alias": "dlq", "access": "rw", **keyspace(EVENTING_SCOPE, DLQ_COLLECTION)},
            ],
            "curl": [{
                "hostname": BACKEND_URL,
                "value": "backend",
                "auth_type": "bearer",
                "bearer_key": HOOK_TOKEN,
                "allow_cookies": False,
                "validate_ssl_certificate": False,
            }],
        },
        "settings": {
            "dcp_stream_boundary": "from_now",
            "deployment_status": False,
            "processing_status": False,
            "worker_count": 1,
            "log_level": "INFO",
            "description": "Pousse les mutations de agent_event vers le backend SSE (avec relances)",
        },
    }

    status, _ = request("GET", f"{EVENTING}/api/v1/functions/{FUNCTION_NAME}")
    if status == 200:
        print(f"• Fonction {FUNCTION_NAME} déjà présente")
    else:
        # Le service Eventing peut mettre quelques secondes à voir les nouvelles collections.
        def create():
            s, body = request("POST", f"{EVENTING}/api/v1/functions/{FUNCTION_NAME}",
                              json_body=definition)
            return s == 200, f"HTTP {s} {body}"
        retry(f"Fonction Eventing {FUNCTION_NAME} créée", create)

    if function_status() == "deployed":
        print(f"• Fonction {FUNCTION_NAME} déjà déployée")
        return

    def deploy():
        s, body = request("POST", f"{EVENTING}/api/v1/functions/{FUNCTION_NAME}/deploy")
        already = s != 200 and "already" in body.lower()
        return s == 200 or already, f"HTTP {s} {body}"
    retry("Déploiement demandé", deploy)

    retry(f"Fonction {FUNCTION_NAME} déployée",
          lambda: (function_status() == "deployed", f"statut : {function_status()}"),
          attempts=90)


def function_status():
    status, body = request("GET", f"{EVENTING}/api/v1/status/{FUNCTION_NAME}")
    if status != 200:
        return None
    return json.loads(body).get("app", {}).get("composite_status")


def main():
    retry("Couchbase joignable",
          lambda: (request("GET", f"{MGMT}/ui/index.html", auth=False)[0] == 200, "pas encore prêt"))
    init_cluster()
    retry("Authentification administrateur",
          lambda: (request("GET", f"{MGMT}/pools/default")[0] == 200, "cluster pas prêt"))
    create_bucket()
    create_scope(SCOPE)
    create_collection(SCOPE, COLLECTION)
    create_scope(EVENTING_SCOPE)
    create_collection(EVENTING_SCOPE, METADATA_COLLECTION)
    create_collection(EVENTING_SCOPE, DLQ_COLLECTION)
    deploy_eventing_function()
    print(f"\n✅ Couchbase prêt : {BUCKET}.{SCOPE}.{COLLECTION} -> Eventing -> {BACKEND_URL}/hook")


if __name__ == "__main__":
    main()
