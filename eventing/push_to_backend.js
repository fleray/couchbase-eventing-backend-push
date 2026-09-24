/*
 * Fonction Eventing Couchbase : pousse chaque mutation / suppression vers le
 * backend Node.js (POST /hook), qui relaie ensuite aux navigateurs en SSE.
 * En cas d'échec, l'envoi est retenté via des timers Eventing (backoff exponentiel).
 *
 * Réglages de la fonction (console Couchbase / Capella > Eventing) :
 *   - Source keyspace   : le bucket.scope.collection à surveiller
 *   - Metadata keyspace : une collection DÉDIÉE (jamais la source) ; les timers y sont persistés
 *   - Bindings :
 *       URL Alias     backend = https://api.monapp.com   (URL de base, sans /hook)
 *                     Auth Bearer, token = même valeur que HOOK_TOKEN côté Node.js
 *       Bucket Alias  src     = la collection source, en Read Only
 *                     (relecture de l'état courant du document au moment de la relance)
 *       Bucket Alias  dlq     = (optionnel) collection des événements abandonnés, en Read and Write
 *                     Ne JAMAIS pointer sur la collection source.
 *   - Feed boundary     : "From now" (sinon tout l'historique est rejoué au déploiement)
 *
 * Le token est porté par le binding : il n'apparaît pas dans le code.
 */

// Réglages. Eventing n'autorise que des fonctions dans l'espace global (pas de `var`).
function settings() {
    return {
        // Optionnel : ne relayer que certains documents (préfixe de clé). Vide = tout.
        keyPrefix: "",
        // Relances : 5s, 10s, 20s, 40s, ... plafonné à 5 min, abandon après maxAttempts envois.
        // Avec ces valeurs : ~20 min de relances avant abandon.
        maxAttempts: 8,
        baseDelaySec: 5,
        maxDelaySec: 300
    };
}

function OnUpdate(doc, meta) {
    if (!shouldForward(meta)) return;

    deliver({
        type: "mutation",
        id: meta.id,
        cas: meta.cas,
        expiration: meta.expiration,
        keyspace: meta.keyspace,
        doc: doc
    }, 1);
}

function OnDelete(meta, options) {
    if (!shouldForward(meta)) return;

    deliver({
        type: options.expired ? "expiration" : "deletion",
        id: meta.id,
        cas: meta.cas,
        keyspace: meta.keyspace
    }, 1);
}

/*
 * Callback des timers de relance. Le contexte ne contient que le strict nécessaire
 * (limite par défaut : 1 Ko) ; pour une mutation, le document est relu à ce moment-là
 * afin d'envoyer son état COURANT plutôt qu'une version périmée.
 */
function RetryCallback(context) {
    var payload;

    if (context.type === "mutation") {
        var res = couchbase.get(src, { id: context.id });
        if (!res.success) {
            // Document supprimé entre-temps : c'est l'événement de suppression qui fait foi.
            log("Relance abandonnée, document introuvable : " + context.id);
            return;
        }
        payload = {
            type: "mutation",
            id: context.id,
            cas: res.meta.cas,
            expiration: res.meta.expiration,
            keyspace: context.keyspace,
            doc: res.doc
        };
    } else {
        payload = {
            type: context.type,
            id: context.id,
            cas: context.cas,
            keyspace: context.keyspace
        };
    }

    deliver(payload, context.attempt);
}

function shouldForward(meta) {
    var prefix = settings().keyPrefix;
    return prefix === "" || meta.id.indexOf(prefix) === 0;
}

/** Tente un envoi ; en cas d'échec récupérable, programme une relance. */
function deliver(payload, attempt) {
    payload.attempt = attempt;
    var reference = "retry::" + payload.id;
    var result = send(payload);

    if (result.ok) {
        // Un succès rend obsolète toute relance encore en attente pour ce document.
        if (attempt === 1) cancelTimer(RetryCallback, reference);
        return;
    }

    if (!result.retryable) {
        log("Échec définitif (" + result.reason + ") pour " + payload.type + " " + payload.id);
        deadLetter(payload, result.reason);
        return;
    }

    var cfg = settings();
    if (attempt >= cfg.maxAttempts) {
        log("Abandon après " + attempt + " tentatives (" + result.reason + ") pour " + payload.type + " " + payload.id);
        deadLetter(payload, result.reason);
        return;
    }

    var delaySec = Math.min(cfg.baseDelaySec * Math.pow(2, attempt - 1), cfg.maxDelaySec);
    var fireAt = new Date();
    fireAt.setSeconds(fireAt.getSeconds() + delaySec);

    // Même référence = un seul timer par document : un nouvel événement remplace
    // la relance en attente (le plus récent gagne).
    createTimer(RetryCallback, fireAt, reference, {
        type: payload.type,
        id: payload.id,
        cas: payload.cas,
        keyspace: payload.keyspace,
        attempt: attempt + 1
    });
    log("Tentative " + attempt + " échouée (" + result.reason + "), relance dans " + delaySec + " s pour " + payload.type + " " + payload.id);
}

/** POST /hook. Retourne { ok, retryable, reason }. */
function send(payload) {
    var request = {
        path: "/hook",
        headers: { "Content-Type": "application/json" },
        body: payload // un objet JS est sérialisé en JSON par curl()
    };

    try {
        var response = curl("POST", backend, request);
        var status = response.status;
        if (status >= 200 && status < 300) return { ok: true };
        // 5xx, 408 (timeout) et 429 (rate limit) sont transitoires ; les autres 4xx
        // (401 token invalide, 400 payload refusé...) ne s'arrangeront pas en réessayant.
        var retryable = status >= 500 || status === 408 || status === 429;
        return { ok: false, retryable: retryable, reason: "HTTP " + status };
    } catch (e) {
        // Backend injoignable, DNS, TLS, timeout réseau...
        return { ok: false, retryable: true, reason: String(e) };
    }
}

/** Conserve l'événement abandonné si le binding optionnel `dlq` est configuré. */
function deadLetter(payload, reason) {
    if (typeof dlq === "undefined") return;
    var key = "dlq::" + payload.id + "::" + Date.now();
    dlq[key] = {
        payload: payload,
        reason: reason,
        failedAt: new Date().toISOString()
    };
}
