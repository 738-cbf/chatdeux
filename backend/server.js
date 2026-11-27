const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const cors = require('cors');
const axios = require('axios');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const MAX_MESSAGES = 50;
const MAX_MESSAGE_LENGTH = 1000;
const PORT = Number(process.env.PORT) || 3000;
const HOST = process.env.HOST || '127.0.0.1';
const MESSAGE_STORE_PATH = process.env.MESSAGE_STORE_PATH || path.join(__dirname, 'messages.json');
const MESSAGE_STORE_ENABLED = process.env.DISABLE_MESSAGE_PERSISTENCE !== '1';
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || '';
const WEBHOOK_SECRET = process.env.WEBHOOK_SECRET || '';
const SENDER_HASH_SALT = process.env.SENDER_HASH_SALT || 'change-me-chatdeux-salt';
const MESSAGE_RATE_LIMIT_WINDOW_MS = Number(process.env.MESSAGE_RATE_LIMIT_WINDOW_MS) || 60000;
const MESSAGE_RATE_LIMIT_MAX = Number(process.env.MESSAGE_RATE_LIMIT_MAX) || 20;
const WEBHOOK_RATE_LIMIT_WINDOW_MS = Number(process.env.WEBHOOK_RATE_LIMIT_WINDOW_MS) || 60000;
const WEBHOOK_RATE_LIMIT_MAX = Number(process.env.WEBHOOK_RATE_LIMIT_MAX) || 60;

let messagePersistTimer = null;
let shuttingDown = false;
let messages = [];

const clients = new Set();
const rateBuckets = new Map();

const WEBHOOK_ENDPOINTS = [
    // 'https://your-webhook-endpoint.com/chat-update',
    // 'https://another-webhook.com/receive-message'
];

function parseTrustProxy(value) {
    if (!value) {
        // Safe default for reverse-proxy deployments on the same host.
        return 'loopback';
    }

    const normalized = String(value).trim().toLowerCase();

    if (['true', '1', 'yes'].includes(normalized)) {
        return true;
    }

    if (['false', '0', 'no'].includes(normalized)) {
        return false;
    }

    if (/^\d+$/.test(normalized)) {
        return Number(normalized);
    }

    if (['loopback', 'linklocal', 'uniquelocal'].includes(normalized)) {
        return normalized;
    }

    return false;
}

const trustProxySetting = parseTrustProxy(process.env.TRUST_PROXY);
app.set('trust proxy', trustProxySetting);

app.use(cors());
app.use(express.json({ limit: '20kb' }));

app.use((err, req, res, next) => {
    if (err instanceof SyntaxError && err.status === 400 && 'body' in err) {
        return res.status(400).json({ error: 'Invalid JSON body' });
    }
    return next(err);
});

function stripIPv6Prefix(ip) {
    if (typeof ip !== 'string') {
        return 'unknown';
    }

    if (ip.startsWith('::ffff:')) {
        return ip.slice(7);
    }

    return ip;
}

function getClientIP(req) {
    return stripIPv6Prefix(
        req.ip ||
        req.socket?.remoteAddress ||
        req.connection?.remoteAddress ||
        'unknown'
    );
}

function extractBearerToken(req) {
    const header = req.headers.authorization;
    if (typeof header !== 'string') {
        return null;
    }

    const match = header.match(/^Bearer\s+(.+)$/i);
    if (!match) {
        return null;
    }

    return match[1].trim();
}

function getToken(req, customHeaderName) {
    const bearer = extractBearerToken(req);
    if (bearer) {
        return bearer;
    }

    const directHeader = req.headers[customHeaderName];
    return typeof directHeader === 'string' ? directHeader.trim() : null;
}

function requireAdminToken(req, res, next) {
    if (!ADMIN_TOKEN) {
        return res.status(503).json({ error: 'Admin operations disabled: ADMIN_TOKEN is not configured' });
    }

    const token = getToken(req, 'x-admin-token');
    if (!token || token !== ADMIN_TOKEN) {
        return res.status(401).json({ error: 'Unauthorized' });
    }

    return next();
}

function requireWebhookSecret(req, res, next) {
    if (!WEBHOOK_SECRET) {
        return res.status(503).json({ error: 'Webhook ingestion disabled: WEBHOOK_SECRET is not configured' });
    }

    const token = getToken(req, 'x-webhook-secret');
    if (!token || token !== WEBHOOK_SECRET) {
        return res.status(401).json({ error: 'Unauthorized webhook request' });
    }

    return next();
}

function makeRateLimitKey(req, namespace) {
    return `${namespace}:${getClientIP(req)}`;
}

function createRateLimiter({ windowMs, max, namespace }) {
    return (req, res, next) => {
        const now = Date.now();
        const key = makeRateLimitKey(req, namespace);
        const bucket = rateBuckets.get(key) || [];
        const fresh = bucket.filter((ts) => now - ts < windowMs);

        if (fresh.length >= max) {
            return res.status(429).json({
                error: 'Rate limit exceeded',
                retryAfterMs: windowMs
            });
        }

        fresh.push(now);
        rateBuckets.set(key, fresh);
        return next();
    };
}

function normalizeContent(content) {
    if (typeof content !== 'string') {
        return null;
    }

    const trimmed = content.trim();
    if (!trimmed) {
        return null;
    }

    return trimmed.slice(0, MAX_MESSAGE_LENGTH);
}

function normalizeSourceUrl(sourceUrl) {
    if (typeof sourceUrl !== 'string') {
        return null;
    }

    try {
        const parsed = new URL(sourceUrl);
        if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
            return null;
        }
        return parsed.toString();
    } catch (error) {
        return null;
    }
}

function normalizeClientId(clientId) {
    if (typeof clientId !== 'string') {
        return null;
    }

    const normalized = clientId.trim();
    if (!normalized) {
        return null;
    }

    if (normalized.length > 128) {
        return null;
    }

    if (!/^[a-zA-Z0-9._:-]+$/.test(normalized)) {
        return null;
    }

    return normalized;
}

function resolveSourceSite(sourceSite, sourceUrl) {
    if (typeof sourceSite === 'string' && sourceSite.trim()) {
        return sourceSite.trim().slice(0, 200);
    }

    try {
        return new URL(sourceUrl).hostname;
    } catch (error) {
        return 'unknown';
    }
}

function makeSenderTag(seed) {
    return crypto
        .createHash('sha256')
        .update(`${SENDER_HASH_SALT}:${seed}`)
        .digest('hex')
        .slice(0, 16);
}

function createMessage({ content, sourceUrl, sourceSite, senderSeed, external = false }) {
    return {
        id: crypto.randomUUID(),
        content,
        sourceUrl,
        sourceSite,
        timestamp: new Date().toISOString(),
        senderTag: makeSenderTag(senderSeed),
        external
    };
}

function normalizeStoredMessage(raw) {
    if (!raw || typeof raw !== 'object') {
        return null;
    }

    const content = normalizeContent(raw.content);
    const sourceUrl = normalizeSourceUrl(raw.sourceUrl);
    const sourceSite = resolveSourceSite(raw.sourceSite, sourceUrl || '');
    const senderTag = typeof raw.senderTag === 'string' && raw.senderTag.trim() ? raw.senderTag.trim().slice(0, 64) : null;

    if (!content || !sourceUrl || !senderTag) {
        return null;
    }

    return {
        id: typeof raw.id === 'string' && raw.id.trim() ? raw.id : crypto.randomUUID(),
        content,
        sourceUrl,
        sourceSite,
        timestamp: typeof raw.timestamp === 'string' ? raw.timestamp : new Date().toISOString(),
        senderTag,
        external: Boolean(raw.external)
    };
}

function loadMessagesFromDisk() {
    if (!MESSAGE_STORE_ENABLED) {
        return [];
    }

    try {
        if (!fs.existsSync(MESSAGE_STORE_PATH)) {
            return [];
        }

        const parsed = JSON.parse(fs.readFileSync(MESSAGE_STORE_PATH, 'utf8'));
        if (!Array.isArray(parsed)) {
            return [];
        }

        return parsed
            .map(normalizeStoredMessage)
            .filter(Boolean)
            .slice(-MAX_MESSAGES);
    } catch (error) {
        console.error('Failed to load message store:', error.message);
        return [];
    }
}

function persistMessagesNow() {
    if (!MESSAGE_STORE_ENABLED) {
        return;
    }

    try {
        fs.writeFileSync(MESSAGE_STORE_PATH, JSON.stringify(messages, null, 2), 'utf8');
    } catch (error) {
        console.error('Failed to persist message store:', error.message);
    }
}

function queuePersistMessages() {
    if (!MESSAGE_STORE_ENABLED) {
        return;
    }

    if (messagePersistTimer) {
        clearTimeout(messagePersistTimer);
    }

    messagePersistTimer = setTimeout(() => {
        messagePersistTimer = null;
        persistMessagesNow();
    }, 150);
}

messages = loadMessagesFromDisk();

function broadcastEvent(type, payload = {}) {
    const serialized = JSON.stringify({ type, ...payload });

    clients.forEach((client) => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(serialized);
        }
    });
}

function appendMessage(message) {
    messages.push(message);

    let pruned = false;
    if (messages.length > MAX_MESSAGES) {
        messages = messages.slice(-MAX_MESSAGES);
        pruned = true;
    }

    queuePersistMessages();
    return pruned;
}

async function sendWebhooks(message) {
    const webhookPromises = WEBHOOK_ENDPOINTS.map(async (endpoint) => {
        try {
            await axios.post(endpoint, {
                type: 'new_message',
                message,
                timestamp: new Date().toISOString()
            }, {
                timeout: 5000,
                headers: {
                    'Content-Type': 'application/json'
                }
            });
            console.log(`Webhook sent to ${endpoint}`);
        } catch (error) {
            console.error(`Failed to send webhook to ${endpoint}:`, error.message);
        }
    });

    Promise.allSettled(webhookPromises);
}

const messageRateLimiter = createRateLimiter({
    windowMs: MESSAGE_RATE_LIMIT_WINDOW_MS,
    max: MESSAGE_RATE_LIMIT_MAX,
    namespace: 'message'
});

const webhookRateLimiter = createRateLimiter({
    windowMs: WEBHOOK_RATE_LIMIT_WINDOW_MS,
    max: WEBHOOK_RATE_LIMIT_MAX,
    namespace: 'webhook'
});

wss.on('connection', (ws) => {
    ws.isAlive = true;
    clients.add(ws);

    ws.on('pong', () => {
        ws.isAlive = true;
    });

    ws.on('close', () => {
        clients.delete(ws);
    });

    ws.on('error', () => {
        clients.delete(ws);
    });
});

const heartbeatInterval = setInterval(() => {
    clients.forEach((client) => {
        if (client.isAlive === false) {
            clients.delete(client);
            client.terminate();
            return;
        }

        if (client.readyState !== WebSocket.OPEN) {
            clients.delete(client);
            return;
        }

        client.isAlive = false;
        try {
            client.ping();
        } catch (error) {
            clients.delete(client);
        }
    });
}, 30000);

const rateBucketCleanupInterval = setInterval(() => {
    const now = Date.now();
    const longestWindow = Math.max(MESSAGE_RATE_LIMIT_WINDOW_MS, WEBHOOK_RATE_LIMIT_WINDOW_MS);

    rateBuckets.forEach((bucket, key) => {
        const fresh = bucket.filter((ts) => now - ts < longestWindow);
        if (fresh.length === 0) {
            rateBuckets.delete(key);
        } else {
            rateBuckets.set(key, fresh);
        }
    });
}, 60000);

app.get('/messages', (req, res) => {
    res.json(messages);
});

app.post('/messages', messageRateLimiter, (req, res) => {
    const content = normalizeContent(req.body?.content);
    const sourceUrl = normalizeSourceUrl(req.body?.sourceUrl);
    const sourceSite = resolveSourceSite(req.body?.sourceSite, sourceUrl || '');
    const clientId = normalizeClientId(req.body?.clientId);

    if (!content || !sourceUrl) {
        return res.status(400).json({ error: 'Content and sourceUrl (http/https) are required' });
    }

    const senderSeed = clientId ? `client:${clientId}` : `ip:${getClientIP(req)}`;
    const message = createMessage({
        content,
        sourceUrl,
        sourceSite,
        senderSeed,
        external: false
    });

    const pruned = appendMessage(message);
    broadcastEvent('new_message', { message });

    if (pruned) {
        broadcastEvent('messages_pruned', { total: messages.length, maxMessages: MAX_MESSAGES });
    }

    if (WEBHOOK_ENDPOINTS.length > 0) {
        sendWebhooks(message);
    }

    return res.status(201).json({ success: true, message });
});

app.post('/webhook/message', requireWebhookSecret, webhookRateLimiter, (req, res) => {
    const content = normalizeContent(req.body?.content);
    const sourceUrl = normalizeSourceUrl(req.body?.sourceUrl);
    const sourceSite = resolveSourceSite(req.body?.sourceSite, sourceUrl || '');

    if (!content || !sourceUrl) {
        return res.status(400).json({ error: 'Content and sourceUrl (http/https) are required' });
    }

    const senderSeed = `external:${sourceSite}:${sourceUrl}`;
    const message = createMessage({
        content,
        sourceUrl,
        sourceSite,
        senderSeed,
        external: true
    });

    const pruned = appendMessage(message);
    broadcastEvent('new_message', { message });

    if (pruned) {
        broadcastEvent('messages_pruned', { total: messages.length, maxMessages: MAX_MESSAGES });
    }

    return res.status(200).json({ success: true });
});

app.get('/health', (req, res) => {
    res.json({
        status: 'ok',
        messages: messages.length,
        clients: clients.size,
        webhooks: WEBHOOK_ENDPOINTS.length,
        maxMessages: MAX_MESSAGES,
        persistenceEnabled: MESSAGE_STORE_ENABLED,
        trustProxy: trustProxySetting
    });
});

app.delete('/messages', requireAdminToken, (req, res) => {
    messages = [];
    queuePersistMessages();
    broadcastEvent('clear_messages');
    return res.json({ success: true });
});

function startServer() {
    return new Promise((resolve) => {
        server.listen(PORT, HOST, () => {
            console.log(`ChatDeux server running on ${HOST}:${PORT}`);
            console.log(`WebSocket server running on ws://${HOST}:${PORT}`);
            console.log(`Configured webhook endpoints: ${WEBHOOK_ENDPOINTS.length}`);
            console.log(`Message persistence: ${MESSAGE_STORE_ENABLED ? MESSAGE_STORE_PATH : 'disabled'}`);
            resolve();
        });
    });
}

function shutdown(signal, exitCode) {
    if (shuttingDown) {
        return;
    }
    shuttingDown = true;

    console.log(`Shutting down server (${signal})...`);

    clients.forEach((client) => {
        if (client.readyState === WebSocket.OPEN) {
            client.close(1001, 'Server shutting down');
        }
    });

    clearInterval(heartbeatInterval);
    clearInterval(rateBucketCleanupInterval);

    if (messagePersistTimer) {
        clearTimeout(messagePersistTimer);
        messagePersistTimer = null;
    }
    persistMessagesNow();

    wss.close(() => {
        server.close(() => {
            if (typeof exitCode === 'number') {
                process.exit(exitCode);
            }
        });
    });

    if (typeof exitCode === 'number') {
        setTimeout(() => {
            process.exit(1);
        }, 5000);
    }
}

function resetStateForTests() {
    messages = [];
    rateBuckets.clear();
    queuePersistMessages();
}

if (require.main === module) {
    startServer();

    process.on('SIGINT', () => shutdown('SIGINT', 0));
    process.on('SIGTERM', () => shutdown('SIGTERM', 0));

    process.on('uncaughtException', (error) => {
        console.error('Uncaught exception:', error);
        shutdown('uncaughtException', 1);
    });

    process.on('unhandledRejection', (reason) => {
        console.error('Unhandled rejection:', reason);
        shutdown('unhandledRejection', 1);
    });
}

module.exports = {
    app,
    server,
    startServer,
    shutdown,
    resetStateForTests,
    constants: {
        MAX_MESSAGES,
        MAX_MESSAGE_LENGTH
    }
};
