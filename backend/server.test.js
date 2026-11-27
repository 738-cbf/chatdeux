process.env.DISABLE_MESSAGE_PERSISTENCE = '1';
process.env.ADMIN_TOKEN = 'admin-test-token';
process.env.WEBHOOK_SECRET = 'webhook-test-token';
process.env.SENDER_HASH_SALT = 'sender-test-salt';
process.env.MESSAGE_RATE_LIMIT_WINDOW_MS = '60000';
process.env.MESSAGE_RATE_LIMIT_MAX = '100';
process.env.WEBHOOK_RATE_LIMIT_WINDOW_MS = '60000';
process.env.WEBHOOK_RATE_LIMIT_MAX = '5';

const test = require('node:test');
const assert = require('node:assert/strict');
const request = require('supertest');
const WebSocket = require('ws');

const { app, server, shutdown, resetStateForTests, constants } = require('./server');

function waitFor(condition, timeoutMs = 3000) {
    const start = Date.now();
    return new Promise((resolve, reject) => {
        const tick = () => {
            if (condition()) {
                resolve();
                return;
            }

            if (Date.now() - start > timeoutMs) {
                reject(new Error('Timed out waiting for condition'));
                return;
            }

            setTimeout(tick, 20);
        };

        tick();
    });
}

function postMessage(content, sourceUrl = 'https://example.com/page') {
    return request(app)
        .post('/messages')
        .send({
            content,
            sourceUrl,
            sourceSite: 'example.com'
        });
}

test.before(async () => {
    await new Promise((resolve) => {
        server.listen(0, '127.0.0.1', resolve);
    });
});

test.beforeEach(() => {
    resetStateForTests();
});

test.after(async () => {
    if (server.listening) {
        await new Promise((resolve) => {
            server.once('close', resolve);
            shutdown('test-suite');
        });
    }
});

test('DELETE /messages requires admin token', async () => {
    await request(app).delete('/messages').expect(401);

    await request(app)
        .delete('/messages')
        .set('x-admin-token', 'admin-test-token')
        .expect(200);
});

test('POST /webhook/message requires webhook secret', async () => {
    await request(app)
        .post('/webhook/message')
        .send({ content: 'x', sourceUrl: 'https://example.com' })
        .expect(401);

    await request(app)
        .post('/webhook/message')
        .set('x-webhook-secret', 'webhook-test-token')
        .send({ content: 'x', sourceUrl: 'https://example.com' })
        .expect(200);
});

test('POST /messages rejects non-http(s) source URLs', async () => {
    const res = await postMessage('Hello', 'javascript:alert(1)').expect(400);
    assert.match(res.body.error, /sourceUrl/i);
});

test('POST /messages returns senderTag and does not expose userIP', async () => {
    const res = await postMessage('privacy check').expect(201);
    assert.equal(typeof res.body.message.senderTag, 'string');
    assert.equal(res.body.message.senderTag.length, 16);
    assert.equal('userIP' in res.body.message, false);

    const list = await request(app).get('/messages').expect(200);
    assert.equal(list.body.length, 1);
    assert.equal('userIP' in list.body[0], false);
    assert.equal(typeof list.body[0].senderTag, 'string');
});

test('POST /messages uses clientId to vary senderTag behind shared proxy IP', async () => {
    const one = await request(app)
        .post('/messages')
        .send({
            content: 'same-ip-1',
            sourceUrl: 'https://example.com/a',
            sourceSite: 'example.com',
            clientId: 'browser-a'
        })
        .expect(201);

    const two = await request(app)
        .post('/messages')
        .send({
            content: 'same-ip-2',
            sourceUrl: 'https://example.com/b',
            sourceSite: 'example.com',
            clientId: 'browser-b'
        })
        .expect(201);

    assert.notEqual(one.body.message.senderTag, two.body.message.senderTag);
});

test('POST /messages enforces write rate limiting', async () => {
    for (let i = 0; i < 100; i += 1) {
        await postMessage(`msg-${i}`).expect(201);
    }

    await postMessage('msg-101').expect(429);
});

test('server retains max 50 messages and emits prune sync event', async () => {
    const { port } = server.address();
    const ws = new WebSocket(`ws://127.0.0.1:${port}`);
    const events = [];

    ws.on('message', (data) => {
        events.push(JSON.parse(String(data)));
    });

    await new Promise((resolve, reject) => {
        ws.once('open', resolve);
        ws.once('error', reject);
    });

    for (let i = 0; i < constants.MAX_MESSAGES + 1; i += 1) {
        await postMessage(`bulk-${i}`).expect(201);
    }

    await waitFor(() => events.some((evt) => evt.type === 'messages_pruned'));

    const list = await request(app).get('/messages').expect(200);
    assert.equal(list.body.length, constants.MAX_MESSAGES);
    assert.equal(list.body[0].content, 'bulk-1');

    ws.close();
});

test('DELETE /messages emits clear_messages event for websocket clients', async () => {
    const { port } = server.address();
    const ws = new WebSocket(`ws://127.0.0.1:${port}`);
    const events = [];

    ws.on('message', (data) => {
        events.push(JSON.parse(String(data)));
    });

    await new Promise((resolve, reject) => {
        ws.once('open', resolve);
        ws.once('error', reject);
    });

    await postMessage('to-be-cleared').expect(201);

    await request(app)
        .delete('/messages')
        .set('x-admin-token', 'admin-test-token')
        .expect(200);

    await waitFor(() => events.some((evt) => evt.type === 'clear_messages'));

    const list = await request(app).get('/messages').expect(200);
    assert.equal(list.body.length, 0);

    ws.close();
});
