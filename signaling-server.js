const http = require('http');
const { WebSocketServer } = require('ws');
const crypto = require('crypto');

const PORT = process.env.PORT || 8080;
const rooms = new Map(); // Map<roomId, { peers: Set<ws>, pin: string }>
const ipRegistry = new Map(); // Rate limiting: Map<ip, lastJoinTime>

const server = http.createServer((req, res) => {
    // Basic HTTP endpoint for room creation / status
    if (req.url === '/health') {
        res.writeHead(200);
        res.end('OK');
    } else {
        res.writeHead(404);
        res.end();
    }
});

const wss = new WebSocketServer({ server });

console.log(`NexusData Production Signaling Server active on port ${PORT}`);

wss.on('connection', (ws, req) => {
    const ip = req.socket.remoteAddress;
    let currentRoomId = null;

    ws.on('message', (data) => {
        try {
            const msg = JSON.parse(data);

            if (msg.type === 'join') {
                // 1. Rate Limiting check
                const now = Date.now();
                if (ipRegistry.has(ip) && (now - ipRegistry.get(ip) < 333)) { // ~3 per second
                    return ws.send(JSON.stringify({ type: 'error', message: 'Rate limit exceeded. Slow down.' }));
                }
                ipRegistry.set(ip, now);

                const { room, pin } = msg;
                if (!room) return ws.send(JSON.stringify({ type: 'error', message: 'Room ID required.' }));

                // 2. Room initialization or validation
                if (!rooms.has(room)) {
                    rooms.set(room, { peers: new Set(), pin: pin || null });
                    console.log(`Room created: ${room} (PIN: ${pin ? 'SET' : 'NONE'})`);
                }

                const roomData = rooms.get(room);

                // 3. Auth Check
                if (roomData.pin && roomData.pin !== pin) {
                    return ws.send(JSON.stringify({ type: 'error', message: 'Invalid PIN for this room.' }));
                }

                // 4. Capacity Check (Max 2 for P2P)
                if (roomData.peers.size >= 2) {
                    return ws.send(JSON.stringify({ type: 'error', message: 'Room is full.' }));
                }

                // 5. Join Room
                currentRoomId = room;
                roomData.peers.add(ws);
                console.log(`Peer joined room: ${room} (Total: ${roomData.peers.size})`);

                // 6. Notify Peers
                if (roomData.peers.size === 2) {
                    const peers = [...roomData.peers];
                    // Designated initiator (the first one)
                    peers[0].send(JSON.stringify({ type: 'ready', initiator: true }));
                    peers[1].send(JSON.stringify({ type: 'ready', initiator: false }));
                }
            } else if (currentRoomId && rooms.has(currentRoomId)) {
                // Relay signaling messages (offer, answer, ice)
                const roomData = rooms.get(currentRoomId);
                roomData.peers.forEach(client => {
                    if (client !== ws && client.readyState === 1) {
                        client.send(JSON.stringify(msg));
                    }
                });
            }
        } catch (e) {
            ws.send(JSON.stringify({ type: 'error', message: 'Malformed JSON payload.' }));
        }
    });

    ws.on('close', () => {
        if (currentRoomId && rooms.has(currentRoomId)) {
            const roomData = rooms.get(currentRoomId);
            roomData.peers.delete(ws);
            if (roomData.peers.size === 0) {
                rooms.delete(currentRoomId);
                console.log(`Room closed: ${currentRoomId}`);
            } else {
                // Notify remaining peer
                roomData.peers.forEach(peer => peer.send(JSON.stringify({ type: 'peer-left' })));
            }
        }
    });
});

server.listen(PORT);
