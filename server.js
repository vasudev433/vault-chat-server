// ═══════════════════════════════════════════════════
//  Vault Chat — WebSocket Server
//  Setup:
//    npm install ws
//    node server.js
// ═══════════════════════════════════════════════════

const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = 3001;
const rooms = new Map(); // Map<roomId, Map<ws, userId>>

const server = http.createServer((_, res) => {
  res.writeHead(200); res.end('Vault Chat Server');
});

const wss = new WebSocketServer({ server });

const send = (ws, obj) => ws.readyState === WebSocket.OPEN && ws.send(JSON.stringify(obj));

function broadcast(roomId, obj, exclude) {
  const room = rooms.get(roomId);
  if (!room) return;
  const msg = JSON.stringify(obj);
  room.forEach((_, ws) => { if (ws !== exclude && ws.readyState === WebSocket.OPEN) ws.send(msg); });
}

wss.on('connection', ws => {
  ws._room = null; ws._uid = null;

  ws.on('message', raw => {
    let m; try { m = JSON.parse(raw); } catch { return; }
    switch (m.type) {
      case 'create_room': {
        if (rooms.has(m.roomId)) { send(ws, { type:'create_error', message:'Room exists.' }); return; }
        rooms.set(m.roomId, new Map([[ws, m.userId]]));
        ws._room = m.roomId; ws._uid = m.userId;
        send(ws, { type:'room_created', roomId:m.roomId });
        console.log(`[+] Room ${m.roomId} created`);
        break;
      }
      case 'join_room': {
        const room = rooms.get(m.roomId);
        if (!room) { send(ws, { type:'join_error', message:'Room not found. Check the ID and try again.' }); return; }
        if (room.size >= 2) { send(ws, { type:'join_error', message:'Room is full (max 2 people).' }); return; }
        room.set(ws, m.userId);
        ws._room = m.roomId; ws._uid = m.userId;
        send(ws, { type:'joined_ok', roomId:m.roomId });
        broadcast(m.roomId, { type:'peer_joined' }, ws);
        console.log(`[+] ${m.userId} joined ${m.roomId}`);
        break;
      }
      case 'message':
        if (m.roomId && m.text) broadcast(m.roomId, { type:'message', text:m.text, time:m.time }, ws);
        break;
      case 'leave_room':
        leave(ws); break;
    }
  });

  ws.on('close', () => leave(ws));
  ws.on('error', err => { console.error(err.message); leave(ws); });
});

function leave(ws) {
  const rid = ws._room; if (!rid) return;
  const room = rooms.get(rid);
  if (room) {
    room.delete(ws);
    broadcast(rid, { type:'peer_left' });
    if (room.size === 0) { rooms.delete(rid); console.log(`[-] Room ${rid} closed`); }
  }
  ws._room = null; ws._uid = null;
}

setInterval(() => rooms.forEach((r,id) => r.size === 0 && rooms.delete(id)), 3_600_000);

server.listen(PORT, () => {
  console.log(`\n🔐 Vault Chat Server running on ws://localhost:${PORT}\n`);
});
