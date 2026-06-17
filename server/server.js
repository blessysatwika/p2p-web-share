const express = require("express");
const { createServer } = require("http");
const { Server } = require("socket.io");

const app = express();
const httpServer = createServer(app);

const MAX_ROOM_SIZE = 2;

// Set CLIENT_ORIGIN to a comma-separated list of allowed origins in production,
// e.g. CLIENT_ORIGIN="https://myapp.com,https://www.myapp.com"
const allowedOrigins = process.env.CLIENT_ORIGIN
  ? process.env.CLIENT_ORIGIN.split(",")
  : "*";

const io = new Server(httpServer, {
  cors: {
    origin: allowedOrigins,
    methods: ["GET", "POST"],
  },
});

function isValidRoomId(roomId) {
  return typeof roomId === "string" && roomId.length > 0 && roomId.length <= 100;
}

// Use socket.io's own room bookkeeping instead of a parallel Map,
// so the two can never drift out of sync.
function getRoomSize(roomId) {
  return io.sockets.adapter.rooms.get(roomId)?.size || 0;
}

function leaveCurrentRoom(socket) {
  const roomId = socket.data.roomId;
  if (!roomId) return;

  // Let the remaining peer know so it can tear down its RTCPeerConnection
  // instead of hanging indefinitely.
  socket.to(roomId).emit("user-left", { socketId: socket.id });
  socket.leave(roomId);
  socket.data.roomId = undefined;
}

io.on("connection", (socket) => {
  console.log("Client connected:", socket.id);

  socket.on("join-room", (roomId) => {
    if (!isValidRoomId(roomId)) {
      socket.emit("error", { message: "Invalid room id" });
      return;
    }

    if (socket.data.roomId) {
      socket.emit("error", { message: "Already in a room. Leave first." });
      return;
    }

    if (getRoomSize(roomId) >= MAX_ROOM_SIZE) {
      socket.emit("room-full", { roomId });
      return;
    }

    socket.join(roomId);
    socket.data.roomId = roomId;

    const members = getRoomSize(roomId);
    console.log(`Room ${roomId}: ${members} member(s)`);

    if (members > 1) {
      socket.to(roomId).emit("user-joined", { socketId: socket.id });
    }
  });

  socket.on("leave-room", () => {
    leaveCurrentRoom(socket);
  });

  // Each relay checks that the caller actually belongs to the room they're
  // signaling on, and tags the payload with `from` so the receiver knows
  // who sent it (useful once you support more than 2 peers).
  socket.on("offer", ({ roomId, offer }) => {
    if (roomId !== socket.data.roomId) return;
    socket.to(roomId).emit("offer", { offer, from: socket.id });
  });

  socket.on("answer", ({ roomId, answer }) => {
    if (roomId !== socket.data.roomId) return;
    socket.to(roomId).emit("answer", { answer, from: socket.id });
  });

  socket.on("ice-candidate", ({ roomId, candidate }) => {
    if (roomId !== socket.data.roomId) return;
    socket.to(roomId).emit("ice-candidate", { candidate, from: socket.id });
  });

  socket.on("disconnect", () => {
    console.log("Client disconnected:", socket.id);
    leaveCurrentRoom(socket);
  });
});

const PORT = process.env.PORT || 5000;
httpServer.listen(PORT, () => {
  console.log(`Signaling server running on port ${PORT}`);
});