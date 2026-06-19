import { io } from "socket.io-client";
import { useEffect, useState, useRef, useCallback } from "react";

const socket = io(import.meta.env.VITE_SERVER_URL || "http://localhost:5000");

const CHUNK_SIZE = 16 * 1024; // 16KB chunks

function formatBytes(bytes) {
  if (bytes === 0) return "0 B";
  const k = 1024;
  const sizes = ["B", "KB", "MB", "GB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return (bytes / Math.pow(k, i)).toFixed(2) + " " + sizes[i];
}

function formatSpeed(bytesPerSec) {
  return formatBytes(bytesPerSec) + "/s";
}

export default function App() {
  const [roomId, setRoomId] = useState("");
  const [joinRoomId, setJoinRoomId] = useState("");
  const [isConnected, setPeerConnected] = useState(false);
  const [files, setFiles] = useState([]); // multiple files
  const [sendProgress, setSendProgress] = useState({}); // { filename: { percent, speed } }
  const [receiveProgress, setReceiveProgress] = useState({}); // { filename: { percent } }
  const [history, setHistory] = useState([]); // transfer log
  const [log, setLog] = useState([]); // debug/event log
  const [copied, setCopied] = useState(false);
  const [role, setRole] = useState(null); // 'creator' | 'joiner'

  const peerRef = useRef(null);
  const dataChannelRef = useRef(null);
  const roomIdRef = useRef("");

  // Receive state per file
  const receiveBuffers = useRef({}); // { filename: { chunks: [], receivedSize, totalSize, startTime } }

  const addLog = useCallback((msg) => {
    setLog((prev) => [
      { time: new Date().toLocaleTimeString(), msg },
      ...prev.slice(0, 49),
    ]);
  }, []);

  const addHistory = useCallback((entry) => {
    setHistory((prev) => [entry, ...prev]);
  }, []);

  // ── Setup peer connection ──────────────────────────────────────────
  const setupPeer = useCallback(() => {
    if (peerRef.current) {
      peerRef.current.close();
    }

    const peer = new RTCPeerConnection({
      iceServers: [{ urls: "stun:stun.l.google.com:19302" }],
    });
    peerRef.current = peer;

    peer.onicecandidate = (event) => {
      if (event.candidate) {
        socket.emit("ice-candidate", {
          roomId: roomIdRef.current,
          candidate: event.candidate,
        });
      }
    };

    peer.onconnectionstatechange = () => {
      addLog(`Connection state: ${peer.connectionState}`);
      if (peer.connectionState === "connected") {
        setPeerConnected(true);
        addLog("✅ Peer connected — ready to transfer files");
      } else if (
        peer.connectionState === "disconnected" ||
        peer.connectionState === "failed"
      ) {
        setPeerConnected(false);
        addLog("❌ Peer disconnected");
      }
    };

    // Receiver side: incoming data channel
    peer.ondatachannel = (event) => {
      const channel = event.channel;
      dataChannelRef.current = channel;
      addLog(`Data channel received: ${channel.label}`);

      channel.onopen = () => {
        setPeerConnected(true);
        addLog("✅ Receive channel open");
      };
      channel.onclose = () => {
        setPeerConnected(false);
        addLog("Receive channel closed");
      };

      channel.onmessage = (e) => handleIncomingMessage(e.data);
    };

    return peer;
  }, [addLog]);

  // ── Incoming message handler (receiver) ───────────────────────────
  const handleIncomingMessage = useCallback(
    (data) => {
      // Control messages are JSON strings
      if (typeof data === "string") {
        const msg = JSON.parse(data);

        if (msg.type === "file-start") {
          addLog(`📥 Receiving: ${msg.name} (${formatBytes(msg.size)})`);
          receiveBuffers.current[msg.name] = {
            chunks: [],
            receivedSize: 0,
            totalSize: msg.size,
            name: msg.name,
            fileType: msg.fileType,
            startTime: Date.now(),
          };
          setReceiveProgress((prev) => ({
            ...prev,
            [msg.name]: { percent: 0, size: msg.size },
          }));
        } else if (msg.type === "file-end") {
          const state = receiveBuffers.current[msg.name];
          if (!state) return;

          const blob = new Blob(state.chunks, { type: state.fileType });
          const url = URL.createObjectURL(blob);
          const elapsed = (Date.now() - state.startTime) / 1000;
          const speed = state.totalSize / elapsed;

          // Auto-download
          const a = document.createElement("a");
          a.href = url;
          a.download = state.name;
          a.click();
          URL.revokeObjectURL(url);

          addLog(`✅ Received: ${state.name}`);
          setReceiveProgress((prev) => ({
            ...prev,
            [msg.name]: { percent: 100, size: state.totalSize },
          }));
          addHistory({
            direction: "received",
            name: state.name,
            size: state.totalSize,
            speed,
            time: new Date().toLocaleTimeString(),
          });
          delete receiveBuffers.current[msg.name];
        }
        return;
      }

      // Binary chunk — find which file is currently receiving
      // We track the active file in receiveBuffers via insertion order
      const activeFile = Object.values(receiveBuffers.current).find(
        (f) => f.receivedSize < f.totalSize
      );
      if (!activeFile) return;

      activeFile.chunks.push(data);
      activeFile.receivedSize += data.byteLength;

      const percent = Math.round(
        (activeFile.receivedSize / activeFile.totalSize) * 100
      );
      setReceiveProgress((prev) => ({
        ...prev,
        [activeFile.name]: { percent, size: activeFile.totalSize },
      }));
    },
    [addLog, addHistory]
  );

  // ── Socket events ──────────────────────────────────────────────────
  useEffect(() => {
    socket.on("user-joined", async () => {
      addLog("👤 Someone joined the room — creating offer");
      const peer = peerRef.current;

      const channel = peer.createDataChannel("fileTransfer");
      dataChannelRef.current = channel;

      channel.onopen = () => {
        setPeerConnected(true);
        addLog("✅ Data channel open (sender side)");
      };
      channel.onclose = () => {
        setPeerConnected(false);
        addLog("Data channel closed");
      };
      channel.onmessage = (e) => handleIncomingMessage(e.data);

      const offer = await peer.createOffer();
      await peer.setLocalDescription(offer);
      socket.emit("offer", { roomId: roomIdRef.current, offer });
    });

    socket.on("offer", async ({ offer }) => {
      addLog("📨 Received offer");
      const peer = peerRef.current;
      await peer.setRemoteDescription(new RTCSessionDescription(offer));
      const answer = await peer.createAnswer();
      await peer.setLocalDescription(answer);
      socket.emit("answer", { roomId: roomIdRef.current, answer });
    });

    socket.on("answer", async ({ answer }) => {
      addLog("📨 Received answer");
      await peerRef.current.setRemoteDescription(
        new RTCSessionDescription(answer)
      );
    });

    socket.on("ice-candidate", async ({ candidate }) => {
      try {
        await peerRef.current.addIceCandidate(new RTCIceCandidate(candidate));
      } catch (e) {
        addLog("ICE candidate error: " + e.message);
      }
    });

    return () => {
      socket.off("user-joined");
      socket.off("offer");
      socket.off("answer");
      socket.off("ice-candidate");
    };
  }, [addLog]);

  // ── Create room ────────────────────────────────────────────────────
  const createRoom = () => {
    const id = Math.random().toString(36).substring(2, 8).toUpperCase();
    setRoomId(id);
    roomIdRef.current = id;
    setRole("creator");
    setupPeer();
    socket.emit("join-room", id);
    addLog(`🏠 Room created: ${id}`);
  };

  // ── Join room ──────────────────────────────────────────────────────
  const joinRoom = () => {
    if (!joinRoomId.trim()) return;
    const id = joinRoomId.trim().toUpperCase();
    setRoomId(id);
    roomIdRef.current = id;
    setRole("joiner");
    setupPeer();
    socket.emit("join-room", id);
    addLog(`🔗 Joined room: ${id}`);
  };

  // ── Send files with chunking ───────────────────────────────────────
  const sendFiles = async () => {
    const channel = dataChannelRef.current;
    if (!channel || channel.readyState !== "open") {
      addLog("❌ Data channel not open");
      return;
    }
    if (files.length === 0) {
      addLog("No files selected");
      return;
    }

    for (const file of files) {
      addLog(`📤 Sending: ${file.name} (${formatBytes(file.size)})`);
      const startTime = Date.now();

      // Send metadata
      channel.send(
        JSON.stringify({
          type: "file-start",
          name: file.name,
          size: file.size,
          fileType: file.type,
        })
      );

      // Send chunks
      const buffer = await file.arrayBuffer();
      let offset = 0;
      let sentBytes = 0;
      let lastTime = Date.now();
      let lastBytes = 0;

      while (offset < buffer.byteLength) {
        // Backpressure: wait if buffer is full
        while (channel.bufferedAmount > 4 * 1024 * 1024) {
          await new Promise((r) => setTimeout(r, 50));
        }

        const chunk = buffer.slice(offset, offset + CHUNK_SIZE);
        channel.send(chunk);
        offset += chunk.byteLength;
        sentBytes += chunk.byteLength;

        const now = Date.now();
        const elapsed = (now - lastTime) / 1000;
        if (elapsed >= 0.2) {
          const speed = (sentBytes - lastBytes) / elapsed;
          const percent = Math.round((sentBytes / file.size) * 100);
          setSendProgress((prev) => ({
            ...prev,
            [file.name]: { percent, speed },
          }));
          lastTime = now;
          lastBytes = sentBytes;
        }
      }

      // Send end marker
      channel.send(JSON.stringify({ type: "file-end", name: file.name }));

      const totalElapsed = (Date.now() - startTime) / 1000;
      const avgSpeed = file.size / totalElapsed;
      setSendProgress((prev) => ({
        ...prev,
        [file.name]: { percent: 100, speed: avgSpeed },
      }));
      addLog(`✅ Sent: ${file.name}`);
      addHistory({
        direction: "sent",
        name: file.name,
        size: file.size,
        speed: avgSpeed,
        time: new Date().toLocaleTimeString(),
      });
    }
  };

  const copyRoomId = () => {
    navigator.clipboard.writeText(roomId);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  // ── UI ─────────────────────────────────────────────────────────────
  const s = styles;

  return (
    <div style={s.page}>
      <div style={s.container}>
        {/* Header */}
        <div style={s.header}>
          <div style={s.logo}>⚡</div>
          <h1 style={s.title}>P2P Web Share</h1>
          <p style={s.subtitle}>Direct browser-to-browser file transfer</p>
        </div>

        {/* Connection status */}
        <div style={{ ...s.badge, background: isConnected ? "#16a34a22" : "#94a3b822", color: isConnected ? "#4ade80" : "#94a3b8", border: `1px solid ${isConnected ? "#16a34a" : "#334155"}` }}>
          <span style={{ ...s.dot, background: isConnected ? "#4ade80" : "#475569" }} />
          {isConnected ? "Peer Connected" : "Waiting for peer…"}
        </div>

        {/* Room setup */}
        {!roomId ? (
          <div style={s.card}>
            <h2 style={s.cardTitle}>Get Started</h2>
            <button style={s.btnPrimary} onClick={createRoom}>
              + Create Room
            </button>
            <div style={s.divider}><span style={s.dividerText}>or join existing</span></div>
            <div style={s.row}>
              <input
                style={s.input}
                placeholder="Enter Room Code"
                value={joinRoomId}
                onChange={(e) => setJoinRoomId(e.target.value.toUpperCase())}
                onKeyDown={(e) => e.key === "Enter" && joinRoom()}
              />
              <button style={s.btnSecondary} onClick={joinRoom}>Join</button>
            </div>
          </div>
        ) : (
          <>
            {/* Room info */}
            <div style={s.card}>
              <div style={s.roomRow}>
                <div>
                  <p style={s.label}>Room Code</p>
                  <p style={s.roomCode}>{roomId}</p>
                </div>
                <button style={s.btnCopy} onClick={copyRoomId}>
                  {copied ? "✓ Copied" : "Copy"}
                </button>
              </div>
              <p style={s.hint}>
                {role === "creator"
                  ? "Share this code with the other person. They should join using it."
                  : "Waiting for the room creator to connect…"}
              </p>
            </div>

            {/* File picker + send */}
            <div style={s.card}>
              <h2 style={s.cardTitle}>Send Files</h2>
              <label style={s.fileLabel}>
                <input
                  type="file"
                  multiple
                  style={{ display: "none" }}
                  onChange={(e) => setFiles(Array.from(e.target.files))}
                />
                📁 {files.length > 0 ? `${files.length} file(s) selected` : "Choose files…"}
              </label>

              {files.length > 0 && (
                <div style={s.fileList}>
                  {files.map((f) => (
                    <div key={f.name} style={s.fileItem}>
                      <div style={s.fileRow}>
                        <span style={s.fileName}>{f.name}</span>
                        <span style={s.fileSize}>{formatBytes(f.size)}</span>
                      </div>
                      {sendProgress[f.name] !== undefined && (
                        <ProgressBar
                          percent={sendProgress[f.name].percent}
                          label={
                            sendProgress[f.name].percent < 100
                              ? formatSpeed(sendProgress[f.name].speed)
                              : "Done"
                          }
                          color="#6366f1"
                        />
                      )}
                    </div>
                  ))}
                </div>
              )}

              <button
                style={{ ...s.btnPrimary, marginTop: "12px", opacity: !isConnected ? 0.5 : 1 }}
                onClick={sendFiles}
                disabled={!isConnected}
              >
                Send {files.length > 1 ? `${files.length} Files` : "File"}
              </button>
            </div>

            {/* Receive progress */}
            {Object.keys(receiveProgress).length > 0 && (
              <div style={s.card}>
                <h2 style={s.cardTitle}>Receiving</h2>
                {Object.entries(receiveProgress).map(([name, info]) => (
                  <div key={name} style={s.fileItem}>
                    <div style={s.fileRow}>
                      <span style={s.fileName}>{name}</span>
                      <span style={s.fileSize}>{formatBytes(info.size)}</span>
                    </div>
                    <ProgressBar percent={info.percent} label={info.percent === 100 ? "Saved ✓" : `${info.percent}%`} color="#10b981" />
                  </div>
                ))}
              </div>
            )}
          </>
        )}

        {/* Transfer history */}
        {history.length > 0 && (
          <div style={s.card}>
            <h2 style={s.cardTitle}>Transfer History</h2>
            <div style={s.historyList}>
              {history.map((h, i) => (
                <div key={i} style={s.historyItem}>
                  <span style={{ fontSize: "18px" }}>{h.direction === "sent" ? "📤" : "📥"}</span>
                  <div style={{ flex: 1 }}>
                    <p style={s.historyName}>{h.name}</p>
                    <p style={s.historyMeta}>{formatBytes(h.size)} · {formatSpeed(h.speed)} · {h.time}</p>
                  </div>
                  <span style={{ ...s.badge, padding: "2px 8px", fontSize: "11px", background: h.direction === "sent" ? "#6366f122" : "#10b98122", color: h.direction === "sent" ? "#818cf8" : "#34d399" }}>
                    {h.direction}
                  </span>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Event log */}
        <div style={s.card}>
          <h2 style={s.cardTitle}>Event Log</h2>
          <div style={s.logBox}>
            {log.length === 0 && <p style={{ color: "#475569", fontSize: "13px" }}>No events yet…</p>}
            {log.map((l, i) => (
              <p key={i} style={s.logLine}>
                <span style={s.logTime}>{l.time}</span> {l.msg}
              </p>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

function ProgressBar({ percent, label, color }) {
  return (
    <div style={{ marginTop: "6px" }}>
      <div style={{ display: "flex", justifyContent: "space-between", marginBottom: "4px" }}>
        <span style={{ fontSize: "12px", color: "#94a3b8" }}>{percent}%</span>
        <span style={{ fontSize: "12px", color: "#94a3b8" }}>{label}</span>
      </div>
      <div style={{ background: "#1e293b", borderRadius: "999px", height: "6px", overflow: "hidden" }}>
        <div style={{ width: `${percent}%`, background: color, height: "100%", borderRadius: "999px", transition: "width 0.2s ease" }} />
      </div>
    </div>
  );
}

const styles = {
  page: {
    minHeight: "100vh",
    background: "#0a0f1e",
    color: "white",
    fontFamily: "'Inter', 'Segoe UI', Arial, sans-serif",
    display: "flex",
    justifyContent: "center",
    padding: "40px 16px",
  },
  container: { width: "100%", maxWidth: "520px", display: "flex", flexDirection: "column", gap: "16px" },
  header: { textAlign: "center", marginBottom: "8px" },
  logo: { fontSize: "48px", marginBottom: "8px" },
  title: { fontSize: "32px", fontWeight: "800", margin: 0, letterSpacing: "-1px", background: "linear-gradient(135deg, #6366f1, #a78bfa)", WebkitBackgroundClip: "text", WebkitTextFillColor: "transparent" },
  subtitle: { color: "#64748b", marginTop: "6px", fontSize: "15px" },
  badge: { display: "flex", alignItems: "center", gap: "8px", padding: "8px 14px", borderRadius: "999px", fontSize: "13px", fontWeight: "500", alignSelf: "center" },
  dot: { width: "8px", height: "8px", borderRadius: "50%" },
  card: { background: "#0f172a", border: "1px solid #1e293b", borderRadius: "16px", padding: "20px" },
  cardTitle: { margin: "0 0 16px", fontSize: "16px", fontWeight: "600", color: "#e2e8f0" },
  btnPrimary: { width: "100%", padding: "12px", background: "linear-gradient(135deg, #6366f1, #8b5cf6)", color: "white", border: "none", borderRadius: "10px", fontSize: "15px", fontWeight: "600", cursor: "pointer" },
  btnSecondary: { padding: "12px 20px", background: "#1e293b", color: "#e2e8f0", border: "1px solid #334155", borderRadius: "10px", fontSize: "15px", cursor: "pointer", whiteSpace: "nowrap" },
  btnCopy: { padding: "8px 16px", background: "#1e293b", color: "#a78bfa", border: "1px solid #4f46e5", borderRadius: "8px", cursor: "pointer", fontSize: "13px", fontWeight: "600" },
  divider: { display: "flex", alignItems: "center", margin: "16px 0" },
  dividerText: { color: "#334155", fontSize: "12px", padding: "0 12px", background: "#0f172a", whiteSpace: "nowrap" },
  row: { display: "flex", gap: "8px" },
  input: { flex: 1, padding: "12px", background: "#1e293b", border: "1px solid #334155", borderRadius: "10px", color: "white", fontSize: "15px", letterSpacing: "2px", fontWeight: "600" },
  roomRow: { display: "flex", justifyContent: "space-between", alignItems: "center" },
  label: { color: "#64748b", fontSize: "12px", margin: "0 0 4px", textTransform: "uppercase", letterSpacing: "1px" },
  roomCode: { fontSize: "28px", fontWeight: "800", letterSpacing: "4px", color: "#a78bfa", margin: 0 },
  hint: { color: "#475569", fontSize: "13px", marginTop: "10px", marginBottom: 0 },
  fileLabel: { display: "block", padding: "12px", background: "#1e293b", border: "1px dashed #334155", borderRadius: "10px", textAlign: "center", cursor: "pointer", color: "#94a3b8", fontSize: "14px" },
  fileList: { display: "flex", flexDirection: "column", gap: "10px", marginTop: "12px" },
  fileItem: { background: "#1e293b", borderRadius: "10px", padding: "12px" },
  fileRow: { display: "flex", justifyContent: "space-between", alignItems: "center" },
  fileName: { fontSize: "13px", color: "#e2e8f0", fontWeight: "500", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", maxWidth: "280px" },
  fileSize: { fontSize: "12px", color: "#64748b", flexShrink: 0, marginLeft: "8px" },
  historyList: { display: "flex", flexDirection: "column", gap: "8px" },
  historyItem: { display: "flex", alignItems: "center", gap: "12px", padding: "10px", background: "#1e293b", borderRadius: "10px" },
  historyName: { margin: 0, fontSize: "13px", fontWeight: "500", color: "#e2e8f0" },
  historyMeta: { margin: "2px 0 0", fontSize: "12px", color: "#64748b" },
  logBox: { background: "#020817", borderRadius: "10px", padding: "12px", maxHeight: "180px", overflowY: "auto", display: "flex", flexDirection: "column", gap: "4px" },
  logLine: { margin: 0, fontSize: "12px", color: "#94a3b8", lineHeight: "1.5" },
  logTime: { color: "#475569", marginRight: "6px" },
};