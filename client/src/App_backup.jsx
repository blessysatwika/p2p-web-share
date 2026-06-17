import { io } from "socket.io-client";
import { useEffect, useState, useRef } from "react";
const socket = io("http://localhost:5000");

function App() {
  const [file, setFile] = useState(null);

const peerRef = useRef(null);
const dataChannelRef = useRef(null);
  useEffect(() => {
    peerRef.current = new RTCPeerConnection();
    peerRef.current.onicecandidate = (event) => {
  if (event.candidate) {
    socket.emit("ice-candidate", {
      candidate: event.candidate,
    });
  }
};
peerRef.current.ondatachannel = (event) => {
  const receiveChannel = event.channel;

  receiveChannel.onmessage = (e) => {
    const blob = new Blob([e.data]);

    const url = URL.createObjectURL(blob);

    const a = document.createElement("a");
    a.href = url;
    a.download = "received-file";

    a.click();
  };
};
socket.on("offer", async (offer) => {
  await peerRef.current.setRemoteDescription(offer);

  const answer = await peerRef.current.createAnswer();

  await peerRef.current.setLocalDescription(answer);

  socket.emit("answer", {
    roomId,
    answer,
  });
});

socket.on("answer", async (answer) => {
  await peerRef.current.setRemoteDescription(answer);
});

socket.on("ice-candidate", async (candidate) => {
  await peerRef.current.addIceCandidate(candidate);
});
  socket.on("user-joined", () => {
    alert("Someone joined your room!");
  });

  return () => socket.off("user-joined");
}, []);
  const [roomId, setRoomId] = useState("");

  const createRoom = async () => {
  const id = Math.random().toString(36).substring(2, 8);

  setRoomId(id);

  socket.emit("join-room", id);

  dataChannelRef.current =
    peerRef.current.createDataChannel("fileTransfer");

  const offer =
    await peerRef.current.createOffer();

  await peerRef.current.setLocalDescription(
    offer
  );

  socket.emit("offer", {
    roomId: id,
    offer,
  });
};
const [joinRoomId, setJoinRoomId] = useState("");
  return (
    <div
      style={{
        minHeight: "100vh",
        background: "#0f172a",
        color: "white",
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        paddingTop: "80px",
        fontFamily: "Arial",
      }}
    >
      <h1>P2P Web Share</h1>

      <p>Direct Browser-to-Browser File Transfer</p>

      <button
        onClick={createRoom}
        style={{
          padding: "12px 24px",
          fontSize: "18px",
          borderRadius: "10px",
          border: "none",
          cursor: "pointer",
          marginTop: "20px",
        }}
      >
        Create Room
      </button>

      {roomId && (
        <div
          style={{
            marginTop: "30px",
            padding: "20px",
            background: "#1e293b",
            borderRadius: "12px",
          }}
        >
          <h3>Room Created</h3>
          <p>{roomId}</p>
          <input
  type="file"
  onChange={(e) => setFile(e.target.files[0])}
/>

{file && <p>Selected: {file.name}</p>}
<button
  onClick={() => {
    if (!file) {
      alert("Select a file first");
      return;
    }

    alert(`Pretending to send: ${file.name}`);
  }}
  style={{
    marginTop: "10px",
    padding: "10px",
    borderRadius: "8px",
  }}
>
  Send File
</button>
          <div style={{ marginTop: "20px" }}>
  <input
    type="text"
    placeholder="Enter Room ID"
    value={joinRoomId}
    onChange={(e) => setJoinRoomId(e.target.value)}
    style={{
      padding: "10px",
      borderRadius: "8px",
      marginRight: "10px"
    }}
  />

  <button
  onClick={() => {
    if (!joinRoomId) return;

    socket.emit("join-room", joinRoomId);

    alert(`Joined room: ${joinRoomId}`);
  }}
>
  Join Room
</button>
</div>
          <p>{roomId}</p>
        </div>
      )}
    </div>
  );
}

export default App;