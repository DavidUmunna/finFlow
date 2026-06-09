import express from "express";
import { spawn } from "child_process";
import { randomUUID } from "crypto";

const app = express();
app.use(express.json());
app.use((req, res, next) => {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Headers", "*");
  res.header("Access-Control-Allow-Methods", "*");
  if (req.method === "OPTIONS") return res.sendStatus(200);
  next();
});

const CONNECTION_STRING = process.env.MONGODB_URI || "mongodb+srv://user:pass@cluster.mongodb.net/finflow";
const sessions = new Map();

function spawnMCP() {
  return spawn("npx", ["-y", "@mongodb-js/mongodb-mcp-server", "--connectionString", CONNECTION_STRING], {
    stdio: ["pipe", "pipe", "pipe"],
  });
}

// SSE endpoint — each connection gets its own MCP process
app.get("/sse", (req, res) => {
  const sessionId = randomUUID();
  console.log(`[session ${sessionId}] SSE connected`);

  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders();

  const mcp = spawnMCP();
  const pending = new Map(); // id → res (not used in SSE but tracks inflight)

  sessions.set(sessionId, { mcp, res });

  // Forward MCP stdout → SSE
  let buffer = "";
  mcp.stdout.on("data", (chunk) => {
    buffer += chunk.toString();
    const lines = buffer.split("\n");
    buffer = lines.pop();
    for (const line of lines) {
      if (line.trim()) {
        console.log(`[session ${sessionId}] MCP →`, line);
        res.write(`data: ${line}\n\n`);
      }
    }
  });

  mcp.stderr.on("data", (d) => console.error(`[session ${sessionId}] stderr:`, d.toString()));

  mcp.on("close", () => {
    console.log(`[session ${sessionId}] MCP process closed`);
    sessions.delete(sessionId);
    res.end();
  });

  req.on("close", () => {
    console.log(`[session ${sessionId}] Client disconnected`);
    mcp.kill();
    sessions.delete(sessionId);
  });

  // Send sessionId to client
  res.write(`event: endpoint\ndata: /message?sessionId=${sessionId}\n\n`);
  // Keep-alive ping every 15 seconds
    const keepAlive = setInterval(() => {
    res.write(`: ping\n\n`);
  }, 15000);
  
  req.on("close", () => {
    clearInterval(keepAlive);
  });
});

// Message endpoint — route to correct MCP process by sessionId
app.post("/message", (req, res) => {
  const { sessionId } = req.query;
  const session = sessions.get(sessionId);

  if (!session) {
    console.error(`[message] No session found: ${sessionId}`);
    return res.status(404).json({ error: "Session not found" });
  }

  const msg = JSON.stringify(req.body);
  console.log(`[session ${sessionId}] → MCP:`, msg);
  session.mcp.stdin.write(msg + "\n");
  res.sendStatus(202);
});

// Health check
app.get("/health", (req, res) => res.json({ status: "ok", sessions: sessions.size }));

const PORT = process.env.PORT || 8000;
app.listen(PORT, () => {
  console.log(`\n✅ FinFlow MCP Bridge running on http://localhost:${PORT}`);
  console.log(`   SSE endpoint:  http://localhost:${PORT}/sse`);
  console.log(`   Health check:  http://localhost:${PORT}/health`);
  console.log(`   MongoDB:       ${CONNECTION_STRING}\n`);
});