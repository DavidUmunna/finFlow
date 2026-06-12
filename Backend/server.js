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

const CONNECTION_STRING = process.env.MONGODB_URI
const sseClients = new Map(); // sessionId -> res
const pendingById = new Map(); // request id -> sessionId

// --- Single persistent MCP process ---
let mcp;
let buffer = "";

function startMCP() {
  mcp = spawn("npx", ["-y", "@mongodb-js/mongodb-mcp-server", "--connectionString", CONNECTION_STRING], {
    stdio: ["pipe", "pipe", "pipe"],
  });

  mcp.stdout.on("data", (chunk) => {
    buffer += chunk.toString();
    const lines = buffer.split("\n");
    buffer = lines.pop();
    for (const line of lines) {
      if (!line.trim()) continue;
      console.log("[MCP →]", line);

      let targetSessionId = null;
      try {
        const parsed = JSON.parse(line);
        if (parsed.id !== undefined && pendingById.has(parsed.id)) {
          targetSessionId = pendingById.get(parsed.id);
          pendingById.delete(parsed.id);
        }
      } catch {
        // not JSON (e.g. notifications) — broadcast to all
      }

      if (targetSessionId) {
        const res = sseClients.get(targetSessionId);
        if (res) res.write(`data: ${line}\n\n`);
      } else {
        for (const res of sseClients.values()) {
          res.write(`data: ${line}\n\n`);
        }
      }
    }
  });

  mcp.stderr.on("data", (d) => console.error("[MCP stderr]", d.toString()));

  mcp.on("close", (code) => {
    console.error(`[MCP] process exited with code ${code}, restarting...`);
    setTimeout(startMCP, 1000);
  });

  console.log("[MCP] persistent process started");
}

startMCP();

// SSE endpoint — clients attach to the shared MCP process
app.get("/sse", (req, res) => {
  const sessionId = randomUUID();
  console.log(`[session ${sessionId}] SSE connected`);

  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders();

  sseClients.set(sessionId, res);

  req.on("close", () => {
    console.log(`[session ${sessionId}] Client disconnected`);
    sseClients.delete(sessionId);
  });

  res.write(`event: endpoint\ndata: /message?sessionId=${sessionId}\n\n`);

  const keepAlive = setInterval(() => {
    res.write(`: ping\n\n`);
  }, 15000);

  req.on("close", () => clearInterval(keepAlive));
});

// Message endpoint — write to the shared MCP process, track which session expects the reply
app.post("/message", (req, res) => {
  const { sessionId } = req.query;
  if (!sseClients.has(sessionId)) {
    console.error(`[message] No session found: ${sessionId}`);
    return res.status(404).json({ error: "Session not found" });
  }

  if (req.body?.id !== undefined) {
    pendingById.set(req.body.id, sessionId);
  }

  const msg = JSON.stringify(req.body);
  console.log(`[session ${sessionId}] → MCP:`, msg);
  mcp.stdin.write(msg + "\n");
  res.sendStatus(202);
});

// Health check
app.get("/health", (req, res) => res.json({ status: "ok", sessions: sseClients.size }));

const PORT = process.env.PORT || 8000;
app.listen(PORT, () => {
  console.log(`\n✅ FinFlow MCP Bridge running on http://localhost:${PORT}`);
  console.log(`   SSE endpoint:  http://localhost:${PORT}/sse`);
  console.log(`   Health check:  http://localhost:${PORT}/health`);
  console.log(`   MongoDB:       ${CONNECTION_STRING}\n`);
});