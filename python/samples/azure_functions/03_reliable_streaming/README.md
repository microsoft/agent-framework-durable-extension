# Agent Response Callbacks with Redis Streaming

This sample uses Redis Streams with agent response callbacks for resumable output. Clients can
reconnect with a cursor while entries remain in Redis, subject to its configured TTL and persistence.

> [!IMPORTANT]
> Redis chunks and SSE `event: done` / `[DONE]` are **provisional output**, not a durable commit
> acknowledgement. The callback runs before retention checks and durable state persistence.
> A completion marker can therefore exist even when the operation later fails to commit.

## Key Concepts Demonstrated

- Using `AgentResponseCallbackProtocol` to capture streaming agent responses
- Persisting streaming chunks to Redis Streams for reliable delivery
- Building a custom HTTP endpoint to read from Redis with Server-Sent Events (SSE) format
- Supporting cursor-based resumption for disconnected clients
- Managing Redis client lifecycle with async context managers

## Prerequisites

In addition to the [common setup steps](../README.md), this sample requires Redis:

```bash
# Start Redis
docker run -d --name redis -p 6379:6379 redis:latest
```

Update `local.settings.json` with your Redis connection string:

```json
{
  "Values": {
    "REDIS_CONNECTION_STRING": "redis://localhost:6379"
  }
}
```

## Running the Sample

### Start the agent run

The agent executes in a durable entity. Request non-blocking execution explicitly to receive the
acceptance response while `RedisStreamCallback` writes generation output to Redis:

```bash
curl -X POST "http://localhost:7071/api/agents/TravelPlanner/run?wait_for_response=false" \
  -H "Content-Type: text/plain" \
  -H "Accept: application/json" \
  -d "Plan a 3-day trip to Tokyo"
```

Response (202 Accepted):
```json
{
  "status": "accepted",
  "response": "Agent request accepted",
  "session_id": "abc-123-def-456",
  "correlation_id": "xyz-789"
}
```

### Stream the response from Redis

Use the returned `session_id` as `{conversation_id}` in the custom
`/api/agent/stream/{conversation_id}` endpoint to read Redis chunks:

```bash
curl http://localhost:7071/api/agent/stream/abc-123-def-456 \
  -H "Accept: text/event-stream"
```

Response (SSE format):
```
id: 1734649123456-0
event: message
data: Here's a wonderful 3-day Tokyo itinerary...

id: 1734649123789-0
event: message
data: Day 1: Arrival and Shibuya...

id: 1734649124012-0
event: done
data: [DONE]
```

### Verify durable completion before business success

The stream route reads only Redis. It does **not** check committed entity state, and this sample
does not supply a separate correlation-status endpoint.

- Save both `session_id` and `correlation_id` from the accepted response. A session-scoped Redis
  marker or cursor cannot identify a committed result for the current request.
- For non-blocking applications, implement an application-owned status path using the Functions
  durable client binding. Read `EntityId("dafx-TravelPlanner", session_id)` with `read_entity_state`
  from the same configured backend and hub. Decode the stored snapshot with `read_agent_state`
  from `agent_framework_durabletask`, then call `try_get_agent_response(correlation_id)` to check
  the canonical completion receipt and result for that request.
- Require a committed successful outcome and any needed available result before performing a
  business-success action. Missing state, a read error, or a timeout remains unverified. Handle
  committed failure and completed-but-result-unavailable explicitly. Never reconstruct success
  from chunks or transcript history, and do not resubmit the POST as a status check.

If a custom status path is not needed, submit the **original** request with
`wait_for_response=true` instead. The standard run route polls the durable result for that request.
Require its `status="success"` response, not a Redis marker or HTTP acceptance. A timeout is not
confirmation of success or failure. This choice does not move the callback after the commit.

Retries can leave provisional or repeated output in Redis, and older turns can leave completion
markers in the same session stream. Cursor resumption only resumes that output, not durable
transaction verification.

### Resume from a cursor

Use a cursor ID from an SSE event to skip already-processed messages:

```bash
curl "http://localhost:7071/api/agent/stream/abc-123-def-456?cursor=1734649123456-0" \
  -H "Accept: text/event-stream"
```

## How It Works

### 1. Redis Callback

The `RedisStreamCallback` class implements `AgentResponseCallbackProtocol` to capture streaming updates:

```python
class RedisStreamCallback(AgentResponseCallbackProtocol):
    async def on_streaming_response_update(self, update, context):
        session_id = context.session_id
        # Track the chunk sequence number per session
        sequence = self._sequence_numbers.setdefault(session_id, 0)

        # Write chunk to Redis Stream
        async with await get_stream_handler() as handler:
            await handler.write_chunk(session_id, update.text, sequence)
            self._sequence_numbers[session_id] += 1

    async def on_agent_response(self, response, context):
        session_id = context.session_id
        sequence = self._sequence_numbers.get(session_id, 0)

        # Write end-of-stream marker
        async with await get_stream_handler() as handler:
            await handler.write_completion(session_id, sequence)
```

### 2. Custom Streaming Endpoint

The `/api/agent/stream/{conversation_id}` endpoint reads from Redis:

```python
@app.route(route="agent/stream/{conversation_id}", methods=["GET"])
async def stream(req):
    conversation_id = req.route_params.get("conversation_id")
    cursor = req.params.get("cursor")  # Optional

    async with await get_stream_handler() as handler:
        async for chunk in handler.read_stream(conversation_id, cursor):
            # Format and return chunks
```

### 3. Redis Streams

Messages are stored in Redis Streams with automatic TTL (default: 10 minutes):

```
Stream Key: agent-stream:{conversation_id}
Entry: {
  "text": "chunk content",
  "sequence": "0",
  "timestamp": "1734649123456"
}
```