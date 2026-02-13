# AI Coding Agent: Critical Building Blocks (from OpenCode analysis)

Source: https://github.com/sst/opencode (`packages/opencode/src/`)

---

## 1. The Agent Loop

The core is a **while(true)** loop that cycles:

```
User message → LLM call (streaming) → process response → if tool_calls: execute tools → loop again → if no tool_calls: break
```

**Key file**: `session/prompt.ts` (loop function, line 276)

### Termination conditions (in priority order):
1. LLM finish_reason is NOT `tool-calls` and assistant message is newer than last user message → **done**
2. Max steps reached (configurable per agent) → inject "wrap it up" prompt, then stop
3. Processor returns `"stop"` (error or user rejected a permission) → **done**
4. Context overflow detected → trigger **compaction**, then continue loop

### The inner processing loop
`session/processor.ts` has its own `while(true)` for **retries**. If the LLM API call fails with a retryable error, it retries with exponential backoff inside the same iteration. Non-retryable errors break out.

### Doom loop detection
Track last N tool calls. If the same tool is called 3 consecutive times with identical arguments, flag it and ask the user before continuing. Simple but effective.

---

## 2. LLM Interaction

**Key file**: `session/llm.ts`

### Streaming (must-have)
Uses Vercel AI SDK's `streamText()`. The stream emits typed events:
- `text-delta` — streaming text tokens
- `tool-call` — LLM wants to call a tool (name + args)
- `tool-result` — result fed back
- `reasoning-delta` — chain-of-thought (for reasoning models)
- `finish-step` — step boundary with token usage

You process these events in a `for await` loop and dispatch accordingly.

### What gets sent to the LLM each iteration:
```
[system messages]  +  [conversation history]  +  [tool definitions]
```

That's it. No magic. The conversation history includes prior tool calls and their results as structured messages.

---

## 3. System Prompt Construction

**Key file**: `session/system.ts`, `session/prompt/`

### Structure (2 parts for cache efficiency):
1. **Part 1 — Static base prompt**: Model-specific instructions (how to behave, tool usage guidelines, coding principles). ~80 lines. This part stays stable across turns → **provider-side prompt caching kicks in**.
2. **Part 2 — Dynamic context**: Environment info (working dir, platform, date, git status) + project instructions (loaded from `CLAUDE.md`/`AGENTS.md` files in the repo).

### Cache control
Apply `cache_control: { type: "ephemeral" }` to:
- First 2 system message blocks (static content → cached)
- Last 2 conversation messages (recent context → cached for next turn)

This is provider-specific (Anthropic, Bedrock, OpenRouter each have their own header). This is the single biggest cost optimization — cache hits on the system prompt save massive input tokens.

---

## 4. Context Window Management

**Key file**: `session/compaction.ts`

### The problem
Conversations grow. Eventually you hit the model's context limit.

### The solution: Compaction
When `current_tokens >= (context_limit - reserved_buffer)`:

1. Send the full conversation to a **compaction agent** (a cheaper/faster model)
2. It produces a structured summary: goal, discoveries, accomplished work, relevant files
3. Replace old history with the summary
4. Continue the loop with fresh context budget

### Pruning (complementary to compaction)
Before compaction triggers, prune old tool outputs from the middle of the conversation. Keep the last 2 turns intact. This buys time before a full compaction is needed.

### Overflow detection
Regex patterns per provider to detect "context too long" errors from the API (13 different patterns for Anthropic, OpenAI, Gemini, Bedrock, etc). When detected, trigger compaction instead of failing.

---

## 5. Tool System

**Key file**: `tool/tool.ts`, `tool/registry.ts`

### Tool definition shape:
```typescript
{
  id: string
  description: string
  parameters: ZodSchema        // input validation
  execute(args, ctx) → {
    output: string             // text result sent back to LLM
    title: string              // display name
    metadata: Record<string, any>
  }
}
```

### Registration
Tools are registered in a list. Each is converted to the AI SDK's `tool()` format with a JSON Schema derived from the Zod schema. The tool list is passed to `streamText()`.

### Tool execution flow:
1. LLM emits `tool-call` event with tool name + JSON args
2. Validate args against Zod schema
3. Execute the tool function
4. Format result as string
5. **Truncate** if >2000 lines or >50KB (save full output to disk, tell LLM it was truncated and suggest using grep/read with offset)
6. Result automatically goes back into the conversation as a `tool-result` message
7. Loop continues → LLM sees the result and decides next action

### Essential tools for a coding agent:
| Tool | What it does |
|------|-------------|
| **read** | Read file contents (with line offset/limit) |
| **write** | Create new files |
| **edit** | String replacement in existing files (oldString → newString) |
| **bash** | Execute shell commands |
| **glob** | Find files by pattern |
| **grep** | Search file contents (regex) |
| **task** | Spawn a sub-agent (recursive agent call with different config) |

### Output truncation (important for cost/reliability)
Tool outputs can be huge (e.g. `cat` a large file). Cap at 2000 lines / 50KB. Store full output on disk. Tell the LLM: "output was truncated, use grep or read with offset to see more." This prevents blowing the context window on a single tool call.

---

## 6. Retry & Error Handling

**Key file**: `session/retry.ts`, `provider/error.ts`

### Exponential backoff:
- Base delay: 2s, factor: 2x, max: 30s (without headers)
- Respect `retry-after` / `retry-after-ms` response headers when present
- Retryable: rate limits, overloaded, 5xx, OpenAI 404s (they're flaky)
- NOT retryable: context overflow (handle via compaction instead)

### Error classification:
Parse API errors into two buckets:
1. `context_overflow` → trigger compaction
2. `api_error` → retry if retryable, fail otherwise

---

## 7. Provider Abstraction

**Key file**: `provider/provider.ts`

Use the Vercel AI SDK. It abstracts Anthropic, OpenAI, Google, Bedrock, Azure, etc. behind a unified interface. You call `streamText()` the same way regardless of provider. Provider-specific quirks are handled in thin adapter layers.

### What you actually need for a POC:
Pick ONE provider (Anthropic or OpenAI). Skip the abstraction layer. Call the API directly or use their SDK. The abstraction is a nice-to-have for multi-provider support.

---

## 8. Message Format

### Internal message structure:
```
Message {
  role: "user" | "assistant"
  parts: [TextPart | ToolPart | ReasoningPart | FilePart | ...]
}

ToolPart {
  tool: string
  state: "pending" | "running" | "completed" | "error"
  input: any
  output: string
}
```

Convert to the LLM's expected format before each API call. The AI SDK handles this, but if doing it yourself: user messages have `content`, assistant messages have `content` + `tool_calls`, and tool results are separate messages with `tool_call_id`.

---

## Summary: What to build for a POC

### Must-haves (these make or break performance):

1. **Agent loop** — `while(true)` that calls LLM, executes tool calls, feeds results back, until LLM stops requesting tools
2. **Streaming** — Stream LLM responses. Parse tool calls from the stream. Execute tools as they come in.
3. **System prompt** — Clear instructions on behavior + environment context (cwd, platform, date). Model-specific prompt if you support multiple.
4. **Core tools** — file read, file write, file edit (string replacement), bash exec, file search (glob), content search (grep). These 6 cover 95% of coding tasks.
5. **Tool output truncation** — Cap tool output size. Without this, one bad `cat` command kills your context window.
6. **Context window management** — At minimum: detect overflow, summarize conversation, continue with summary. Without this, long tasks just crash.
7. **Prompt caching** — Structure your system prompt so the static part is cacheable. Apply cache control headers. This cuts cost 5-10x on multi-turn conversations.
8. **Retry with backoff** — LLM APIs fail. Exponential backoff with retry-after header support.
9. **Conversation history** — Store messages and tool results. Send relevant history each turn.

### Architecture in one diagram:

```
User Input
    │
    ▼
┌─────────────────────────────────────────┐
│            AGENT LOOP                    │
│                                          │
│  ┌─────────────────────────────────┐     │
│  │ Build messages:                 │     │
│  │  [system] + [history] + [tools] │     │
│  └──────────────┬──────────────────┘     │
│                 │                         │
│                 ▼                         │
│  ┌──────────────────────────────┐        │
│  │ LLM API (streaming)          │        │
│  │  ← retry on failure          │        │
│  └──────────────┬───────────────┘        │
│                 │                         │
│          ┌──────┴──────┐                  │
│          │             │                  │
│     text only    tool_calls              │
│          │             │                  │
│          ▼             ▼                  │
│       DONE     ┌──────────────┐          │
│                │ Execute tools │          │
│                │ (validate,    │          │
│                │  run,         │          │
│                │  truncate)    │          │
│                └──────┬───────┘          │
│                       │                  │
│                       ▼                  │
│              Append tool results         │
│              to conversation             │
│                       │                  │
│                       └──→ LOOP ─────────┘
│                                          │
│  If context overflow → COMPACT & LOOP    │
└──────────────────────────────────────────┘
```
