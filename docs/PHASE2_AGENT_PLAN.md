# Phase 2: an AI agent on top of the lakehouse (build after Phase 1b works)

## Goal
A "risk analyst" assistant: ask "Which grade has the highest charge-off rate for 2022 originations, and why might that be?"
and get a correct, cited answer computed from the gold tables, not guessed.

## Learning path (in order)
1. **LLM basics in code**: call a model, structured output (JSON), temperature, tokens. (Day 1)
2. **Tool calling**: give the model a `run_sql(query)` tool restricted to read-only queries on `gold_*` tables. (Days 2-3)
3. **RAG**: chunk a short synthetic "credit policy" document, embed it, store in a vector index, retrieve at question time. (Days 4-6)
4. **Agent loop with LangGraph**: router node (data question vs policy question), SQL node, retrieval node, answer node
   with citations. (Days 7-10)
5. **Evaluation**: 30 golden questions with known answers; score exact-match for numbers and an LLM judge for explanations.
   Track the score as you change prompts. (Days 11-12)
6. **Guardrails and MCP**: block non-SELECT SQL, cap rows, log every tool call; expose `run_sql` as an MCP server. (Days 13-14)

## Platform notes (check before building)
- Free Edition restricts outbound internet to trusted domains, so calling external LLM APIs from notebooks may be blocked.
  Prefer Databricks-hosted foundation models, or run the agent code locally against exported tables.
- Keep the agent in `agent/` with its own tests. Your data engineering skills (clean tables, quality checks) are the
  differentiator: agents fail on bad data, and you can prove yours is good.

## Done when
- [ ] 30-question eval set with a score above 80% on numeric questions
- [ ] Every answer shows the SQL it ran
- [ ] Unsafe SQL (DROP, UPDATE, multiple statements) is rejected by a test
