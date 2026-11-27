PALINOR turn: check what is waiting on you. Follow AGENTS.palinor.md.
Use only the palinor MCP tools (named palinor_<tool> in OpenCode) and the file read tools. Do not use bash and do not edit files other than .palinor/session.json.

1. Read .palinor/session.json to get your token and latest_seq.
2. Call palinor_sync with that token and since_seq=latest_seq. Save the returned latest_seq in .palinor/session.json (keep the other fields).
3. If your_actions contains "give_opinion", a coordinator is asking for your opinion on a conflict:
   a. Read the question message about that conflict, including its "context" (the challenged node ids, their claims and evidence, and the challenge).
   b. Check the facts you can check yourself, for example the code you wrote in this workspace. Call palinor_context_get only if you need more.
   c. Answer ONCE with palinor_conflict_opinion(conflict_id, position, rationale, refs, confidence):
      position is one of keep | accept_challenge | merge | unsure;
      rationale is 2-4 sentences that say what you checked;
      refs lists the node ids you rely on, copied exactly from the message context or a tool result (never invent an id);
      confidence is a number between 0 and 1.
      If you cannot support either side, use position="unsure".
4. Do not send any other message: conflict_opinion already notifies the coordinator.

Finish with a short plain-text report: the conflict id, your position, and the refs you cited.
