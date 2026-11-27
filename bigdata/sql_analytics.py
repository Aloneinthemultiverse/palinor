"""Spark SQL analytics over the PALINOR data lake.

Task / command / decision analytics plus the collaboration layer: who talks to whom (communication
matrix), how fast each platform answers and reads, and how debates flow through Laya
(perspectives → votes → synthesis → decision / tie-break).

spark-submit bigdata/sql_analytics.py
"""
from __future__ import annotations

from pyspark.sql import functions as F

from common import load_events, spark_session


def plat(id_col: str, members_alias: str) -> str:
    """Platform of a participant id: PARTICIPANT_CONNECTED payload first, else the id prefix
    (claude-…, local-llm-…, human-<project> for the dashboard user, laya, system)."""
    return f"COALESCE({members_alias}.platform, regexp_extract({id_col}, '^(local-llm|[a-z]+)', 1))"


# Views over the collaboration events, created in this order (each may use the previous ones).
VIEWS = {
    "members": """
        SELECT project_id, participant_id, MAX(platform) AS platform FROM events
        WHERE event_type = 'PARTICIPANT_CONNECTED' AND platform IS NOT NULL
        GROUP BY project_id, participant_id""",
    "msgs": f"""
        SELECT e.project_id, e.message_id, COALESCE(e.thread_id, e.message_id) AS thread_id, e.reply_to, e.kind,
               e.participant_id AS sender, {plat('e.participant_id', 'm')} AS sender_platform,
               e.event_time AS sent_at, e.recipients, e.chars,
               COALESCE(array_contains(e.recipients, 'all'), false) AS broadcast
        FROM events e LEFT JOIN members m ON m.project_id = e.project_id AND m.participant_id = e.participant_id
        WHERE e.event_type = 'MESSAGE_SENT' AND e.message_id IS NOT NULL""",
    "msg_edges": f"""
        SELECT x.project_id, x.message_id, x.kind, x.sender_platform,
               CASE WHEN x.rcpt = 'all' THEN 'broadcast' ELSE {plat('x.rcpt', 'm')} END AS recipient_platform
        FROM (SELECT g.project_id, g.message_id, g.kind, g.sender_platform, lower(rr) AS rcpt
              FROM msgs g LATERAL VIEW explode(g.recipients) t AS rr) x
        LEFT JOIN members m ON m.project_id = x.project_id AND m.participant_id = x.rcpt""",
    "reads": f"""
        SELECT r.project_id, r.participant_id AS reader, {plat('r.participant_id', 'm')} AS reader_platform,
               r.message_id, r.event_time AS read_at
        FROM events r LEFT JOIN members m ON m.project_id = r.project_id AND m.participant_id = r.participant_id
        WHERE r.event_type = 'MESSAGE_READ' AND r.message_id IS NOT NULL""",
    "debates": """
        SELECT project_id, question_key, MIN(debate_id) AS debate_id, MIN(mode) AS mode, MIN(event_time) AS opened_at
        FROM events WHERE event_type = 'DEBATE_OPENED' AND question_key IS NOT NULL
        GROUP BY project_id, question_key""",
    "options": """
        SELECT DISTINCT project_id, question_key, proposal_id, COALESCE(kind, 'perspective') AS kind,
               participant_id AS author
        FROM events WHERE event_type IN ('PROPOSAL_CREATED', 'SYNTHESIS_CREATED') AND proposal_id IS NOT NULL""",
    "votes": """
        SELECT project_id, proposal_id, participant_id AS voter, vote_value, event_time
        FROM events WHERE event_type = 'VOTE_CAST' AND proposal_id IS NOT NULL""",
    "verdicts": """
        SELECT project_id, question_key, event_time,
               CASE WHEN event_type = 'DECISION_COMMITTED' THEN IF(COALESCE(is_tiebreak, false), 'TIEBREAK', 'DECIDED')
                    WHEN event_type = 'VERIFICATION_REQUESTED' THEN 'VERIFY'
                    ELSE substr(event_type, 6) END AS outcome
        FROM events
        WHERE (substr(event_type, 1, 5) = 'LAYA_' OR event_type IN ('DECISION_COMMITTED', 'VERIFICATION_REQUESTED'))
          AND question_key IS NOT NULL""",
    "debate_facts": """
        SELECT d.project_id, d.question_key, d.mode, d.opened_at,
               COALESCE(o.perspectives, 0) AS perspectives, COALESCE(o.syntheses, 0) AS syntheses,
               COALESCE(v.votes_on_perspectives, 0) AS votes_on_perspectives,
               COALESCE(v.votes_on_syntheses, 0) AS votes_on_syntheses,
               COALESCE(l.synth, 0) AS needed_synthesis, COALESCE(l.escalated, 0) AS escalated,
               COALESCE(l.nudged, 0) AS needed_vote_nudge,
               dc.decided_at, COALESCE(dc.tiebreak, 0) AS tiebreak, COALESCE(dc.synthesis_won, 0) AS synthesis_won,
               (unix_millis(dc.decided_at) - unix_millis(d.opened_at)) / 60000.0 AS minutes_to_decision
        FROM debates d
        LEFT JOIN (SELECT project_id, question_key, SUM(IF(kind = 'synthesis', 0, 1)) AS perspectives,
                          SUM(IF(kind = 'synthesis', 1, 0)) AS syntheses
                   FROM options GROUP BY project_id, question_key) o USING (project_id, question_key)
        LEFT JOIN (SELECT o.project_id, o.question_key, SUM(IF(o.kind = 'synthesis', 0, 1)) AS votes_on_perspectives,
                          SUM(IF(o.kind = 'synthesis', 1, 0)) AS votes_on_syntheses
                   FROM votes v JOIN options o ON o.project_id = v.project_id AND o.proposal_id = v.proposal_id
                   GROUP BY o.project_id, o.question_key) v USING (project_id, question_key)
        LEFT JOIN (SELECT project_id, question_key, MAX(IF(outcome = 'SYNTHESIZE', 1, 0)) AS synth,
                          MAX(IF(outcome = 'ESCALATE', 1, 0)) AS escalated, MAX(IF(outcome = 'NEED_VOTES', 1, 0)) AS nudged
                   FROM verdicts GROUP BY project_id, question_key) l USING (project_id, question_key)
        LEFT JOIN (SELECT project_id, question_key, MIN(event_time) AS decided_at,
                          MAX(CAST(COALESCE(is_tiebreak, false) AS INT)) AS tiebreak,
                          MAX(CAST(COALESCE(is_synthesis, false) AS INT)) AS synthesis_won
                   FROM events WHERE event_type = 'DECISION_COMMITTED' GROUP BY project_id, question_key) dc
          USING (project_id, question_key)""",
}


def communication_matrix(spark):
    """Sender platform × recipient platform; a message to several recipients counts once per recipient,
    a broadcast (to 'all') counts once in the 'broadcast' column."""
    m = spark.table("msg_edges").groupBy("sender_platform").pivot("recipient_platform").count().na.fill(0)
    cols = sorted((c for c in m.columns if c not in ("sender_platform", "broadcast")))
    cols += ["broadcast"] if "broadcast" in m.columns else []
    return (m.select("sender_platform", *cols)
             .withColumn("total", sum((F.col(c) for c in cols), F.lit(0)))
             .orderBy(F.desc("total")))


QUERIES = {
    "Task duration by task type": """
        SELECT task_type, COUNT(*) AS completed, ROUND(AVG(duration_ms)/60000, 1) AS avg_min,
               ROUND(PERCENTILE_APPROX(duration_ms, 0.9)/60000, 1) AS p90_min
        FROM events WHERE event_type = 'TASK_COMPLETED' AND task_type IS NOT NULL
        GROUP BY task_type ORDER BY avg_min DESC""",
    "Failure rate by execution environment": """
        SELECT environment,
               SUM(CASE WHEN event_type='TASK_FAILED' THEN 1 ELSE 0 END) AS failed, COUNT(*) AS total,
               ROUND(100.0*SUM(CASE WHEN event_type='TASK_FAILED' THEN 1 ELSE 0 END)/COUNT(*), 2) AS failure_pct
        FROM events WHERE event_type IN ('TASK_COMPLETED','TASK_FAILED') AND environment IS NOT NULL
        GROUP BY environment ORDER BY failure_pct DESC""",
    "Participant workload / contribution (top 10)": """
        SELECT participant_id, COUNT(*) AS tasks_done, ROUND(SUM(duration_ms)/3600000, 1) AS busy_hours
        FROM events WHERE event_type='TASK_COMPLETED' GROUP BY participant_id
        ORDER BY tasks_done DESC LIMIT 10""",
    "Retries per task type": """
        SELECT task_type, ROUND(AVG(retry_count), 3) AS avg_retries, MAX(retry_count) AS max_retries
        FROM events WHERE event_type IN ('TASK_COMPLETED','TASK_FAILED') AND task_type IS NOT NULL
        GROUP BY task_type ORDER BY avg_retries DESC""",
    "Command acknowledgement latency (seconds)": """
        SELECT ROUND(AVG(duration_ms)/1000, 1) AS avg_ack_s,
               ROUND(PERCENTILE_APPROX(duration_ms, 0.95)/1000, 1) AS p95_ack_s,
               SUM(CASE WHEN event_type='COMMAND_EXPIRED' THEN 1 ELSE 0 END) AS expired
        FROM events WHERE event_type IN ('COMMAND_ACCEPTED','COMMAND_EXPIRED')""",
    "Average decision latency by project (slowest 10)": """
        SELECT project_id, COUNT(*) AS decisions, ROUND(AVG(duration_ms)/60000, 1) AS avg_decision_min
        FROM events WHERE event_type='DECISION_COMMITTED'
        GROUP BY project_id ORDER BY avg_decision_min DESC LIMIT 10""",
    "Contradiction → resolution time (minutes)": """
        WITH c AS (SELECT project_id, question, MIN(event_time) AS raised FROM events
                   WHERE event_type='CONTRADICTION_CREATED' GROUP BY project_id, question),
             d AS (SELECT project_id, question, MIN(event_time) AS decided FROM events
                   WHERE event_type='DECISION_COMMITTED' GROUP BY project_id, question)
        SELECT COUNT(*) AS resolved,
               ROUND(AVG((unix_timestamp(d.decided)-unix_timestamp(c.raised))/60), 1) AS avg_min_to_resolve,
               ROUND(100.0*SUM(CASE WHEN v.project_id IS NOT NULL THEN 1 ELSE 0 END)/COUNT(*), 1) AS pct_needed_verification
        FROM c JOIN d ON c.project_id=d.project_id AND c.question=d.question AND d.decided >= c.raised
        LEFT JOIN (SELECT DISTINCT project_id, question FROM events WHERE event_type='VERIFICATION_REQUESTED') v
          ON v.project_id=c.project_id AND v.question=c.question""",
    "PR creation → merge latency (minutes)": """
        SELECT ROUND(AVG((unix_timestamp(m.t)-unix_timestamp(o.t))/60), 1) AS avg_merge_min, COUNT(*) AS merged
        FROM (SELECT commit_sha, MIN(event_time) t FROM events WHERE event_type='PULL_REQUEST_CREATED' GROUP BY commit_sha) o
        JOIN (SELECT commit_sha, MIN(event_time) t FROM events WHERE event_type='PULL_REQUEST_MERGED' GROUP BY commit_sha) m
          ON o.commit_sha = m.commit_sha""",
    "Hourly throughput (time series, last 12 h of data)": """
        SELECT date_trunc('hour', event_time) AS hour, COUNT(*) AS events,
               SUM(CASE WHEN event_type='TASK_COMPLETED' THEN 1 ELSE 0 END) AS tasks_done
        FROM events GROUP BY 1 ORDER BY 1 DESC LIMIT 12""",
    # ------------------------------------------------------------------ collaboration
    "Communication matrix: sender platform x recipient platform (messages)": communication_matrix,
    "Messaging activity by sender platform": """
        SELECT sender_platform, COUNT(*) AS messages, ROUND(100.0 * AVG(IF(broadcast, 1, 0)), 1) AS pct_broadcast,
               SUM(IF(kind = 'question', 1, 0)) AS questions, SUM(IF(kind = 'answer', 1, 0)) AS answers,
               SUM(IF(kind = 'context', 1, 0)) AS context_msgs, SUM(IF(kind IN ('request', 'alert'), 1, 0)) AS requests,
               ROUND(AVG(chars)) AS avg_chars
        FROM msgs GROUP BY sender_platform ORDER BY messages DESC""",
    "Question -> answer response latency by responder platform (reply joined to its parent question)": """
        WITH q AS (SELECT project_id, message_id, thread_id, sender, sent_at FROM msgs WHERE kind = 'question'),
             a AS (SELECT project_id, message_id, thread_id, reply_to, sender, sender_platform, sent_at
                   FROM msgs WHERE kind = 'answer'),
             pairs AS (
               SELECT a.sender_platform AS responder_platform,
                      (unix_millis(a.sent_at) - unix_millis(q.sent_at)) / 1000.0 AS latency_s,
                      ROW_NUMBER() OVER (PARTITION BY a.project_id, a.message_id ORDER BY q.sent_at DESC) AS rn
               FROM a JOIN q ON q.project_id = a.project_id AND q.thread_id = a.thread_id
                AND q.sender <> a.sender AND q.sent_at <= a.sent_at
                AND (a.reply_to IS NULL OR q.message_id = a.reply_to))  -- parent: reply_to, else latest question in thread
        SELECT responder_platform, COUNT(*) AS answers,
               ROUND(PERCENTILE_APPROX(latency_s, 0.5) / 60, 1) AS p50_min,
               ROUND(PERCENTILE_APPROX(latency_s, 0.9) / 60, 1) AS p90_min,
               ROUND(AVG(latency_s) / 60, 1) AS avg_min
        FROM pairs WHERE rn = 1 GROUP BY responder_platform ORDER BY p50_min""",
    "Answer rate by addressee platform (direct questions)": f"""
        WITH dq AS (SELECT g.project_id, g.message_id, g.thread_id, g.sent_at, lower(rr) AS addressee
                    FROM msgs g LATERAL VIEW explode(g.recipients) t AS rr
                    WHERE g.kind = 'question' AND NOT g.broadcast),
             answered AS (SELECT dq.project_id, dq.message_id, dq.addressee, MIN(a.sent_at) AS answered_at
                          FROM dq JOIN msgs a ON a.project_id = dq.project_id AND a.thread_id = dq.thread_id
                           AND a.kind = 'answer' AND a.sender = dq.addressee AND a.sent_at >= dq.sent_at
                          GROUP BY dq.project_id, dq.message_id, dq.addressee)
        SELECT {plat('dq.addressee', 'm')} AS addressee_platform, COUNT(*) AS asked,
               ROUND(100.0 * COUNT(x.answered_at) / COUNT(*), 1) AS answered_pct,
               ROUND(PERCENTILE_APPROX((unix_millis(x.answered_at) - unix_millis(dq.sent_at)) / 60000.0, 0.5), 1)
                 AS p50_min_to_answer
        FROM dq LEFT JOIN answered x
          ON x.project_id = dq.project_id AND x.message_id = dq.message_id AND x.addressee = dq.addressee
        LEFT JOIN members m ON m.project_id = dq.project_id AND m.participant_id = dq.addressee
        GROUP BY 1 ORDER BY answered_pct DESC""",
    "Read latency by reader platform (MESSAGE_SENT -> MESSAGE_READ)": """
        SELECT reader_platform, COUNT(*) AS reads, ROUND(100.0 * AVG(IF(broadcast, 1, 0)), 1) AS pct_broadcast,
               ROUND(PERCENTILE_APPROX(lat_s, 0.5) / 60, 1) AS p50_min,
               ROUND(PERCENTILE_APPROX(lat_s, 0.9) / 60, 1) AS p90_min,
               ROUND(AVG(lat_s) / 60, 1) AS avg_min,
               ROUND(100.0 * AVG(IF(lat_s <= 600, 1, 0)), 1) AS pct_within_10min
        FROM (SELECT r.reader_platform, g.broadcast, (unix_millis(r.read_at) - unix_millis(g.sent_at)) / 1000.0 AS lat_s
              FROM reads r JOIN msgs g ON g.project_id = r.project_id AND g.message_id = r.message_id)
        GROUP BY reader_platform ORDER BY p50_min""",
    "Context sharing and chat ingestion by platform": f"""
        SELECT {plat('e.participant_id', 'm')} AS platform,
               SUM(IF(e.event_type = 'CONTEXT_SHARED', 1, 0)) AS context_shares,
               ROUND(AVG(IF(e.event_type = 'CONTEXT_SHARED', CAST(get_json_object(e.payload, '$.nodes') AS INT), NULL)), 2)
                 AS avg_nodes_shared,
               SUM(IF(e.event_type = 'CONVERSATION_INGESTED', 1, 0)) AS chats_ingested,
               ROUND(AVG(IF(e.event_type = 'CONVERSATION_INGESTED', e.chars, NULL))) AS avg_chat_chars,
               SUM(IF(e.event_type = 'CONVERSATION_INGESTED',
                      CAST(get_json_object(e.payload, '$.findings') AS INT)
                      + CAST(get_json_object(e.payload, '$.agreements') AS INT), 0)) AS findings_extracted,
               SUM(IF(e.event_type = 'CONVERSATION_INGESTED',
                      CAST(get_json_object(e.payload, '$.tasks') AS INT), 0)) AS tasks_extracted
        FROM events e LEFT JOIN members m ON m.project_id = e.project_id AND m.participant_id = e.participant_id
        WHERE e.event_type IN ('CONTEXT_SHARED', 'CONVERSATION_INGESTED')
        GROUP BY 1 ORDER BY context_shares DESC""",
    "Debate funnel by mode": """
        SELECT COALESCE(mode, 'ALL') AS mode, COUNT(*) AS debates,
               ROUND(AVG(perspectives), 2) AS perspectives_per_debate,
               ROUND(SUM(votes_on_perspectives) / NULLIF(SUM(perspectives), 0), 2) AS votes_per_perspective,
               ROUND(100.0 * AVG(needed_vote_nudge), 1) AS pct_needed_vote_nudge,
               ROUND(100.0 * AVG(needed_synthesis), 1) AS pct_needed_synthesis,
               ROUND(100.0 * AVG(escalated), 1) AS pct_escalated,
               ROUND(100.0 * AVG(IF(decided_at IS NOT NULL, 1, 0)), 1) AS pct_decided,
               ROUND(100.0 * SUM(tiebreak) / NULLIF(COUNT(decided_at), 0), 1) AS tiebreak_rate_pct,
               ROUND(100.0 * SUM(synthesis_won) / NULLIF(COUNT(decided_at), 0), 1) AS synthesis_selected_pct
        FROM debate_facts GROUP BY ROLLUP(mode) ORDER BY debates""",
    "Laya outcome distribution (debates)": """
        SELECT v.outcome, SUM(IF(d.mode = 'deliberative', 1, 0)) AS deliberative,
               SUM(IF(d.mode = 'empirical', 1, 0)) AS empirical, COUNT(*) AS total,
               ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
        FROM verdicts v JOIN debates d ON d.project_id = v.project_id AND d.question_key = v.question_key
        GROUP BY v.outcome ORDER BY total DESC""",
    "Time to decision by mode and path (minutes, debate opened -> decision committed)": """
        SELECT mode, CASE WHEN tiebreak = 1 THEN 'tie-break' WHEN needed_synthesis = 1 THEN 'via synthesis'
                          ELSE 'direct' END AS path,
               COUNT(*) AS decided, ROUND(PERCENTILE_APPROX(minutes_to_decision, 0.5), 1) AS p50_min,
               ROUND(PERCENTILE_APPROX(minutes_to_decision, 0.9), 1) AS p90_min,
               ROUND(AVG(minutes_to_decision), 1) AS avg_min
        FROM debate_facts WHERE decided_at IS NOT NULL GROUP BY 1, 2 ORDER BY 1, p50_min""",
    # ---------------------------------------------------------------- orchestration (one coordinator commands)
    "Orchestration: plans, dispatches and re-dispatch rate": """
        SELECT SUM(CASE WHEN event_type='PLAN_SUBMITTED' THEN 1 ELSE 0 END) AS plans,
               SUM(CASE WHEN event_type='PLAN_COMPLETED' THEN 1 ELSE 0 END) AS plans_completed,
               ROUND(100.0*SUM(CASE WHEN event_type='PLAN_COMPLETED' THEN 1 ELSE 0 END)
                     / NULLIF(SUM(CASE WHEN event_type='PLAN_SUBMITTED' THEN 1 ELSE 0 END),0), 1) AS completion_pct,
               SUM(CASE WHEN event_type='TASK_DISPATCHED' THEN 1 ELSE 0 END) AS dispatches,
               SUM(CASE WHEN event_type='TASK_REASSIGNED' AND get_json_object(payload,'$.reason') LIKE 'rejected%' THEN 1 ELSE 0 END) AS after_rejection,
               SUM(CASE WHEN event_type='TASK_REASSIGNED' AND get_json_object(payload,'$.reason') LIKE '%offline%' THEN 1 ELSE 0 END) AS after_offline,
               ROUND(100.0*SUM(CASE WHEN event_type='TASK_REASSIGNED' THEN 1 ELSE 0 END)
                     / NULLIF(SUM(CASE WHEN event_type='TASK_DISPATCHED' THEN 1 ELSE 0 END),0), 2) AS redispatch_pct
        FROM events""",
    "Hand-off latency: upstream TASK_COMPLETED -> next TASK_DISPATCHED (seconds)": """
        WITH seq AS (
          SELECT project_id, event_type, event_time,
                 last(CASE WHEN event_type='TASK_COMPLETED' THEN event_time END, true)
                   OVER (PARTITION BY project_id ORDER BY event_time
                         ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_done
          FROM events WHERE event_type IN ('TASK_COMPLETED','TASK_DISPATCHED'))
        SELECT COUNT(*) AS handoffs,
               ROUND(PERCENTILE_APPROX(unix_timestamp(event_time)-unix_timestamp(prev_done), 0.5), 1) AS p50_s,
               ROUND(PERCENTILE_APPROX(unix_timestamp(event_time)-unix_timestamp(prev_done), 0.9), 1) AS p90_s
        FROM seq WHERE event_type='TASK_DISPATCHED' AND prev_done IS NOT NULL
          AND unix_timestamp(event_time)-unix_timestamp(prev_done) BETWEEN 0 AND 600""",
    "Dispatches by receiving platform (who the coordinator's work goes to)": """
        SELECT regexp_extract(target_participant_id, '^(local-llm|[a-z]+)', 1) AS platform,
               COUNT(*) AS dispatched, ROUND(AVG(CAST(get_json_object(payload,'$.load') AS INT)), 2) AS avg_load_at_dispatch
        FROM events WHERE event_type='TASK_DISPATCHED' GROUP BY 1 ORDER BY dispatched DESC""",
    # ---------------------------------------------------------------- conflict protocol
    "Conflict funnel: raised -> consulted -> opinions -> resolved": """
        WITH c AS (SELECT get_json_object(payload,'$.conflict_id') AS cid, event_type, event_time, payload
                   FROM events WHERE event_type LIKE 'CONFLICT_%')
        SELECT COUNT(DISTINCT CASE WHEN event_type='CONFLICT_RAISED' THEN cid END) AS raised,
               COUNT(DISTINCT CASE WHEN event_type='CONFLICT_CONSULTED' THEN cid END) AS consulted,
               SUM(CASE WHEN event_type='CONFLICT_OPINION' THEN 1 ELSE 0 END) AS opinions,
               COUNT(DISTINCT CASE WHEN event_type='CONFLICT_RESOLVED' THEN cid END) AS resolved,
               COUNT(DISTINCT CASE WHEN event_type='CONFLICT_WITHDRAWN' THEN cid END) AS withdrawn,
               ROUND(100.0*SUM(CASE WHEN event_type='CONFLICT_RESOLVED' AND get_json_object(payload,'$.against_evidence')='true' THEN 1 ELSE 0 END)
                     / NULLIF(COUNT(DISTINCT CASE WHEN event_type='CONFLICT_RESOLVED' THEN cid END),0), 1) AS pct_against_evidence,
               ROUND(100.0*SUM(CASE WHEN event_type='CONFLICT_RESOLVED' AND get_json_object(payload,'$.forced')='true' THEN 1 ELSE 0 END)
                     / NULLIF(COUNT(DISTINCT CASE WHEN event_type='CONFLICT_RESOLVED' THEN cid END),0), 1) AS pct_forced
        FROM c""",
    "Conflict resolution mix and time to resolve (minutes)": """
        WITH r AS (SELECT get_json_object(payload,'$.conflict_id') cid, MIN(event_time) raised FROM events
                   WHERE event_type='CONFLICT_RAISED' GROUP BY 1),
             s AS (SELECT get_json_object(payload,'$.conflict_id') cid, get_json_object(payload,'$.resolution') resolution,
                          MIN(event_time) resolved FROM events WHERE event_type='CONFLICT_RESOLVED' GROUP BY 1, 2)
        SELECT s.resolution, COUNT(*) AS conflicts,
               ROUND(PERCENTILE_APPROX((unix_timestamp(resolved)-unix_timestamp(raised))/60, 0.5), 1) AS p50_min,
               ROUND(PERCENTILE_APPROX((unix_timestamp(resolved)-unix_timestamp(raised))/60, 0.9), 1) AS p90_min
        FROM s JOIN r ON s.cid=r.cid GROUP BY 1 ORDER BY conflicts DESC""",
    "Consultation answer latency by consulted platform (minutes)": """
        WITH q AS (SELECT get_json_object(payload,'$.conflict_id') cid, MIN(event_time) asked FROM events
                   WHERE event_type='CONFLICT_CONSULTED' GROUP BY 1),
             o AS (SELECT get_json_object(payload,'$.conflict_id') cid, participant_id, event_time answered,
                          get_json_object(payload,'$.position') position FROM events WHERE event_type='CONFLICT_OPINION')
        SELECT regexp_extract(o.participant_id, '^(local-llm|[a-z]+)', 1) AS platform, COUNT(*) AS opinions,
               ROUND(PERCENTILE_APPROX((unix_timestamp(answered)-unix_timestamp(asked))/60, 0.5), 1) AS p50_min,
               ROUND(100.0*AVG(CASE WHEN position='unsure' THEN 1 ELSE 0 END), 1) AS pct_unsure
        FROM o JOIN q ON o.cid=q.cid GROUP BY 1 ORDER BY opinions DESC""",
    "Who raises conflicts (platform) and how severe": """
        SELECT regexp_extract(participant_id, '^(local-llm|[a-z]+)', 1) AS raiser_platform,
               COUNT(*) AS raised,
               SUM(CASE WHEN get_json_object(payload,'$.severity')='high' THEN 1 ELSE 0 END) AS high,
               ROUND(AVG(CAST(get_json_object(payload,'$.evidence') AS INT)), 2) AS avg_evidence_items
        FROM events WHERE event_type='CONFLICT_RAISED' GROUP BY 1 ORDER BY raised DESC""",
}


def main() -> None:
    spark = spark_session("sql-analytics")
    spark.sparkContext.setLogLevel("WARN")
    load_events(spark).cache().createOrReplaceTempView("events")
    for name, sql in VIEWS.items():
        spark.sql(sql).createOrReplaceTempView(name)
    for title, query in QUERIES.items():
        print(f"\n=== {title} ===")
        (query(spark) if callable(query) else spark.sql(query)).show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
