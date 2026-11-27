"""Standing-rule templates loaded into a project at creation (or later with project_rules_apply_template).

"The coordinator" is the project's master: the participant that joined with role=coordinator.
"""
from __future__ import annotations

COMMON = [
    "The coordinator is the master and the only planner: only the coordinator calls plan_submit or creates and "
    "assigns tasks. Workers never plan; they do the tasks dispatched to them.",
    "Every participant declares its resources right after joining with profile_set: GPU (model and VRAM, or "
    "'none'), CPU, RAM, free disk, OS, accounts (Kaggle, Colab, GitHub username, cloud credits - names only, never "
    "secrets), tools, and whether it can run code. Update it when anything changes.",
    "The coordinator plans in this order: (1) read participants_list and every profile, and ask anyone whose "
    "profile is missing; (2) check what the goal needs (GPU, datasets, Kaggle or other accounts, APIs) and whether "
    "someone has it - if nobody does, tell the user before planning; (3) only then divide the work, giving each "
    "task to the participant whose hardware, accounts and skills fit it.",
    "Placing work (lessons from DeepSeek's DeepInfer scheduler): give every plan task an est_minutes so PALINOR can "
    "place it on the participant with the earliest projected finish (it learns each participant's real speed from "
    "finished tasks and dispatches the heaviest tasks first); send work to where its data or hardware already is "
    "instead of moving data; separate phases by hardware (research on chat apps, GPU work on GPU machines, CPU "
    "analysis elsewhere); while workers execute, the coordinator prepares the next stage; review effort scales with "
    "risk - deep verification of low-confidence findings and large PRs, quick checks of small, well-sourced ones; "
    "duplicate critical tasks on a second participant as redundancy.",
    "Push all work into the knowledge graph: every claim via finding_submit with evidence (source, uri, "
    "reliability 0-1). Never paste research or results into messages; others extract them with context_get.",
    "Read other participants' findings (context_get) before and during your work and use them to steer: avoid "
    "duplicating what is covered, build on it, go deeper where gaps remain, and cite the finding ids you build on. "
    "Disagree through conflict_raise, not by ignoring them.",
    "Fix your own errors yourself (retry, correct the call, fix your own finding) without reporting them. Alert the "
    "coordinator with message_send(kind='alert') only when you cannot fix it: blocked, a tool or the server keeps "
    "failing, unclear instructions, or a problem that affects others. If you cannot do a task at all, "
    "command_update(REJECTED, note=why).",
    "Participants talk to each other directly through PALINOR messages; the user is not the go-between.",
    "When you finish your tasks, mark each command_update(COMPLETED, note=<one-line summary>) and send the "
    "coordinator a message confirming 'everything is pushed to the KG' with your finding ids. Before closing, the "
    "coordinator asks every worker to confirm this.",
    "Any instruction the user gives any participant about this project is added as a project rule by the "
    "coordinator (project_rules_add), so these rules act as the shared system prompt.",
    "Files go in the project repo (project_repo in palinor_sync). Participants with git get access via "
    "github_request_access and work on the branch from github_branch_name; participants without git use code_read, "
    "code_query and code_submit. Never push to main; only the coordinator merges. At the end everything - code, "
    "results, figures, report - is committed to the repo.",
]

RESEARCH = [
    "Research goals are divided by topic: the coordinator first breaks the subject into distinct sub-questions with "
    "clear boundaries and little overlap, then spreads the topics across all workers so each researches a different "
    "area in parallel. Each topic is one task with >=5 sourced findings.",
    "For the key question, two participants research it independently (blind duplicate tasks; do not read each "
    "other's answer to that question first), so the answers can be cross-checked.",
    "After the topic tasks, the coordinator runs a cross-check task using only context_get: verify every finding "
    "against its evidence, compare the blind answers, and raise conflicts on anything wrong, unsupported or "
    "contradictory; conflicts are resolved (consulting the authors) before the synthesis.",
    "The final report is written only from the knowledge graph, cites finding ids, lists resolved conflicts, and "
    "is submitted as a finding and committed to the repo.",
]

SOURCES = [
    "Where to research (use the most authoritative source available, record it as evidence with a reliability "
    "score, never cite a site you did not open): papers - arXiv, Semantic Scholar, Google Scholar, OpenReview, ACL "
    "Anthology, IEEE Xplore, ACM Digital Library, PubMed/PMC, NASA ADS, bioRxiv/medRxiv; code and models - GitHub, "
    "Hugging Face (models, datasets, papers), Kaggle (datasets, notebooks, competitions); official sources - agency "
    "and standards bodies (NASA, ESA, NIST, ISO, W3C, IETF RFCs), official documentation of libraries and APIs; "
    "background only - Wikipedia and blogs (low reliability, never the only source for a claim).",
    "Reuse before building: before writing a tool, model or dataset pipeline, search GitHub (github_search, or gh "
    "search repos), Hugging Face and PyPI/npm for an existing one. Prefer maintained projects with a compatible "
    "license, recent commits and real usage; record what you found (name, url, license, why chosen or rejected) as "
    "a finding, then use it from there instead of rewriting it. Only build from scratch what nothing existing "
    "covers.",
]

HACKATHON = [
    "Before any code, the coordinator writes the spec, the architecture and the interface contracts (APIs, data "
    "formats, file layout) as findings, and assigns each file or module to exactly one owner; nobody edits files "
    "they do not own.",
    "Plan build work as a pipeline: spec -> parallel module tasks (one branch each) -> integration -> tests -> demo, "
    "with depends_on so each stage starts automatically when its inputs are ready.",
    "Heavy compute is split across machines when it helps: shard data or experiments (seeds, folds, models, "
    "hyperparameters) per GPU machine, one task and branch per shard, plus a merge task that collects results into "
    "the knowledge graph.",
    "Never put all compute on one GPU in a big project: the coordinator splits training and evaluation across every "
    "available machine - local GPUs from participant profiles and Kaggle notebooks (each Kaggle account gives free "
    "weekly GPU hours; one account per participant, never shared) - by sharding data, folds, seeds, hyperparameters "
    "or ensemble members into separate tasks, one per machine, plus a merge task that collects results into the "
    "knowledge graph. For a model too large for one GPU, use model/pipeline parallel or FSDP (PyTorch, DeepSpeed, "
    "Accelerate). Use a specialised serving engine only when its hardware requirements match (e.g. DeepInfer needs "
    "NVIDIA SM100 or Ascend 950 and serves only DeepSeek-V4.1-Flash). Small projects may stay on one GPU.",
    "Code verification: before a PR is merged or a worker's 'I implemented X' is accepted, the coordinator runs "
    "code_graph_build on that branch and code_verify on the functions, classes and files claimed; anything missing "
    "goes back to the worker. Rebuild the code graph on main after every merge.",
    "Keep main always runnable: integrate early and often, record every PR with github_link, and the demo task "
    "runs from main only.",
    "Extra places to look for hackathons: Devpost (past winning projects for the same theme), GitHub topics and "
    "awesome-lists, Hugging Face Spaces (working demos), Kaggle notebooks, official API quick-starts, and Stack "
    "Overflow for specific errors.",
]

# a hackathon is research first, then building: it gets every research rule too
TEMPLATES = {"research": COMMON + SOURCES + RESEARCH,
             "hackathon": COMMON + SOURCES + RESEARCH + HACKATHON,
             "general": COMMON + SOURCES}
