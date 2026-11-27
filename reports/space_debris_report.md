# Active Space Debris Removal: what works, what it costs, what to do first

PALINOR project `P-f0a7a814` · plan `PLAN-2c6a1abb` · planned by Master-Claude, researched by claude-coordinator (worker) and Master-Claude.
Built from the knowledge graph: 42 sourced findings + 1 cross-check finding (F-57e77ce5) + 1 resolved conflict (CF-d9f6a9f6).

> **Status: final.** The worker confirmed "everything is pushed to the KG" (36 findings) and agreed with the resolution of CF-d9f6a9f6.

## Recommendation

1. **Remove large, intact derelicts first, not fragments.** Removing the 50 most-concerning objects would roughly halve the risk of new debris; removing ~5 large objects a year could stabilise LEO. (F-187eca96, F-cbde2d84)
2. **First targets, chosen independently by both researchers (identical lists):**
   1. Soviet/Russian **SL-16 (Zenit-2) upper stages**, ~9 t each, ~840 km cluster
   2. **Envisat**, ~8 t, ~765 km, dead since 2012
   3. **SL-8 (Kosmos-3M) stages**, ~1.4 t, numerous and identical, so one servicer design could remove many
   4. **Chinese Long March upper stages**, the main ongoing source of new massive rocket bodies
   5. **H-IIA upper stage (GOSAT, 2009)**: lower risk, but the most removal-ready: already inspected, and Japan is a willing owner
3. **Method:** a capture-and-tug servicer with a **robotic arm**, followed by a **controlled reentry** for multi-tonne objects. Lasers are the most cost-effective option for 1–10 cm debris but can't move large objects. Drag sails only suit small craft below ~850 km. (F-557d01ad, F-4a4f51b2, F-98dba4b3, F-d75f92c3)
4. **Start with H-IIA, then Envisat.** Both owners are willing, which avoids the Outer Space Treaty consent problem that blocks removing Soviet-era stages. (F-2bc93ec0, F-e6377034)

## Key evidence

| Area | Findings |
|---|---|
| Population | ~47,140 tracked objects; >17,400 t in orbit; ESA MASTER-8 estimates ~68,450 objects >10 cm and ~1.5 M of 1–10 cm. Worst band: 750–1000 km, especially ~840–850 km. LEO is already unstable. |
| Capture | Nets: flown 2018, TRL 6–7. Harpoons: TRL 5–6, fragment risk. Magnetic plates: cooperative targets only (ELSA-d). Robotic arms: lead option, not yet flown on a tumbling target. Foam, adhesive, tethers: TRL 2–4. |
| Deorbit | Drag only clears low orbits. Drag sails work only below ~850 km. Electrodynamic tethers are prototypes. Ion-beam shepherd is unflown. Big derelicts need a tug and a controlled reentry (1:10,000 casualty limit). |
| Missions | RemoveDEBRIS (2018–19: net and harpoon on its own targets). ELSA-d (2021: magnetic capture, then thruster anomalies). ADRAS-J (2024: inspected H-IIA, closest ~15 m). ADRAS-J2 (~2027: first real capture). ClearSpace-1 retargeted to PROBA-1 (~2029). **No real debris has been removed yet.** |
| Economics | NASA: ~$3k–40k per kg for tug removal, so tens to hundreds of millions of dollars per rocket body. Remediation is competitive with mitigation per dollar. Governments are the only paying customers (ESA ~€86M, JAXA CRD2). Orbital-use fees are proposed. |
| Policy | OST Art. VIII: no legal "abandonment", so removal needs the owner state's consent. Liability Convention exposure if a removal fails. FCC 5-year rule (satellites launched after Sept 2024). ESA Zero Debris (non-binding, 2030 goal). France's binding law. First FCC fine (DISH). |

## Cross-check and conflicts

- All 42 findings carry evidence. The worker's sources are stronger (McKnight 2021, ESA, NASA OTPS). Master-Claude's picks #3 and #4 lean on a low-reliability blog, but the worker's independent findings support them.
- **CF-d9f6a9f6, resolved by accepting the challenge:** finding F-15eabfa8 said ADRAS-J came "within metres". Corrected to **~15 m in Nov 2024, then an abort on an attitude anomaly**. The worker's agreeing opinion was already submitted (19:27). The master's watcher missed it because it only watched messages, not conflict opinions.
- No other contradictions were found.

## How this was produced

- One planner (Master-Claude) set 10 tasks. The worker researched 7 in parallel in ~9 minutes, pushing every claim to the knowledge graph with sources.
- The cross-check and this report used only `context_get`: the content was extracted from the graph, not passed in messages.
- The project's 7 owner rules acted as a shared system prompt for both Claudes.
