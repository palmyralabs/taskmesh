# Peer-design comparison

`orchestrator_comparison.xlsx` — 32-row, 7-section comparison across three
designs, built during the grill session that led to this project:

1. **nm_cleanser** (`E:\mydev\customer\senthil\nm_cleanser`) — single-JVM
   pipeline orchestrator (batches → ordered processor sequence).
2. **task-execution.md** — the HPE/LIC toolkit's original multi-node,
   policy-folder task-execution design (this project's direct ancestor).
3. **palmyra76/workflow** (`E:\mydev\palmyra76\workflow`) — multi-node BPMN
   workflow engine.

Sections: Unit of work & scheduling, Concurrency model, Failure & retry,
Pause & hang detection, Processor/task contract, Telemetry & audit,
Developer experience. Each row: a Description column (what the dimension
answers), each project's actual behavior with file/class citations, whether
the mechanism is a confirmed cross-project pattern, and a gap/open question
for taskmesh specifically.

This is research material, not itself a source of decisions — see
`../decisions.md` and `../open-questions.md` for what was actually concluded
from it.
