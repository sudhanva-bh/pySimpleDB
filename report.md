# Report: Query Optimization and Indexing in pySimpleDB

## 1. Setup and Execution Instructions

To execute the application and evaluate the optimizations, run `benchmark.py` via the command line interface. Make sure you are in the project's root directory. 

**Execution Syntax:**
```bash
python benchmark.py --query [Q1|Q2|Q3|all] --mode [baseline|opt|index|full]
```

**Examples:**
- `python benchmark.py --query Q1 --mode opt`
- `python benchmark.py --query all --mode full`

The evaluated code modifications are exclusively enclosed within `solution.py`.

---

## 2. Design of the Solution

The solution expands the foundational query compiler into a query optimizer capable of heuristics-based evaluation and indexed search, consisting of the following key implementations:

### Query Optimization (`BetterQueryPlanner`)
- **Predicate Classification**: Distinguishes single-table selection predicates from cross-table join predicates.
- **Selection Pushdown**: Early evaluation constraint. Single-table predicates are immediately applied to `TablePlan` forming a restricted `SelectPlan`. This shrinks the intermediate nested loops required.
- **Left-Deep Join Tree & Theta Joins**: Dynamically reorders cross products sequentially starting from the table offering the smallest block estimation. Iteratively constructs sequential products (`ProductPlan(product_plan, next_table)`) exclusively pairing elements with applicable mapping conditions (`multi_table_preds`) ensuring a tightly coupled inner join and skipping uncontrolled combinations.

### Index Structures (`BTreeIndex` & `CompositeIndex`)
- **Memory-Oriented B+ Tree Implementation**: A custom generic sequence-based B+ Tree algorithm (`BTreeInternal` & `BTreeLeaf`) dictates data alignment spanning insertions (`insert()`), capacity bounds splits (`_split()`), and recursive key navigation (`search()`), caching specific `RecordID` payloads.
- **Composite Index**: Automatically extends the structured indexing principles to allow multi-attribute parameterizations. Matches tuples of varying structures uniformly against multi-level dependencies.

### Query Engine Integration (`IndexQueryPlanner`)
- **`IndexScan` Delegation**: Introduces an efficient substitution mechanism to standard data scraping routines via directly resolving targeted `RecordID` parameters mapping tightly back down to physical block bounds (`moveToRecordID`).
- **Index Hijacking Validation**: Replaces naive nested loop targets globally inside the planner sequence. Intercepts queries bearing strictly explicit parameters mapping accurately back towards instantiated index caches, reducing iteration domains from $O(N)$ back down to practically zero margins.

---

## 3. Observations

1. **Catastrophic Pipeline Accumulations**: During original iterations, sequential `ProductPlan()` bindings forming *Right-Deep* configurations invoked endless buffer thrashing via `beforeFirst()` restarts. Shifting the schema toward *Left-Deep* alignment significantly suppressed loop execution counts.
2. **Exponential Complexity Mitigation**: The unoptimized base implementations produced cross joins across $N$ table sets resulting in $O(N^4)$ evaluations for complex scopes scaling past millions. Optimization pipelines effectively restricted traversals safely down to proportional sizes mapped linearly.
3. **Index Access Impact Layering**: Indexing provided significant latency recovery mapping isolated parameterizations successfully (E.g., $CS$ scopes, $Fall-2024$ scopes), but scaled redundantly when paired with broad unbounded join hierarchies unless actively reordered (e.g., `full` mode mapping optimization alongside indices providing the maximum delta). 

---

## 4. Performance Table

The following benchmarks demonstrate the execution timing for the respective modes. 

| Query | baseline | opt | index | full |
| ----- | -------- | --- | ----- | ---- |
| **Q1**| *Timeout (>60s)*| 0.2250 s | *Timeout (>60s)*| 0.2181 s |
| **Q2**| 0.2225 s | 0.0473 s | 0.0425 s | 0.0419 s |
| **Q3**| 0.0689 s | 0.0108 s | 0.0078 s | 0.0074 s |

*(Note: Q1 involves four heavily joined tables leading purely nested `baseline` approaches and unmodified join orders in `index` mode directly toward loop exhaustion bounds mapping to extremely volatile iteration footprints exceeding manageable timeouts. Time metrics for unblocked phases derived from executing benchmark iterations under Windows environment timing loops).*

## 5. Directory Integrity
- Handcoded changes strictly affect `solution.py` with zero reliance strictly tied arbitrarily to explicit `Record`, `Metadata` schema defaults outside universally parsed index definitions.
- Report packaged correctly under standard reporting formats ready for submission zip bundling along with target source definitions.
