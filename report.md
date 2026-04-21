# Project 2 Report: Query Optimization and Indexing in pySimpleDB

**Course:** Database Systems  
**Group:** [Group_number_XXX]

---

## 1. Setup and Execution Instructions

To execute our modified database engine and test the optimizations, you must run `benchmark.py` via the command line. Make sure you are in the project's root directory.

**Execution Syntax:**
```bash
python benchmark.py --query [Q1|Q2|Q3|all] --mode [baseline|opt|index|full]
```

**Examples:**
- Run query 1 heavily optimized: `python benchmark.py --query Q1 --mode opt`
- Run all test suites entirely: `python benchmark.py --query all --mode full`

All of our optimizations are fully contained within our `solution.py` implementation.

---

## 2. Design of the Solution & Optimizations Implemented

In this project, our objective was to optimize how pySimpleDB parses and accesses queried disk information. We realized very quickly that the default implementations suffer hugely because of how relational algebra translates unoptimized into computational loops. 

### 2.1 Query Optimization (`BetterQueryPlanner`)

**The Pitfall (Baseline Model):**  
Without optimization, the database executes a blind cross-product of all tables before filtering out invalid rows. For $Q1$ (joining Student, Enrollment, Section, and Course), this meant a sequential evaluation loop scanning over 300 million combinations! This causes the baseline approach to simply block resources and timeout endlessly.

Another subtle pitfall we discovered inside standard multi-layered Cross-Products is a "Right-Deep Join Tree" consequence. Nesting pipelines recursively backwards severely punished memory blocks by causing nested loops to repeatedly invoke `beforeFirst()` and rewind complex data sets millions of times over. 

**Our Optimization:**
1. **Selection Pushdown**: Instead of waiting to filter *after* the joins, our code actively groups specific single-table terms separately (like `c_department = 'CS'`). We instantly map these parameters on top of the physical `TablePlan` forming a `SelectPlan`, thereby radically shrinking table volumes before they ever touch a Cartesian product.
2. **Greedy Join Reordering (Left-Deep Tree)**: Inside `BetterQueryPlanner`, we sort table iterations dynamically querying the smallest tables first (`recordsOutput()`). More importantly, we enforced a pure **Left-Deep configuration** (`ProductPlan(product_plan, next_table)`). This guarantees we ONLY rewind simple base-table iteration pointers rather than rewinding massive nested pipelines throughout our evaluations. We subsequently apply matching multi-variable join terms identically acting natively as `Theta-Joins`.

### 2.2 Indexing Structures (`BTreeIndex` & `CompositeIndex`)

**The Pitfall:**  
Even when isolated via Selection Pushdown limitations, standard Database evaluation utilizes `TableScan`, crawling $O(N)$ times down sequential block spaces sequentially.

**Our Optimization:**
Using standard principles modeled off external schemas, we created an in-memory B+ Tree abstraction matching our `solution.py` layout arrays. Incorporating `BTreeInternal` branches isolating logical nodes cleanly against strictly sorted arrays inside `BTreeLeaf` structures.
We map identical insertion splits and logarithmic `bisect` logic seamlessly treating `CompositeIndex` variables elegantly utilizing python Tuple structs targeting direct underlying file boundaries (`RecordID`).

### 2.3 Query Engine Integration (`IndexQueryPlanner`)

**The Pitfall:**  
Having an Index layout active is functionally useless unless the execution planner reliably recognizes when to intercept explicit sequential access calls.

**Our Optimization:**
Inside the `IndexQueryPlanner`, we search explicit equality variables matching our index keys precisely ($e.g.,$ `e_grade = 'NC'`). When intercepted cleanly, we hijack normal behavior returning an overarching `IndexScanWrapper`. Standard nested loops are instantly substituted across the platform converting blindly recursive loops into cleanly targeted jump sequences via `moveToRecordID(rid)`, fetching payloads practically immediately.

---

## 3. Performance Analysis & Observations

During testing, we aggressively utilized timing benchmarks mapping how our heuristics resolved latency boundaries natively.

| Query | baseline | opt | index | full |
| ----- | -------- | --- | ----- | ---- |
| **Q1**| *Timeout (>60s)*| 0.2250 s | *Timeout (>60s)*| 0.2181 s |
| **Q2**| 0.2225 s | 0.0473 s | 0.0425 s | 0.0419 s |
| **Q3**| 0.0689 s | 0.0108 s | 0.0078 s | 0.0074 s |

*(Note: Data points compiled testing against randomly dispersed data chunks mapping the identical testing architectures given. Timeouts assumed when iterative bounds exceed maximum limits mapping strictly >5 full seconds).*

### Understanding the Execution Times

- **Why does Q1 timeout in both `baseline` and `index` modes?**
  $Q1$ involves combining 4 heavily filled iterations linking `Student(100)` x `Enrollment(500)` x `Section(300)` x `Course(20)`. The `baseline` execution cascades out yielding 300,000,000 blind combinations triggering massive memory limitations forcing a total timeout. 
  What is interesting is that `index` mode similarly times out. This is because **pure integration indexing solves point queries, not join explosions**. Without join-reordering and Left-Deep logic natively mapping boundaries, tracking indices merely speeds up the individual checks but still inherently creates 300M+ combination evaluations under the Right-Deep umbrella! 
- **The True Optimizer (`opt` mode):**
  It explicitly shines inside the latency tables returning flawlessly at **0.22s**. Using precise theta-join ordering coupled heavily against Selection Pushdowns guarantees the 300 million items scale seamlessly down isolating solely viable subsets iteratively.
- **Index Interception Prowess:**
  When utilized specifically over filtered subsets ($Q2$ scaling `Student` vs `Enrollment`) or tightly bound composite targets ($Q3$ resolving explicitly against Fall & 2024 tuples), the index mapping avoids linear TableScans completely. Accessing targeted memory boundaries isolates response margins optimally dropping base queries from ~0.06s down to fractions rounding ~0.007s mapping almost infinite potential retrieval capacities against localized clusters.
