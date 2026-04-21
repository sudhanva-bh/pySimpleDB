Project Report: Group 42
Query Optimization and Indexing in pySimpleDB
1. Problem Description and Assumptions
Problem Description
The objective of this project is to enhance the performance of a lightweight educational database system, pySimpleDB, by incorporating core database optimization techniques. The baseline system supports query execution but lacks:
Efficient query optimization
Index structures for fast data retrieval
Intelligent execution planning
As a result, queries—especially those involving joins and selective conditions—suffer from poor performance due to:
Full table scans
Inefficient join ordering
Large intermediate results
This project addresses these limitations by implementing:
Query optimization techniques (selection pushdown, join reordering)
B+ Tree indexing
Composite indexing
Index-aware query execution
Assumptions
All queries follow relational algebra semantics supported by pySimpleDB
Predicate conditions are expressed as conjunctions of terms
Only equality-based conditions are used for index matching
Indexes are maintained in-memory for fast lookup
The system must work for arbitrary schemas and not rely on hardcoded queries
2. System Design and Architecture
The system is designed as an extension of pySimpleDB, introducing optimization and indexing layers while maintaining compatibility with the existing execution engine.
Core Components
1. Query Planner
Responsible for:
Parsing query structure
Identifying tables, fields, and predicates
2. Optimizer (BetterQueryPlanner)
Implements:
Predicate classification
Selection pushdown
Join reordering (left-deep trees)
3. Indexing Layer
Includes:
BTreeIndex (single attribute index)
CompositeIndex (multi-attribute index)
4. Index-Aware Planner (IndexQueryPlanner)
Detects applicable indexes
Replaces table scans with index scans
Integrates with optimizer
5. Execution Engine
Executes query plans using:
SelectPlan
ProductPlan
ProjectPlan
IndexScan
3. Query Optimization Techniques
3.1 Predicate Classification
Predicates are divided into:
Single-table predicates → used for selection pushdown
Multi-table predicates → used for join conditions
Implementation
Each predicate term is analyzed
Fields are mapped to corresponding tables
Terms are classified based on table involvement

Impact
Enables early filtering
Improves join efficiency
Reduces unnecessary computation
3.2 Selection Pushdown
Selection conditions are applied directly on individual tables before performing joins.
Implementation
Combine all single-table predicates per table
Wrap table plan using:

 SelectPlan(table_plan, predicate)


Effect
Transforms execution from: Join → Filter To: Filter → Join
Performance Benefit
Reduces input size for joins
Minimizes intermediate results
Improves execution speed significantly
3.3 Join Order Optimization
A greedy algorithm is used to determine the optimal join order.
Strategy
Estimate table size using recordsOutput()
Sort tables in ascending order of size
Join smallest tables first
3.4 Left-Deep Join Tree Construction
Joins are constructed in a left-deep manner:
ProductPlan(product_plan, next_table)
Why Left-Deep?
Avoids repeated scanning of complex intermediate results
Prevents exponential growth in nested loops
Ensures efficient pipeline execution
3.5 Early Join Predicate Application
Join conditions are applied immediately after joining two tables.
Implementation
Identify applicable multi-table predicates
Apply using SelectPlan
Benefit
Prevents Cartesian product explosion
Filters invalid tuples early
4. Indexing Implementation
4.1 B+ Tree Index
A fully functional in-memory B+ Tree structure is implemented.

Structure
Leaf Nodes
Store keys and corresponding RecordIDs
Linked sequentially
Internal Nodes
Store separator keys
Guide traversal
Key Features
Sorted insertion using binary search
Automatic node splitting
Balanced tree structure
Logarithmic search time
Operations
Insert
Traverse to leaf
Insert key in sorted order
Split node if capacity exceeded
Search
Traverse internal nodes
Perform binary search in leaf
Return matching RecordIDs
4.2 Composite Index
Supports indexing on multiple attributes.
Implementation
Keys stored as tuples:  (attr1, attr2, ...)


Matching Condition
Index is used only if all attributes are present in query
Benefit
Efficient multi-condition filtering
Reduces multiple scans
5. Index Integration with Query Execution
5.1 IndexQueryPlanner
Responsible for integrating index usage into query execution.
Index Detection
Scans predicates for equality conditions
Matches fields with available indexes
Selects best index
Composite Index Matching
Collects all constant conditions
Checks full attribute match
Constructs tuple key
IndexScanWrapper
Replaces standard table scans with index-based access.
Optimization Trick
recordsOutput() = 1
Effect
Forces optimizer to prioritize indexed tables
Improves join ordering
5.2 IndexScan Execution
Instead of scanning all records:
moveToRecordID(rid)
Benefit
Direct access to required records
Eliminates full table scans
Improves performance drastically
6. Index Creation Mechanism
Indexes are dynamically created using a scanning process.
Steps
Initialize index structures
Perform full table scan
Insert each record into index
Key Features
Works for any schema
No hardcoding
Supports both single and composite indexes
7. Performance Evaluation
Observations
Q1 (Join-intensive query)
Optimization is essential
Indexing alone does not help
Q2 (Selection query)
Index significantly improves performance

Q3 (Multi-attribute filtering)
Composite index provides maximum benefit.

