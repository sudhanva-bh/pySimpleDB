from Planner import TablePlan, SelectPlan, ProjectPlan, ProductPlan
from RelationalOp import Predicate, Term, Expression, Constant, SelectScan, ProjectScan, ProductScan
from Record import Schema, Layout, TableScan, RecordID
from Metadata import MetadataMgr
from Transaction import Transaction
import bisect

class BetterQueryPlanner:
    """
    Optimized query planner implementing Selection Pushdown and left-deep Join Reordering.
    """
    def __init__(self, mm):
        self.mm = mm

    def createPlan(self, tx, query_data):
        table_plans = {}
        for table_name in query_data['tables']:
            table_plans[table_name] = TablePlan(tx, table_name, self.mm)

        # -------------------------------------------------------------
        # Step 1: Predicate Classification
        # -------------------------------------------------------------
        # We classify predicates into single-table conditions (for selection pushdown)
        # and multi-table conditions (for join filtering / theta joins).
        single_table_preds = {t: [] for t in query_data['tables']}
        multi_table_preds = []

        predicate = query_data['predicate']
        if predicate and hasattr(predicate, 'terms'):
            for term in predicate.terms:
                term_fields = []
                # Check LHS and RHS to extract the fields being evaluated
                if not isinstance(term.lhs.exp_value, Constant):
                    term_fields.append(term.lhs.exp_value)
                if not isinstance(term.rhs.exp_value, Constant):
                    term_fields.append(term.rhs.exp_value)
                
                # Identify which tables the fields in this term belong to
                term_tables = set()
                for f in term_fields:
                    for t_name, t_plan in table_plans.items():
                        if f in t_plan.plan_schema().getFields():
                            term_tables.add(t_name)
                
                # If it only spans 1 table, it's a pushdown candidate. 
                # Otherwise, it's a join condition.
                if len(term_tables) == 1:
                    single_table_preds[list(term_tables)[0]].append(term)
                else:
                    multi_table_preds.append(term)
        
        # -------------------------------------------------------------
        # Step 2: Selection Pushdown
        # -------------------------------------------------------------
        # Apply single-table conditions to shrink the inputs BEFORE joining
        for t_name in table_plans:
            if single_table_preds[t_name]:
                pred = Predicate(single_table_preds[t_name][0])
                for pt in single_table_preds[t_name][1:]:
                    pred.terms.append(pt)
                table_plans[t_name] = SelectPlan(table_plans[t_name], pred)

        # -------------------------------------------------------------
        # Step 3: Greedy Join Reordering (Left-Deep Tree)
        # -------------------------------------------------------------
        tables_to_join = list(table_plans.values())
        
        # Start the join chain with the smallest table to minimize intermediate data
        tables_to_join.sort(key=lambda p: p.recordsOutput() if p.recordsOutput() is not None else float('inf'))
        product_plan = tables_to_join.pop(0)

        # Continuously join the next smallest table that shares a condition with the current joined result
        while tables_to_join:
            best_idx = 0
            best_preds = []
            
            # Find a table that shares a join condition with our current pipeline
            for i, p in enumerate(tables_to_join):
                temp_schema = ProductPlan(product_plan, p).plan_schema()
                applicable_preds = []
                for mtp in multi_table_preds:
                    tf = []
                    if not isinstance(mtp.lhs.exp_value, Constant): tf.append(mtp.lhs.exp_value)
                    if not isinstance(mtp.rhs.exp_value, Constant): tf.append(mtp.rhs.exp_value)
                    
                    # If this predicate spans exactly the merged schema, we can apply it
                    if all(f in temp_schema.getFields() for f in tf):
                        applicable_preds.append(mtp)
                
                if applicable_preds:
                    best_idx = i
                    best_preds = applicable_preds
                    break
            
            next_table = tables_to_join.pop(best_idx)
            
            # IMPORTANT: Left-deep Join Tree construction!
            # Argument order must be (product_plan, next_table). This prevents the complex 
            # nested pipeline (product_plan) from being constantly re-evaluated/rewound 
            # each iteration, resulting in O(N) operations rather than O(N^x).
            product_plan = ProductPlan(product_plan, next_table)
            
            # Apply the join condition(s) immediately to filter out cross-product pairs
            if best_preds:
                pred = Predicate(best_preds[0])
                for pt in best_preds[1:]:
                    pred.terms.append(pt)
                for pt in best_preds:
                    multi_table_preds.remove(pt)
                product_plan = SelectPlan(product_plan, pred)

        # -------------------------------------------------------------
        # Step 4: Finalize Plan
        # -------------------------------------------------------------
        # If any cross-product-only conditions still linger, apply them now 
        if multi_table_preds:
            pred = Predicate(multi_table_preds[0])
            for pt in multi_table_preds[1:]:
                pred.terms.append(pt)
            product_plan = SelectPlan(product_plan, pred)

        return ProjectPlan(product_plan, *query_data['fields'])


# -------------------------------------------------------------
# B+ Tree Index Components
# -------------------------------------------------------------
class BTreeLeaf:
    """Leaf nodes store actual keys mapping directly to payload arrays (RecordIDs)."""
    def __init__(self):
        self.keys = []
        self.values = []
        self.next = None

class BTreeInternal:
    """Internal nodes act purely as navigation markers toward leaves based on boundaries."""
    def __init__(self):
        self.keys = []
        self.children = []

class BTreeIndex:
    """
    Robust in-memory B+ Tree managing splits and logarithmic navigations.
    """
    def __init__(self, tx, index_name, key_type, key_length):
        self.tx = tx
        self.index_name = index_name
        self.order = 10  # Maximum keys a node can hold before cracking/splitting
        self.root = BTreeLeaf()

    def insert(self, key_value, record_id):
        node = self.root
        parents = []
        
        # Traverse downwards to proper leaf node
        while not isinstance(node, BTreeLeaf):
            parents.append(node)
            idx = bisect.bisect_right(node.keys, key_value)
            node = node.children[idx]
        
        # Insert or append payload into the selected leaf
        idx = bisect.bisect_left(node.keys, key_value)
        if idx < len(node.keys) and node.keys[idx] == key_value:
            node.values[idx].append(record_id)
        else:
            node.keys.insert(idx, key_value)
            node.values.insert(idx, [record_id])
            
        # Re-balance the tree upward if thresholds are crossed
        if len(node.keys) > self.order:
            self._split(node, parents)

    def _split(self, node, parents):
        mid = len(node.keys) // 2
        
        if isinstance(node, BTreeLeaf):
            new_node = BTreeLeaf()
            new_node.keys = node.keys[mid:]
            new_node.values = node.values[mid:]
            node.keys = node.keys[:mid]
            node.values = node.values[:mid]
            new_node.next = node.next
            node.next = new_node
            split_key = new_node.keys[0] # Median copied upwards
        else:
            new_node = BTreeInternal()
            new_node.keys = node.keys[mid+1:]
            new_node.children = node.children[mid+1:]
            split_key = node.keys[mid] # Median pushed upwards and eliminated here
            node.keys = node.keys[:mid]
            node.children = node.children[:mid+1]
            
        # Handle Root node explosion
        if not parents:
            new_root = BTreeInternal()
            new_root.keys = [split_key]
            new_root.children = [node, new_node]
            self.root = new_root
        else:
            # Handle standard internal explosion pushing to parent
            parent = parents.pop()
            idx = bisect.bisect_left(parent.keys, split_key)
            parent.keys.insert(idx, split_key)
            parent.children.insert(idx + 1, new_node)
            
            # Recursive check if pushing upwards violates parent caps
            if len(parent.keys) > self.order:
                self._split(parent, parents)

    def search(self, key_value):
        """Scans the B+ tree logarithmically descending into the leaf array matches."""
        node = self.root
        while not isinstance(node, BTreeLeaf):
            idx = bisect.bisect_right(node.keys, key_value)
            node = node.children[idx]
            
        idx = bisect.bisect_left(node.keys, key_value)
        if idx < len(node.keys) and node.keys[idx] == key_value:
            return node.values[idx]
        return []

    def close(self):
        pass


class CompositeIndex(BTreeIndex):
    """
    Subclasses the BTreeIndex logic directly but orchestrates insertion/search 
    treating combined attributes cleanly as unified tuple structs instead.
    """
    def __init__(self, tx, index_name, field_names, field_types, field_lengths):
        super().__init__(tx, index_name, None, None)
        self.field_names = field_names


class IndexScan:
    """
    A lightweight traversal interface that relies on explicit RecordID matching 
    rather than exhaustively looping blind data blocks.
    """
    def __init__(self, table_scan, index, search_key):
        self.table_scan = table_scan
        self.rids = index.search(search_key)
        self.current_idx = -1

    def beforeFirst(self):
        self.current_idx = -1

    def nextRecord(self):
        self.current_idx += 1
        if self.current_idx < len(self.rids):
            rid = self.rids[self.current_idx]
            # Jump directly to target chunk bypassing O(N) evaluation bounds!
            self.table_scan.moveToRecordID(rid) 
            return True
        return False
        
    def getInt(self, field_name): return self.table_scan.getInt(field_name)
    def getString(self, field_name): return self.table_scan.getString(field_name)
    def getVal(self, field_name): return self.table_scan.getVal(field_name)
    def hasField(self, field_name): return self.table_scan.hasField(field_name)
    def closeRecordPage(self): self.table_scan.closeRecordPage()


class IndexQueryPlanner:
    """
    Planner orchestrating the injection of IndexScans anywhere queries 
    utilize direct attribute equality alignments overlapping stored Index definitions.
    """
    def __init__(self, mm, indexes, better_planner=None):
        self.mm = mm
        self.indexes = indexes
        self.better_planner = better_planner if better_planner else BetterQueryPlanner(mm)

    def createPlan(self, tx, query_data):
        predicate = query_data['predicate']
        table_plans = {}
        
        for table_name in query_data['tables']:
            table_plan = TablePlan(tx, table_name, self.mm)
            
            best_index = None
            best_key = None
            
            if predicate and hasattr(predicate, 'terms'):
                for term in predicate.terms:
                    # Catch assignments corresponding to T1.attribute = Constant
                    if isinstance(term.rhs.exp_value, Constant) and not isinstance(term.lhs.exp_value, Constant):
                        field = term.lhs.exp_value
                        const_val = term.rhs.exp_value.const_value
                        
                        if field in table_plan.plan_schema().getFields():
                            if table_name in self.indexes and field in self.indexes[table_name]:
                                best_index = self.indexes[table_name][field]
                                best_key = const_val
                                
                    elif isinstance(term.lhs.exp_value, Constant) and not isinstance(term.rhs.exp_value, Constant):
                        field = term.rhs.exp_value
                        const_val = term.lhs.exp_value.const_value
                        
                        if field in table_plan.plan_schema().getFields():
                            if table_name in self.indexes and field in self.indexes[table_name]:
                                best_index = self.indexes[table_name][field]
                                best_key = const_val
            
            # Check Composite indexing compatibility seamlessly
            if table_name in self.indexes:
                for idx_key, idx_obj in self.indexes[table_name].items():
                    if isinstance(idx_key, tuple):
                        match_vals = {}
                        if predicate and hasattr(predicate, 'terms'):
                            for term in predicate.terms:
                                if isinstance(term.rhs.exp_value, Constant) and not isinstance(term.lhs.exp_value, Constant):
                                    match_vals[term.lhs.exp_value] = term.rhs.exp_value.const_value
                                elif isinstance(term.lhs.exp_value, Constant) and not isinstance(term.rhs.exp_value, Constant):
                                    match_vals[term.rhs.exp_value] = term.lhs.exp_value.const_value
                        
                        # Only bind to this composition if ALL sub-target fields present matches 
                        if all(f in match_vals for f in idx_key):
                            best_index = idx_obj
                            best_key = tuple(match_vals[f] for f in idx_key)
                            break
                            
            if best_index:
                # Wrap intercepted table plan inside lightweight Indexing layer 
                class IndexScanWrapper:
                    def __init__(self, tp, index, key):
                        self.tp = tp
                        self.index = index
                        self.key = key
                    def open(self): return IndexScan(self.tp.open(), self.index, self.key)
                    # Fake optimal metadata profiles tricking the greedy planner sorting phase:
                    def blocksAccessed(self): return 1 
                    def recordsOutput(self): return 1 
                    def distinctValues(self, field_name): return self.tp.distinctValues(field_name)
                    def plan_schema(self): return self.tp.plan_schema()
                        
                table_plans[table_name] = IndexScanWrapper(table_plan, best_index, best_key)
            else:
                table_plans[table_name] = table_plan

        # =========================================================
        # Execute exactly the same pipeline layout mapping inside BetterQueryPlanner 
        # (Handling pushdown/left-deep structure identically) but inheriting Index wrappers.
        # =========================================================
        single_table_preds = {t: [] for t in query_data['tables']}
        multi_table_preds = []

        if predicate and hasattr(predicate, 'terms'):
            for term in predicate.terms:
                term_fields = []
                if not isinstance(term.lhs.exp_value, Constant): term_fields.append(term.lhs.exp_value)
                if not isinstance(term.rhs.exp_value, Constant): term_fields.append(term.rhs.exp_value)
                
                term_tables = set()
                for f in term_fields:
                    for t_name, t_plan in table_plans.items():
                        if f in t_plan.plan_schema().getFields():
                            term_tables.add(t_name)
                
                if len(term_tables) == 1:
                    single_table_preds[list(term_tables)[0]].append(term)
                else:
                    multi_table_preds.append(term)
        
        for t_name in table_plans:
            if single_table_preds[t_name]:
                pred = Predicate(single_table_preds[t_name][0])
                for pt in single_table_preds[t_name][1:]: pred.terms.append(pt)
                table_plans[t_name] = SelectPlan(table_plans[t_name], pred)

        tables_to_join = list(table_plans.values())
        tables_to_join.sort(key=lambda p: p.recordsOutput() if p.recordsOutput() is not None else float('inf'))
        product_plan = tables_to_join.pop(0)

        while tables_to_join:
            best_idx = 0
            best_preds = []
            
            for i, p in enumerate(tables_to_join):
                temp_schema = ProductPlan(product_plan, p).plan_schema()
                applicable_preds = []
                for mtp in multi_table_preds:
                    tf = []
                    if not isinstance(mtp.lhs.exp_value, Constant): tf.append(mtp.lhs.exp_value)
                    if not isinstance(mtp.rhs.exp_value, Constant): tf.append(mtp.rhs.exp_value)
                    if all(f in temp_schema.getFields() for f in tf): applicable_preds.append(mtp)
                
                if applicable_preds:
                    best_idx = i
                    best_preds = applicable_preds
                    break
            
            next_table = tables_to_join.pop(best_idx)
            product_plan = ProductPlan(product_plan, next_table)
            
            if best_preds:
                pred = Predicate(best_preds[0])
                for pt in best_preds[1:]: pred.terms.append(pt)
                for pt in best_preds: multi_table_preds.remove(pt)
                product_plan = SelectPlan(product_plan, pred)

        if multi_table_preds:
            pred = Predicate(multi_table_preds[0])
            for pt in multi_table_preds[1:]: pred.terms.append(pt)
            product_plan = SelectPlan(product_plan, pred)

        return ProjectPlan(product_plan, *query_data['fields'])


def create_indexes(db, tx, index_defs=None, composite_index_defs=None):
    """
    Handles dynamically iterating given definitions configuring and populating caches 
    ahead of complex operations using TableScans. 
    Returns organized dictionary dictating {table_name: {field_key: IndexObject}}
    """
    indexes = {}
    
    # 1. Instantiate Core Tree structures 
    if index_defs:
        for t_name, fields in index_defs.items():
            if t_name not in indexes: indexes[t_name] = {}
            for f_name, f_type, f_length in fields:
                indexes[t_name][f_name] = BTreeIndex(tx, f"{t_name}_{f_name}_idx", f_type, f_length)
                
    # 2. Instantiate Complex composite arrays 
    if composite_index_defs:
        for t_name, composites in composite_index_defs.items():
            if t_name not in indexes: indexes[t_name] = {}
            for f_names, f_types, f_lengths in composites:
                indexes[t_name][f_names] = CompositeIndex(tx, f"{t_name}_comp_idx", f_names, f_types, f_lengths)

    # 3. Stream base relations directly copying targeted payloads to mappings cache
    for t_name, t_indexes in indexes.items():
        ts = TableScan(tx, t_name, db.mm.getLayout(tx, t_name))
        ts.beforeFirst()
        while ts.nextRecord():
            rid = ts.currentRecordID()
            for key, idx in t_indexes.items():
                if isinstance(key, tuple):
                    idx.insert(tuple(ts.getVal(k) for k in key), rid)
                else:
                    idx.insert(ts.getVal(key), rid)
        ts.closeRecordPage()
                
    return indexes
