from Planner import TablePlan, SelectPlan, ProjectPlan, ProductPlan
from RelationalOp import Predicate, Term, Expression, Constant, SelectScan, ProjectScan, ProductScan
from Record import Schema, Layout, TableScan, RecordID
from Metadata import MetadataMgr
from Transaction import Transaction
import bisect

class BetterQueryPlanner:
    def __init__(self, mm):
        self.mm = mm

    def createPlan(self, tx, query_data):
        """
        Step 1: Create a TablePlan for each table referenced in the query.
        Step 2: Classify your predicates (terms).
                - Which terms apply to a single table? (Selection Pushdown)
                - Which terms apply to two tables? (Join Conditions)
        Step 3: Apply selection pushdown by wrapping TablePlans with SelectPlans.
        Step 4: Reorder your joins.
                - Start with the smallest table.
                - Iteratively join the next table that has a connecting join condition.
                - Apply the join condition immediately after the ProductPlan.
        Step 5: Apply any remaining conditions and project the required fields.
        """
        table_plans = {}
        for table_name in query_data['tables']:
            table_plans[table_name] = TablePlan(tx, table_name, self.mm)

        single_table_preds = {t: [] for t in query_data['tables']}
        multi_table_preds = []

        predicate = query_data['predicate']
        if predicate and hasattr(predicate, 'terms'):
            for term in predicate.terms:
                term_fields = []
                if not isinstance(term.lhs.exp_value, Constant):
                    term_fields.append(term.lhs.exp_value)
                if not isinstance(term.rhs.exp_value, Constant):
                    term_fields.append(term.rhs.exp_value)
                
                term_tables = set()
                for f in term_fields:
                    for t_name, t_plan in table_plans.items():
                        if f in t_plan.plan_schema().getFields():
                            term_tables.add(t_name)
                
                if len(term_tables) == 1:
                    single_table_preds[list(term_tables)[0]].append(term)
                else:
                    multi_table_preds.append(term)
        
        # Apply Selection Pushdown
        for t_name in table_plans:
            if single_table_preds[t_name]:
                pred = Predicate(single_table_preds[t_name][0])
                for pt in single_table_preds[t_name][1:]:
                    pred.terms.append(pt)
                table_plans[t_name] = SelectPlan(table_plans[t_name], pred)

        # Join Reordering
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
                    
                    if all(f in temp_schema.getFields() for f in tf):
                        applicable_preds.append(mtp)
                
                if applicable_preds:
                    best_idx = i
                    best_preds = applicable_preds
                    break
            
            next_table = tables_to_join.pop(best_idx)
            product_plan = ProductPlan(product_plan, next_table)
            
            if best_preds:
                pred = Predicate(best_preds[0])
                for pt in best_preds[1:]:
                    pred.terms.append(pt)
                for pt in best_preds:
                    multi_table_preds.remove(pt)
                product_plan = SelectPlan(product_plan, pred)

        if multi_table_preds:
            pred = Predicate(multi_table_preds[0])
            for pt in multi_table_preds[1:]:
                pred.terms.append(pt)
            product_plan = SelectPlan(product_plan, pred)

        return ProjectPlan(product_plan, *query_data['fields'])


class BTreeLeaf:
    def __init__(self):
        self.keys = []
        self.values = []
        self.next = None

class BTreeInternal:
    def __init__(self):
        self.keys = []
        self.children = []

class BTreeIndex:
    def __init__(self, tx, index_name, key_type, key_length):
        self.tx = tx
        self.index_name = index_name
        self.order = 10
        self.root = BTreeLeaf()

    def insert(self, key_value, record_id):
        node = self.root
        parents = []
        while not isinstance(node, BTreeLeaf):
            parents.append(node)
            idx = bisect.bisect_right(node.keys, key_value)
            node = node.children[idx]
        
        idx = bisect.bisect_left(node.keys, key_value)
        if idx < len(node.keys) and node.keys[idx] == key_value:
            node.values[idx].append(record_id)
        else:
            node.keys.insert(idx, key_value)
            node.values.insert(idx, [record_id])
            
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
            split_key = new_node.keys[0]
        else:
            new_node = BTreeInternal()
            new_node.keys = node.keys[mid+1:]
            new_node.children = node.children[mid+1:]
            split_key = node.keys[mid]
            node.keys = node.keys[:mid]
            node.children = node.children[:mid+1]
            
        if not parents:
            new_root = BTreeInternal()
            new_root.keys = [split_key]
            new_root.children = [node, new_node]
            self.root = new_root
        else:
            parent = parents.pop()
            idx = bisect.bisect_left(parent.keys, split_key)
            parent.keys.insert(idx, split_key)
            parent.children.insert(idx + 1, new_node)
            if len(parent.keys) > self.order:
                self._split(parent, parents)

    def search(self, key_value):
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
    def __init__(self, tx, index_name, field_names, field_types, field_lengths):
        super().__init__(tx, index_name, None, None)
        self.field_names = field_names


class IndexScan:
    """
    A scan that uses an index instead of scanning all table records.
    Hint: Retrieve the RecordIDs from the index using `search_key`, 
          then iterate through them and position `table_scan` at each RecordID.
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
    A planner that optimizes queries by using indexes for equality conditions (field = constant).
    Hint: For each table, if an equality predicate matches an available index, 
          create a custom Plan node that wraps your IndexScan instead of TablePlan.
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
                    # T1.a = 5
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
            
            # Check Composite
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
                        
                        if all(f in match_vals for f in idx_key):
                            best_index = idx_obj
                            best_key = tuple(match_vals[f] for f in idx_key)
                            break
                            
            if best_index:
                class IndexScanWrapper:
                    def __init__(self, tp, index, key):
                        self.tp = tp
                        self.index = index
                        self.key = key
                    def open(self):
                        return IndexScan(self.tp.open(), self.index, self.key)
                    def blocksAccessed(self): return 1
                    def recordsOutput(self): return 1
                    def distinctValues(self, field_name): return self.tp.distinctValues(field_name)
                    def plan_schema(self): return self.tp.plan_schema()
                        
                table_plans[table_name] = IndexScanWrapper(table_plan, best_index, best_key)
            else:
                table_plans[table_name] = table_plan

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
    Step 1: Instantiate BTreeIndex objects for each entry in index_defs.
            - `index_defs` is a dict {table_name: [(field_name, field_type, field_length), ...]}
    
    Step 2: Instantiate CompositeIndex objects for each entry in composite_index_defs.
            - `composite_index_defs` is a dict {table_name: [((field_names,...), (field_types,...), (field_lengths,...)), ...]}
    
    Step 3: Populate all indexes by scanning each table once.
    
    Returns:
        dict {table_name: {field_key: IndexObject}}
        - field_key is the field name (str) for BTreeIndex 
        - field_key is the tuple of field names for CompositeIndex
    """  
    indexes = {}
    if index_defs:
        for t_name, fields in index_defs.items():
            if t_name not in indexes: indexes[t_name] = {}
            for f_name, f_type, f_length in fields:
                indexes[t_name][f_name] = BTreeIndex(tx, f"{t_name}_{f_name}_idx", f_type, f_length)
                
    if composite_index_defs:
        for t_name, composites in composite_index_defs.items():
            if t_name not in indexes: indexes[t_name] = {}
            for f_names, f_types, f_lengths in composites:
                indexes[t_name][f_names] = CompositeIndex(tx, f"{t_name}_comp_idx", f_names, f_types, f_lengths)

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
