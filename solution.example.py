"""
This file contains stubs and hints for the Query Optimization & Indexing project.
Rename or copy this file to `solution.py` and implement the methods.
"""

from Planner import TablePlan, SelectPlan, ProjectPlan, ProductPlan
from RelationalOp import Predicate, Term, Expression, Constant, SelectScan, ProjectScan, ProductScan
from Record import Schema, Layout, TableScan, RecordID
from Metadata import MetadataMgr
from Transaction import Transaction


class BetterQueryPlanner:
    """
    Optimized query planner with selection pushdown and join reordering.
    """

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

        # TODO: Implement Selection Pushdown
        
        # TODO: Implement Join Reordering
        
        # TODO: Apply final join conditions and projection
        
        raise NotImplementedError("BetterQueryPlanner.createPlan() not implemented")


class BTreeIndex:
    def __init__(self, tx, index_name, key_type, key_length):
        raise NotImplementedError("BTreeIndex not implemented")
    def insert(self, key_value, record_id):
        raise NotImplementedError
    def search(self, key_value):
        raise NotImplementedError
    def close(self):
        raise NotImplementedError

class CompositeIndex:
    def __init__(self, tx, index_name, field_names, field_types, field_lengths):
        raise NotImplementedError("CompositeIndex not implemented")
    def insert(self, field_values, record_id):
        raise NotImplementedError
    def search(self, field_values):
        raise NotImplementedError
    def close(self):
        raise NotImplementedError

class IndexScan:
    """
    A scan that uses an index instead of scanning all table records.
    Hint: Retrieve the RecordIDs from the index using `search_key`, 
          then iterate through them and position `table_scan` at each RecordID.
    """
    def __init__(self, table_scan, index, search_key):
        raise NotImplementedError("IndexScan not implemented")
    def nextRecord(self):
        raise NotImplementedError
    def getInt(self, field_name):
        return self.table_scan.getInt(field_name)
    def getString(self, field_name):
        return self.table_scan.getString(field_name)
    def getVal(self, field_name):
        return self.table_scan.getVal(field_name)
    def hasField(self, field_name):
        return self.table_scan.hasField(field_name)
    def closeRecordPage(self):
        self.table_scan.closeRecordPage()

class IndexQueryPlanner:
    """
    A planner that optimizes queries by using indexes for equality conditions (field = constant).
    Hint: For each table, if an equality predicate matches an available index, 
          create a custom Plan node that wraps your IndexScan instead of TablePlan.
    """
    def __init__(self, mm, indexes, better_planner=None):
        raise NotImplementedError("IndexQueryPlanner not implemented")
    def createPlan(self, tx, query_data):
        raise NotImplementedError

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
    raise NotImplementedError("create_indexes() not implemented")
