from pyiceberg import catalog
from pyiceberg.catalog import Identifier
from pyiceberg.expressions import AlwaysTrue
from typing import List, Union
import os

class LakeView():

    def __init__(self):        
        self.catalog = catalog.load_catalog("default", 
            **{
                'uri': os.environ.get("PYICEBERG_CATALOG__DEFAULT__URI"),
                'token': os.environ.get("PYICEBERG_CATALOG__DEFAULT__TOKEN"),
                's3.endpoint':  os.environ.get("AWS_ENDPOINT"),
                'py-io-impl':   'pyiceberg.io.fsspec.FsspecFileIO',
            })
        self.namespace_options = []

    def get_namespaces(_self, include_nested: bool = True):
        result = []
        namespaces = _self.catalog.list_namespaces()
        for ns in namespaces:
            new_ns = ns if len(ns) == 1 else ns[:1]
            result.append(new_ns)
            if (include_nested):
                result += _self._get_nested_namespaces(new_ns, 1)
        result = list(result)
        result.sort()
        return result

    def _get_nested_namespaces(self, namespace: Union[str, Identifier] = (), level: int = 1) -> List[Identifier]:
        result = []
        namespaces = self.catalog.list_namespaces(namespace)
        for ns in namespaces:
            #pyiceberg includes the initial level at the beginning for nested namespaces
            fixed_ns = ns if (len(ns) == (level + 1)) else ns[level:]
            result.append(fixed_ns)
            result += self._get_nested_namespaces(fixed_ns, level + 1)
        return result
    
    def get_tables(self, namespace: str):
        tables = self.catalog.list_tables(namespace)
        tables.sort()
        return tables
    
    def load_table(self, table_id: str):
        table = self.catalog.load_table(table_id)
        return table
    
    def get_partition_data(self, table_id):        
        table = self.catalog.load_table(table_id)
        pa_partitions = table.inspect.partitions().sort_by([('partition', 'ascending')])
        cols = self.paTable_to_dataTable(pa_partitions)
        return cols

    def get_snapshot_data(self, table_id):        
        table = self.catalog.load_table(table_id)
        pa_snaps = table.inspect.snapshots().sort_by([('committed_at', 'descending')])
        cols = self.paTable_to_dataTable(pa_snaps)        
        return cols

    def get_sample_data(self, table_id, partition, limit=100):
        table = self.catalog.load_table(table_id)
        row_filter = self.get_row_filter(partition, table) 
        tab_scan = table.scan(limit=limit, row_filter = row_filter)
        
        if table.metadata.current_snapshot_id is None:
                return None
        else:            
            try:
                rbr = tab_scan.to_arrow_batch_reader()
                for batch in rbr:
                    return self.paTable_to_dataTable(batch)                   
            except PermissionError:                
                if row_filter == AlwaysTrue():
                    row_filter = ''

    def get_row_filter(self, partition, table):
        if partition is None or len(partition) == 0:
            return AlwaysTrue()
        fields = table.spec().fields
        use_fields = []
        for field in fields:
            source_field = table.schema().find_column_name(field.source_id)            
            if 'bucket' in str(field.transform):
                continue  #filter by bucket not yet supported, add others not supported too
            use_fields.append(source_field)
        expression=''
        idx = 0
        for key, value in partition.items():
            if key in use_fields:
                if idx == 0 or len(use_fields)==1:
                    expression = f"{key}=='{value}'"
                else:
                    expression += f" and {key}=='{value}'"
            idx += 1
        return expression if len(expression) > 0 else AlwaysTrue()
    
    def paTable_to_dataTable(self, paTable):
        data = paTable.to_pandas().to_json(orient='records')
        return data
