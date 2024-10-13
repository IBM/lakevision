from pyiceberg import catalog
from pyiceberg.catalog import Identifier
from pyiceberg.expressions import AlwaysTrue
from typing import List, Union
import json, os, time
import pandas as pd
import pyarrow as pa
import daft
import numpy as np
import google.auth
from google.auth.transport.requests import Request

class LakeView():
    
    def __init__(self):        
        service_account_file = os.environ.get("GCP_KEYFILE")
        if service_account_file and service_account_file != "":
            scopes = ["https://www.googleapis.com/auth/cloud-platform"]
            access_token = get_gcp_access_token(service_account_file, scopes)                        
            self.catalog = catalog.load_catalog("default", 
                **{
                    "gcs.oauth2.token-expires-at": time.mktime(access_token.expiry.timetuple()) * 1000,
                    "gcs.oauth2.token": access_token.token,        
                })
        else:
            self.catalog = catalog.load_catalog("default")
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
        pa_partitions = table.inspect.partitions()        
        if pa_partitions.num_rows >1:
            pa_partitions = pa_partitions.sort_by([('partition', 'ascending')])
        cols = self.paTable_to_dataTable(pa_partitions)
        return cols

    def get_snapshot_data(self, table_id):        
        table = self.catalog.load_table(table_id)
        pa_snaps = table.inspect.snapshots().sort_by([('committed_at', 'descending')])
        df = pa_snaps.to_pandas()
        df['committed_at'] = df['committed_at'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
        cols = df.to_json(orient='records')          
        return cols
    
    def get_data_change(self, table_id):        
        table = self.catalog.load_table(table_id)
        pa_snaps = table.inspect.snapshots().sort_by([('committed_at', 'ascending')])
        pa_snaps = pa_snaps.drop(['snapshot_id', 'parent_id', 'operation', 'manifest_list'])
        df = pa_snaps.to_pandas()
        df['committed_at'] = df['committed_at'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))              
        df_summ = pd.DataFrame(df['summary'].apply(self.flatten_tuples).tolist())
        df_flattened = pd.concat([df.drop('summary', axis=1), df_summ], axis=1)        
        cols = df_flattened.to_json(orient='records', default_handler = BinaryEncoder)                
        return cols

    def get_sample_data(self, table_id, partition, limit=100):
        table = self.catalog.load_table(table_id)        
        df = daft.read_iceberg(table)
        df = df.limit(limit)
        paT = df.to_arrow()
        return self.paTable_to_dataTable(paT) 
        '''
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
        '''

    def get_schema(self, table_id):
        table = self.catalog.load_table(table_id)
        df = pd.DataFrame(columns=["Field_id", "Field", "DataType", "Required", "Comments"])
        for field in table.schema().fields:
            df2 = pd.DataFrame([[str(field.field_id), str(field.name), str(field.field_type), str(field.required), field.doc]], columns=["Field_id", "Field", "DataType", "Required", "Comments"])
            df = pd.concat([df, df2])
        pa_table = pa.Table.from_pandas(df)
        return self.paTable_to_dataTable(pa_table)
    
    def get_summary(self, table_id):
        table = self.catalog.load_table(table_id)
        ret = {}         
        ret['Location'] = table.location()
        ret['Current snapshotid'] = table.metadata.current_snapshot_id
        if table.metadata.current_snapshot_id:
            paTable = table.inspect.snapshots().sort_by([('committed_at', 'descending')]).select(['summary', 'committed_at'])
            ret['Last updated (UTC)'] = paTable.to_pydict()['committed_at'][0].strftime('%Y-%m-%d %H:%M:%S')
            ret['Total records'] = paTable.to_pydict()['summary'][0][5][1]
            ret['Total file size'] = paTable.to_pydict()['summary'][0][6][1]
            ret['Total data files'] = paTable.to_pydict()['summary'][0][7][1]        
        ret['Format version'] = table.metadata.format_version
        ret['Identifier fields'] = ''
        if len(table.schema().identifier_field_names()) > 0:
                ret['Identifier fields'] = list(table.schema().identifier_field_names())
        return json.dumps(ret)

    def get_properties(self, table_id):
        table = self.catalog.load_table(table_id)
        return json.dumps(table.properties)         
        
    def get_partition_specs(self, table_id):
        table = self.catalog.load_table(table_id)
        partitionfields=table.spec().fields
        c1, c2, c3 = [], [], []
        for f in partitionfields:
            c1.append(table.schema().find_column_name(f.source_id))
            c2.append(f.name)
            c3.append(str(f.transform))
        df = pd.DataFrame({"Field": c1, "Name": c2, "Transform": c3})
        pa_table = pa.Table.from_pandas(df)
        return self.paTable_to_dataTable(paTable=pa_table)
    
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
    
    def paTable_to_dataTable(self, paTable, encoder=None):
        if encoder:
            data = json.dumps(paTable.to_pandas().to_dict(orient='records'), cls = BinaryEncoder)
            return data
        else:            
            data = paTable.to_pandas().to_json(orient='records', default_handler = BinaryEncoder)
            return data
        
    # Flattening the tuple array into separate columns
    def flatten_tuples(self, row):    
        return {k: v for k, v in row}
        
def get_gcp_access_token(service_account_file, scopes):
    """
    Retrieves an access token from Google Cloud Platform using service account credentials.

    Args:
        service_account_file: Path to the service account JSON key file.
        scopes: List of OAuth scopes required for your application.

    Returns:
        The access token as a string.
    """
    credentials, name = google.auth.load_credentials_from_file(
        service_account_file, scopes=scopes)

    request = Request()
    credentials.refresh(request)  # Forces token refresh if needed
    return credentials

class BinaryEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, bytes):
            return '__binary_data__'
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)