from pyiceberg import catalog
from pyiceberg.expressions import AlwaysTrue
from pyiceberg.catalog import Identifier
from typing import List, Union
import streamlit as st
import pandas as pd
import os
import pyarrow as pa
import pyarrow.ipc as ipc
from streamlit.components.v1 import html
import json
import dotenv
dotenv.load_dotenv(dotenv.find_dotenv(usecwd=True)) #Use current working directory to load .env file


class LakeView():

    def __init__(self):
        storage_location = os.environ.get("ICEBERG_STORAGE_LOCATION")
        match location:
            case "GCP":
                service_account_file = os.environ.get("GCP_KEYFILE")
                scopes = ["https://www.googleapis.com/auth/cloud-platform"]
                access_token = get_gcp_access_token(service_account_file, scopes)                        
                self.catalog = catalog.load_catalog("default", 
                    **{
                        "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
                        "gcs.oauth2.token-expires-at": time.mktime(access_token.expiry.timetuple()) * 1000,
                        "gcs.oauth2.token": access_token.token,        
                    })
            case _: 
                self.catalog = catalog.load_catalog("default", 
                    **{
                        'py-io-impl':   'pyiceberg.io.fsspec.FsspecFileIO',
                    })
        self.namespace_options = []

    @st.dialog("Go to Table")
    def search(self):
        tablename = st.text_input("Full table name: ",placeholder="<namespace>.<table>")
        if st.button("Submit"):
            parts = tablename.split(".")
            num_parts = len(parts)
            if num_parts >= 2:
                namespace, table =  '.'.join(parts[:num_parts -1]), parts[-1]
                if namespace in self.namespace_options:
                    st.session_state['namespace_selection'] = self.namespace_options.index(namespace)
                    st.session_state['table_name'] = table
                    st.rerun()
                else:
                    st.write(f"Namespace {namespace} does not exist!")
                    st.session_state['namespace_selection'] = None
            else:
                st.write("Invalid tablename")
        
    def create_filters(self, namespaces: list[str], ns: str = None, tb : str = None, partition : str = None):
        st.sidebar.markdown( f' <b> :orange[Apache Iceberg Lakehouse ]', unsafe_allow_html=True)
        if st.sidebar.button("Go to Table"):
            self.search()

        #-- Namespace        
        self.namespace_options = [".".join(namespace) for namespace in namespaces]
        if 'namespace_selection' not in st.session_state:
            st.session_state['namespace_selection'] = None
        if st.session_state['namespace_selection'] is None and ns:
            if ns in self.namespace_options:
                st.session_state['namespace_selection'] = self.namespace_options.index(ns)
        select_namespace = st.sidebar.selectbox('Namespace',self.namespace_options,index=st.session_state['namespace_selection'])

        if select_namespace:
            tables = self.catalog.list_tables(select_namespace)
            if len(tables) > 0:
                table_options = [".".join(table).split(".")[-1] for table in tables]
                index = 0 if len(tables) == 1 else None
                if 'table_name' in st.session_state:
                    if st.session_state['table_name'] in table_options:
                        index = table_options.index(st.session_state['table_name'])
                    else:
                        if st.session_state['table_name'] is not None:
                            st.sidebar.write(f"Table '{st.session_state['table_name']}' not found in the namespace!")
                        st.session_state['table_name'] = None
                else:
                    if tb in table_options:
                        index = table_options.index(tb)
                
                select_table = st.sidebar.selectbox('Table',table_options,index)
                
                #-- table
                if select_table:
                    select_limit = st.sidebar.number_input("Samples", min_value=50, max_value=1000, step=50)

                    #generate 
                    if 'selected_partition' in st.session_state:
                        self.tables(ns=select_namespace,tb=select_table,partition=st.session_state.selected_partition,limit=select_limit)
                    else:
                        selected_partition = partition if partition is not None else None
                        self.tables(ns=select_namespace,tb=select_table,partition=selected_partition,limit=select_limit)
            else:
                st.sidebar.markdown("No tables found")
        
    def create_ns_contents(self, ns: str = None, tb : str = None, partition : str = None):                
        nsl = self.get_namespaces()        
        self.create_filters(namespaces = nsl, ns=ns, tb=tb, partition=partition)
        
    @st.cache_data(ttl = '30m')
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

    def tables(self, ns, tb, partition, limit: int):
        t = self.catalog.load_table(f"{ns}.{tb}")
        df = pd.DataFrame(columns=["Field_id", "Field", "DataType", "Required", "Comments"])
        for field in t.schema().fields:
            df2 = pd.DataFrame([[str(field.field_id), str(field.name), str(field.field_type), str(field.required), field.doc]], columns=["Field_id", "Field", "DataType", "Required", "Comments"])
            df = pd.concat([df, df2])
        

        #create shareable buttons
        left, right = st.columns([0.93,0.07])
        left.subheader(f'Namespace: :blue[_{ns}_]   Table: :blue[_{tb}_]', divider='orange')
        relative_path = f"?namespace={ns}&table={tb}"
        if partition:
            relative_path = f"{relative_path}&partition={partition}"
        right.link_button(":rocket:",f"/{relative_path}" , help="Open current table view in a new tab")

        tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(["Partitions & Sample", "Table",  "Snapshots", "Refs", "Manifests", "Entries", "History"])
            
        with tab1:    
            st.markdown("**PartitionSpec**")
            self.get_partitions(t.spec(), t)
            st.markdown("**Partitions**")
            try:
                if t.metadata.current_snapshot_id is None:
                    st.write("No Data")
                else:
                    with st.spinner('Wait for it...'):
                        curr_snapshot = t.metadata.current_snapshot_id
                        file_name = f'/tmp/{curr_snapshot}_partitions'
                        if os.path.isfile(file_name):
                            with pa.OSFile(file_name, 'rb') as source:
                                reader = ipc.open_file(source)
                                pa_partitions = reader.read_all()
                        else:
                            pa_partitions = t.inspect.partitions()
                            with pa.OSFile(file_name, 'wb') as sink:
                                with ipc.new_file(sink, pa_partitions.schema) as writer:
                                    writer.write_table(pa_partitions)
                        pdf = pa_partitions.to_pandas()

                        #create partition select in sidebar
                        if len(pdf) > 0 and 'partition' in pdf:
                            options = pdf['partition'].tolist()
                            if 'selected_partition' not in st.session_state and partition and partition in options:
                                st.session_state['selected_partition'] = partition
                            index = None if 'selected_partition' not in st.session_state or st.session_state.selected_partition not in options else options.index(st.session_state.selected_partition)
                            st.sidebar.selectbox('Partition', options, index, key="selected_partition") 

                        st.dataframe(pdf, hide_index=False )
            except PermissionError:
                    st.markdown("<b><i> Restricted </i></b>", unsafe_allow_html=True)
                    return
            row_filter = self.get_row_filter(partition, t) 
            tab_scan = t.scan(limit=limit, row_filter = row_filter)

            st.markdown("**Sample Data**")
            dfr = None
            if t.metadata.current_snapshot_id is None:
                    st.write("No Data")
            else:
                with st.spinner('Wait for it...'):
                    try:
                        rbr = tab_scan.to_arrow_batch_reader()
                        for batch in rbr:
                            dfr = batch.to_pandas()
                            break
                    except PermissionError:
                        st.write("No Permission")
                    if row_filter == AlwaysTrue():
                        row_filter = ''
                    st.write(f'Showing records with shape: {dfr.shape}, Filtered by: {row_filter}')
                    st.dataframe(dfr, use_container_width=True, hide_index = True)
        with tab2:
            st.markdown("**Schema**")
            st.dataframe(df, hide_index = True, use_container_width=True)
            st.markdown("**Table Properties**")
            st.dataframe(t.properties, height=(5+1)*35+3, use_container_width=True)
            col1, col2 = st.columns(2)
            contA  = col1.container(height=280, border=False)
            contB  = col2.container(height=280, border=False)
            contB.markdown("**Identifier Fields**")
            if len(t.schema().identifier_field_names()) > 0:
                contB.write(t.schema().identifier_field_names())
            else:
                contB.write("None")
            contA.markdown("**Location**")
            contA.write(t.location())

            contB.markdown("**Format Version**")
            contB.write(t.metadata.format_version)

            contA.markdown("**SortOrder**")
            self.get_sort_order(t.sort_order(), t, contA)
        with tab3:
            st.markdown("**Current SnapshotId**")
            st.write(t.metadata.current_snapshot_id)
            st.markdown("**Snapshots**")
            try:
                pdf = t.inspect.snapshots().sort_by([('committed_at', 'descending')]).to_pandas()
                st.dataframe(pdf, use_container_width=True)
            except:
                st.write("Not found")

        with tab4:
            st.markdown("**Refs**")
            self.get_refs(t.metadata.refs)

        with tab5:
            try:
                pat = t.inspect.manifests()
                if pat:
                    pdf = pat.to_pandas()
                    st.dataframe(pdf, use_container_width=True)
                else:
                    st.write("No data")
            except:
                st.write("No data")

        with tab6:
            try:
                pat = t.inspect.entries()
                if pat:
                    pdf = pat.to_pandas()
                    st.dataframe(pdf, use_container_width=True)
                else:
                    st.write("No data")
            except:
                st.write("No data")
        with tab7:
            pdf = t.inspect.history().to_pandas()
            st.dataframe(pdf, use_container_width=True)

    def get_row_filter(self, partition, table):
        if partition is None or len(partition) == 0:
            return AlwaysTrue()
        fields = table.spec().fields
        use_fields = []
        for field in fields:
            source_field = table.schema().find_column_name(field.source_id)
            #field.name
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

    def get_sort_order(self, so, t, container):
        c2, c3, c4, c5 = [], [], [], []
        fs = so.fields
        for f in fs:
            c2.append(t.schema().find_column_name(f.source_id))
            c3.append(str(f.transform))
            c4.append(f.direction.name)
            c5.append(f.null_order.name)
        df = pd.DataFrame({"Field": c2, "Transform": c3, "Direction": c4, "Null_order": c5})
        with container:
            st.write("Order_id: "+str(so.order_id))
            st.dataframe(df, hide_index = True, use_container_width=True)

    def get_refs(self, ref):
        c1, c2, c3 = [], [], []
        for k in ref.keys():
            c1.append(k)
            c2.append(str(ref[k].snapshot_id))
            c3.append(ref[k].snapshot_ref_type.name)
        df = pd.DataFrame({"Ref":c1, "Snapshot_id": c2, "Ref type": c3})
        st.dataframe(df, hide_index = True, use_container_width=True)

    def get_partitions(self, ps, t):
        pf=ps.fields
        c1, c2, c3 = [], [], []
        for f in pf:
            c1.append(t.schema().find_column_name(f.source_id))
            c2.append(f.name)
            c3.append(str(f.transform))
        df = pd.DataFrame({"Field": c1, "Name": c2, "Transform": c3})
        st.dataframe(df, hide_index = True, use_container_width=True)

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
  print(credentials)
  return credentials

def local_css(file_name):
    with open(file_name) as f:
        st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

def remote_css(url):
    st.markdown(f'<link href="{url}" rel="stylesheet">', unsafe_allow_html=True)

local_css("lv.css")
remote_css('https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css')

def main():
    lv = LakeView()
    ns = st.query_params['namespace'] if 'namespace' in st.query_params else None
    tb = st.query_params['table'] if 'table' in st.query_params else None
    partition = json.loads(st.query_params['partition'].replace("\'", "\"")) if 'partition' in st.query_params else None
    lv.create_ns_contents(ns=ns,tb=tb, partition=partition)

main() 
