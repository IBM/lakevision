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
import dotenv
dotenv.load_dotenv(dotenv.find_dotenv(usecwd=True)) #Use current working directory to load .env file

class LakeView():

    def __init__(self):        
        self.catalog = catalog.load_catalog("default", 
            **{
                'uri': os.environ.get("PYICEBERG_CATALOG__DEFAULT__URI"),
                'token': os.environ.get("PYICEBERG_CATALOG__DEFAULT__TOKEN"),
                's3.endpoint':  os.environ.get("AWS_ENDPOINT"),
                'py-io-impl':   'pyiceberg.io.fsspec.FsspecFileIO',
            })
        self.select_partition = None
        
    def create_filters(self, namespaces: list[str]):
        st.sidebar.markdown( f' <b> :orange[Apache Iceberg Lakehouse ]', unsafe_allow_html=True)
        #-- Namespace
        select_namespace = st.sidebar.selectbox('Namespace',[".".join(namespace) for namespace in namespaces],None)

        if select_namespace:
            tables = self.catalog.list_tables(select_namespace)
            if len(tables) > 0:
                index = 0 if len(tables) == 1 else None
                select_table = st.sidebar.selectbox('Table',[".".join(table).split(".")[-1] for table in tables],index)
                
                #-- table
                if select_table:
                    select_limit = st.sidebar.selectbox('Samples',[50, 100, 200])

                    #generate 
                    if 'selected_partition' in st.session_state:
                        self.tables(ns=select_namespace,tb=select_table,partition=[st.session_state.selected_partition],limit=select_limit)
                    else:
                        self.tables(ns=select_namespace,tb=select_table,partition=[],limit=select_limit)
            else:
                st.sidebar.markdown("No tables found")
        
    def create_ns_contents(self):                
        nsl = self.get_namespaces()        
        self.create_filters(nsl)
        
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
        if len(partition) >0: 
            partition = partition[0]
        t = self.catalog.load_table(f"{ns}.{tb}")
        df = pd.DataFrame(columns=["Field_id", "Field", "DataType", "Required", "Comments"])
        for field in t.schema().fields:
            df2 = pd.DataFrame([[str(field.field_id), str(field.name), str(field.field_type), str(field.required), field.doc]], columns=["Field_id", "Field", "DataType", "Required", "Comments"])
            df = pd.concat([df, df2])
        st.subheader(f'Namespace: :blue[_{ns}_]   Table: :blue[_{tb}_]', divider='orange')
        
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
            pdf = t.inspect.snapshots().sort_by([('committed_at', 'descending')]).to_pandas()
            st.markdown("**Snapshots**")
            st.dataframe(pdf, use_container_width=True)

        with tab4:
            st.markdown("**Refs**")
            self.get_refs(t.metadata.refs)

        with tab5:
            pat = t.inspect.manifests()
            if pat:
                pdf = pat.to_pandas()
                st.dataframe(pdf, use_container_width=True)
            else:
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
        return expression

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
    
    def create_partition_link(self, row, namespace, table, limit):
        if not 'partition' in row:
            return row
        part = row['partition']
        res = f"""/?namespace={namespace}&table={table}&sample_limit={limit}&partition={part} """
        row['partition'] = res
        return row
    
def local_css(file_name):
    with open(file_name) as f:
        st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

def remote_css(url):
    st.markdown(f'<link href="{url}" rel="stylesheet">', unsafe_allow_html=True)

local_css("lv.css")
remote_css('https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css')

def main():
    lv = LakeView()
    lv.create_ns_contents()

main() 
