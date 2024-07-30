from pyiceberg import catalog
import streamlit as st
import pandas as pd
import datetime
import os
import duckdb

def init():
    cat = catalog.load_catalog("default", **{
        's3.endpoint': os.environ.get("AWS_ENDPOINT"),
	'py-io-impl':   'pyiceberg.io.fsspec.FsspecFileIO',
        })
    return cat

def add_sidebar(ns_nav):
    env_label = os.environ.get("ENV_LABEL")
    other_env_label = os.environ.get("OTHER_ENV_LABEL")
    other_env_url = os.environ.get("OTHER_ENV_URL")
    st.sidebar.markdown( f"""
    <b>Apache Iceberg  Lakehouse </b> <br />
    :orange[ {env_label} ] <br />
    :blue[{ns_nav}] <br />
    <a href='{other_env_url}'>:gray[{other_env_label}]</a>
    """, unsafe_allow_html=True)


def create_ns_contents():
    #st.markdown("<h2>Apache Iceberg Lakehouse </h2>", unsafe_allow_html=True)
    cat  = init()
    nss = cat.list_namespaces()
    nsl = list(nss)
    nsl.sort()
    nav_item = ''
    for ns in nsl:
        nav_item = nav_item + f' <a href="#{ns[0]}">{ns[0]}</a> <br /> '
        nested_nss = cat.list_namespaces(ns)
        for nested_ns in nested_nss:
            if len(nested_ns) == 3:
                nav_item = nav_item + f' <a href="#{nested_ns[1]}.{nested_ns[2]}">{nested_ns[1]}.{nested_ns[2]}</a> <br />'
    add_sidebar(nav_item)
    list_ns(nsl, cat)

def list_ns(nsl, cat):
    for ns in nsl:
        nested_nss = cat.list_namespaces(ns)
        for nested_ns in nested_nss:
            if len(nested_ns) == 3:
                namespaces(nested_ns[1:], cat)
                st.divider()
        namespaces(ns, cat)
        st.divider()

def namespaces(ns, cat):
    link = ns[0]
    if len(ns)>1:
        link = ns[0]+"."+ns[1]
    st.markdown(f'<a id="{link}"><h6>{link}</h6></a>', unsafe_allow_html=True)

    tables = cat.list_tables(ns)
    col1 = []
    col2 = []
    col3 = []
    for tab in tables:
        namespace = tab[0]
        table = tab[1]
        if len(tab) == 3:
            namespace = f'{tab[0]}.{tab[1]}'
            table = f'{tab[2]}'
        col1.append(namespace)
        col2.append(f'<a href="/?namespace={namespace}&table={table}">{table}</a>')
    data_df = pd.DataFrame({"Namespace": col1, "Table": col2})
    st.markdown(data_df.to_html(render_links=True, escape=False),unsafe_allow_html=True)

def tables(namespace, table):
    add_sidebar('')
    cat = init()
    t = cat.load_table(f"{namespace}.{table}")
    df = pd.DataFrame(columns=["Field_id", "Field", "DataType", "Required", "Comments"])
    for field in t.schema().fields:
        df2 = pd.DataFrame([[str(field.field_id), str(field.name), str(field.field_type), str(field.required), field.doc]], columns=["Field_id", "Field", "DataType", "Required", "Comments"])
        df = pd.concat([df, df2])
    st.subheader(f'Namespace: :blue[_{namespace}_]   Table: :blue[_{table}_]', divider='orange')
    st.markdown("**Schema**")
    st.dataframe(df, hide_index = True, use_container_width=True)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Identifier Fields**")
        if len(t.schema().identifier_field_names()) > 0:
            st.write(t.schema().identifier_field_names())
        else:
            st.write("None")
        st.markdown("**SortOrder**")
        get_sort_order(t.sort_order(), t)    
        st.markdown("**PartitionSpec**")
        get_partitions(t.spec(), t)
        st.markdown("**Refs**")
        get_refs(t.metadata.refs)
        st.markdown("**Table Properties**")
        st.write(t.properties)
        st.markdown("**Location**")
        st.write(t.location())
        st.markdown("**Format Version**")
        st.write(t.metadata.format_version)
    with col2:    
        st.markdown("**Current SnapshotID**")
        st.write(t.metadata.current_snapshot_id)
        st.markdown("**Snapshots**")
        get_snapshots(t.metadata.snapshots, t.metadata.current_snapshot_id)

    curr_env = os.environ.get("ENV_LABEL")
    if curr_env and 'Staging' in os.environ.get("ENV_LABEL"):
        st.markdown("**Sample Data**")
        dfr = None
        with st.spinner('Wait for it...'):
            dfr = t.scan(limit=30).to_pandas()
        st.dataframe(dfr, use_container_width=True, hide_index = True)

def get_snapshots(snaps, current_snap_id):
    c1, c2, c3, c4 = [], [], [], []
    curr_snap_props = {}
    last_updated_ts = None
    for snap in snaps:
        ts = datetime.datetime.fromtimestamp(float(snap.timestamp_ms)/1000, datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')
        c1.append(ts)
        c2.append(str(snap.snapshot_id))
        c3.append(str(snap.parent_snapshot_id))
        c4.append(snap.summary._additional_properties)
        if current_snap_id == snap.snapshot_id:
            curr_snap_props = snap.summary._additional_properties
            last_updated_ts = ts
    df = pd.DataFrame({"Time (utc)": c1, "Snapshot_id": c2, "Parent_snapshot_id": c3, "Summary": c4})
    st.dataframe(df, hide_index = False)
    st.write("Current Summary")
    summary = {}
    for k in curr_snap_props.keys():
        if 'total' in k:
            summary[k] = curr_snap_props[k]
    st.write(summary)
    st.markdown("**Last Updated** (utc)")
    st.write(last_updated_ts)

def get_sort_order(so, t):
    c2, c3, c4, c5 = [], [], [], []
    fs = so.fields
    st.write("Order_id: "+str(so.order_id))
    for f in fs:
        c2.append(t.schema().find_column_name(f.source_id))
        c3.append(str(f.transform))
        c4.append(f.direction.name)
        c5.append(f.null_order.name)
    df = pd.DataFrame({"Field": c2, "Transform": c3, "Direction": c4, "Null_order": c5})
    st.dataframe(df, hide_index = True)

def get_refs(ref):
    c1, c2, c3 = [], [], []
    for k in ref.keys():
        c1.append(k)
        c2.append(str(ref[k].snapshot_id))
        c3.append(ref[k].snapshot_ref_type.name)
    df = pd.DataFrame({"Ref":c1, "Snapshot_id": c2, "Ref type": c3})
    st.dataframe(df, hide_index = True)

def get_partitions(ps, t):
    pf=ps.fields
    c1, c2, c3 = [], [], []
    for f in pf:
        c1.append(t.schema().find_column_name(f.source_id))
        c2.append(f.name)
        c3.append(str(f.transform))
    df = pd.DataFrame({"Field": c1, "Name": c2, "Transform": c3})
    st.dataframe(df, hide_index = True)

def get_partition_table(table_scan):
    con = duckdb.connect(database = ":memory:")
    con.execute("CREATE TABLE partitions (partitions VARCHAR[], record_count INT, file_count INT, file_size_in_bytes BIGINT )")
    fst = table_scan.plan_files()
    pd = None
    for ft in fst:        
        fl = ft.file.partition.record_fields()
        rec_cnt, file_cnt, file_size = 0, 0, 0
        rec_cnt = ft.file.record_count
        file_size = ft.file.file_size_in_bytes
        file_cnt = 1
        con.execute(f'insert into partitions values ({fl}, {rec_cnt}, {file_cnt}, {file_size} )')
        pd = con.sql(f'SELECT partitions, sum(record_count) as record_count, sum(file_count) as file_count, sum(file_size_in_bytes)*1.0/1024/1024/1024 as file_size_gb FROM partitions group by partitions').df()
    st.dataframe(pd, hide_index = True)

ns = st.query_params.get_all('namespace')
tb = st.query_params.get_all('table')
if len(ns)>0 and len(tb)>0:
    tables(ns[0], tb[0])
else:
    create_ns_contents()
