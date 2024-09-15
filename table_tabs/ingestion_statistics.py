import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import timedelta
import numpy as np
import json

def show_ingestion_patterns(table):
    st.markdown("**Ingestion Patterns**")
    
    # Get snapshot data
    snapshots_df = table.inspect.snapshots().sort_by([('committed_at', 'descending')]).to_pandas()
    
    if snapshots_df.empty:
        st.write("No snapshot data available.")
        return

    # Convert committed_at to datetime
    snapshots_df['committed_at'] = pd.to_datetime(snapshots_df['committed_at'])

    # Sort by committed_at
    snapshots_df = snapshots_df.sort_values('committed_at')    
    
    def extract_summary_dict(summary):
        if isinstance(summary, list) and summary:
            return {item[0]: item[1] for item in summary}
        else:
            return {}
    
    snapshots_df['summary'] = snapshots_df['summary'].apply(extract_summary_dict)
    snapshots_df['records_added'] = snapshots_df['summary'].apply(lambda x: int(x.get('added-records', 0))).fillna(0)
    snapshots_df['bytes_added'] = snapshots_df['summary'].apply(lambda x: int(x.get('added-files-size', 0))).fillna(0)

    # Get the date range
    max_date = snapshots_df['committed_at'].max()
    min_date = snapshots_df['committed_at'].min()

    # Display info box at the top
    st.info(f"These statistics cover the period from {min_date.date()} (oldest snapshot) to {max_date.date()} (latest snapshot).")

    # Calculate the default start date (30 days before max_date or min_date, whichever is later)
    default_start_date = max(min_date, max_date - timedelta(days=30)).date()

    # Create two date input widgets for start and end dates
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        start_date = st.date_input("Start Date", value=default_start_date, min_value=min_date.date(), max_value=max_date.date())
    with col2:
        end_date = st.date_input("End Date", value=max_date.date(), min_value=start_date, max_value=max_date.date())
    with col3:
        granularity = st.selectbox("Granularity", ["None", "Hour", "Day", "Week"])
    with col4:
        chart_type = st.selectbox("Chart Type", ["Records Ingested", "Data Size Ingested"])

    # Ensure end_date is not before start_date
    if end_date < start_date:
        st.error("End date must be after start date.")
        end_date = start_date

    date_range = (start_date, end_date)

    # Filter data based on selected date range
    mask = (snapshots_df['committed_at'].dt.date >= date_range[0]) & (snapshots_df['committed_at'].dt.date <= date_range[1])
    filtered_df = snapshots_df.loc[mask]

    # Apply granularity
    if granularity == "Hour":
        filtered_df = filtered_df.groupby(filtered_df['committed_at'].dt.floor('H')).agg({'records_added': 'sum', 'bytes_added': 'sum', 'summary': 'last'}).reset_index()
    elif granularity == "Day":
        filtered_df = filtered_df.groupby(filtered_df['committed_at'].dt.date).agg({'records_added': 'sum', 'bytes_added': 'sum', 'summary': 'last'}).reset_index()
    elif granularity == "Week":
        filtered_df = filtered_df.groupby(filtered_df['committed_at'].dt.to_period('W')).agg({'records_added': 'sum', 'bytes_added': 'sum', 'summary': 'last'}).reset_index()
        filtered_df['committed_at'] = filtered_df['committed_at'].dt.start_time

    # Create the bar chart
    fig = go.Figure()
    # Generate a color palette with a different color for each bar
    num_bars = len(filtered_df)
    colors = [f'rgb({r},{g},255)' for r, g in zip(
        np.random.randint(0, 100, num_bars),
        np.random.randint(100, 200, num_bars)
    )]

    if chart_type == "Records Ingested":
        y_data = filtered_df['records_added']
        y_title = 'Records Ingested'
    else:
        y_data = filtered_df['bytes_added'] / (1024 * 1024)  # Convert to MB
        y_title = 'Data Size Ingested (MB)'

    fig.add_trace(go.Bar(
        x=filtered_df['committed_at'],
        y=y_data,
        name=y_title,
        marker_color=colors
    ))

    fig.update_layout(
        title='Ingestion Pattern',
        xaxis_title='Date',
        yaxis_title=y_title,
        height=500,
        showlegend=False  # Hide legend as each bar has a unique color
    )

    st.plotly_chart(fig, use_container_width=True)

    total_records = filtered_df['records_added'].sum()
    total_bytes = filtered_df['bytes_added'].sum()
    date_diff = (date_range[1] - date_range[0]).days + 1
    
    if granularity == "Week":
        avg_records = total_records / (date_diff / 7)
        avg_bytes = total_bytes / (date_diff / 7)
        avg_period = "Week"
    elif granularity == "Hour":
        avg_records = total_records / (date_diff * 24)
        avg_bytes = total_bytes / (date_diff * 24)
        avg_period = "Hour"
    else:
        avg_records = total_records / date_diff
        avg_bytes = total_bytes / date_diff
        avg_period = "Day"
    
    st.markdown("""
    <style>
    .modern-table {
        width: 100%;
        border-collapse: separate;
        border-spacing: 0 15px;
    }
    .modern-table th, .modern-table td {
        padding: 15px;
        text-align: left;
        background-color: #f8f9fa;
        border: none;
    }
    .modern-table th {
        font-weight: bold;
        color: #495057;
        text-transform: uppercase;
        font-size: 14px;
    }
    .modern-table td {
        font-size: 18px;
        color: #0066cc;
        font-weight: bold;
    }
    .modern-table tr {
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
        transition: all 0.3s ease;
    }
    .modern-table tr:hover {
        transform: translateY(-3px);
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.15);
    }
    </style>
    """, unsafe_allow_html=True)

    st.markdown(f"""
    <table class="modern-table">
        <tr>
            <th>Metric</th>
            <th>Value</th>
        </tr>
        <tr>
            <td>Total Records Ingested</td>
            <td>{total_records:,}</td>
        </tr>
        <tr>
            <td>Average Records per {avg_period}</td>
            <td>{avg_records:,.2f}</td>
        </tr>
        <tr>
            <td>Total Data Ingested</td>
            <td>{total_bytes / (1024 * 1024):.2f} MB</td>
        </tr>
        <tr>
            <td>Average Data per {avg_period}</td>
            <td>{avg_bytes / (1024 * 1024):.2f} MB</td>
        </tr>
    </table>
    """, unsafe_allow_html=True)