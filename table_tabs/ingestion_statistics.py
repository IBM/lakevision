import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import timedelta
import numpy as np

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

    # Get the date range
    max_date = snapshots_df['committed_at'].max()
    min_date = snapshots_df['committed_at'].min()

    # Calculate the default start date (30 days before max_date or min_date, whichever is later)
    default_start_date = max(min_date, max_date - timedelta(days=30)).date()

    # Create two date input widgets for start and end dates
    col1, col2, col3 = st.columns(3)
    with col1:
        start_date = st.date_input("Start Date", value=default_start_date, max_value=max_date.date())
    with col2:
        end_date = st.date_input("End Date", value=max_date.date(), min_value=start_date, max_value=max_date.date())
    with col3:
        granularity = st.selectbox("Granularity", ["None", "Hour", "Day", "Week"])

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
        filtered_df = filtered_df.groupby(filtered_df['committed_at'].dt.floor('H'))['records_added'].sum().reset_index()
    elif granularity == "Day":
        filtered_df = filtered_df.groupby(filtered_df['committed_at'].dt.date)['records_added'].sum().reset_index()
    elif granularity == "Week":
        filtered_df = filtered_df.groupby(filtered_df['committed_at'].dt.to_period('W'))['records_added'].sum().reset_index()
        filtered_df['committed_at'] = filtered_df['committed_at'].dt.start_time

    # Create the bar chart
    fig = go.Figure()
    # Generate a color palette with a different color for each bar
    num_bars = len(filtered_df)
    colors = [f'rgb({r},{g},255)' for r, g in zip(
        np.random.randint(0, 100, num_bars),
        np.random.randint(100, 200, num_bars)
    )]

    fig.add_trace(go.Bar(
        x=filtered_df['committed_at'],
        y=filtered_df['records_added'],
        name='Records Ingested',
        marker_color=colors
    ))

    fig.update_layout(
        title='Ingestion Pattern',
        xaxis_title='Date',
        yaxis_title='Records Ingested',
        height=500,
        showlegend=False  # Hide legend as each bar has a unique color
    )

    st.plotly_chart(fig, use_container_width=True)

    total_records = filtered_df['records_added'].sum()
    date_diff = (date_range[1] - date_range[0]).days + 1
    
    if granularity == "Week":
        avg_records = total_records / (date_diff / 7)
        avg_period = "Week"
    elif granularity == "Hour":
        avg_records = total_records / (date_diff * 24)
        avg_period = "Hour"
    else:
        avg_records = total_records / date_diff
        avg_period = "Day"
    
    st.markdown("""
    <style>
    .stat-card {
        padding: 20px;
        border-radius: 5px;
        background-color: #f0f2f6;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        margin-bottom: 20px;
    }
    .stat-title {
        font-size: 18px;
        font-weight: bold;
        margin-bottom: 10px;
    }
    .stat-value {
        font-size: 24px;
        font-weight: bold;
        color: #0066cc;
    }
    </style>
    """, unsafe_allow_html=True)

    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown(f"""
        <div class="stat-card">
            <div class="stat-title">Total Records Ingested</div>
            <div class="stat-value">{total_records:,}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div class="stat-card">
            <div class="stat-title">Average Records per {avg_period}</div>
            <div class="stat-value">{avg_records:,.2f}</div>
        </div>
        """, unsafe_allow_html=True)