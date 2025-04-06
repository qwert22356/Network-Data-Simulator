import streamlit as st
import subprocess
import pandas as pd
import os
import time
import glob
from datetime import datetime

# Page configuration - use a narrower layout
st.set_page_config(
    page_title="Network Data Simulator v1.1",
    page_icon="🌐",
    layout="centered",
    initial_sidebar_state="collapsed"
)

# Custom CSS for modern, lightweight UI
st.markdown("""
<style>
    .main .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        max-width: 800px;
    }
    .card {
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
        padding: 1.5rem;
        margin-bottom: 1.5rem;
        background-color: white;
    }
    .title-section {
        text-align: center;
        margin-bottom: 1.5rem;
    }
    .title-section h1 {
        font-size: 1.8rem;
        font-weight: 600;
        margin-bottom: 0.5rem;
    }
    .title-section p {
        color: #666;
        font-size: 1rem;
    }
    .section-title {
        font-size: 1.1rem;
        font-weight: 600;
        margin-bottom: 1rem;
        color: #333;
        border-bottom: 1px solid #eee;
        padding-bottom: 0.5rem;
    }
    .sub-section {
        margin-bottom: 1rem;
    }
    .stButton button {
        background-color: #4361ee;
        color: white;
        font-weight: 500;
        border: none;
        padding: 0.5rem 1rem;
        border-radius: 4px;
    }
    .stButton button:hover {
        background-color: #3a56d4;
    }
    .progress-step {
        display: flex;
        align-items: center;
        margin-bottom: 0.5rem;
        padding: 0.5rem;
        border-radius: 4px;
    }
    .progress-step.pending {
        background-color: #f8f9fa;
        color: #6c757d;
    }
    .progress-step.running {
        background-color: #e8f4fd;
        color: #0d6efd;
    }
    .progress-step.success {
        background-color: #e8f8e8;
        color: #198754;
    }
    .progress-step.error {
        background-color: #feeceb;
        color: #dc3545;
    }
    .step-icon {
        margin-right: 0.5rem;
        font-size: 1rem;
    }
    .small-info {
        font-size: 0.8rem;
        color: #6c757d;
    }
    .data-viewer {
        margin-top: 1rem;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 1rem;
    }
    .stTabs [data-baseweb="tab"] {
        height: 3rem;
        white-space: pre-wrap;
        border-radius: 4px;
    }
    .stTabs [data-baseweb="tab-list"] button {
        font-weight: 500;
    }
</style>
""", unsafe_allow_html=True)

# Title Section
st.markdown("""
<div class="title-section">
    <h1>🌐 Network Data Simulator v1.1</h1>
    <p>Generate and analyze realistic network telemetry data</p>
</div>
""", unsafe_allow_html=True)

# Create tabs for the different functionality
sim_tab, view_tab = st.tabs(["📊 Data Generation", "🔍 Data Viewer"])

# Data Generation Tab
with sim_tab:
    # Main card with simulation parameters
    with st.container():
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">Simulation Parameters</div>', unsafe_allow_html=True)
        
        # Date Range
        st.markdown('<div class="sub-section">', unsafe_allow_html=True)
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Start Date", datetime(2025, 3, 1), key="start_date")
        with col2:
            end_date = st.date_input("End Date", datetime(2025, 4, 1), key="end_date")
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Data Volume and Environment
        st.markdown('<div class="sub-section">', unsafe_allow_html=True)
        col1, col2 = st.columns(2)
        with col1:
            count_options = {
                "Small (1,000)": 1000,
                "Medium (10,000)": 10000,
                "Large (100,000)": 100000,
                "Very Large (1M)": 1000000
            }
            selected_count = st.selectbox("Data Size", options=list(count_options.keys()), index=1)
            record_count = count_options[selected_count]
        
        with col2:
            environment = st.selectbox(
                "Environment", 
                ["datacenter", "enterprise", "isp", "campus", "complete"],
                index=4
            )
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Advanced options in expander
        with st.expander("Advanced Options"):
            col1, col2 = st.columns(2)
            with col1:
                grpc_devices = st.number_input("gRPC Devices", min_value=10, max_value=1000, value=100)
                fault_ratio = st.slider("Fault Ratio (%)", min_value=0.1, max_value=5.0, value=1.0, step=0.1)
            
            with col2:
                # Output file names with shorter labels
                grpc_output = st.text_input("gRPC Filename", "grpc_data.parquet")
                snmp_output = st.text_input("SNMP Filename", "snmp_data.parquet")
                syslog_output = st.text_input("Syslog Filename", "syslog_data.parquet")
                ddm_output = st.text_input("DDM Filename", "ddm_data.parquet")
                predict_output = st.text_input("Prediction Filename", "predict_data.parquet")
        
        # Generate button
        generate_btn = st.button("🚀 Generate Data", use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)  # Close main card

    # Progress tracking section
    if generate_btn:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.markdown('<div class="section-title">Generation Progress</div>', unsafe_allow_html=True)
        
        # Format dates for command line
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")
        
        # Define scripts to run
        scripts = [
            {
                "name": "gRPC Data", 
                "command": f"python generate_grpc_data.py --count {record_count} --environment {environment} --devices {grpc_devices} --start-date {start_date_str} --end-date {end_date_str} --output {grpc_output}",
                "status": "pending"
            },
            {
                "name": "SNMP Data", 
                "command": f"python generate_snmp_data.py --count {record_count} --start {start_date_str} --end {end_date_str} --output {snmp_output} --environment {'datacenter' if environment == 'complete' else environment}",
                "status": "pending"
            },
            {
                "name": "Syslog Data", 
                "command": f"python generate_syslog_data.py --count {record_count} --start {start_date_str} --end {end_date_str} --output {syslog_output}",
                "status": "pending"
            },
            {
                "name": "DDM Data", 
                "command": f"python generate_ddm_data_with_fault.py --count {record_count} --fault_ratio {fault_ratio/100} --out {ddm_output}",
                "status": "pending"
            },
            {
                "name": "Prediction Data", 
                "command": f"python generate_life_prediction.py --count {record_count} --start-date {start_date_str} --end-date {end_date_str} --output {predict_output}",
                "status": "pending"
            }
        ]
        
        # Create placeholders for progress indicators
        progress_bar = st.progress(0)
        status_indicators = []
        
        # Create status indicators for each script
        for script in scripts:
            status_indicators.append(st.empty())
        
        # Create time placeholder
        time_placeholder = st.empty()
        
        # Status display function
        def update_status_display(index, status, message=""):
            icon = "⏱️"
            css_class = "pending"
            
            if status == "running":
                icon = "🔄"
                css_class = "running"
            elif status == "success":
                icon = "✅"
                css_class = "success"
            elif status == "error":
                icon = "❌"
                css_class = "error"
            
            status_text = f"{scripts[index]['name']}: {message}"
            status_indicators[index].markdown(
                f'<div class="progress-step {css_class}"><span class="step-icon">{icon}</span> {status_text}</div>',
                unsafe_allow_html=True
            )
        
        # Initialize all statuses
        for i in range(len(scripts)):
            update_status_display(i, "pending", "Waiting...")
        
        # Start timing
        start_time = time.time()
        
        # Run each script and update progress
        for i, script in enumerate(scripts):
            # Update status to running
            update_status_display(i, "running", "Generating...")
            scripts[i]["status"] = "running"
            
            # Update progress bar
            progress_bar.progress((i) / len(scripts))
            
            try:
                # Execute the command
                cmd_output = subprocess.run(script["command"], shell=True, capture_output=True, text=True)
                
                if cmd_output.returncode == 0:
                    scripts[i]["status"] = "success"
                    update_status_display(i, "success", "Completed successfully")
                else:
                    scripts[i]["status"] = "error"
                    update_status_display(i, "error", f"Error: {cmd_output.stderr[:50]}...")
            except Exception as e:
                scripts[i]["status"] = "error"
                update_status_display(i, "error", f"Exception: {str(e)[:50]}...")
            
            # Update progress bar
            progress_bar.progress(min((i + 1) / len(scripts), 1.0))
            
            # Update elapsed time
            elapsed_time = time.time() - start_time
            time_placeholder.markdown(f'<div class="small-info">Time elapsed: {elapsed_time:.1f} seconds</div>', unsafe_allow_html=True)
        
        # Final progress update and summary
        success_count = sum(1 for script in scripts if script["status"] == "success")
        error_count = sum(1 for script in scripts if script["status"] == "error")
        
        total_time = time.time() - start_time
        
        # Final summary
        st.markdown(f"""
        <div style="margin-top: 1rem; padding: 0.75rem; border-radius: 4px; background-color: #f8f9fa;">
            <b>Generation Complete:</b> {success_count} successful, {error_count} failed<br/>
            <span class="small-info">Total execution time: {total_time:.2f} seconds</span>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown('</div>', unsafe_allow_html=True)  # Close progress card

# Data Viewer Tab
with view_tab:
    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">Data Explorer</div>', unsafe_allow_html=True)
    
    # File selection options
    option = st.radio(
        "Choose data source",
        ["Upload parquet file", "Select existing file"],
        horizontal=True
    )
    
    df = None
    if option == "Upload parquet file":
        uploaded_file = st.file_uploader("Upload a parquet file", type=['parquet'])
        if uploaded_file is not None:
            try:
                # Save the uploaded file temporarily
                with open("temp_upload.parquet", "wb") as f:
                    f.write(uploaded_file.getbuffer())
                
                # Read the parquet file
                df = pd.read_parquet("temp_upload.parquet")
                st.success(f"File loaded successfully: {uploaded_file.name}")
            except Exception as e:
                st.error(f"Error loading file: {str(e)}")
    else:
        # Find all parquet files in the current directory
        parquet_files = glob.glob("*.parquet")
        if parquet_files:
            selected_file = st.selectbox("Select a parquet file", parquet_files)
            if selected_file:
                try:
                    df = pd.read_parquet(selected_file)
                    file_size = os.path.getsize(selected_file) / (1024 * 1024)  # in MB
                    st.success(f"File loaded successfully: {selected_file} ({file_size:.2f} MB)")
                except Exception as e:
                    st.error(f"Error loading file: {str(e)}")
        else:
            st.info("No parquet files found in the current directory.")
    
    if df is not None:
        # Data summary
        st.markdown('<div class="section-title">Data Summary</div>', unsafe_allow_html=True)
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Records", f"{len(df):,}")
        with col2:
            st.metric("Columns", f"{len(df.columns):,}")
        
        # Query options
        st.markdown('<div class="section-title">Data Exploration</div>', unsafe_allow_html=True)
        
        # Tab options for different ways to explore the data
        explore_tab, filter_tab, stats_tab = st.tabs(["Browse Data", "Filter Data", "Statistics"])
        
        with explore_tab:
            # Export options
            export_col1, export_col2, export_col3, export_col4 = st.columns([1, 1, 1, 4])
            with export_col1:
                if st.button("📄 CSV"):
                    csv = df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="Download CSV",
                        data=csv,
                        file_name="data_export.csv",
                        mime="text/csv",
                        key='download-csv'
                    )
            with export_col2:
                if st.button("📊 Excel"):
                    # Create Excel in memory
                    import io
                    from io import BytesIO
                    output = BytesIO()
                    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                        df.to_excel(writer, index=False, sheet_name='Data')
                    excel_data = output.getvalue()
                    st.download_button(
                        label="Download Excel",
                        data=excel_data,
                        file_name="data_export.xlsx",
                        mime="application/vnd.ms-excel",
                        key='download-excel'
                    )
            with export_col3:
                if st.button("📋 JSON"):
                    json_str = df.to_json(orient='records')
                    st.download_button(
                        label="Download JSON",
                        data=json_str,
                        file_name="data_export.json",
                        mime="application/json",
                        key='download-json'
                    )
                    
            # Pagination controls
            total_rows = len(df)
            page_size = st.slider("Rows per page", min_value=10, max_value=100, value=25, step=5)
            total_pages = (total_rows - 1) // page_size + 1
            
            # Initialize page_number in session state if not exists
            if 'page_number' not in st.session_state:
                st.session_state.page_number = 1
                
            # Get current page number
            page_number = st.session_state.get('page_number', 1)
            
            # Better pagination controls
            pagination_cols = st.columns([1, 3, 1])
            
            with pagination_cols[0]:
                if st.button("◀ Previous", disabled=page_number <= 1):
                    st.session_state.page_number = max(1, page_number - 1)
                    st.experimental_rerun()
                    
            with pagination_cols[1]:
                # Center aligned page indicator
                st.markdown(f"<div style='text-align: center; padding: 5px;'>Page {page_number} of {total_pages}</div>", unsafe_allow_html=True)
                
            with pagination_cols[2]:
                if st.button("Next ▶", disabled=page_number >= total_pages):
                    st.session_state.page_number = min(total_pages, page_number + 1)
                    st.experimental_rerun()
            
            # Display paginated data
            page = st.session_state.page_number
            start_idx = (page - 1) * page_size
            end_idx = min(start_idx + page_size, total_rows)
            
            # Full screen toggle for better viewing
            with st.expander("Expand for full view", expanded=False):
                st.dataframe(df.iloc[start_idx:end_idx], use_container_width=True, height=500)
            
            # Regular view
            st.dataframe(df.iloc[start_idx:end_idx], use_container_width=True)
            st.caption(f"Showing records {start_idx+1} to {end_idx} of {total_rows}")
            
            # Key navigation instructions
            st.markdown("""
            <div style="background-color: #f8f9fa; padding: 10px; border-radius: 5px; font-size: 0.8rem; margin-top: 10px;">
            <b>Tip:</b> Use the pagination controls above to navigate between pages. You can also adjust the number of rows per page.
            </div>
            """, unsafe_allow_html=True)
        
        with filter_tab:
            # Simple filtering options
            col1, col2 = st.columns(2)
            
            with col1:
                if 'vendor' in df.columns:
                    vendors = df['vendor'].unique().tolist()
                    selected_vendors = st.multiselect("Filter by Vendor", vendors)
            
            with col2:
                if 'data_type' in df.columns:
                    data_types = df['data_type'].unique().tolist()
                    selected_data_types = st.multiselect("Filter by Data Type", data_types)
            
            # Apply filters
            filtered_df = df.copy()
            if 'vendor' in df.columns and selected_vendors:
                filtered_df = filtered_df[filtered_df['vendor'].isin(selected_vendors)]
            
            if 'data_type' in df.columns and selected_data_types:
                filtered_df = filtered_df[filtered_df['data_type'].isin(selected_data_types)]
            
            # Show filtering results
            st.dataframe(filtered_df.head(100), use_container_width=True)
            st.caption(f"Showing first 100 records of {len(filtered_df):,} matching records")
            
            # Export option for filtered data
            if st.button("Export Filtered Data"):
                filtered_df.to_parquet("filtered_data.parquet")
                st.success("Data exported to filtered_data.parquet")
        
        with stats_tab:
            # Show basic statistics and distributions
            if 'vendor' in df.columns:
                st.subheader("Vendor Distribution")
                vendor_counts = df['vendor'].value_counts().reset_index()
                vendor_counts.columns = ['Vendor', 'Count']
                st.bar_chart(vendor_counts.set_index('Vendor'))
            
            if 'data_type' in df.columns:
                st.subheader("Data Type Distribution")
                type_counts = df['data_type'].value_counts().reset_index()
                type_counts.columns = ['Data Type', 'Count']
                st.bar_chart(type_counts.set_index('Data Type'))
            
            # Show numeric column statistics
            numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns.tolist()
            if numeric_cols:
                st.subheader("Numeric Column Statistics")
                st.dataframe(df[numeric_cols].describe(), use_container_width=True)
    
    st.markdown('</div>', unsafe_allow_html=True)  # Close card

# Minimal footer
st.markdown("""
<div style="text-align: center; margin-top: 1rem;">
    <span class="small-info">Network Data Simulator v1.1 • 2025</span>
</div>
""", unsafe_allow_html=True) 