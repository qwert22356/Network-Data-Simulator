import streamlit as st
import subprocess
import pandas as pd
import os
import time
from datetime import datetime

# Page configuration - use a narrower layout
st.set_page_config(
    page_title="Network Data Simulator",
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
</style>
""", unsafe_allow_html=True)

# Title Section
st.markdown("""
<div class="title-section">
    <h1>🌐 Network Data Simulator</h1>
    <p>Generate realistic network telemetry data for testing and development</p>
</div>
""", unsafe_allow_html=True)

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
            "command": f"python generate_snmp_data.py --count {record_count} --start-date {start_date_str} --end-date {end_date_str} --output {snmp_output}",
            "status": "pending"
        },
        {
            "name": "Syslog Data", 
            "command": f"python generate_syslog_data.py --count {record_count} --start-date {start_date_str} --end-date {end_date_str} --output {syslog_output}",
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
        progress_bar.progress((i + 1) / len(scripts))
        
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

# Minimal footer
st.markdown("""
<div style="text-align: center; margin-top: 1rem;">
    <span class="small-info">Network Data Simulator v1.0 • 2025</span>
</div>
""", unsafe_allow_html=True) 