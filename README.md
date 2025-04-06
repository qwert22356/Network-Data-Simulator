# Network Data Simulator

A comprehensive tool for generating realistic network telemetry data for testing and development.

## 🌟 Features

- **Multi-format Data Generation**: Create gRPC/gNMI, SNMP, Syslog, DDM (optical), and Lifecycle prediction data
- **Customizable Parameters**: Control data volume, time ranges, network environments, and more
- **Realistic Data**: Simulates real-world network devices, vendors, and telemetry formats
- **Modern UI**: Clean, intuitive interface with real-time progress tracking

## 📊 Data Types

The simulator generates these types of network telemetry data:

1. **gRPC/gNMI Data**: Streaming telemetry from modern network devices with vendor-specific paths and formats
2. **SNMP Data**: Traditional monitoring data with MIBs, OIDs, and interface statistics
3. **Syslog Messages**: System and network event logs with proper severity and facility codes
4. **DDM Optical Data**: Digital Diagnostic Monitoring data for optical interfaces
5. **Lifecycle Prediction**: Predictive maintenance data for network hardware

## 🚀 Getting Started

### Prerequisites

- Python 3.7 or higher
- Required packages:
  ```
  pip install streamlit pandas plotly pyarrow
  ```

### Running the Simulator

1. Clone this repository
2. Navigate to the project directory
3. Run the UI:
   ```
   streamlit run simulation_ui.py
   ```
4. Open your browser to the URL shown in the terminal (typically http://localhost:8501)

## 📝 Usage

1. Set your desired parameters:
   - Choose date range for the data
   - Select data volume (from 1,000 to 1,000,000 records)
   - Pick a network environment type
   
2. Advanced options:
   - Configure the number of devices
   - Set fault ratio for simulations
   - Customize output filenames

3. Click "Generate Data" and monitor progress in real-time

## 📋 Output Format

All data is saved in Parquet format for efficient storage and fast loading. Files include:

- `grpc_data.parquet`: gRPC/gNMI subscription data
- `snmp_data.parquet`: SNMP polling data
- `syslog_data.parquet`: System and network events
- `ddm_data.parquet`: Optical interface metrics
- `predict_data.parquet`: Device lifecycle predictions

## 📸 Screenshots

![Network Data Simulator UI](screenshots/simulator_ui.png)
*Main interface with simulation parameters*

![Generation Progress](screenshots/generation_progress.png)
*Real-time progress tracking during data generation*

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- Built with Streamlit for a responsive UI
- Uses Pandas and PyArrow for efficient data handling

## 📊 数据结构

为确保跨数据类型的一致性，所有生成的数据都遵循以下统一结构：

### 共同字段

所有表都包含以下共同字段:

| 字段名 | 类型 | 说明 |
|--------|------|------|
| timestamp | datetime | 数据时间戳（ISO格式）|
| module_id | string | 唯一标识，如 CISCO-DC1-Pod01-Rack01-SW01-Eth1/1-100G |
| datacenter | string | 数据中心标识 |
| room | string | 机房/Pod |
| rack | string | 机架 |
| device_hostname | string | 设备名称 |
| device_ip | string | 设备IP |
| device_vendor | string | 厂商名称 |
| interface | string | 接口名称，如 Ethernet1/1 |
| speed | string | 接口速率，如 100G |

### 数据类型和字段特点

1. **gRPC接口指标 (grpc_interface_metrics)**
   - 用途：采集gRPC实时指标数据，1分钟粒度
   - 包含接口状态、性能指标、光模块参数等

2. **SNMP接口状态 (snmp_interface_status)**
   - 用途：采集SNMP接口状态、广播风暴、链路异常、MAC统计等
   - 包含标准MIB字段如ifIndex、ifAdminStatus、ifOperStatus等

3. **系统日志事件 (syslog-events)**
   - 用途：结构化存储网络设备syslog日志
   - 包含facility、severity、事件类型解析等字段

4. **光模块指标 (module_ddm_metrics)**
   - 用途：光模块历史DDM指标，供模型训练与趋势分析
   - 包含温度、电压、电流、发射功率、接收功率等

5. **预测结果 (prediction_result)**
   - 用途：记录AI模型对光模块的预测结果和命中规则
   - 包含故障概率、预测故障时间、模型名称、剩余寿命等

### 数据关联

各数据类型可通过共同字段进行关联分析，特别是:
- `module_id`：跨所有数据类型的唯一标识
- `timestamp`：支持时间序列分析
- `device_hostname`、`interface`：支持设备和接口级别分析

完整字段定义请参考代码中的`data_structure.md`文件。 