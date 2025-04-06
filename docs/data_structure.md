## ✅ 数据库建表建议（统一表名 / 字段说明 / 类型 / 用途）

---

### 📈 1. 表名：grpc_interface_metrics（InfluxDB）
- **用途**：采集 gRPC 实时指标数据，1分钟粒度，用于性能监控与趋势预测。

| 字段名 | 类型 | 说明 |
|--------|------|------|
| timestamp | datetime | 采集时间戳 |
| module_id | string | 光模块唯一标识（含厂商+位置+接口） |
| datacenter | string | 所在数据中心 |
| room | string | 机房 / Pod |
| rack | string | 机架位 |
| device_ip | string | 设备IP地址 |
| device_hostname | string | 交换机名 |
| device_vendor | string | 厂商名称 |
| interface | string | 接口名 |
| speed | string | 接口速率（如100G） |
| admin_status | string | 接口管理状态 |
| oper_status | string | 接口运行状态 |
| mtu | int | 接口MTU大小 |
| duplex | string | 全双工/半双工 |
| rx_bytes / tx_bytes | int | 接收/发送字节数 |
| rx_packets / tx_packets | int | 接收/发送包数 |
| rx_dropped / tx_dropped | int | 丢包数 |
| rx_errors / tx_errors | int | 错误包数 |
| crc_errors / fcs_errors | int | 校验错误计数 |
| flap_count | int | 接口抖动次数 |
| temperature | float | 模块温度（°C） |
| voltage | float | 模块电压（V） |
| current | float | 模块电流（A） |
| tx_power / rx_power | float | 发射/接收功率（dBm） |
| optic_vendor | string | 光模块厂商 |
| optic_type | string | 光模块类型（如QSFP28） |
| optic_serial | string | 光模块序列号 |

---

### 📡 2. 表名：snmp_interface_status（TimescaleDB）
- **用途**：采集 SNMP 接口状态、广播风暴、链路异常、MAC统计等信息。

| 字段名 | 类型 | 说明 |
|--------|------|------|
| timestamp | timestamptz | 时间戳 |
| module_id | text | 模块唯一标识 |
| datacenter / room / rack | text | 位置信息 |
| device_ip / hostname / vendor | text | 设备信息 |
| interface | text | 接口名 |
| speed | text | 接口速率 |
| ifIndex / ifDescr / ifAlias | text/int | 接口索引/描述/别名 |
| ifType | text | 接口类型（ethernet等） |
| ifAdminStatus / ifOperStatus | int | 管理/操作状态 |
| ifLastChange | bigint | 接口状态最后变更时间（ticks） |
| ifSpeed | bigint | 接口速率（bps） |
| ifHCInOctets / ifHCOutOctets | bigint | 接收/发送字节数（高精度） |
| ifInErrors / ifOutErrors | int | 错误数 |
| ifInDiscards / ifOutDiscards | int | 丢弃数 |
| ifInBroadcastPkts / ifOutBroadcastPkts | int | 广播包数 |
| ifInMulticastPkts / ifOutMulticastPkts | int | 组播包数 |
| ifInUcastPkts / ifOutUcastPkts | int | 单播包数 |

---

### 📜 3. 索引名：syslog-events（Elasticsearch）
- **用途**：结构化存储网络设备 syslog 日志，用于协议事件、链路故障、状态变更分析。

| 字段名 | 类型 | 说明 |
|--------|------|------|
| timestamp | datetime | 日志时间戳 |
| module_id | string | 对应模块标识（从interface映射） |
| device_ip / hostname / vendor | string | 设备信息 |
| facility | string | Syslog facility 类型 |
| severity | string | 严重等级（如critical） |
| message | string | 原始日志内容 |
| parsed_event.protocol | string | 协议名称（如OSPF） |
| parsed_event.event | string | 事件类型（如neighbor-down） |
| parsed_event.interface | string | 相关接口 |
| parsed_event.neighbor_ip | string | 邻居IP（如OSPF邻居） |
| parsed_event.reason | string | 事件触发原因 |

---

### 🧭 4. 表名：lldp_neighbors（TimescaleDB）
- **用途**：存储 LLDP 邻居发现信息，用于自动拓扑分析。

| 字段名 | 类型 | 说明 |
|--------|------|------|
| timestamp | timestamptz | 时间戳 |
| module_id | text | 模块ID（本地端口） |
| device_ip / hostname / vendor | text | 本地设备信息 |
| datacenter / room / rack | text | 位置信息 |
| interface | text | 本地接口 |
| remote_device_id | text | 邻居设备ID（系统名） |
| remote_device_ip | text | 邻居设备IP（可选） |
| remote_port_id | text | 邻居接口ID |
| remote_chassis_id | text | 邻居机箱ID |
| remote_system_name | text | 邻居系统名称 |
| remote_port_desc | text | 邻居端口描述 |
| remote_system_desc | text | 邻居设备描述 |
| remote_capabilities | text | 支持功能（路由器/交换机） |
| topology_role | text | 拓扑角色（Leaf/Spine等） |

---

### 🧠 5. 表名：module_ddm_metrics（Parquet）
- **用途**：光模块历史 DDM 指标，供模型训练与趋势分析。

| 字段名 | 类型 | 说明 |
|--------|------|------|
| timestamp | datetime | 时间戳 |
| module_id | string | 模块标识 |
| temperature / voltage / current | float | 温度/电压/电流 |
| tx_power / rx_power | float | 发射/接收功率 |
| device_vendor | string | 厂商信息 |
| interface | string | 接口信息 |
| datacenter / room / rack | string | 位置信息 |

---

### 📊 6. 表名：prediction_result（Parquet）
- **用途**：记录 AI 模型对光模块的预测结果和命中规则。

| 字段名 | 类型 | 说明 |
|--------|------|------|
| timestamp | datetime | 预测时间点 |
| module_id | string | 模块ID |
| failure_probability | float | 故障概率（0-100） |
| failure_time_estimate | datetime | 预测故障时间点 |
| model_name | string | 模型名称（如ARIMA+IF） |
| remaining_days | int | 剩余寿命天数 |
| hit_rules | json | 命中规则详情 |

---

## 所有表共有字段
✅ 通用字段说明（所有表应包含）
| 字段名 | 类型 | 说明 |
|--------|------|------|
| timestamp | datetime | 数据时间戳（ISO格式）|
| module_id	| string | 唯一标识，如 CISCO-DC1-Pod01-Rack01-SW01-Eth1/1-100G |
| datacenter | string | 数据中心标识 |
| room | string | 机房/Pod |
| rack | string | 机架 |
| device_hostname | string | 设备名称 |
| device_ip	| string | 设备IP |
| device_vendor	| string | 厂商名称 |
| interface	| string | 接口名称，如 Ethernet1/1 |
| speed | string |接口速率，如 100G | 