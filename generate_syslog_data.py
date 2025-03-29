#!/usr/bin/env python3
import random
import time
import datetime
import pandas as pd
import ipaddress
import numpy as np
from random import choice, randint, uniform, sample
from datetime import datetime, timedelta
import argparse

# Constants
VENDORS = ['Cisco', 'Huawei', 'Juniper', 'Arista', 'Dell', 'Broadcom Sonic', 'Community Sonic']
OPTICAL_VENDORS = ['Innolight', 'Luxshare', 'Finisar', 'HGTECH', 'Eoptolink', 'Accelink']
SPEEDS = ['1G', '10G', '25G', '100G', '200G', '400G', '800G']
FACILITY_LEVELS = ['kern', 'user', 'mail', 'daemon', 'auth', 'syslog', 'lpr', 'news', 'uucp', 'cron', 'authpriv', 'ftp', 'local0', 'local1', 'local2', 'local3', 'local4', 'local5', 'local6', 'local7']
SEVERITY_LEVELS = ['emerg', 'alert', 'crit', 'err', 'warning', 'notice', 'info', 'debug']
PROTOCOLS = ['OSPF', 'BGP', 'VXLAN', 'MPLS', 'LLDP', 'STP', 'LACP', 'PIM', 'ISIS', 'VRRP']
DATACENTERS = ["DC1", "DC2", "DC3"]
PODS = ["Pod01", "Pod02", "Pod03", "Pod04"]
RACKS = ["Rack01", "Rack02", "Rack03", "Rack04", "Rack05"]
SWITCHES = [f"SW{i:02d}" for i in range(1, 21)]
INTERFACES = [f"Eth{i}/{j}" for i in range(1, 9) for j in range(1, 9)]

# Environment presets
ENVIRONMENTS = {
    'datacenter': {
        'device_prefix': ['spine', 'leaf', 'border', 'core'],
        'network': '10.0.0.0/8',
        'protocols': ['BGP', 'VXLAN', 'LLDP'],
        'primary_vendors': ['Cisco', 'Arista', 'Juniper'],
        'port_density': (24, 64),
        'vxlan_enabled': True,
        'mpls_enabled': True,
    },
    'enterprise': {
        'device_prefix': ['core', 'dist', 'access', 'edge'],
        'network': '192.168.0.0/16',
        'protocols': ['OSPF', 'STP', 'LLDP'],
        'primary_vendors': ['Cisco', 'Huawei', 'Juniper'],
        'port_density': (8, 48)
    },
    'isp': {
        'device_prefix': ['edge', 'agg', 'core', 'pe', 'p'],
        'network': '100.64.0.0/10',
        'protocols': ['BGP', 'MPLS', 'ISIS'],
        'primary_vendors': ['Cisco', 'Juniper', 'Huawei'],
        'port_density': (4, 32)
    },
    'campus': {
        'device_prefix': ['bb', 'dist', 'access', 'wifi'],
        'network': '172.16.0.0/12',
        'protocols': ['OSPF', 'STP', 'VRRP'],
        'primary_vendors': ['Cisco', 'Aruba', 'Huawei'],
        'port_density': (24, 48)
    }
}

def setup_network_devices(environment, num_devices):
    """Generate network devices based on environment settings"""
    env_settings = ENVIRONMENTS.get(environment, ENVIRONMENTS['datacenter'])
    
    # Network device ranges
    network = ipaddress.IPv4Network(env_settings['network'])
    host_ips = [str(ip) for ip in list(network.hosts())[0:num_devices]]
    
    # Favor specific vendors based on environment
    primary_vendors = env_settings['primary_vendors']
    vendors_weights = [0.7 if v in primary_vendors else 0.3 for v in VENDORS]
    total_weight = sum(vendors_weights)
    vendors_weights = [w/total_weight for w in vendors_weights]
    
    # Generate device names
    device_names = [
        f"{choice(env_settings['device_prefix'])}-{randint(1, 100)}-{randint(1, 10)}" 
        for _ in range(len(host_ips))
    ]
    
    # Assign vendors with appropriate weighting
    device_vendors = []
    for _ in range(len(host_ips)):
        vendor = np.random.choice(VENDORS, p=vendors_weights)
        device_vendors.append(vendor)
    
    return list(zip(device_names, host_ips, device_vendors))

def generate_device_optics(devices, environment):
    """Generate optic modules for devices"""
    env_settings = ENVIRONMENTS.get(environment, ENVIRONMENTS['datacenter'])
    min_ports, max_ports = env_settings['port_density']
    
    device_optics = {}
    for device, _, vendor in devices:
        num_optics = randint(min_ports, max_ports)
        device_optics[device] = [
            {
                'port': choice(INTERFACES),
                'vendor': choice(OPTICAL_VENDORS),
                'speed': choice(SPEEDS),
                'datacenter': choice(DATACENTERS),
                'pod': choice(PODS),
                'rack': choice(RACKS),
                'module_id': f"{choice(OPTICAL_VENDORS)}-{choice(DATACENTERS)}-{choice(PODS)}-{choice(RACKS)}-{device}-{choice(INTERFACES)}-{choice(SPEEDS)}",
                'serial': f"{''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=8))}"
            } for _ in range(num_optics)
        ]
    return device_optics

def generate_device_l3_config(devices, environment):
    """Generate L3 protocols configuration for devices"""
    env_settings = ENVIRONMENTS.get(environment, ENVIRONMENTS['datacenter'])
    preferred_protocols = env_settings['protocols']
    
    device_l3 = {}
    for device, ip, _ in devices:
        # Increase likelihood of preferred protocols for this environment
        has_bgp = random.random() > (0.3 if 'BGP' in preferred_protocols else 0.7)
        has_ospf = random.random() > (0.3 if 'OSPF' in preferred_protocols else 0.7)
        has_vxlan = random.random() > (0.3 if 'VXLAN' in preferred_protocols else 0.7)
        has_mpls = random.random() > (0.3 if 'MPLS' in preferred_protocols else 0.7)
        
        l3_config = {
            'bgp': has_bgp,
            'bgp_as': randint(1000, 65000) if has_bgp else None,
            'ospf': has_ospf,
            'ospf_area': randint(0, 100) if has_ospf else None,
            'vxlan': has_vxlan,
            'vxlan_vni': [randint(1000, 9000) for _ in range(randint(1, 10))] if has_vxlan else [],
            'mpls': has_mpls,
            'mpls_label': [randint(16, 1048575) for _ in range(randint(1, 5))] if has_mpls else []
        }
        device_l3[device] = l3_config
    return device_l3

def generate_cisco_syslog(timestamp, device, ip, severity, facility, message):
    return f"{timestamp} {ip} {severity}: {facility}: {message}"

def generate_juniper_syslog(timestamp, device, ip, severity, facility, message):
    return f"{timestamp} {ip} {device} {facility}[{randint(1000, 9999)}]: {severity}: {message}"

def generate_huawei_syslog(timestamp, device, ip, severity, facility, message):
    return f"{timestamp} {ip} %%{severity}/{facility}/{message}"

def generate_arista_syslog(timestamp, device, ip, severity, facility, message):
    return f"{timestamp} {ip} {device}: {facility}: %{severity}-{randint(0,7)}-{facility}: {message}"

def generate_generic_syslog(timestamp, device, ip, severity, facility, message):
    return f"{timestamp} {ip} {device} {facility}[{randint(100, 999)}]: {severity}: {message}"

def get_syslog_generator(vendor):
    if vendor.lower() == 'cisco':
        return generate_cisco_syslog
    elif vendor.lower() == 'juniper':
        return generate_juniper_syslog
    elif vendor.lower() == 'huawei':
        return generate_huawei_syslog
    elif vendor.lower() == 'arista':
        return generate_arista_syslog
    else:
        return generate_generic_syslog

def generate_physical_port_event():
    event_types = [
        "Link down",
        "Link up",
        "Interface disabled",
        "Interface enabled",
        "Auto-negotiation failed",
        "CRC errors detected",
        "FCS errors detected",
        "Packet drop detected",
        "Port flapping detected",
        "Input errors",
        "Output errors",
        "Collision detected",
        "Excessive collisions",
        "Late collision",
        "Speed mismatch",
        "Duplex mismatch"
    ]
    return choice(event_types)

def generate_optical_module_event():
    event_types = [
        "Rx power high",
        "Rx power low",
        "Tx power high",
        "Tx power low",
        "Temperature high",
        "Temperature low",
        "Voltage high",
        "Voltage low",
        "Bias current high",
        "Bias current low",
        "Module inserted",
        "Module removed",
        "Module not compatible",
        "Module authentication failed",
        "DDM threshold crossed"
    ]
    return choice(event_types)

def generate_l3_protocol_event(protocol):
    events = {
        'OSPF': [
            "Neighbor up",
            "Neighbor down",
            "Adjacency change",
            "SPF calculation",
            "Interface state change",
            "Area border router change",
            "Authentication failure",
            "Packet received with bad checksum",
            "Virtual link state change",
            "DR/BDR election"
        ],
        'BGP': [
            "Peer up",
            "Peer down",
            "Prefix limit exceeded",
            "Route dampening",
            "Path attribute error",
            "Session reset",
            "Hold timer expired",
            "Authentication failure",
            "Route flap",
            "Notification received"
        ],
        'VXLAN': [
            "VTEP discovery",
            "VNI state change",
            "Duplicate IP detected",
            "ARP/ND suppression",
            "Flood list change",
            "Tunnel established",
            "Tunnel down",
            "MAC mobility detected",
            "Unknown VNI",
            "MTU issues"
        ],
        'MPLS': [
            "LDP session up",
            "LDP session down",
            "Label allocation failure",
            "LSP up",
            "LSP down",
            "Path switch",
            "Label space exhausted",
            "TTL expired in transit",
            "RSVP reservation failure",
            "Unreachable destination"
        ],
        'LLDP': [
            "Neighbor added",
            "Neighbor removed",
            "Neighbor information changed",
            "Chassid ID TLV missing",
            "Port ID TLV missing",
            "TTL expired",
            "Unrecognized TLV received",
            "Remote port shutdown",
            "Remote system name change",
            "Management address changed"
        ],
        'STP': [
            "Topology change",
            "Root bridge change",
            "Port state change",
            "BPDU guard triggered",
            "Root guard triggered",
            "Loop guard triggered",
            "Bridge ID change",
            "Path cost change",
            "Multiple roots detected",
            "Inconsistent port state"
        ],
        'LACP': [
            "Port added to port-channel",
            "Port removed from port-channel",
            "Bundle up",
            "Bundle down",
            "Peer timeout",
            "System ID changed",
            "Port priority changed",
            "Key mismatch",
            "LACP rate changed",
            "Individual/Aggregate state change"
        ],
        'PIM': [
            "Neighbor up",
            "Neighbor down",
            "Join/Prune received",
            "Assert received",
            "Register stop received",
            "RP changed",
            "RPF change",
            "Multicast state timeout",
            "Bootstrap message received",
            "DR election"
        ],
        'ISIS': [
            "Adjacency up",
            "Adjacency down",
            "LSP received",
            "LSP generated",
            "DIS election",
            "Area address mismatch",
            "Authentication failure",
            "LSP database overload",
            "Circuit state change",
            "SPF calculation"
        ],
        'VRRP': [
            "State transition",
            "Virtual IP conflict",
            "Authentication failure",
            "Advertisement timer expired",
            "Priority zero received",
            "Master down interval expired",
            "Protocol error",
            "Interface tracking state change",
            "Master/Backup transition",
            "Configuration error"
        ]
    }
    return choice(events.get(protocol, ["State change"]))

def generate_optical_module_message(device, optics_info):
    """Generate a message related to an optical module"""
    if not optics_info:
        return ""

    # Select a random optic from this device
    optic = choice(optics_info)
    
    # Generate module_id if not already present
    if 'module_id' not in optic:
        optic['module_id'] = f"{optic['vendor']}-{optic['datacenter']}-{optic['pod']}-{optic['rack']}-{device}-{optic['port']}-{optic['speed']}"
        
    event = generate_optical_module_event()
    
    if event == "Module inserted":
        return f"Interface {optic['port']}: Transceiver module ({optic['module_id']}) inserted"
    elif event == "Module removed":
        return f"Interface {optic['port']}: Transceiver module ({optic['module_id']}) removed"
    elif event == "Module not compatible":
        return f"Interface {optic['port']}: Transceiver module ({optic['module_id']}) not compatible with port configuration"
    elif event == "Module authentication failed":
        return f"Interface {optic['port']}: Transceiver module ({optic['module_id']}) authentication failed"
    elif event == "DDM threshold crossed":
        ddm_type = choice(["temperature", "voltage", "tx-power", "rx-power", "tx-bias"])
        threshold = choice(["high alarm", "high warning", "low warning", "low alarm"])
        value = round(uniform(1.0, 100.0), 2)
        return f"Interface {optic['port']}: Transceiver module ({optic['module_id']}) {ddm_type} {threshold} threshold crossed. Current value: {value}"
    elif "power" in event.lower():
        if "high" in event.lower():
            value = round(uniform(2.0, 5.0), 2)
        else:  # low
            value = round(uniform(-30.0, -15.0), 2)
        if "rx" in event.lower():
            return f"Interface {optic['port']}: Transceiver module ({optic['module_id']}) RX power {value} dBm threshold crossed"
        else:  # tx
            return f"Interface {optic['port']}: Transceiver module ({optic['module_id']}) TX power {value} dBm threshold crossed"
    elif "temperature" in event.lower():
        if "high" in event.lower():
            value = round(uniform(70.0, 95.0), 2)
        else:  # low
            value = round(uniform(-5.0, 10.0), 2)
        return f"Interface {optic['port']}: Transceiver module ({optic['module_id']}) temperature {value}C threshold crossed"
    elif "voltage" in event.lower():
        if "high" in event.lower():
            value = round(uniform(3.6, 4.5), 2)
        else:  # low
            value = round(uniform(2.0, 2.9), 2)
        return f"Interface {optic['port']}: Transceiver module ({optic['module_id']}) voltage {value}V threshold crossed"
    elif "bias" in event.lower():
        if "high" in event.lower():
            value = round(uniform(80, 120), 2)
        else:  # low
            value = round(uniform(1, 10), 2)
        return f"Interface {optic['port']}: Transceiver module ({optic['module_id']}) bias current {value}mA threshold crossed"
    else:
        return f"Interface {optic['port']}: Transceiver module ({optic['module_id']}) {event}"

def generate_message(device, device_info, optics_info, l3_info):
    device_name, ip, vendor = device_info
    
    # Pick a message type
    message_types = [
        'physical_port', 
        'optical_module', 
        'protocol', 
        'system', 
        'authentication'
    ]
    
    # Adjust probabilities based on environment - more optical and protocol messages
    message_type_weights = [0.25, 0.25, 0.25, 0.15, 0.1]
    message_type = random.choices(message_types, weights=message_type_weights, k=1)[0]
    
    if message_type == 'physical_port':
        # Physical port events (interface up/down, errors, etc.)
        event = generate_physical_port_event()
        port = f"Eth{randint(1,8)}/{randint(1,48)}"
        message = f"Interface {port}: {event}"
        
    elif message_type == 'optical_module':
        # Optical module events
        if optics_info and device in optics_info and optics_info[device]:
            message = generate_optical_module_message(device, optics_info[device])
        else:
            # Fallback if no optical info is available
            port = f"Eth{randint(1,8)}/{randint(1,48)}"
            event = generate_optical_module_event()
            message = f"Interface {port}: {event}"
            
    elif message_type == 'protocol':
        # Protocol-related events
        protocol = choice(PROTOCOLS)
        event = generate_l3_protocol_event(protocol)
        if protocol == 'BGP' and l3_info.get(device, {}).get('bgp', False):
            peer_ip = f"10.{randint(1,254)}.{randint(1,254)}.{randint(1,254)}"
            bgp_as = l3_info[device]['bgp_as']
            message = f"{protocol}: Neighbor {peer_ip} (AS {bgp_as}) {event}"
        elif protocol == 'OSPF' and l3_info.get(device, {}).get('ospf', False):
            area_id = l3_info[device]['ospf_area']
            message = f"{protocol}: {event} in area {area_id}"
        elif protocol == 'VXLAN' and l3_info.get(device, {}).get('vxlan', False):
            vni = choice(l3_info[device]['vxlan_vni']) if l3_info[device]['vxlan_vni'] else randint(1000, 9000)
            message = f"{protocol}: VNI {vni} {event}"
        elif protocol == 'MPLS' and l3_info.get(device, {}).get('mpls', False):
            label = choice(l3_info[device]['mpls_label']) if l3_info[device]['mpls_label'] else randint(16, 1048575)
            message = f"{protocol}: Label {label} {event}"
        else:
            message = f"{protocol}: {event}"
            
    elif message_type == 'system':
        # System events (CPU, memory, power, fan, etc.)
        events = [
            f"System CPU utilization is high: {randint(80,100)}%",
            f"Memory utilization threshold exceeded: {randint(80,95)}%",
            f"Power supply {randint(1,2)} state changed to {'up' if random.random() > 0.2 else 'down'}",
            f"Fan module {randint(1,4)} {'OK' if random.random() > 0.2 else 'failure'}",
            f"Temperature sensor {randint(1,5)} reading: {randint(25,95)}C",
            f"System configuration {'saved' if random.random() > 0.5 else 'changed'}",
            f"NTP synchronization {'successful' if random.random() > 0.3 else 'failed'}",
            f"Logging {'started' if random.random() > 0.5 else 'stopped'}",
            f"File system utilization: {randint(60,95)}%",
            f"SNMP agent {'started' if random.random() > 0.5 else 'stopped'}"
        ]
        message = choice(events)
        
    else:  # authentication
        # Authentication events
        auth_events = [
            f"User {'admin' if random.random() > 0.7 else 'operator'} login {'successful' if random.random() > 0.3 else 'failed'} from {'.'.join([str(randint(1,255)) for _ in range(4)])}",
            f"SSH session established from {'.'.join([str(randint(1,255)) for _ in range(4)])}",
            f"Telnet connection attempt blocked from {'.'.join([str(randint(1,255)) for _ in range(4)])}",
            f"TACACS+ authentication {'succeeded' if random.random() > 0.3 else 'failed'}",
            f"RADIUS authentication {'succeeded' if random.random() > 0.3 else 'failed'}",
            f"User {'admin' if random.random() > 0.7 else 'operator'} entered privileged mode",
            f"Configuration change by user {'admin' if random.random() > 0.7 else 'operator'}",
            f"Account locked due to excessive login failures: {'admin' if random.random() > 0.5 else 'operator'}",
            f"Password change for user {'admin' if random.random() > 0.7 else 'operator'}"
        ]
        message = choice(auth_events)
    
    # Facility selection based on message type
    if message_type == 'physical_port' or message_type == 'optical_module':
        facility = choice(['local3', 'local4', 'daemon'])
    elif message_type == 'protocol':
        facility = choice(['local0', 'local1', 'local2'])
    elif message_type == 'system':
        facility = choice(['kern', 'daemon', 'syslog'])
    else:  # authentication
        facility = choice(['auth', 'authpriv', 'local7'])
    
    # Severity selection based on content
    if 'error' in message.lower() or 'fail' in message.lower() or 'down' in message.lower():
        severity = choice(['err', 'crit', 'alert'])
    elif 'warn' in message.lower() or 'high' in message.lower():
        severity = 'warning'
    elif 'notif' in message.lower() or 'up' in message.lower() or 'success' in message.lower():
        severity = 'notice'
    else:
        severity = choice(['info', 'debug'])
    
    return facility, severity, message

def generate_syslog_data(num_events, start_date, end_date):
    """Generate synthetic syslog messages for network devices"""
    # Parse start and end dates
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    
    # Calculate time window in seconds
    time_window = int((end_dt - start_dt).total_seconds())
    
    # Environment defaults
    environment = 'datacenter'  # Default to datacenter environment
    
    # Generate device information
    num_devices = min(100, num_events // 10)  # Ensure reasonable number of devices
    devices = setup_network_devices(environment, num_devices)
    
    # Generate optics information
    device_optics = generate_device_optics(devices, environment)
    
    # Generate L3 protocol information
    device_l3 = generate_device_l3_config(devices, environment)
    
    # Generate the events
    syslog_events = []
    
    for _ in range(num_events):
        # Random timestamp within date range
        event_timestamp = start_dt + timedelta(seconds=randint(0, time_window))
        timestamp_str = event_timestamp.strftime('%Y-%m-%d %H:%M:%S')
        
        # Select a random device
        device_info = choice(devices)
        device_name, ip, vendor = device_info
        
        # Get device-specific information
        optics_info = device_optics.get(device_name, [])
        l3_info = device_l3.get(device_name, {})
        
        # Generate message for event
        facility, severity, message = generate_message(device_name, device_info, optics_info, l3_info)
        
        # Get appropriate syslog format function based on vendor
        syslog_generator = get_syslog_generator(vendor)
        
        # Generate syslog message
        syslog_msg = syslog_generator(timestamp_str, device_name, ip, severity, facility, message)
        
        # Create syslog record with standard fields
        record = {
            'timestamp': timestamp_str,
            'device_ip': ip,
            'device_name': device_name,
            'facility': facility,
            'severity': severity,
            'vendor': vendor,
            'message': message,
            'syslog_message': syslog_msg,
            'module_id': ""  # 默认为空字符串
        }
        
        # 判断是否为光模块相关消息并提取/生成module_id
        optical_related_keywords = [
            'module', 'transceiver', 'optical', 'tx power', 'rx power', 
            'temperature', 'voltage', 'bias', 'ddm'
        ]
        
        is_optical_message = False
        # 检查消息中是否包含关键字(不区分大小写)
        lowercase_message = message.lower()
        for keyword in optical_related_keywords:
            if keyword in lowercase_message:
                is_optical_message = True
                break
        
        # 如果是光模块相关消息，生成或提取module_id
        if is_optical_message:
            # 从消息中提取module_id (如果有)
            if "(" in message and ")" in message:
                module_id_part = message[message.find("(")+1:message.find(")")]
                if "-" in module_id_part and module_id_part.count("-") >= 6:  # 符合格式的module_id应该有至少6个连字符
                    record['module_id'] = module_id_part
                else:
                    # 生成新的module_id
                    record['module_id'] = create_new_module_id(device_name, message, optics_info)
            else:
                # 没有括号中的module_id，需要新生成
                record['module_id'] = create_new_module_id(device_name, message, optics_info)
                
        syslog_events.append(record)
    
    return syslog_events

def create_new_module_id(device_name, message, optics_info):
    """从消息和可用的光模块信息创建module_id"""
    # 尝试从消息中提取接口名称
    interface = None
    if "Interface " in message:
        # 消息格式可能是 "Interface Eth1/1: something"
        parts = message.split(":")
        if len(parts) > 0:
            interface_part = parts[0].strip()
            if "Interface " in interface_part:
                interface = interface_part.replace("Interface ", "").strip()
    
    # 如果接口提取成功，尝试在已有optics信息中查找匹配的module_id
    if interface and optics_info:
        for optic in optics_info:
            if optic.get('port') == interface and 'module_id' in optic:
                return optic['module_id']
    
    # 如果没有找到匹配的module_id，创建一个新的
    optical_vendor = choice(OPTICAL_VENDORS)
    datacenter = choice(DATACENTERS)
    pod = choice(PODS)
    rack = choice(RACKS)
    
    # 如果接口名称未能提取，生成一个随机接口
    if not interface:
        interface = f"Eth{randint(1,8)}/{randint(1,48)}"
        
    speed = choice(SPEEDS)
    
    return f"{optical_vendor}-{datacenter}-{pod}-{rack}-{device_name}-{interface}-{speed}"

def write_to_parquet(syslog_events, output_file='syslog_data.parquet'):
    """Convert syslog events to a Parquet file"""
    # Convert to DataFrame
    df = pd.DataFrame(syslog_events)
    
    # Ensure all required columns exist
    required_columns = ['timestamp', 'device_ip', 'device_name', 'facility', 
                        'severity', 'vendor', 'message', 'syslog_message', 'module_id']
    
    for col in required_columns:
        if col not in df.columns:
            if col == 'module_id':
                df[col] = ""
            else:
                df[col] = None
    
    # Write to Parquet format
    import pyarrow as pa
    import pyarrow.parquet as pq
    
    table = pa.Table.from_pandas(df)
    pq.write_table(table, output_file)
    
    print(f"Generated {len(syslog_events)} syslog events and saved to {output_file}")
    print(f"Data includes {df['device_name'].nunique()} devices")
    print(f"Fields included: {len(df.columns)}")
    
    return output_file

def main():
    # Add argument parsing
    parser = argparse.ArgumentParser(description='Generate synthetic Syslog data for network devices')
    parser.add_argument('--count', type=int, default=1000000, help='Number of events to generate (default: 1,000,000)')
    parser.add_argument('--devices', type=int, default=1000, help='Number of network devices to simulate (default: 1,000)')
    parser.add_argument('--start-date', type=str, default="2025-02-01", help='Start date in YYYY-MM-DD format (default: 2025-02-01)')
    parser.add_argument('--end-date', type=str, default="2025-03-01", help='End date in YYYY-MM-DD format (default: 2025-03-01)')
    parser.add_argument('--output', type=str, default="network_syslog_data.parquet", help='Output filename (default: network_syslog_data.parquet)')
    parser.add_argument('--environment', type=str, choices=['datacenter', 'enterprise', 'isp', 'campus'], default='datacenter',
                       help='Network environment to simulate (default: datacenter)')
    
    args = parser.parse_args()
    
    # Parameters from arguments
    num_events = args.count
    num_devices = min(args.devices, 10000)  # Limit to reasonable number
    start_date = args.start_date
    end_date = args.end_date
    output_file = args.output
    environment = args.environment
    
    print(f"Generating {num_events:,} Syslog events from {start_date} to {end_date}...")
    print(f"Network environment: {environment}")
    print(f"Simulating {num_devices:,} devices")
    
    # Generate data
    syslog_events = generate_syslog_data(num_events, start_date, end_date)
    
    # Save to parquet
    write_to_parquet(syslog_events, output_file)
    
    print(f"Generated {len(syslog_events):,} Syslog events and saved to {output_file}")
    print(f"Data range: {start_date} to {end_date}")
    print(f"Unique devices: {len(set(event['device_name'] for event in syslog_events))}")
    print(f"Vendors: {', '.join(set(event['vendor'] for event in syslog_events))}")
    
if __name__ == "__main__":
    main() 