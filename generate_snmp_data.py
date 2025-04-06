#!/usr/bin/env python3
import random
import pandas as pd
import numpy as np
import ipaddress
import argparse
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq
from random import choice, randint, uniform, sample

# Constants
VENDORS = ['Cisco', 'Huawei', 'Juniper', 'Arista', 'Dell', 'Broadcom Sonic', 'Community Sonic']
OPTICAL_VENDORS = ['Innolight', 'Luxshare', 'Finisar', 'HGTECH', 'Eoptolink', 'Accelink']
SPEEDS = ['1G', '10G', '25G', '100G', '200G', '400G', '800G']
DATACENTERS = ["DC1", "DC2", "DC3"]
PODS = ["Pod01", "Pod02", "Pod03", "Pod04"]
RACKS = ["Rack01", "Rack02", "Rack03", "Rack04", "Rack05"]
SWITCHES = [f"SW{i:02d}" for i in range(1, 21)]
INTERFACES = [f"Eth{i}/{j}" for i in range(1, 9) for j in range(1, 9)]
SPEED_TO_CAPACITY = {
    '1G': 1000000000,
    '10G': 10000000000,
    '25G': 25000000000,
    '100G': 100000000000,
    '200G': 200000000000,
    '400G': 400000000000,
    '800G': 800000000000
}
INTERFACE_STATES = ['up', 'down', 'testing', 'unknown', 'dormant', 'notPresent', 'lowerLayerDown']
INTERFACE_TYPES = ['ethernetCsmacd', 'softwareLoopback', 'other', 'propVirtual', 'propPointToPointSerial']
ADMIN_STATES = ['up', 'down', 'testing']
OPER_STATES = ['up', 'down', 'testing', 'unknown', 'dormant', 'notPresent', 'lowerLayerDown']

# Environment presets
ENVIRONMENTS = {
    'datacenter': {
        'device_prefix': ['spine', 'leaf', 'border', 'core'],
        'network': '10.0.0.0/8',
        'primary_vendors': ['Cisco', 'Arista', 'Juniper'],
        'port_density': (24, 64)
    },
    'enterprise': {
        'device_prefix': ['core', 'dist', 'access', 'edge'],
        'network': '192.168.0.0/16',
        'primary_vendors': ['Cisco', 'Huawei', 'Juniper'],
        'port_density': (8, 48)
    },
    'isp': {
        'device_prefix': ['edge', 'agg', 'core', 'pe', 'p'],
        'network': '100.64.0.0/10',
        'primary_vendors': ['Cisco', 'Juniper', 'Huawei'],
        'port_density': (4, 32)
    },
    'campus': {
        'device_prefix': ['bb', 'dist', 'access', 'wifi'],
        'network': '172.16.0.0/12',
        'primary_vendors': ['Cisco', 'Aruba', 'Huawei'],
        'port_density': (24, 48)
    }
}

# OID prefixes by vendor for system info
VENDOR_OID_MAP = {
    'Cisco': '1.3.6.1.4.1.9',
    'Huawei': '1.3.6.1.4.1.2011',
    'Juniper': '1.3.6.1.4.1.2636',
    'Arista': '1.3.6.1.4.1.30065',
    'Dell': '1.3.6.1.4.1.674',
    'Broadcom Sonic': '1.3.6.1.4.1.7244',
    'Community Sonic': '1.3.6.1.4.1.50852'
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
    
    # Generate system info
    device_info = []
    for i, (name, ip, vendor) in enumerate(zip(device_names, host_ips, device_vendors)):
        uptime = randint(86400, 31536000)  # 1 day to 1 year in seconds
        device_info.append({
            'name': name,
            'ip': ip,
            'vendor': vendor,
            'oid_prefix': VENDOR_OID_MAP.get(vendor, '1.3.6.1.4.1.9'),  # default to Cisco
            'sys_descr': f"{vendor} {choice(['NX-OS', 'IOS-XR', 'IOS', 'EOS', 'JunOS', 'VRP'])} Software, Version {randint(7,18)}.{randint(1,9)}.{randint(1,5)}",
            'sys_uptime': uptime,
            'sys_contact': f"admin@{name.split('-')[0]}.example.com",
            'sys_name': name,
            'sys_location': f"DC{randint(1,4)}-Rack{randint(1,100)}-U{randint(1,42)}",
            'cpu_5s': randint(5, 70),
            'cpu_1m': randint(5, 60),
            'cpu_5m': randint(5, 50),
            'memory_used': randint(1000000, 8000000000),
            'memory_total': randint(8000000000, 16000000000)
        })
    
    return device_info

def generate_interfaces(devices, environment):
    """Generate interface information for devices"""
    env_settings = ENVIRONMENTS.get(environment, ENVIRONMENTS['datacenter'])
    min_ports, max_ports = env_settings['port_density']
    
    all_interfaces = []
    
    for device in devices:
        num_ports = randint(min_ports, max_ports)
        
        for i in range(1, num_ports + 1):
            # Basic interface properties
            if_name = f"Eth{randint(1,8)}/{i}"
            if_alias = choice([f"to_{choice(['spine', 'leaf', 'core', 'border'])}-{randint(1,100)}", "", f"Server{randint(1,500)}"])
            if_type = choice(INTERFACE_TYPES)
            if_mtu = choice([1500, 9000, 9216])
            
            admin_status = choice(ADMIN_STATES)
            # If admin down, oper should be down
            if admin_status == 'down':
                oper_status = 'down'
            else:
                oper_status = choice(OPER_STATES)
            
            # Random speed based on environment
            if_speed = choice(SPEEDS)
            speed_bps = SPEED_TO_CAPACITY[if_speed]
            
            # Random optical module (if applicable)
            optical_present = random.random() > 0.3  # 70% chance of having optics
            
            if optical_present and if_speed != '1G':  # 1G usually doesn't have pluggable optics
                datacenter = choice(DATACENTERS)
                pod = choice(PODS)
                rack = choice(RACKS)
                optical_vendor = choice(OPTICAL_VENDORS)
                
                # 使用统一格式创建module_id
                module_id = f"{optical_vendor}-{datacenter}-{pod}-{rack}-{device['name']}-{if_name}-{if_speed}"
                
                optical_serial = f"{''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=8))}"
                optical_part = f"{optical_vendor}-{if_speed}-{choice(['SR', 'LR', 'PSM4', 'CWDM4', 'LR4', 'SR4', 'AOC', 'DAC'])}"
                # Optical parameters - modified per requirements
                temp = uniform(10.0, 90.0)  # 10-90 celsius range
                voltage = uniform(2.33, 4.32)  # 2.33-4.32V range
                tx_bias = uniform(10.0, 80.0)  # Match generate_ddm values
                tx_power = uniform(-2.0, 2.0)  # Match generate_ddm values
                rx_power = uniform(-4.0, 1.0)  # Match generate_ddm values
            else:
                module_id = ""
                optical_vendor = ""
                optical_serial = ""
                optical_part = ""
                temp = uniform(10.0, 90.0)  # Still provide temperature values
                voltage = uniform(2.33, 4.32)  # Still provide voltage values
                tx_bias = uniform(10.0, 80.0)  # Still provide meaningful values
                tx_power = uniform(-7.0, -5.0)  # Still provide meaningful values
                rx_power = uniform(-10.0, -8.0)  # Still provide meaningful values
            
            # Traffic statistics - ensure they're consistent with interface status but never zero
            if oper_status == 'up':
                in_octets = randint(1000000, 10000000000)
                out_octets = randint(1000000, 10000000000)
                in_packets = randint(10000, 100000000)
                out_packets = randint(10000, 100000000)
                in_errors = randint(1, 100)
                out_errors = randint(1, 100)
                in_discards = randint(1, 1000)
                out_discards = randint(1, 1000)
            else:
                # Even for down interfaces, provide non-zero historical values
                in_octets = randint(1000, 100000)
                out_octets = randint(1000, 100000)
                in_packets = randint(1000, 100000)
                out_packets = randint(1000, 100000)
                in_errors = randint(1, 100)
                out_errors = randint(1, 100)
                in_discards = randint(1, 100)
                out_discards = randint(1, 100)
            
            # Create a dictionary with all the interface information
            interface_info = {
                'device_ip': device['ip'],
                'device_hostname': device['name'],
                'device_vendor': device['vendor'],
                'interface': if_name,
                'speed': if_speed,
                'datacenter': datacenter if optical_present else choice(DATACENTERS),
                'room': pod if optical_present else choice(PODS),
                'rack': rack if optical_present else choice(RACKS),
                'module_id': module_id,
                'optic_vendor': optical_vendor,
                'optic_serial': optical_serial,
                'optic_part': optical_part,
                'ifIndex': randint(1, 256),
                'ifDescr': if_name,
                'ifAlias': if_alias,
                'ifType': if_type,
                'ifMtu': if_mtu,
                'ifSpeed': speed_bps,
                'ifAdminStatus': admin_status,
                'ifOperStatus': oper_status,
                'ifLastChange': randint(1, 2000000),
                'ifHCInOctets': in_octets,
                'ifHCOutOctets': out_octets,
                'ifInUcastPkts': in_packets,
                'ifOutUcastPkts': out_packets,
                'ifInErrors': in_errors,
                'ifOutErrors': out_errors,
                'ifInDiscards': in_discards,
                'ifOutDiscards': out_discards,
                'ifInBroadcastPkts': randint(100, 10000),
                'ifOutBroadcastPkts': randint(100, 10000),
                'ifInMulticastPkts': randint(100, 10000),
                'ifOutMulticastPkts': randint(100, 10000),
                'opticalTemp': temp,
                'opticalVoltage': voltage,
                'opticalTxBias': tx_bias,
                'opticalTxPower': tx_power,
                'opticalRxPower': rx_power
            }
            
            all_interfaces.append(interface_info)
    
    return all_interfaces

def generate_snmp_data(devices, interfaces, num_samples, start_date, end_date):
    """Generate time-series SNMP data based on devices and interfaces"""
    # Calculate timestamps
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, '%Y-%m-%d')
    
    time_range = (end_date - start_date).total_seconds()
    
    # Create an empty list to hold all the SNMP data
    all_snmp_data = []
    
    # Interface samples
    interface_samples = interfaces * (num_samples // len(interfaces) + 1)
    interface_samples = interface_samples[:num_samples]
    
    # Randomize timestamps
    for i, interface in enumerate(interface_samples):
        # Create a randomized timestamp in the specified range
        random_second = random.randint(0, int(time_range))
        timestamp = start_date + timedelta(seconds=random_second)
        timestamp_str = timestamp.strftime('%Y-%m-%d %H:%M:%S')
        
        # Create a copy of the interface information
        snmp_data = interface.copy()
        
        # Add the timestamp
        snmp_data['timestamp'] = timestamp_str
        
        # Randomize interface counters to simulate traffic changes
        if snmp_data['ifOperStatus'] == 'up':
            delta_in = randint(1000, 1000000)
            delta_out = randint(1000, 1000000)
            delta_pkts_in = randint(10, 10000)
            delta_pkts_out = randint(10, 10000)
            
            # Update counters
            snmp_data['ifHCInOctets'] += delta_in
            snmp_data['ifHCOutOctets'] += delta_out
            snmp_data['ifInUcastPkts'] += delta_pkts_in
            snmp_data['ifOutUcastPkts'] += delta_pkts_out
            
            # Small chance of errors
            if random.random() < 0.1:
                snmp_data['ifInErrors'] += randint(0, 5)
            if random.random() < 0.1:
                snmp_data['ifOutErrors'] += randint(0, 5)
            if random.random() < 0.1:
                snmp_data['ifInDiscards'] += randint(0, 10)
            if random.random() < 0.1:
                snmp_data['ifOutDiscards'] += randint(0, 10)
        
        # Update optical module values
        if snmp_data.get('module_id'):
            # Small random changes in optics measurements
            snmp_data['opticalTemp'] += uniform(-1.0, 1.0)
            snmp_data['opticalVoltage'] += uniform(-0.01, 0.01)
            snmp_data['opticalTxBias'] += uniform(-0.5, 0.5)
            snmp_data['opticalTxPower'] += uniform(-0.1, 0.1)
            snmp_data['opticalRxPower'] += uniform(-0.2, 0.2)
            
            # Enforce reasonable ranges
            snmp_data['opticalTemp'] = max(10.0, min(90.0, snmp_data['opticalTemp']))
            snmp_data['opticalVoltage'] = max(2.33, min(4.32, snmp_data['opticalVoltage']))
            snmp_data['opticalTxBias'] = max(5.0, min(85.0, snmp_data['opticalTxBias']))
            snmp_data['opticalTxPower'] = max(-7.0, min(3.0, snmp_data['opticalTxPower']))
            snmp_data['opticalRxPower'] = max(-12.0, min(2.0, snmp_data['opticalRxPower']))
        
        all_snmp_data.append(snmp_data)
    
    # Convert to DataFrame and save to Parquet
    df = pd.DataFrame(all_snmp_data)
    
    # Ensure we have all required fields based on the new data structure
    if 'module_id' not in df.columns:
        df['module_id'] = ''
    if 'datacenter' not in df.columns:
        df['datacenter'] = [choice(DATACENTERS) for _ in range(len(df))]
    if 'room' not in df.columns:
        df['room'] = [choice(PODS) for _ in range(len(df))]
    if 'rack' not in df.columns:
        df['rack'] = [choice(RACKS) for _ in range(len(df))]
    if 'device_hostname' not in df.columns:
        df['device_hostname'] = df['device_name']
    if 'device_vendor' not in df.columns:
        df['device_vendor'] = df['vendor']
    
    # Rename columns to match the new data structure if needed
    column_mapping = {
        'vendor': 'device_vendor',
        'device_name': 'device_hostname',
        'opticalTemp': 'temperature',
        'opticalVoltage': 'voltage',
        'opticalTxBias': 'bias_current',
        'opticalTxPower': 'tx_power',
        'opticalRxPower': 'rx_power',
    }
    
    df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
    
    return df

def main():
    parser = argparse.ArgumentParser(description='Generate SNMP data for network devices')
    parser.add_argument('--count', type=int, default=10000, help='Number of SNMP samples to generate')
    parser.add_argument('--devices', type=int, default=100, help='Number of network devices to simulate')
    parser.add_argument('--start', type=str, default='2025-02-01', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', type=str, default='2025-03-01', help='End date (YYYY-MM-DD)')
    parser.add_argument('--output', type=str, default='snmp_data.parquet', help='Output file name')
    parser.add_argument('--environment', type=str, choices=['datacenter', 'enterprise', 'isp', 'campus'], 
                       default='datacenter', help='Network environment to simulate')
    args = parser.parse_args()
    
    num_samples = args.count
    num_devices = args.devices
    start_date = args.start
    end_date = args.end
    output_file = args.output
    environment = args.environment
    
    print(f"Generating {num_samples} SNMP samples from {start_date} to {end_date}...")
    print(f"Network environment: {environment}")
    print(f"Simulating {num_devices} devices")
    
    # Generate device configurations
    devices = setup_network_devices(environment, num_devices)
    print(f"Generated {len(devices)} device configurations")
    
    # Generate interface configurations
    interfaces = generate_interfaces(devices, environment)
    print(f"Generated {len(interfaces)} interfaces")
    
    # Generate SNMP samples
    snmp_df = generate_snmp_data(devices, interfaces, num_samples, start_date, end_date)
    
    # Save to parquet
    table = pa.Table.from_pandas(snmp_df)
    pq.write_table(table, output_file)
    
    print(f"Generated {len(snmp_df)} SNMP samples and saved to {output_file}")
    print(f"Data range: {start_date} to {end_date}")
    print(f"Unique devices: {snmp_df['device_hostname'].nunique()}")
    print(f"Unique interfaces: {snmp_df['interface'].nunique()}")
    print(f"Fields included: {len(snmp_df.columns)}")
    
if __name__ == "__main__":
    main() 