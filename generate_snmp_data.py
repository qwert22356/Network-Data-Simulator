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
            if_name = f"Ethernet{randint(1,8)}/{i}"
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
                optical_vendor = choice(OPTICAL_VENDORS)
                optical_serial = f"{''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=8))}"
                optical_part = f"{optical_vendor}-{if_speed}-{choice(['SR', 'LR', 'PSM4', 'CWDM4', 'LR4', 'SR4', 'AOC', 'DAC'])}"
                # Optical parameters - modified per requirements
                temp = uniform(10.0, 90.0)  # 10-90 celsius range
                voltage = uniform(2.33, 4.32)  # 2.33-4.32V range
                tx_bias = uniform(10.0, 80.0)  # Match generate_ddm values
                tx_power = uniform(-2.0, 2.0)  # Match generate_ddm values
                rx_power = uniform(-4.0, 1.0)  # Match generate_ddm values
            else:
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
            
            # Last change timestamp
            last_change = randint(0, device['sys_uptime'])
            
            # Interface index and description
            if_index = i
            if_descr = if_name
            
            # Create interface record
            interface = {
                'device_ip': device['ip'],
                'device_name': device['name'],
                'vendor': device['vendor'],
                'if_index': if_index,
                'if_descr': if_descr,
                'if_name': if_name,
                'if_alias': if_alias,
                'if_type': if_type,
                'if_mtu': if_mtu,
                'if_speed': speed_bps,
                'if_admin_status': admin_status,
                'if_oper_status': oper_status,
                'if_last_change': last_change,
                'if_in_octets': in_octets,
                'if_out_octets': out_octets,
                'if_in_packets': in_packets,
                'if_out_packets': out_packets,
                'if_in_errors': in_errors,
                'if_out_errors': out_errors,
                'if_in_discards': in_discards,
                'if_out_discards': out_discards,
                'if_media_type': if_speed,
                'optical_vendor': optical_vendor,
                'optical_serial': optical_serial,
                'optical_part_number': optical_part,
                'optical_temperature': temp,
                'optical_voltage': voltage,
                'optical_tx_bias': tx_bias,
                'optical_tx_power': tx_power,
                'optical_rx_power': rx_power
            }
            
            all_interfaces.append(interface)
    
    return all_interfaces

def generate_snmp_data(devices, interfaces, num_samples, start_date, end_date):
    """Generate SNMP samples over time for devices and interfaces"""
    # Convert dates to timestamps
    start_timestamp = datetime.strptime(start_date, "%Y-%m-%d").timestamp()
    end_timestamp = datetime.strptime(end_date, "%Y-%m-%d").timestamp()
    
    # Calculate time interval between samples
    time_range = end_timestamp - start_timestamp
    interval = time_range / num_samples
    
    snmp_samples = []
    
    # For each time sample
    for i in range(num_samples):
        # Calculate timestamp for this sample
        event_time = start_timestamp + interval * i
        event_datetime = datetime.fromtimestamp(event_time)
        formatted_timestamp = event_datetime.strftime("%Y-%m-%d %H:%M:%S")
        
        # Select random device and interface
        device = random.choice(devices)
        interface = random.choice([intf for intf in interfaces if intf['device_ip'] == device['ip']])
        
        # Update dynamic values
        # Device metrics
        device_updated = device.copy()
        device_updated['sys_uptime'] = device['sys_uptime'] + int(i * interval)
        device_updated['cpu_5s'] = min(100, max(1, device['cpu_5s'] + randint(-10, 10)))
        device_updated['cpu_1m'] = min(100, max(1, device['cpu_1m'] + randint(-5, 5)))
        device_updated['cpu_5m'] = min(100, max(1, device['cpu_5m'] + randint(-3, 3)))
        
        # Memory usage varies slowly
        memory_fluctuation = uniform(0.9, 1.1)
        device_updated['memory_used'] = min(device_updated['memory_total'], 
                                          int(device['memory_used'] * memory_fluctuation))
        
        # Interface metrics
        interface_updated = interface.copy()
        
        # Always increase traffic metrics regardless of interface status
        # For active interfaces, increase more dramatically
        if interface['if_oper_status'] == 'up':
            traffic_multiplier = 1 + (i / num_samples) * uniform(0.8, 1.2)
            
            interface_updated['if_in_octets'] = max(1000, int(interface['if_in_octets'] * traffic_multiplier))
            interface_updated['if_out_octets'] = max(1000, int(interface['if_out_octets'] * traffic_multiplier))
            interface_updated['if_in_packets'] = max(1000, int(interface['if_in_packets'] * traffic_multiplier))
            interface_updated['if_out_packets'] = max(1000, int(interface['if_out_packets'] * traffic_multiplier))
            
            # Errors and discards may increase slightly but always non-zero
            interface_updated['if_in_errors'] = max(1, interface['if_in_errors'] + randint(0, 2))
            interface_updated['if_out_errors'] = max(1, interface['if_out_errors'] + randint(0, 2))
            interface_updated['if_in_discards'] = max(1, interface['if_in_discards'] + randint(0, 5))
            interface_updated['if_out_discards'] = max(1, interface['if_out_discards'] + randint(0, 5))
        else:
            # For inactive interfaces, keep the values relatively stable
            interface_updated['if_in_octets'] = max(1000, interface['if_in_octets'])
            interface_updated['if_out_octets'] = max(1000, interface['if_out_octets'])
            interface_updated['if_in_packets'] = max(1000, interface['if_in_packets'])
            interface_updated['if_out_packets'] = max(1000, interface['if_out_packets'])
            interface_updated['if_in_errors'] = max(1, interface['if_in_errors'])
            interface_updated['if_out_errors'] = max(1, interface['if_out_errors'])
            interface_updated['if_in_discards'] = max(1, interface['if_in_discards'])
            interface_updated['if_out_discards'] = max(1, interface['if_out_discards'])
        
        # Optical parameters fluctuate according to updated requirements
        if interface['optical_vendor']:
            # Use consistent ranges with optical module values from generate_ddm
            interface_updated['optical_temperature'] = max(10.0, min(90.0, interface['optical_temperature'] + uniform(-2, 2)))
            interface_updated['optical_voltage'] = max(2.33, min(4.32, interface['optical_voltage'] + uniform(-0.05, 0.05)))
            interface_updated['optical_tx_bias'] = max(10.0, min(80.0, interface['optical_tx_bias'] + uniform(-1, 1)))
            interface_updated['optical_tx_power'] = min(2.0, max(-7.0, interface['optical_tx_power'] + uniform(-0.2, 0.2)))
            interface_updated['optical_rx_power'] = min(1.0, max(-10.0, interface['optical_rx_power'] + uniform(-0.5, 0.5)))
        else:
            # Even if no optical module present, provide realistic values
            interface_updated['optical_temperature'] = uniform(10.0, 90.0)
            interface_updated['optical_voltage'] = uniform(2.33, 4.32)
            interface_updated['optical_tx_bias'] = uniform(10.0, 80.0)
            interface_updated['optical_tx_power'] = uniform(-7.0, -5.0)
            interface_updated['optical_rx_power'] = uniform(-10.0, -8.0)
        
        # Combine device and interface info in one sample record
        sample = {
            'timestamp': formatted_timestamp,
            'device_ip': device['ip'],
            'device_name': device['name'],
            'vendor': device['vendor'],
            'sys_descr': device['sys_descr'],
            'sys_uptime': device_updated['sys_uptime'],
            'sys_contact': device['sys_contact'],
            'sys_name': device['sys_name'],
            'sys_location': device['sys_location'],
            'cpu_5s': device_updated['cpu_5s'],
            'cpu_1m': device_updated['cpu_1m'],
            'cpu_5m': device_updated['cpu_5m'],
            'memory_used': device_updated['memory_used'],
            'memory_total': device['memory_total'],
            'if_index': interface['if_index'],
            'if_descr': interface['if_descr'],
            'if_name': interface['if_name'],
            'if_alias': interface['if_alias'],
            'if_type': interface['if_type'],
            'if_mtu': interface['if_mtu'],
            'if_speed': interface['if_speed'],
            'if_admin_status': interface['if_admin_status'],
            'if_oper_status': interface['if_oper_status'],
            'if_last_change': interface['if_last_change'],
            'if_in_octets': interface_updated['if_in_octets'],
            'if_out_octets': interface_updated['if_out_octets'],
            'if_in_packets': interface_updated['if_in_packets'],
            'if_out_packets': interface_updated['if_out_packets'],
            'if_in_errors': interface_updated['if_in_errors'],
            'if_out_errors': interface_updated['if_out_errors'],
            'if_in_discards': interface_updated['if_in_discards'],
            'if_out_discards': interface_updated['if_out_discards'],
            'if_media_type': interface['if_media_type'],
            'optical_vendor': interface['optical_vendor'],
            'optical_serial': interface['optical_serial'],
            'optical_part_number': interface['optical_part_number'],
            'optical_temperature': interface_updated['optical_temperature'],
            'optical_voltage': interface_updated['optical_voltage'],
            'optical_tx_bias': interface_updated['optical_tx_bias'],
            'optical_tx_power': interface_updated['optical_tx_power'],
            'optical_rx_power': interface_updated['optical_rx_power']
        }
        
        snmp_samples.append(sample)
    
    return pd.DataFrame(snmp_samples)

def main():
    # Add argument parsing
    parser = argparse.ArgumentParser(description='Generate synthetic SNMP data for network devices')
    parser.add_argument('--count', type=int, default=1000000, help='Number of SNMP samples to generate (default: 1,000,000)')
    parser.add_argument('--devices', type=int, default=100, help='Number of network devices to simulate (default: 100)')
    parser.add_argument('--start-date', type=str, default="2025-02-01", help='Start date in YYYY-MM-DD format (default: 2025-02-01)')
    parser.add_argument('--end-date', type=str, default="2025-03-01", help='End date in YYYY-MM-DD format (default: 2025-03-01)')
    parser.add_argument('--output', type=str, default="network_snmp_data.parquet", help='Output filename (default: network_snmp_data.parquet)')
    parser.add_argument('--environment', type=str, choices=['datacenter', 'enterprise', 'isp', 'campus'], default='datacenter',
                       help='Network environment to simulate (default: datacenter)')
    
    args = parser.parse_args()
    
    # Parameters from arguments
    num_samples = args.count
    num_devices = min(args.devices, 1000)  # Limit to reasonable number
    start_date = args.start_date
    end_date = args.end_date
    output_file = args.output
    environment = args.environment
    
    print(f"Generating {num_samples:,} SNMP samples from {start_date} to {end_date}...")
    print(f"Network environment: {environment}")
    print(f"Simulating {num_devices:,} devices")
    
    # Generate device information
    devices = setup_network_devices(environment, num_devices)
    print(f"Generated {len(devices)} device configurations")
    
    # Generate interface information
    interfaces = generate_interfaces(devices, environment)
    print(f"Generated {len(interfaces)} interfaces")
    
    # Generate SNMP samples
    snmp_df = generate_snmp_data(devices, interfaces, num_samples, start_date, end_date)
    
    # Save to parquet
    snmp_df.to_parquet(output_file, index=False)
    
    print(f"Generated {len(snmp_df):,} SNMP samples and saved to {output_file}")
    print(f"Data range: {start_date} to {end_date}")
    print(f"Unique devices: {snmp_df['device_name'].nunique()}")
    print(f"Vendors: {', '.join(snmp_df['vendor'].unique())}")
    print(f"Fields included: {len(snmp_df.columns)}")
    
if __name__ == "__main__":
    main() 