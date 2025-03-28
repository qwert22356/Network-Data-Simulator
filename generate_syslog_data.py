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
                'port': f"Ethernet{randint(1,8)}/{randint(1,48)}",
                'vendor': choice(OPTICAL_VENDORS),
                'speed': choice(SPEEDS),
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

def generate_message(device, device_info, optics_info, l3_info):
    device_name, ip, vendor = device_info
    
    # Determine event type
    event_category = choice(["physical_port", "optical_module", "l3_protocol"])
    
    if event_category == "physical_port":
        port = f"Ethernet{randint(1,8)}/{randint(1,48)}"
        event = generate_physical_port_event()
        message = f"{device_name}: {port}: {event}"
        
        # Add more details for specific events
        if "CRC" in event:
            message += f", count: {randint(1, 1000)}"
        elif "FCS" in event:
            message += f", errors: {randint(1, 500)}"
        elif "drop" in event:
            message += f", drops: {randint(1, 10000)}, duration: {randint(1, 60)}s"
            
    elif event_category == "optical_module":
        if not optics_info:
            # Fallback to physical port if no optics defined
            port = f"Ethernet{randint(1,8)}/{randint(1,48)}"
            event = generate_physical_port_event()
            message = f"{device_name}: {port}: {event}"
        else:
            optic = choice(optics_info)
            port = optic['port']
            optic_vendor = optic['vendor']
            optic_speed = optic['speed']
            event = generate_optical_module_event()
            
            message = f"{device_name}: {port}: {optic_speed} transceiver ({optic_vendor}): {event}"
            
            # Add more details for specific events
            if "power" in event:
                if "high" in event:
                    message += f", value: {uniform(2.0, 5.0):.2f} dBm, threshold: {uniform(1.5, 3.0):.2f} dBm"
                else:
                    message += f", value: {uniform(-35.0, -20.0):.2f} dBm, threshold: {uniform(-18.0, -15.0):.2f} dBm"
            elif "Temperature" in event:
                if "high" in event:
                    message += f", value: {uniform(70.0, 85.0):.1f}°C, threshold: {uniform(65.0, 75.0):.1f}°C"
                else:
                    message += f", value: {uniform(-20.0, -5.0):.1f}°C, threshold: {uniform(-15.0, -5.0):.1f}°C"
            
    else:  # l3_protocol
        protocol = choice(PROTOCOLS)
        event = generate_l3_protocol_event(protocol)
        
        if protocol == "OSPF" and l3_info.get('ospf'):
            area = l3_info.get('ospf_area', 0)
            nbr_ip = f"10.{randint(1,254)}.{randint(1,254)}.{randint(1,254)}"
            message = f"{device_name}: {protocol}: {event}: area {area}, neighbor {nbr_ip}"
            
        elif protocol == "BGP" and l3_info.get('bgp'):
            as_num = l3_info.get('bgp_as', 65000)
            peer_ip = f"10.{randint(1,254)}.{randint(1,254)}.{randint(1,254)}"
            peer_as = randint(1000, 65000)
            message = f"{device_name}: {protocol}: {event}: peer {peer_ip} (AS {peer_as})"
            
        elif protocol == "VXLAN" and l3_info.get('vxlan'):
            if l3_info.get('vxlan_vni'):
                vni = choice(l3_info.get('vxlan_vni'))
                message = f"{device_name}: {protocol}: {event}: VNI {vni}"
            else:
                message = f"{device_name}: {protocol}: {event}"
                
        elif protocol == "MPLS" and l3_info.get('mpls'):
            if l3_info.get('mpls_label'):
                label = choice(l3_info.get('mpls_label'))
                message = f"{device_name}: {protocol}: {event}: label {label}"
            else:
                message = f"{device_name}: {protocol}: {event}"
                
        else:
            message = f"{device_name}: {protocol}: {event}"
            
    return message

def generate_syslog_data(num_events, start_date, end_date):
    # Convert dates to timestamps
    start_timestamp = datetime.strptime(start_date, "%Y-%m-%d").timestamp()
    end_timestamp = datetime.strptime(end_date, "%Y-%m-%d").timestamp()
    
    # Calculate time interval between events
    time_range = end_timestamp - start_timestamp
    interval = time_range / num_events
    
    syslog_data = []
    
    for i in range(num_events):
        # Generate timestamp for this event
        event_time = start_timestamp + interval * i
        event_datetime = datetime.fromtimestamp(event_time)
        
        # Format timestamp as "%Y-%m-%d %H:%M:%S" to match generate_ddm_data
        formatted_timestamp = event_datetime.strftime("%Y-%m-%d %H:%M:%S")
        # For syslog format we'll still use the traditional format
        syslog_timestamp = event_datetime.strftime("%b %d %H:%M:%S")
        
        # Select random device
        device_info = choice(devices)
        device_name, ip, vendor = device_info
        
        # Get device specifics
        optics_info = device_optics.get(device_name, [])
        l3_info = device_l3.get(device_name, {})
        
        # Random severity and facility
        severity = choice(SEVERITY_LEVELS)
        facility = choice(FACILITY_LEVELS)
        
        # Generate message for event
        message = generate_message(device_name, device_info, optics_info, l3_info)
        
        # Get appropriate syslog format function based on vendor
        syslog_generator = get_syslog_generator(vendor)
        
        # Generate syslog line
        syslog_line = syslog_generator(syslog_timestamp, device_name, ip, severity, facility, message)
        
        # Create structured data
        data_row = {
            'timestamp': formatted_timestamp,  # Use formatted timestamp for consistency
            'device': device_name,
            'ip': ip,
            'vendor': vendor,
            'severity': severity,
            'facility': facility,
            'message': message,
            'raw_log': syslog_line
        }
        
        syslog_data.append(data_row)
        
    return pd.DataFrame(syslog_data)

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
    
    # Setup devices and configurations
    global devices, device_optics, device_l3
    devices = setup_network_devices(environment, num_devices)
    device_optics = generate_device_optics(devices, environment)
    device_l3 = generate_device_l3_config(devices, environment)
    
    # Generate data
    syslog_df = generate_syslog_data(num_events, start_date, end_date)
    
    # Save to parquet
    syslog_df.to_parquet(output_file, index=False)
    
    print(f"Generated {len(syslog_df):,} Syslog events and saved to {output_file}")
    print(f"Data range: {start_date} to {end_date}")
    print(f"Unique devices: {syslog_df['device'].nunique()}")
    print(f"Vendors: {', '.join(syslog_df['vendor'].unique())}")
    
if __name__ == "__main__":
    main() 