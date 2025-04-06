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
import re
import pyarrow as pa
import pyarrow.parquet as pq

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
    """Generate protocol event and message template for L3 protocols"""
    if protocol == 'BGP':
        events = [
            ('neighbor-down', "BGP neighbor {neighbor} (AS {as_number}) state changed to DOWN: {reason}"),
            ('neighbor-up', "BGP neighbor {neighbor} (AS {as_number}) state changed to ESTABLISHED"),
            ('max-prefix-exceeded', "BGP neighbor {neighbor} (AS {as_number}) maximum prefix limit exceeded"),
            ('route-flap', "BGP route {neighbor}/24 flapping detected"),
            ('md5-auth-failure', "BGP MD5 authentication failure from {neighbor}"),
            ('attribute-discard', "BGP UPDATE from {neighbor} (AS {as_number}) contained disallowed attribute"),
            ('graceful-restart', "BGP neighbor {neighbor} (AS {as_number}) graceful restart initiated"),
            ('admin-shutdown', "BGP neighbor {neighbor} (AS {as_number}) administratively shut down"),
            ('route-refresh', "BGP route refresh requested from {neighbor} (AS {as_number})"),
            ('peer-group-change', "BGP neighbor {neighbor} added to peer group")
        ]
        event, template = choice(events)
        return event, template
        
    elif protocol == 'OSPF':
        events = [
            ('neighbor-down', "OSPF neighbor {neighbor} on interface {interface} is DOWN"),
            ('neighbor-up', "OSPF neighbor {neighbor} on interface {interface} is FULL"),
            ('area-change', "OSPF interface {interface} moved to area {area}"),
            ('lsa-maxage', "OSPF LSA from {neighbor} has reached MaxAge"),
            ('spf-start', "OSPF SPF calculation started for area {area}"),
            ('spf-complete', "OSPF SPF calculation completed for area {area} in {reason} ms"),
            ('mtu-mismatch', "OSPF MTU mismatch detected on {interface} with neighbor {neighbor}"),
            ('authentication-failure', "OSPF authentication failure on {interface} from {neighbor}"),
            ('dd-mismatch', "OSPF DD sequence number mismatch with {neighbor} on {interface}"),
            ('config-change', "OSPF configuration changed for area {area}")
        ]
        event, template = choice(events)
        return event, template
        
    elif protocol == 'VXLAN':
        events = [
            ('vni-up', "VXLAN VNI {vni} state is UP"),
            ('vni-down', "VXLAN VNI {vni} state is DOWN: {reason}"),
            ('flooding-enabled', "VXLAN head-end replication enabled for VNI {vni}"),
            ('vtep-discovered', "VXLAN VTEP {neighbor} discovered for VNI {vni}"),
            ('vtep-removed', "VXLAN VTEP {neighbor} removed from VNI {vni}"),
            ('mac-move', "VXLAN MAC move detected for VNI {vni} from {neighbor} to {interface}"),
            ('bgp-rt-import', "VXLAN BGP RT import for VNI {vni} changed"),
            ('encap-error', "VXLAN encapsulation error for packet to {neighbor} on VNI {vni}"),
            ('decap-error', "VXLAN decapsulation error from {neighbor} on VNI {vni}"),
            ('mtu-exceeded', "VXLAN MTU exceeded on VNI {vni} from {neighbor}")
        ]
        event, template = choice(events)
        return event, template
        
    elif protocol == 'MPLS':
        events = [
            ('ldp-neighbor-up', "MPLS LDP neighbor {neighbor} is UP"),
            ('ldp-neighbor-down', "MPLS LDP neighbor {neighbor} is DOWN: {reason}"),
            ('label-allocation', "MPLS label {label} allocated for {neighbor}/24"),
            ('label-withdrawal', "MPLS label {label} withdrawn for {neighbor}/24"),
            ('tunnel-up', "MPLS TE tunnel to {neighbor} is UP"),
            ('tunnel-down', "MPLS TE tunnel to {neighbor} is DOWN: {reason}"),
            ('rsvp-error', "MPLS RSVP error received from {neighbor} for LSP {label}"),
            ('ldp-session-error', "MPLS LDP session error with {neighbor}"),
            ('ttl-expired', "MPLS TTL expired for packet from {neighbor} with label {label}"),
            ('php-enabled', "MPLS PHP enabled for prefix {neighbor}/24")
        ]
        event, template = choice(events)
        return event, template
        
    elif protocol == 'LLDP':
        events = [
            ('neighbor-add', "LLDP neighbor {neighbor} added on port {interface}"),
            ('neighbor-remove', "LLDP neighbor {neighbor} removed from port {interface}"),
            ('neighbor-change', "LLDP neighbor information change on port {interface}"),
            ('mismatch', "LLDP neighbor information mismatch detected on {interface}"),
            ('chassis-id-change', "LLDP neighbor chassis ID changed on port {interface}"),
            ('port-id-change', "LLDP neighbor port ID changed on port {interface}"),
            ('system-name-change', "LLDP neighbor system name changed on port {interface}"),
            ('capability-change', "LLDP neighbor capability changed on port {interface}"),
            ('max-neighbors', "LLDP maximum neighbors reached on port {interface}"),
            ('ttl-expired', "LLDP neighbor entry TTL expired on port {interface}")
        ]
        event, template = choice(events)
        return event, template
        
    elif protocol == 'STP':
        events = [
            ('port-state-change', "STP port {interface} state changed to {reason}"),
            ('root-change', "STP new root bridge elected: {neighbor}"),
            ('topology-change', "STP topology change detected on port {interface}"),
            ('bpdu-guard', "STP BPDU guard triggered on port {interface}"),
            ('root-guard', "STP root guard triggered on port {interface}"),
            ('loop-guard', "STP loop guard triggered on port {interface}"),
            ('inconsistent-port', "STP inconsistent port state on {interface}"),
            ('bridge-priority-change', "STP bridge priority changed to {reason}"),
            ('port-cost-change', "STP port {interface} cost changed to {reason}"),
            ('instance-change', "STP instance created for VLAN {reason}")
        ]
        event, template = choice(events)
        return event, template
        
    elif protocol == 'LACP':
        events = [
            ('port-added', "LACP port {interface} added to channel {reason}"),
            ('port-removed', "LACP port {interface} removed from channel {reason}"),
            ('port-up', "LACP port {interface} is UP in channel {reason}"),
            ('port-down', "LACP port {interface} is DOWN in channel {reason}"),
            ('system-id-change', "LACP system ID changed for channel {reason}"),
            ('mode-change', "LACP mode changed to {reason} for channel on port {interface}"),
            ('timeout-change', "LACP timeout changed to {reason} for port {interface}"),
            ('synchronization-error', "LACP synchronization error on port {interface}"),
            ('marker-response-timeout', "LACP marker response timeout on port {interface}"),
            ('priority-change', "LACP port priority changed on {interface}")
        ]
        event, template = choice(events)
        return event, template
        
    elif protocol == 'PIM':
        events = [
            ('neighbor-up', "PIM neighbor {neighbor} UP on interface {interface}"),
            ('neighbor-down', "PIM neighbor {neighbor} DOWN on interface {interface}"),
            ('rp-change', "PIM RP changed to {neighbor} for group {reason}"),
            ('join-prune', "PIM Join/Prune message received from {neighbor} on {interface}"),
            ('assert', "PIM Assert received on {interface} for group {reason}"),
            ('register-stop', "PIM Register-Stop received from {neighbor} for source {reason}"),
            ('bootstrap-elected', "PIM Bootstrap router elected: {neighbor}"),
            ('dr-elected', "PIM DR elected on {interface}: {neighbor}"),
            ('multicast-state', "PIM multicast state change for group {reason}"),
            ('invalid-message', "PIM invalid message received from {neighbor} on {interface}")
        ]
        event, template = choice(events)
        return event, template
        
    elif protocol == 'ISIS':
        events = [
            ('adjacency-up', "ISIS adjacency UP with {neighbor} on {interface}"),
            ('adjacency-down', "ISIS adjacency DOWN with {neighbor} on {interface}: {reason}"),
            ('lsp-update', "ISIS LSP updated for {neighbor}"),
            ('lsp-expired', "ISIS LSP expired for {neighbor}"),
            ('overload-bit', "ISIS overload bit set for {neighbor}"),
            ('area-change', "ISIS area changed on {interface} to {reason}"),
            ('authentication-failure', "ISIS authentication failure on {interface} from {neighbor}"),
            ('mtu-mismatch', "ISIS MTU mismatch detected with {neighbor} on {interface}"),
            ('spf-calculation', "ISIS SPF calculation triggered by change from {neighbor}"),
            ('level-change', "ISIS level changed to {reason} on {interface}")
        ]
        event, template = choice(events)
        return event, template
        
    elif protocol == 'VRRP':
        events = [
            ('master-change', "VRRP group {reason} on {interface} state changed to MASTER"),
            ('backup-change', "VRRP group {reason} on {interface} state changed to BACKUP"),
            ('priority-change', "VRRP group {reason} priority changed to {label}"),
            ('advertisement-failure', "VRRP advertisement failure on {interface} for group {reason}"),
            ('authentication-failure', "VRRP authentication failure on {interface} from {neighbor}"),
            ('ip-mismatch', "VRRP IP address mismatch on {interface} for group {reason}"),
            ('preempt', "VRRP preemption occurred on {interface} for group {reason}"),
            ('timer-change', "VRRP timer changed to {label}ms for group {reason}"),
            ('tracking-change', "VRRP tracking state changed for group {reason} on {interface}"),
            ('virtual-mac-change', "VRRP virtual MAC changed for group {reason}")
        ]
        event, template = choice(events)
        return event, template
        
    else:
        # Generic protocol events
        events = [
            ('status-change', "{protocol} status changed to {reason}"),
            ('configuration-change', "{protocol} configuration changed by admin"),
            ('peer-connection', "{protocol} connection established with {neighbor}"),
            ('peer-disconnection', "{protocol} connection lost with {neighbor}"),
            ('error', "{protocol} error detected: {reason}"),
            ('warning', "{protocol} warning: {reason}"),
            ('packet-drop', "{protocol} packet dropped from {neighbor}: {reason}"),
            ('timeout', "{protocol} timeout with {neighbor} on {interface}"),
            ('restart', "{protocol} process restarted"),
            ('resource-limit', "{protocol} resource limit reached: {reason}")
        ]
        event, template = choice(events)
        return event, template

def generate_optical_module_message(device, optic):
    """Generate a syslog message for an optical module event"""
    # Get interface from optic info
    interface = optic['port']
    vendor = optic['vendor']
    speed = optic['speed']
    serial = optic['serial']
    module_id = optic['module_id']
    
    # Generate an optical module event
    event = generate_optical_module_event()
    
    # Create message templates based on event
    templates = {
        "Rx power high": "Transceiver {interface} RX power high warning: {value:.2f}dBm ({module_id})",
        "Rx power low": "Transceiver {interface} RX power low warning: {value:.2f}dBm ({module_id})",
        "Tx power high": "Transceiver {interface} TX power high warning: {value:.2f}dBm ({module_id})",
        "Tx power low": "Transceiver {interface} TX power low warning: {value:.2f}dBm ({module_id})",
        "Temperature high": "Transceiver {interface} Temperature high warning: {value:.1f}°C ({module_id})",
        "Temperature low": "Transceiver {interface} Temperature low warning: {value:.1f}°C ({module_id})",
        "Voltage high": "Transceiver {interface} Voltage high warning: {value:.2f}V ({module_id})",
        "Voltage low": "Transceiver {interface} Voltage low warning: {value:.2f}V ({module_id})",
        "Bias current high": "Transceiver {interface} Bias current high warning: {value:.2f}mA ({module_id})",
        "Bias current low": "Transceiver {interface} Bias current low warning: {value:.2f}mA ({module_id})",
        "Module inserted": "Transceiver {interface} inserted: Vendor {vendor}, Type {speed} ({module_id})",
        "Module removed": "Transceiver {interface} removed",
        "Module not compatible": "Transceiver {interface} not compatible with port: {vendor} {speed} ({module_id})",
        "Module authentication failed": "Transceiver {interface} authentication failed: {vendor} S/N {serial} ({module_id})",
        "DDM threshold crossed": "Transceiver {interface} {parameter} threshold crossed: {value} ({module_id})"
    }
    
    # Get the template for this event, or use a generic one if not found
    template = templates.get(event, "Transceiver {interface} {event} ({module_id})")
    
    # Generate values based on the event
    if "power" in event.lower():
        value = uniform(-10.0, 3.0) if "high" in event.lower() else uniform(-25.0, -15.0)
    elif "temperature" in event.lower():
        value = uniform(70.0, 85.0) if "high" in event.lower() else uniform(0.0, 10.0)
    elif "voltage" in event.lower():
        value = uniform(3.4, 3.6) if "high" in event.lower() else uniform(2.8, 3.0)
    elif "bias" in event.lower():
        value = uniform(70.0, 85.0) if "high" in event.lower() else uniform(1.0, 5.0)
    elif "threshold" in event.lower():
        parameters = ["temperature", "voltage", "bias current", "rx power", "tx power"]
        parameter = choice(parameters)
        value = f"{uniform(0.1, 100.0):.2f}" 
    else:
        value = 0.0
        parameter = ""
    
    # Format the message with appropriate values
    message = template.format(
        interface=interface,
        event=event,
        module_id=module_id,
        vendor=vendor,
        speed=speed,
        serial=serial,
        value=value,
        parameter=parameter if "threshold" in event.lower() else ""
    )
    
    return message

def generate_message(device, device_info, optics_info, l3_info):
    """Generate a syslog message and return with parsed event information"""
    device_name, ip, vendor = device_info
    event_types = ['physical_port', 'optical_module', 'l3_protocol', 'system']
    weights = [0.25, 0.25, 0.30, 0.20]  # Higher weight for protocol events
    
    event_type = np.random.choice(event_types, p=weights)
    
    # Create parsed event dictionary to hold structured data
    parsed_event = {
        'event_type': event_type,
    }
    
    if event_type == 'physical_port':
        # Physical port events
        port_event = generate_physical_port_event()
        interface = choice(INTERFACES)
        message = f"Interface {interface}: {port_event}"
        parsed_event.update({
            'interface': interface,
            'event': port_event,
            'protocol': '',
            'neighbor_ip': '',
            'reason': f"{port_event} on {interface}"
        })
        return message, parsed_event
        
    elif event_type == 'optical_module':
        # Optical module events
        if device_name in optics_info and optics_info[device_name]:
            # Use real optic module information
            optic = choice(optics_info[device_name])
            interface = optic['port']
            vendor = optic['vendor']
            speed = optic['speed']
            module_id = optic['module_id']
            serial = optic['serial']
            datacenter = optic['datacenter']
            pod = optic['pod']
            rack = optic['rack']
            
            # Set all parsed event fields
            parsed_event.update({
                'interface': interface,
                'event': generate_optical_module_event(),
                'optic_vendor': vendor,
                'speed': speed,
                'datacenter': datacenter,
                'room': pod,
                'rack': rack,
                'serial': serial,
                'module_id': module_id
            })
            
            message = generate_optical_module_message(device_name, optic)
        else:
            # Generate synthetic optic information
            interface = choice(INTERFACES)
            vendor = choice(OPTICAL_VENDORS)
            speed = choice(SPEEDS)
            datacenter = choice(DATACENTERS)
            pod = choice(PODS)
            rack = choice(RACKS)
            serial = f"{''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=8))}"
            module_id = f"{vendor}-{datacenter}-{pod}-{rack}-{device_name}-{interface}-{speed}"
            
            # Set parsed event fields
            parsed_event.update({
                'interface': interface,
                'event': generate_optical_module_event(),
                'optic_vendor': vendor,
                'speed': speed,
                'datacenter': datacenter,
                'room': pod,
                'rack': rack,
                'serial': serial,
                'module_id': module_id
            })
            
            message = f"Transceiver {interface} {generate_optical_module_event()} ({module_id}, S/N: {serial})"
        
        return message, parsed_event
        
    elif event_type == 'l3_protocol':
        # L3 protocol events (BGP, OSPF, etc.)
        l3_config = l3_info.get(device_name, {})
        
        # Determine which protocols are enabled for this device
        available_protocols = []
        if l3_config.get('bgp', False):
            available_protocols.append('BGP')
        if l3_config.get('ospf', False):
            available_protocols.append('OSPF')
        if l3_config.get('vxlan', False):
            available_protocols.append('VXLAN')
        if l3_config.get('mpls', False):
            available_protocols.append('MPLS')
        
        # If no protocols enabled, use random from all
        if not available_protocols:
            available_protocols = PROTOCOLS
        
        # Select a protocol
        protocol = choice(available_protocols)
        
        # Protocol-specific information
        neighbor_ip = f"{randint(1,255)}.{randint(1,255)}.{randint(1,255)}.{randint(1,255)}"
        interface = choice(INTERFACES)
        event, message_template = generate_l3_protocol_event(protocol)
        
        # Populate template with values
        # Handle empty lists by providing default values
        vxlan_vni = l3_config.get('vxlan_vni', [])
        if not vxlan_vni:  # If empty list
            vxlan_vni = [randint(1000, 9000)]
            
        mpls_labels = l3_config.get('mpls_label', [])
        if not mpls_labels:  # If empty list
            mpls_labels = [randint(16, 1048575)]
            
        message = message_template.format(
            protocol=protocol,
            neighbor=neighbor_ip,
            interface=interface,
            as_number=l3_config.get('bgp_as', randint(1000, 65000)),
            area=l3_config.get('ospf_area', randint(0, 100)),
            vni=choice(vxlan_vni),
            label=choice(mpls_labels),
            reason=choice(["authentication failure", "link down", "timeout", "admin shutdown", "configuration change"])
        )
        
        # Set parsed event fields
        parsed_event.update({
            'protocol': protocol,
            'event': event,
            'interface': interface,
            'neighbor_ip': neighbor_ip,
            'reason': message.split(':')[-1].strip() if ':' in message else ''
        })
        
        return message, parsed_event
    
    else:
        # System events
        system_events = [
            f"System cold start",
            f"System warm start",
            f"Fan {randint(1,4)} failure",
            f"Power supply {randint(1,2)} failure",
            f"Temperature sensor {randint(1,4)} high threshold exceeded: {randint(70,95)}°C",
            f"CPU utilization threshold exceeded: {randint(80,99)}%",
            f"Memory utilization threshold exceeded: {randint(80,99)}%",
            f"Packet buffer congestion on {choice(INTERFACES)}",
            f"Authentication failure for user admin from {randint(1,255)}.{randint(1,255)}.{randint(1,255)}.{randint(1,255)}",
            f"Configuration changed by user admin",
            f"System time changed from {randint(1,12)}:{randint(0,59)}:{randint(0,59)} to {randint(1,12)}:{randint(0,59)}:{randint(0,59)}",
            f"SNMP authentication failure",
            f"Link flap detected on {choice(INTERFACES)}",
            f"NTP synchronization lost with {randint(1,255)}.{randint(1,255)}.{randint(1,255)}.{randint(1,255)}",
            f"MAC address table full, current entries: {randint(10000,50000)}",
            f"Route table full, current entries: {randint(10000,500000)}",
            f"Software upgrade initiated",
            f"Software upgrade completed successfully",
            f"Software upgrade failed",
            f"Backup configuration to {randint(1,255)}.{randint(1,255)}.{randint(1,255)}.{randint(1,255)} failed",
            f"Interface {choice(INTERFACES)} disabled due to broadcast storm",
            f"Interface {choice(INTERFACES)} disabled due to STP BPDU guard",
            f"DHCP snooping drop: {randint(1,255)}.{randint(1,255)}.{randint(1,255)}.{randint(1,255)} on {choice(INTERFACES)}",
            f"ACL dropped packet from {randint(1,255)}.{randint(1,255)}.{randint(1,255)}.{randint(1,255)} to {randint(1,255)}.{randint(1,255)}.{randint(1,255)}.{randint(1,255)}"
        ]
        
        message = choice(system_events)
        
        # Extract interface from message if present
        interface = None
        if "Interface " in message:
            interface_part = message.split("Interface ")[1].split(" ")[0]
            interface = interface_part.strip()
        
        # Set parsed event fields
        parsed_event.update({
            'event': 'system',
            'interface': interface if interface else '',
            'reason': message
        })
        
        return message, parsed_event

def generate_syslog_data(num_events, start_date, end_date):
    """Generate syslog events for a specified time period"""
    # Setup environment
    environment_type = 'datacenter'
    num_devices = 30  # Enough to create variety
    
    # Create devices, optics and L3 config
    devices = setup_network_devices(environment_type, num_devices)
    optics_info = generate_device_optics(devices, environment_type)
    l3_info = generate_device_l3_config(devices, environment_type)
    
    # Convert dates if needed
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, '%Y-%m-%d')
    
    # Calculate time range in seconds
    time_range = (end_date - start_date).total_seconds()
    
    # Generate random events
    syslog_events = []
    for i in range(num_events):
        # Pick a random device
        device, ip, vendor = random.choice(devices)
        
        # Generate a random timestamp in the range
        random_second = random.randint(0, int(time_range))
        timestamp = start_date + timedelta(seconds=random_second)
        
        # Select severity and facility
        severity = choice(SEVERITY_LEVELS)
        facility = choice(FACILITY_LEVELS)
        
        # Generate message
        raw_message, parsed_event = generate_message(device, (device, ip, vendor), optics_info, l3_info)
        
        # Format for vendor
        syslog_generator = get_syslog_generator(vendor)
        formatted_message = syslog_generator(
            timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            device,
            ip,
            severity,
            facility,
            raw_message
        )
        
        # Extract or generate module_id based on message content
        # For optical module messages, attempt to extract or create module_id
        module_id = ""
        if parsed_event.get('event_type') == 'optical_module':
            # If we have optics info for this device, try to match it
            if optics_info.get(device) and parsed_event.get('interface'):
                for optic in optics_info[device]:
                    if optic['port'] == parsed_event.get('interface'):
                        module_id = optic['module_id']
                        break
            
            # If no match was found, create a module_id
            if not module_id:
                module_id = create_new_module_id(device, raw_message, optics_info.get(device, []))
        
        # Create event record with consistent field names
        event = {
            'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'device_ip': ip,
            'device_hostname': device,  # Use hostname as device name
            'device_vendor': vendor,
            'facility': facility,
            'severity': severity,
            'message': formatted_message,
            'module_id': module_id,
            'datacenter': parsed_event.get('datacenter', choice(DATACENTERS)),
            'room': parsed_event.get('room', choice(PODS)),
            'rack': parsed_event.get('rack', choice(RACKS)),
            'interface': parsed_event.get('interface', ''),
            'speed': parsed_event.get('speed', ''),
            'parsed_event.protocol': parsed_event.get('protocol', ''),
            'parsed_event.event': parsed_event.get('event', ''),
            'parsed_event.interface': parsed_event.get('interface', ''),
            'parsed_event.neighbor_ip': parsed_event.get('neighbor_ip', ''),
            'parsed_event.reason': parsed_event.get('reason', '')
        }
        
        syslog_events.append(event)
    
    # Convert to DataFrame
    df = pd.DataFrame(syslog_events)
    
    # Ensure all required columns are present
    for col in ['datacenter', 'room', 'rack', 'device_hostname', 'device_ip', 
                'device_vendor', 'interface', 'speed', 'module_id']:
        if col not in df.columns:
            if col in ['datacenter', 'room', 'rack']:
                df[col] = [choice(DATACENTERS if col == 'datacenter' else 
                                 PODS if col == 'room' else RACKS) for _ in range(len(df))]
            else:
                df[col] = ''
    
    # Sort by timestamp
    df = df.sort_values(by='timestamp')
    
    return df

def create_new_module_id(device_name, message, optics_info):
    """Create a new module_id from message content when possible"""
    # Try to extract interface name from message
    interface_pattern = r'(Eth\d+/\d+|TenGig\d+/\d+|FortyGig\d+/\d+|HundredGig\d+/\d+|Ethernet\d+/\d+)'
    interface_match = re.search(interface_pattern, message)
    interface = interface_match.group(1) if interface_match else None
    
    if not interface:
        return ""
    
    # Try to find matching optic info for this interface
    matching_optic = None
    for optic in optics_info:
        if optic['port'] == interface:
            matching_optic = optic
            break
    
    # If we found matching optic info, use it to build module_id
    if matching_optic:
        return matching_optic['module_id']
    
    # Otherwise build a generic module_id
    vendor = choice(OPTICAL_VENDORS)
    datacenter = choice(DATACENTERS)
    pod = choice(PODS)
    rack = choice(RACKS)
    speed = choice(SPEEDS)
    
    # Create module_id in standard format
    module_id = f"{vendor}-{datacenter}-{pod}-{rack}-{device_name}-{interface}-{speed}"
    return module_id

def write_to_parquet(syslog_events, output_file='syslog_data.parquet'):
    """Write syslog events to Parquet format"""
    # Ensure we have a DataFrame
    if not isinstance(syslog_events, pd.DataFrame):
        df = pd.DataFrame(syslog_events)
    else:
        df = syslog_events
    
    # Make sure all required fields are present according to new data structure
    required_fields = [
        'timestamp', 'module_id', 'datacenter', 'room', 'rack', 'device_hostname', 
        'device_ip', 'device_vendor', 'interface', 'speed', 'facility', 'severity', 
        'message', 'parsed_event.protocol', 'parsed_event.event', 'parsed_event.interface',
        'parsed_event.neighbor_ip', 'parsed_event.reason'
    ]
    
    for field in required_fields:
        if field not in df.columns:
            df[field] = ''
    
    # Create PyArrow table and write to Parquet
    table = pa.Table.from_pandas(df)
    pq.write_table(table, output_file)
    
    print(f"Generated {len(df)} syslog events saved to {output_file}")
    print(f"Events contain {len([x for x in df['module_id'] if x])} records with valid module_id")
    print(f"Fields included: {len(df.columns)}")
    print(f"Unique devices: {df['device_hostname'].nunique()}")
    print(f"Unique protocols: {df['parsed_event.protocol'].nunique()}")
    return output_file

def main():
    parser = argparse.ArgumentParser(description='Generate syslog data for network devices')
    parser.add_argument('--count', type=int, default=10000, help='Number of syslog events to generate')
    parser.add_argument('--start', type=str, default='2025-02-01', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', type=str, default='2025-03-01', help='End date (YYYY-MM-DD)')
    parser.add_argument('--output', type=str, default='syslog_data.parquet', help='Output file name')
    args = parser.parse_args()
    
    num_events = args.count
    start_date = args.start
    end_date = args.end
    output_file = args.output
    
    print(f"Generating {num_events} Syslog events from {start_date} to {end_date}...")
    print(f"Network environment: datacenter")
    print(f"Simulating 1,000 devices")
    
    # Generate syslog data
    syslog_df = generate_syslog_data(num_events, start_date, end_date)
    
    # Write to Parquet
    write_to_parquet(syslog_df, output_file)
    
    # Print summary info
    print(f"Generated {len(syslog_df):,} Syslog events and saved to {output_file}")
    print(f"Data range: {start_date} to {end_date}")
    print(f"Unique devices: {syslog_df['device_hostname'].nunique()}")
    print(f"Vendors: {', '.join(syslog_df['device_vendor'].unique())}")
    
if __name__ == "__main__":
    main() 