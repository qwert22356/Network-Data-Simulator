#!/usr/bin/env python3
import random
import pandas as pd
import numpy as np
import ipaddress
import argparse
import json
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq
from random import choice, randint, uniform, sample

# Constants
VENDORS = ['Cisco', 'Huawei', 'Juniper', 'Arista', 'Dell', 'Broadcom Sonic', 'Community Sonic']
OPTICAL_VENDORS = ['Innolight', 'Luxshare', 'Finisar', 'HGTECH', 'Eoptolink', 'Accelink']
SPEEDS = ['1G', '10G', '25G', '100G', '200G', '400G', '800G']

# Network protocol constants
IP_PROTOCOLS = ['IPv4', 'IPv6']
ROUTE_TYPES = ['static', 'connected', 'bgp', 'ospf', 'isis', 'rip', 'eigrp']
ROUTE_STATES = ['active', 'inactive']
VRF_NAMES = ['default', 'mgmt', 'customer1', 'customer2', 'core', 'services']
QOS_QUEUES = ['priority', 'control-plane', 'best-effort', 'scavenger', 'video', 'voice']
CONGESTION_ALGORITHMS = ['tail-drop', 'WRED', 'ECN', 'PFC']
MPLS_SERVICES = ['LDP', 'RSVP-TE', 'SR-MPLS', 'L3VPN', 'L2VPN', 'VPLS', 'EVPN']
VXLAN_TYPES = ['L2', 'L3', 'EVPN']

# Environment presets
ENVIRONMENTS = {
    'datacenter': {
        'device_prefix': ['spine', 'leaf', 'border', 'core'],
        'network': '10.0.0.0/8',
        'primary_vendors': ['Cisco', 'Arista', 'Juniper'],
        'port_density': (24, 64),
        'route_table_size': (10000, 100000),
        'mac_table_size': (10000, 100000),
        'tcam_size': (8000, 32000),
        'vxlan_enabled': True,
        'mpls_enabled': False,
        'qos_profiles': 8
    },
    'enterprise': {
        'device_prefix': ['core', 'dist', 'access', 'edge'],
        'network': '192.168.0.0/16',
        'primary_vendors': ['Cisco', 'Huawei', 'Juniper'],
        'port_density': (8, 48),
        'route_table_size': (1000, 20000),
        'mac_table_size': (5000, 50000),
        'tcam_size': (4000, 16000),
        'vxlan_enabled': False,
        'mpls_enabled': True,
        'qos_profiles': 6
    },
    'isp': {
        'device_prefix': ['edge', 'agg', 'core', 'pe', 'p'],
        'network': '100.64.0.0/10',
        'primary_vendors': ['Cisco', 'Juniper', 'Huawei'],
        'port_density': (4, 32),
        'route_table_size': (100000, 1000000),
        'mac_table_size': (1000, 10000),
        'tcam_size': (16000, 64000),
        'vxlan_enabled': False,
        'mpls_enabled': True,
        'qos_profiles': 12
    },
    'campus': {
        'device_prefix': ['bb', 'dist', 'access', 'wifi'],
        'network': '172.16.0.0/12',
        'primary_vendors': ['Cisco', 'Aruba', 'Huawei'],
        'port_density': (24, 48),
        'route_table_size': (1000, 10000),
        'mac_table_size': (5000, 20000),
        'tcam_size': (2000, 8000),
        'vxlan_enabled': True,
        'mpls_enabled': False,
        'qos_profiles': 4
    },
    'complete': {
        'device_prefix': ['spine', 'leaf', 'border', 'core', 'edge', 'pe', 'p', 'agg'],
        'network': '10.0.0.0/8',
        'primary_vendors': VENDORS,
        'port_density': (24, 64),
        'route_table_size': (50000, 200000),
        'mac_table_size': (10000, 100000),
        'tcam_size': (8000, 32000),
        'vxlan_enabled': True,
        'mpls_enabled': True,
        'qos_profiles': 12
    }
}

# gRPC/gNMI paths per vendor
VENDOR_PATHS = {
    'Cisco': {
        'route_table': 'Cisco-IOS-XR-ip-rib-ipv4-oper:rib/vrfs/vrf/afs/af/safs/saf/ip-rib-route-table-names/ip-rib-route-table-name/protocol/bgp/as/information',
        'ecmp': 'Cisco-IOS-XR-fib-common-oper:fib-statistics/nodes/node/drops',
        'fib': 'Cisco-IOS-XR-fib-common-oper:fib/nodes/node/protocols/protocol/fib-summaries/fib-summary',
        'mac_table': 'Cisco-IOS-XR-l2vpn-oper:l2vpn/database/bridge-domain-summary',
        'qos': 'Cisco-IOS-XR-qos-ma-oper:qos/interface-table/interface/input/statistics',
        'tcam': 'Cisco-IOS-XR-asr9k-asic-errors-oper:asic-errors/nodes/node/instances/instance/parity-error',
        'vxlan': 'Cisco-IOS-XR-evpn-oper:evpn/active/evi-detail',
        'mpls': 'Cisco-IOS-XR-mpls-ldp-oper:mpls-ldp/nodes/node/bindings-summary',
        'vni': 'Cisco-IOS-XR-evpn-oper:evpn/active/evi-detail',
        'congestion': 'Cisco-IOS-XR-qos-ma-oper:qos/interface-table/interface/input/statistics'
    },
    'Juniper': {
        'route_table': '/network-instances/network-instance/protocols/protocol/bgp/rib/afi-safis/afi-safi/ipv4-unicast/loc-rib/routes/route',
        'ecmp': '/network-instances/network-instance/fib-state/fib-statistics',
        'fib': '/network-instances/network-instance/fib-state',
        'mac_table': '/network-instances/network-instance/fdb/mac-table/entries',
        'qos': '/components/component/properties/property/state/value',
        'tcam': '/components/component/properties/property/state/value',
        'vxlan': '/network-instances/network-instance/protocols/protocol/bgp/rib/afi-safis/afi-safi/l2vpn-evpn/loc-rib/routes',
        'mpls': '/network-instances/network-instance/mpls/lsps',
        'vni': '/network-instances/network-instance/vlans/vlan/vni',
        'congestion': '/qos/interfaces/interface/output/queues/queue/state/drop-pkts'
    },
    'Arista': {
        'route_table': 'eos_native:/show/ip/route/summary',
        'ecmp': 'eos_native:/show/platform/fib/multipath/summary',
        'fib': 'eos_native:/show/platform/fib/summary',
        'mac_table': 'eos_native:/show/mac/address-table/count',
        'qos': 'eos_native:/show/qos/interfaces',
        'tcam': 'eos_native:/show/platform/tcam/utilization',
        'vxlan': 'eos_native:/show/vxlan/vni',
        'mpls': 'eos_native:/show/mpls/ldp/binding',
        'vni': 'eos_native:/show/vxlan/vni',
        'congestion': 'eos_native:/show/queuing/congestion-drops'
    },
    'Huawei': {
        'route_table': 'huawei-routing:routing-entries',
        'ecmp': 'huawei-ifm:ifm/interfaces/interface/statistics',
        'fib': 'huawei-fib:fib-entries',
        'mac_table': 'huawei-l2vpn:l2vpn/vsi/mac-table',
        'qos': 'huawei-qos:qos/interfaces/interface/statistics',
        'tcam': 'huawei-dev:device/tcam-resources',
        'vxlan': 'huawei-vxlan:vxlan/vni-information',
        'mpls': 'huawei-mpls:mpls/ldp/bindings',
        'vni': 'huawei-vxlan:vxlan/vni-information',
        'congestion': 'huawei-qos:qos/interfaces/interface/statistics/congestion-drops'
    },
    'Dell': {
        'route_table': 'dell-route:route-entries/summary',
        'ecmp': 'dell-fib:fib/statistics',
        'fib': 'dell-fib:fib/summary',
        'mac_table': 'dell-l2:mac-addresses/summary',
        'qos': 'dell-qos:qos/statistics',
        'tcam': 'dell-hw:hardware/tcam/utilization',
        'vxlan': 'dell-vxlan:vxlan/statistics',
        'mpls': 'dell-mpls:mpls/statistics',
        'vni': 'dell-vxlan:vxlan/vni',
        'congestion': 'dell-qos:qos/congestion/statistics'
    },
    'Broadcom Sonic': {
        'route_table': 'openconfig-network-instance:network-instances/network-instance/protocols/protocol/bgp/rib/ipv4-unicast',
        'ecmp': 'openconfig-network-instance:network-instances/network-instance/fib-summary',
        'fib': 'openconfig-network-instance:network-instances/network-instance/fib-summary',
        'mac_table': 'openconfig-network-instance:network-instances/network-instance/fdb/mac-table',
        'qos': 'openconfig-qos:qos/interfaces',
        'tcam': 'openconfig-platform:components/component/state',
        'vxlan': 'openconfig-vxlan:vxlan/vni',
        'mpls': 'openconfig-mpls:mpls',
        'vni': 'openconfig-vxlan:vxlan/vni',
        'congestion': 'openconfig-qos:qos/interfaces/interface/output-queues/output-queue/state/dropped-pkts'
    },
    'Community Sonic': {
        'route_table': 'openconfig-network-instance:network-instances/network-instance/protocols/protocol/bgp/rib/ipv4-unicast',
        'ecmp': 'openconfig-network-instance:network-instances/network-instance/fib-summary',
        'fib': 'openconfig-network-instance:network-instances/network-instance/fib-summary',
        'mac_table': 'openconfig-network-instance:network-instances/network-instance/fdb/mac-table',
        'qos': 'openconfig-qos:qos/interfaces',
        'tcam': 'openconfig-platform:components/component/state',
        'vxlan': 'openconfig-vxlan:vxlan/vni',
        'mpls': 'openconfig-mpls:mpls',
        'vni': 'openconfig-vxlan:vxlan/vni',
        'congestion': 'openconfig-qos:qos/interfaces/interface/output-queues/output-queue/state/dropped-pkts'
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
    
    # Generate system info - include gRPC specific information
    device_info = []
    for i, (name, ip, vendor) in enumerate(zip(device_names, host_ips, device_vendors)):
        uptime = randint(86400, 31536000)  # 1 day to 1 year in seconds
        
        # Generate hardware resources based on environment
        min_routes, max_routes = env_settings['route_table_size']
        min_mac, max_mac = env_settings['mac_table_size']
        min_tcam, max_tcam = env_settings['tcam_size']
        
        # Set hardware capacity
        route_table_capacity = randint(min_routes, max_routes)
        mac_table_capacity = randint(min_mac, max_mac)
        tcam_capacity = randint(min_tcam, max_tcam)
        
        # Current utilization (60-85% is normal range)
        route_table_size = int(route_table_capacity * uniform(0.60, 0.85))
        mac_table_size = int(mac_table_capacity * uniform(0.60, 0.85))
        tcam_utilization = uniform(0.60, 0.85)

        # FIB/hardware forwarding info
        fib_synced = random.random() > 0.05  # 95% chance of being synced
        fib_size = route_table_size
        
        # ECMP groups
        ecmp_groups = randint(100, min(10000, route_table_size // 10))
        ecmp_members = randint(2, 64)
        ecmp_load_balance = choice(["per-packet", "per-flow", "5-tuple", "src-ip", "dst-ip"])
        
        # Tunnel info - 始终确保有一定数量的隧道
        # 即使环境未启用，也设置一个最小值以便于测试/开发
        vxlan_tunnels = randint(10, 1000) if env_settings['vxlan_enabled'] else randint(1, 10)
        mpls_tunnels = randint(10, 1000) if env_settings['mpls_enabled'] else randint(1, 10)
        
        # VNI/EVPN info - 确保有基本的VNI信息
        vni_count = randint(10, 5000) if env_settings['vxlan_enabled'] else randint(1, 100)
        evpn_routes = randint(1000, 50000) if env_settings['vxlan_enabled'] else randint(10, 500)
        
        # Create device record
        device_info.append({
            'name': name,
            'ip': ip,
            'vendor': vendor,
            'grpc_port': 57400,
            'uptime': uptime,
            'route_table_size': route_table_size,
            'route_table_capacity': route_table_capacity,
            'mac_table_size': mac_table_size,
            'mac_table_capacity': mac_table_capacity,
            'tcam_utilization': tcam_utilization,
            'tcam_capacity': tcam_capacity,
            'fib_synced': fib_synced,
            'fib_size': fib_size,
            'ecmp_groups': ecmp_groups,
            'ecmp_members': ecmp_members,
            'ecmp_load_balance': ecmp_load_balance,
            'vxlan_tunnels': vxlan_tunnels,
            'mpls_tunnels': mpls_tunnels,
            'vni_count': vni_count,
            'evpn_routes': evpn_routes,
            'qos_profiles': env_settings['qos_profiles'],
            'grpc_paths': VENDOR_PATHS.get(vendor, VENDOR_PATHS['Cisco'])
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
            if_mtu = choice([1500, 9000, 9216])
            
            admin_status = choice(['up', 'down'])
            # If admin down, oper should be down
            if admin_status == 'down':
                oper_status = 'down'
            else:
                oper_status = choice(['up', 'down'])
            
            # Random speed based on environment
            if_speed = choice(SPEEDS)
            
            # QoS parameters
            qos_enabled = random.random() > 0.2  # 80% chance of QoS enabled
            qos_policy = f"policy-{randint(1,10)}" if qos_enabled else ""
            qos_drops = randint(0, 1000) if oper_status == 'up' else 0
            
            # Queue depths and statistics for active interfaces
            queues = []
            if oper_status == 'up' and qos_enabled:
                # Create queues based on profile count
                for q in range(device['qos_profiles']):
                    queue_name = choice(QOS_QUEUES) if q < len(QOS_QUEUES) else f"queue-{q}"
                    queue_depth = randint(1, 1000)  # Current queue depth
                    queue_max = randint(1000, 10000)  # Maximum queue depth
                    queue_drops = randint(1, 1000) if queue_depth > queue_max * 0.8 else randint(0, 10)
                    
                    queues.append({
                        'queue_id': q,
                        'queue_name': queue_name,
                        'current_depth': queue_depth,
                        'max_depth': queue_max,
                        'drops': queue_drops,
                        'congestion_algorithm': choice(CONGESTION_ALGORITHMS)
                    })
            
            # Congestion statistics
            congestion_drops = sum([q.get('drops', 0) for q in queues]) if queues else 0
            
            # Create interface record
            interface = {
                'device_ip': device['ip'],
                'device_name': device['name'],
                'vendor': device['vendor'],
                'if_name': if_name,
                'if_alias': if_alias,
                'if_mtu': if_mtu,
                'if_speed': if_speed,
                'if_admin_status': admin_status,
                'if_oper_status': oper_status,
                'qos_enabled': qos_enabled,
                'qos_policy': qos_policy,
                'qos_drops': qos_drops,
                'queues': queues,
                'congestion_drops': congestion_drops
            }
            
            all_interfaces.append(interface)
    
    return all_interfaces

def generate_vrf_data(devices, environment):
    """Generate VRF and routing information"""
    all_vrf_data = []
    
    for device in devices:
        # Determine number of VRFs based on device type/role
        if "pe" in device['name'] or "edge" in device['name']:
            num_vrfs = randint(5, 50)
        elif "leaf" in device['name']:
            num_vrfs = randint(3, 10)
        else:
            num_vrfs = randint(1, 5)
            
        # Create VRFs
        for v in range(num_vrfs):
            vrf_name = VRF_NAMES[v] if v < len(VRF_NAMES) else f"vrf-{randint(1, 1000)}"
            
            # Route table statistics
            if vrf_name == "default":
                route_count = device['route_table_size']
            else:
                route_count = randint(100, device['route_table_size'] // 10)
                
            # Distribution of routes by protocol
            if "pe" in device['name'] or "core" in device['name']:
                # Core/PE routers have more BGP routes
                bgp_routes = int(route_count * uniform(0.6, 0.8))
                ospf_routes = int(route_count * uniform(0.1, 0.2))
                static_routes = int(route_count * uniform(0.05, 0.1))
                connected_routes = int(route_count - bgp_routes - ospf_routes - static_routes)
            elif "leaf" in device['name']:
                # Leaf switches have more local routes
                bgp_routes = int(route_count * uniform(0.4, 0.6))
                ospf_routes = int(route_count * uniform(0.1, 0.2))
                static_routes = int(route_count * uniform(0.05, 0.1))
                connected_routes = int(route_count - bgp_routes - ospf_routes - static_routes)
            else:
                # Other devices
                bgp_routes = int(route_count * uniform(0.3, 0.5))
                ospf_routes = int(route_count * uniform(0.2, 0.3))
                static_routes = int(route_count * uniform(0.1, 0.2))
                connected_routes = int(route_count - bgp_routes - ospf_routes - static_routes)
            
            # ECMP statistics for this VRF
            if vrf_name == "default":
                ecmp_routes = int(bgp_routes * uniform(0.1, 0.3))
                ecmp_groups = device['ecmp_groups']
            else:
                ecmp_routes = int(bgp_routes * uniform(0.05, 0.2))
                ecmp_groups = randint(10, device['ecmp_groups'] // 5)
            
            # Create VRF record
            vrf_data = {
                'device_ip': device['ip'],
                'device_name': device['name'],
                'vendor': device['vendor'],
                'vrf_name': vrf_name,
                'route_count': route_count,
                'bgp_routes': bgp_routes,
                'ospf_routes': ospf_routes,
                'static_routes': static_routes,
                'connected_routes': connected_routes,
                'ecmp_routes': ecmp_routes,
                'ecmp_groups': ecmp_groups,
                'fib_synced': device['fib_synced']
            }
            
            all_vrf_data.append(vrf_data)
    
    return all_vrf_data

def generate_vxlan_data(devices, environment):
    """Generate VXLAN, VNI, and EVPN information for devices"""
    env_settings = ENVIRONMENTS.get(environment, ENVIRONMENTS['datacenter'])
    vxlan_data = []
    
    for device in devices:
        # 确保至少有一个隧道
        tunnels = max(1, device['vxlan_tunnels'])
        
        # Generate VNI entries for this device
        for _ in range(min(tunnels * 5, 1000)):  # 最多1000个VNI条目
            vni_id = randint(10000, 16777215)  # 标准VNI范围
            l2_vni = random.random() > 0.3  # 70%是L2 VNI
            l3_vni = not l2_vni
            
            # L2 VNI特定数据
            associated_vlan = randint(1, 4094) if l2_vni else None
            mac_count = randint(10, 2000) if l2_vni else None
            
            # L3 VNI特定数据
            associated_vrf = choice(VRF_NAMES) if l3_vni else None
            route_count = randint(100, 5000) if l3_vni else None
            
            # EVPN信息
            evpn_type = 2 if l2_vni else 5  # L2-L2VNI, L3-L3VNI
            evpn_routes = randint(10, 100) if l2_vni else randint(100, 1000)
            
            # VTEP信息 (VXLAN Tunnel Endpoint)
            vtep_count = randint(2, 100)
            local_vtep = f"10.{randint(1,254)}.{randint(1,254)}.{randint(1,254)}"
            
            # 创建VNI记录
            vxlan_data.append({
                'device_ip': device['ip'],
                'device_name': device['name'],
                'vendor': device['vendor'],
                'vni_id': vni_id,
                'is_l2': l2_vni,
                'is_l3': l3_vni,
                'vlan_id': associated_vlan,
                'vrf_name': associated_vrf,
                'evpn_type': evpn_type,
                'evpn_routes': evpn_routes,
                'mac_count': mac_count,
                'route_count': route_count,
                'vtep_count': vtep_count,
                'local_vtep': local_vtep,
                'protocol': choice(['BGP-EVPN', 'Multicast', 'Ingress-Replication']),
                'status': choice(['Up', 'Up', 'Up', 'Up', 'Down']),  # 80% uptime
                'reason': None if random.random() > 0.2 else choice(['Link flap', 'Protocol down', 'Configuration error'])
            })
            
    return vxlan_data

def generate_mpls_data(devices, environment):
    """Generate MPLS and label information for devices"""
    env_settings = ENVIRONMENTS.get(environment, ENVIRONMENTS['datacenter'])
    mpls_data = []
    
    for device in devices:
        # 确保至少有一个隧道
        tunnels = max(1, device['mpls_tunnels'])
        
        # 确定哪些MPLS服务正在运行
        enabled_services = sample(MPLS_SERVICES, min(len(MPLS_SERVICES), randint(1, len(MPLS_SERVICES))))
        
        # 为每个服务创建条目
        for service in enabled_services:
            # 服务标签范围
            label_min = 16 if service != 'RSVP-TE' else 1000
            label_max = 1048575
            
            # 估计该服务的标签数量
            if service in ['LDP', 'SR-MPLS']:
                # 这些服务使用的标签通常与路由表大小有关
                label_count = int(device['route_table_size'] * uniform(0.05, 0.2))
            elif service in ['L3VPN', 'EVPN']:
                # 这些服务使用的标签通常与VRF或VNI数量有关
                label_count = randint(100, 5000)
            else:
                # 其他服务
                label_count = randint(10, 3000)
            
            # 隧道信息
            tunnel_count = randint(1, tunnels)
            tunnels_up = int(tunnel_count * uniform(0.8, 1.0))  # 80-100% 在线
            
            # 创建MPLS记录
            mpls_data.append({
                'device_ip': device['ip'],
                'device_name': device['name'],
                'vendor': device['vendor'],
                'mpls_service': service,
                'label_count': label_count,
                'label_min': label_min,
                'label_max': label_max,
                'tunnels': tunnel_count,
                'tunnels_up': tunnels_up,
                'status': 'Enabled',
                'protocol': 'MPLS' if service in ['LDP', 'RSVP-TE'] else 'BGP' if service in ['L3VPN', 'EVPN'] else 'Mixed'
            })
    
    return mpls_data

def generate_tcam_data(devices, environment):
    """Generate TCAM utilization and resource data"""
    all_tcam_data = []
    
    for device in devices:
        # Different TCAM resource types
        tcam_resources = [
            'ACL', 'QoS', 'LPM', 'FIB', 'Host', 'Route', 'MPLS', 'PBR', 
            'NAT', 'Tunnel', 'VLAN', 'MAC', 'L3 Interface'
        ]
        
        # Generate data for each TCAM resource
        for resource in tcam_resources:
            # Calculate capacity and utilization
            capacity = randint(1000, device['tcam_capacity'])
            used = int(capacity * device['tcam_utilization'] * uniform(0.8, 1.2))
            used = min(used, capacity)  # Ensure used doesn't exceed capacity
            
            # Create TCAM record
            tcam_data = {
                'device_ip': device['ip'],
                'device_name': device['name'],
                'vendor': device['vendor'],
                'tcam_resource': resource,
                'capacity': capacity,
                'used': used,
                'utilization': used / capacity
            }
            
            all_tcam_data.append(tcam_data)
    
    return all_tcam_data

def format_grpc_message(device, data_type, data, timestamp):
    """Format data according to vendor's gRPC structure"""
    # Select vendor subscription path
    subscription_path = VENDOR_PATHS.get(device['vendor'], {}).get(data_type, "unknown")
    
    # Base message structure
    message = {
        "timestamp": timestamp,
        "device_ip": device['ip'],
        "device_name": device['name'],
        "vendor": device['vendor'],
        "data_type": data_type,
        "subscription_path": subscription_path,
        # Ensure raw_data is always a string (JSON format)
        "raw_data": None,
        
        # TCAM fields
        "tcam_resource": None,
        "tcam_capacity": None,
        "tcam_used": None,
        "tcam_utilization": None,
        
        # VRF/routing fields
        "vrf_name": None,
        "route_count": None,
        "bgp_routes": None,
        "ospf_routes": None,
        "ecmp_groups": None,
        "ecmp_routes": None,
        
        # Interface fields
        "interface": None,
        "qos_enabled": None,
        "congestion_drops": None,
        
        # FIB fields
        "fib_synced": None,
        "fib_size": None,
        
        # MPLS fields
        "mpls_service": None,
        "label_count": None,
        "tunnels": None,
        "tunnels_up": None,
        
        # QoS fields
        "max_queue_depth": None,
        "max_queue_drops": None
    }
    
    # Populate with specific data
    if data_type == 'tcam':
        message["tcam_resource"] = data.get('tcam_resource')
        message["tcam_capacity"] = data.get('tcam_capacity')
        message["tcam_used"] = data.get('tcam_used')
        message["tcam_utilization"] = data.get('tcam_utilization')
    
    elif data_type in ['route_table', 'ecmp', 'fib']:
        message["vrf_name"] = data.get('vrf_name')
        message["route_count"] = data.get('route_count')
        message["bgp_routes"] = data.get('bgp_routes')
        message["ospf_routes"] = data.get('ospf_routes')
        message["ecmp_groups"] = data.get('ecmp_groups')
        message["ecmp_routes"] = data.get('ecmp_routes')
    
    elif data_type in ['qos', 'congestion']:
        message["interface"] = data.get('interface')
        message["qos_enabled"] = data.get('qos_enabled')
        message["congestion_drops"] = data.get('congestion_drops')
        message["max_queue_depth"] = data.get('max_queue_depth')
        message["max_queue_drops"] = data.get('max_queue_drops')
    
    elif data_type == 'mpls':
        message["mpls_service"] = data.get('mpls_service')
        message["label_count"] = data.get('label_count')
        message["tunnels"] = data.get('tunnels')
        message["tunnels_up"] = data.get('tunnels_up')
    
    # Format raw data according to vendor
    if device['vendor'] == 'Cisco':
        message["raw_data"] = format_cisco_data(data_type, data)
    elif device['vendor'] == 'Juniper':
        message["raw_data"] = format_juniper_data(data_type, data)
    elif device['vendor'] == 'Arista':
        message["raw_data"] = format_arista_data(data_type, data)
    elif device['vendor'] == 'Huawei':
        message["raw_data"] = format_huawei_data(data_type, data)
    elif device['vendor'] == 'Dell':
        message["raw_data"] = format_dell_data(data_type, data)
    elif device['vendor'] in ['Broadcom Sonic', 'Community Sonic']:
        message["raw_data"] = format_openconfig_data(data_type, data)
    
    # 确保raw_data总是字符串类型
    if not isinstance(message["raw_data"], str):
        message["raw_data"] = json.dumps({"error": "Failed to format data"})
    
    return message

def format_cisco_data(data_type, data):
    """Format data according to Cisco IOS-XR schemas for gRPC"""
    result = {}
    
    if data_type == 'route_table':
        result["Cisco-IOS-XR-ip-rib-ipv4-oper:rib"] = {
            "vrfs": {
                "vrf": [{
                    "vrf-name": data.get('vrf_name', 'default'),
                    "routes-summary": {
                        "protocol-total-routes": [
                            {"protocol-name": "bgp", "routes": data.get('bgp_routes', 0)},
                            {"protocol-name": "ospf", "routes": data.get('ospf_routes', 0)}
                        ],
                        "total-routes": data.get('route_count', 0)
                    }
                }]
            }
        }
    
    elif data_type == 'ecmp':
        result["Cisco-IOS-XR-fib-common-oper:fib-statistics"] = {
            "nodes": {
                "node": [{
                    "drops": {
                        "drop-type": "ECMP",
                        "number-of-paths": data.get('ecmp_routes', 0),
                        "number-of-groups": data.get('ecmp_groups', 0)
                    }
                }]
            }
        }
    
    elif data_type == 'fib':
        result["Cisco-IOS-XR-fib-common-oper:fib"] = {
            "nodes": {
                "node": [{
                    "protocols": {
                        "protocol": [{
                            "protocol-name": "IPv4",
                            "fib-summary": {
                                "total-routes": data.get('fib_size', 0),
                                "hardware-synced": data.get('fib_synced', True)
                            }
                        }]
                    }
                }]
            }
        }
    
    elif data_type == 'mac_table':
        result["Cisco-IOS-XR-l2vpn-oper:l2vpn"] = {
            "database": {
                "bridge-domain-summary": {
                    "bridge-domains": data.get('mac_table_size', 0),
                    "mac-entries": data.get('mac_count', 0),
                    "mac-limit": data.get('mac_capacity', 100000),
                    "mac-limit-percentage": round(data.get('mac_count', 0) / max(1, data.get('mac_capacity', 100000)) * 100, 2)
                }
            }
        }
    
    elif data_type == 'qos':
        result["Cisco-IOS-XR-qos-ma-oper:qos"] = {
            "interface-table": {
                "interface": [{
                    "interface-name": data.get('interface', "GigabitEthernet0/0/0"),
                    "input": {
                        "statistics": {
                            "queue": [{
                                "queue-id": 0,
                                "queue-max-depth": data.get('max_queue_depth', 0),
                                "queue-max-drop": data.get('max_queue_drops', 0)
                            }]
                        }
                    }
                }]
            }
        }
    
    elif data_type == 'tcam':
        result["Cisco-IOS-XR-asr9k-asic-errors-oper:asic-errors"] = {
            "nodes": {
                "node": [{
                    "instances": {
                        "instance": [{
                            "parity-error": {
                                "tcam-info": {
                                    "resource": data.get('tcam_resource', "L3_ENTRY"),
                                    "used": data.get('tcam_used', 0),
                                    "available": data.get('tcam_capacity', 0) - data.get('tcam_used', 0),
                                    "utilization": round(data.get('tcam_utilization', 0) * 100, 2)
                                }
                            }
                        }]
                    }
                }]
            }
        }
    
    elif data_type == 'vxlan' or data_type == 'vni':
        result["Cisco-IOS-XR-evpn-oper:evpn"] = {
            "active": {
                "evi-detail": [{
                    "evi": data.get('vni_id', 10000),
                    "bridge-domain": f"bridge-domain{data.get('vlan_id', 100)}",
                    "type": "L2" if data.get('is_l2', True) else "L3",
                    "state": data.get('status', 'Up'),
                    "mac-count": data.get('mac_count', 0),
                    "route-count": data.get('route_count', 0)
                }]
            }
        }
    
    elif data_type == 'mpls':
        result["Cisco-IOS-XR-mpls-ldp-oper:mpls-ldp"] = {
            "nodes": {
                "node": [{
                    "bindings-summary": {
                        "label-entries": data.get('label_count', 0),
                        "local-label-min": data.get('label_min', 16),
                        "local-label-max": data.get('label_max', 1048575),
                        "ldp-tunnels": data.get('tunnels', 0),
                        "ldp-tunnels-up": data.get('tunnels_up', 0)
                    }
                }]
            }
        }
    
    elif data_type == 'congestion':
        result["Cisco-IOS-XR-qos-ma-oper:qos"] = {
            "interface-table": {
                "interface": [{
                    "interface-name": data.get('interface', "GigabitEthernet0/0/0"),
                    "output": {
                        "statistics": {
                            "queue": [{
                                "queue-id": 0,
                                "tail-drops": data.get('congestion_drops', 0)
                            }]
                        }
                    }
                }]
            }
        }
    
    return json.dumps(result)

def format_juniper_data(data_type, data):
    """Format data according to Juniper Junos schemas for gRPC"""
    result = {}
    
    if data_type == 'route_table':
        result["/network-instances/network-instance/protocols/protocol/bgp/rib/afi-safis/afi-safi/ipv4-unicast/loc-rib/routes"] = {
            "network-instance": data.get('vrf_name', 'default'),
            "route-count": data.get('route_count', 0),
            "bgp-origin": data.get('bgp_routes', 0),
            "ospf-origin": data.get('ospf_routes', 0)
        }
    
    elif data_type == 'ecmp':
        result["/network-instances/network-instance/fib-state/fib-statistics"] = {
            "network-instance": data.get('vrf_name', 'default'),
            "ecmp-routes": data.get('ecmp_routes', 0),
            "ecmp-groups": data.get('ecmp_groups', 0)
        }
    
    elif data_type == 'fib':
        result["/network-instances/network-instance/fib-state"] = {
            "network-instance": data.get('vrf_name', 'default'),
            "route-entries": data.get('fib_size', 0),
            "synchronized": "true" if data.get('fib_synced', True) else "false"
        }
    
    elif data_type == 'mac_table':
        result["/network-instances/network-instance/fdb/mac-table/entries"] = {
            "network-instance": "default",
            "total-entries": data.get('mac_count', 0),
            "max-entries": data.get('mac_capacity', 100000),
            "utilization": round(data.get('mac_count', 0) / max(1, data.get('mac_capacity', 100000)) * 100, 2)
        }
    
    elif data_type == 'qos':
        result["/components/component/properties/property/state/value"] = {
            "component": data.get('interface', "ge-0/0/0"),
            "property": "qos",
            "enabled": "true" if data.get('qos_enabled', True) else "false",
            "queue-depth": data.get('max_queue_depth', 0),
            "queue-drops": data.get('max_queue_drops', 0)
        }
    
    elif data_type == 'tcam':
        result["/components/component/properties/property/state/value"] = {
            "component": "FPC0",
            "property": "tcam",
            "resource": data.get('tcam_resource', "L3_ROUTES"),
            "used": data.get('tcam_used', 0),
            "available": data.get('tcam_capacity', 0) - data.get('tcam_used', 0),
            "utilization": round(data.get('tcam_utilization', 0) * 100, 2)
        }
    
    elif data_type == 'vxlan' or data_type == 'vni':
        result["/network-instances/network-instance/protocols/protocol/bgp/rib/afi-safis/afi-safi/l2vpn-evpn/loc-rib/routes"] = {
            "network-instance": data.get('vrf_name', 'default'),
            "vni": data.get('vni_id', 10000),
            "type": "2" if data.get('is_l2', True) else "5",
            "status": data.get('status', 'Up'),
            "mac-count": data.get('mac_count', 0) if data.get('is_l2', True) else 0,
            "ip-count": data.get('route_count', 0) if data.get('is_l3', False) else 0
        }
    
    elif data_type == 'mpls':
        result["/network-instances/network-instance/mpls/lsps"] = {
            "network-instance": "default",
            "service": data.get('mpls_service', 'LDP'),
            "label-count": data.get('label_count', 0),
            "label-range-min": data.get('label_min', 16),
            "label-range-max": data.get('label_max', 1048575),
            "tunnels": data.get('tunnels', 0),
            "tunnels-up": data.get('tunnels_up', 0)
        }
    
    elif data_type == 'congestion':
        result["/qos/interfaces/interface/output/queues/queue/state/drop-pkts"] = {
            "interface": data.get('interface', "ge-0/0/0"),
            "queue-id": 0,
            "dropped-packets": data.get('congestion_drops', 0)
        }
    
    return json.dumps(result)

def format_arista_data(data_type, data):
    """Format data according to Arista devices schemas for gRPC"""
    result = {}
    path_prefix = 'eos_native:/'
    
    if data_type == 'route_table':
        result[f"{path_prefix}show/ip/route/summary"] = {
            "routes": {
                "total": data['route_count'],
                "bgp": data['bgp_routes'],
                "ospf": data['ospf_routes'],
                "directly_connected": data['route_count'] - data['bgp_routes'] - data['ospf_routes'],
                "vrf": data['vrf_name']
            }
        }
    
    elif data_type == 'ecmp':
        result[f"{path_prefix}show/platform/fib/multipath/summary"] = {
            "statistics": {
                "routes_with_ecmp": data['ecmp_routes'],
                "ecmp_groups": data['ecmp_groups'],
                "members_per_group_avg": round(data['ecmp_routes'] / max(1, data['ecmp_groups']), 2)
            }
        }
    
    elif data_type == 'fib':
        sync_status = "Synchronized" if data.get('fib_synced', True) else "Not synchronized"
        result[f"{path_prefix}show/platform/fib/summary"] = {
            "fib": {
                "status": sync_status,
                "route_count": data.get('fib_size', 0),
                "hardware_entries": data.get('fib_size', 0)
            }
        }
    
    elif data_type == 'mac_table':
        result[f"{path_prefix}show/mac/address-table/count"] = {
            "mac_addresses": {
                "dynamic": int(data.get('mac_count', 0) * 0.9),
                "static": int(data.get('mac_count', 0) * 0.1),
                "total": data.get('mac_count', 0),
                "total_capacity": data.get('mac_capacity', 100000)
            }
        }
    
    elif data_type == 'qos':
        result[f"{path_prefix}show/qos/interfaces"] = {
            "interface": {
                "name": data.get('interface', 'Ethernet1/1'),
                "queues": {
                    "control_plane": {
                        "drops": random.randint(0, 10),
                        "depth": random.randint(0, 500)
                    },
                    "data": {
                        "drops": data.get('max_queue_drops', 0),
                        "depth": data.get('max_queue_depth', 0)
                    }
                }
            }
        }
    
    elif data_type == 'tcam':
        result[f"{path_prefix}show/platform/tcam/utilization"] = {
            "resources": [{
                "name": data.get('tcam_resource', 'L3_ROUTES'),
                "used": data.get('tcam_used', 0),
                "free": data.get('tcam_capacity', 0) - data.get('tcam_used', 0),
                "utilization_pct": round(data.get('tcam_utilization', 0) * 100, 2)
            }]
        }
    
    elif data_type == 'vxlan' or data_type == 'vni':
        result[f"{path_prefix}show/vxlan/vni"] = {
            "vnis": [{
                "vni": data.get('vni_id', 10000),
                "mode": "L2" if data.get('is_l2', True) else "L3",
                "vlan": data.get('vlan_id', None),
                "source_interface": "Loopback0",
                "flood_list": f"{data.get('vtep_count', 2)} VTEPs",
                "status": data.get('status', 'Up')
            }]
        }
    
    elif data_type == 'mpls':
        result[f"{path_prefix}show/mpls/summary"] = {
            "services": [{
                "service": data.get('mpls_service', 'LDP'),
                "labelCount": data.get('label_count', 0),
                "labelMin": data.get('label_min', 16),
                "labelMax": data.get('label_max', 1048575),
                "tunnels": data.get('tunnels', 0),
                "tunnelsUp": data.get('tunnels_up', 0)
            }]
        }
    
    elif data_type == 'congestion':
        result[f"{path_prefix}show/queuing/congestion-drops"] = {
            "interfaces": [{
                "interface": data.get('interface', 'Ethernet1/1'),
                "congestion_drops": {
                    "unicast": data.get('congestion_drops', 0),
                    "multicast": int(data.get('congestion_drops', 0) * 0.1)
                }
            }]
        }
    
    return json.dumps(result)

# Functions for other vendors (Huawei, Dell, OpenConfig) would be similar but with their specific paths and structures

def format_huawei_data(data_type, data):
    """Format data according to Huawei schemas for gRPC"""
    result = {}
    
    if data_type == 'route_table':
        result["huawei-routing:routing-entries"] = {
            "vrf": data.get('vrf_name', 'default'),
            "total": data.get('route_count', 0),
            "per-protocol": [
                {"protocol": "BGP", "count": data.get('bgp_routes', 0)},
                {"protocol": "OSPF", "count": data.get('ospf_routes', 0)}
            ]
        }
    
    elif data_type == 'ecmp':
        result["huawei-ifm:ifm/interfaces/interface/statistics"] = {
            "ecmp-enabled": "true",
            "ecmp-groups": data.get('ecmp_groups', 0),
            "ecmp-routes": data.get('ecmp_routes', 0)
        }
    
    elif data_type == 'fib':
        result["huawei-fib:fib-entries"] = {
            "vrf": data.get('vrf_name', 'default'),
            "entry-count": data.get('fib_size', 0),
            "synced": "yes" if data.get('fib_synced', True) else "no"
        }
    
    elif data_type == 'mac_table':
        result["huawei-l2vpn:l2vpn/vsi/mac-table"] = {
            "mac-entry-count": data.get('mac_count', 0),
            "mac-capacity": data.get('mac_capacity', 100000),
            "utilization": round(data.get('mac_count', 0) / max(1, data.get('mac_capacity', 100000)) * 100, 2)
        }
    
    elif data_type == 'qos':
        result["huawei-qos:qos/interfaces/interface/statistics"] = {
            "interface-name": data.get('interface', 'GigabitEthernet0/0/0'),
            "qos-enabled": "true" if data.get('qos_enabled', True) else "false",
            "queues": [{
                "queue-id": 0,
                "max-depth": data.get('max_queue_depth', 0),
                "dropped-packets": data.get('max_queue_drops', 0)
            }]
        }
    
    elif data_type == 'tcam':
        result["huawei-dev:device/tcam-resources"] = {
            "tcam-resource": data.get('tcam_resource', 'IPV4_ROUTE'),
            "used": data.get('tcam_used', 0),
            "total": data.get('tcam_capacity', 0),
            "utilization": round(data.get('tcam_utilization', 0) * 100, 2)
        }
    
    elif data_type == 'vxlan' or data_type == 'vni':
        result["huawei-vxlan:vxlan/vni-information"] = {
            "vni-id": data.get('vni_id', 10000),
            "vni-type": "l2-vni" if data.get('is_l2', True) else "l3-vni",
            "vlan-id": data.get('vlan_id', None) if data.get('is_l2', True) else None,
            "vrf-name": data.get('vrf_name', None) if data.get('is_l3', False) else None,
            "oper-state": data.get('status', 'Up'),
            "mac-count": data.get('mac_count', 0) if data.get('is_l2', True) else 0,
            "route-count": data.get('route_count', 0) if data.get('is_l3', False) else 0
        }
    
    elif data_type == 'mpls':
        result["huawei-mpls:mpls/ldp/bindings"] = {
            "service": data.get('mpls_service', 'LDP'),
            "label-count": data.get('label_count', 0),
            "label-range": {
                "min": data.get('label_min', 16),
                "max": data.get('label_max', 1048575)
            },
            "tunnels": data.get('tunnels', 0),
            "tunnels-up": data.get('tunnels_up', 0)
        }
    
    elif data_type == 'congestion':
        result["huawei-qos:qos/interfaces/interface/statistics/congestion-drops"] = {
            "interface-name": data.get('interface', 'GigabitEthernet0/0/0'),
            "dropped-packets": data.get('congestion_drops', 0)
        }
    
    return json.dumps(result)

def format_dell_data(data_type, data):
    """Format data according to Dell schemas for gRPC"""
    result = {}
    
    if data_type == 'route_table':
        result["dell-route:route-entries/summary"] = {
            "vrf": data.get('vrf_name', 'default'),
            "total": data.get('route_count', 0),
            "bgp": data.get('bgp_routes', 0),
            "ospf": data.get('ospf_routes', 0)
        }
    
    elif data_type == 'ecmp':
        result["dell-fib:fib/statistics"] = {
            "ecmp-routes": data.get('ecmp_routes', 0),
            "ecmp-groups": data.get('ecmp_groups', 0)
        }
    
    elif data_type == 'fib':
        result["dell-fib:fib/summary"] = {
            "vrf": data.get('vrf_name', 'default'),
            "total-entries": data.get('fib_size', 0),
            "synced": "yes" if data.get('fib_synced', True) else "no"
        }
    
    elif data_type == 'mac_table':
        result["dell-l2:mac-addresses/summary"] = {
            "dynamic": int(data.get('mac_count', 0) * 0.9),
            "static": int(data.get('mac_count', 0) * 0.1),
            "total": data.get('mac_count', 0),
            "capacity": data.get('mac_capacity', 100000)
        }
    
    elif data_type == 'qos':
        result["dell-qos:qos/statistics"] = {
            "interface": data.get('interface', 'ethernet1/1/1'),
            "queue-stats": [{
                "queue": 0,
                "max-depth": data.get('max_queue_depth', 0),
                "dropped": data.get('max_queue_drops', 0)
            }]
        }
    
    elif data_type == 'tcam':
        result["dell-hw:hardware/tcam/utilization"] = {
            "resource": data.get('tcam_resource', 'L3_ENTRY'),
            "used": data.get('tcam_used', 0),
            "total": data.get('tcam_capacity', 0),
            "utilization-percent": round(data.get('tcam_utilization', 0) * 100, 2)
        }
    
    elif data_type == 'vxlan' or data_type == 'vni':
        result["dell-vxlan:vxlan/vni"] = {
            "vni-id": data.get('vni_id', 10000),
            "type": "l2" if data.get('is_l2', True) else "l3",
            "oper-status": data.get('status', 'Up'),
            "vlan-id": data.get('vlan_id', None),
            "vrf": data.get('vrf_name', None),
            "mac-count": data.get('mac_count', 0),
            "route-count": data.get('route_count', 0)
        }
    
    elif data_type == 'mpls':
        result["dell-mpls:mpls/statistics"] = {
            "service": data.get('mpls_service', 'LDP'),
            "labels": data.get('label_count', 0),
            "min-label": data.get('label_min', 16),
            "max-label": data.get('label_max', 1048575),
            "tunnels": data.get('tunnels', 0),
            "active-tunnels": data.get('tunnels_up', 0)
        }
    
    elif data_type == 'congestion':
        result["dell-qos:qos/congestion/statistics"] = {
            "interface": data.get('interface', 'ethernet1/1/1'),
            "drops": data.get('congestion_drops', 0)
        }
    
    return json.dumps(result)

def format_openconfig_data(data_type, data):
    """Format data in OpenConfig gRPC style (used by SONiC)"""
    # Simplified implementation
    return {"openconfig-format": {"data-type": data_type, "data": data}}

def generate_grpc_data(devices, interfaces, vrf_data, vxlan_data, mpls_data, tcam_data, count, start_date, end_date):
    """Generate gRPC subscription data samples over time range"""
    # Convert date strings to datetime objects
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    
    # Calculate time interval window
    time_window = (end_dt - start_dt).total_seconds()
    
    # Data types for subscription
    data_types = ['route_table', 'ecmp', 'fib', 'mac_table', 'qos', 'tcam', 'vxlan', 'mpls', 'vni', 'congestion']
    
    # Weight data types (some are more common than others)
    data_type_weights = {
        'route_table': 10,
        'ecmp': 10,
        'fib': 10,
        'mac_table': 10,
        'qos': 10,
        'tcam': 10,
        'vxlan': 15,      # 增加VXLAN数据的权重
        'mpls': 15,       # 增加MPLS数据的权重
        'vni': 10,        # 增加VNI数据的权重
        'congestion': 10
    }
    
    # Normalize weights to probabilities
    total_weight = sum(data_type_weights.values())
    data_type_probs = {k: v/total_weight for k, v in data_type_weights.items()}
    
    samples = []
    
    # Generate requested number of samples
    for _ in range(count):
        # Select random timestamp within date range
        sample_timestamp = start_dt + timedelta(seconds=random.randint(0, int(time_window)))
        timestamp_str = sample_timestamp.strftime('%Y-%m-%d %H:%M:%S')
        
        # Select random device
        device = random.choice(devices)
        
        # Select data type with weighted probability
        data_type = random.choices(
            population=list(data_type_probs.keys()),
            weights=list(data_type_probs.values()),
            k=1
        )[0]
        
        # Retrieve data based on type
        data = None
        
        if data_type in ['route_table', 'ecmp', 'fib']:
            # Find VRF data for this device
            device_vrf_data = [d for d in vrf_data if d['device_ip'] == device['ip']]
            if device_vrf_data:
                data = random.choice(device_vrf_data)
            else:
                # 如果没有找到VRF数据，创建一个基本的数据对象
                data = {
                    'device_ip': device['ip'],
                    'device_name': device['name'],
                    'vendor': device['vendor'],
                    'vrf_name': 'default',
                    'route_count': device['route_table_size'],
                    'bgp_routes': int(device['route_table_size'] * 0.6),
                    'ospf_routes': int(device['route_table_size'] * 0.3),
                    'ecmp_groups': device['ecmp_groups'],
                    'ecmp_routes': int(device['route_table_size'] * 0.4)
                }
                
        elif data_type in ['qos', 'congestion']:
            # Find interface data for this device
            device_interfaces = [i for i in interfaces if i['device_ip'] == device['ip']]
            if device_interfaces:
                data = random.choice(device_interfaces)
            else:
                # 如果没有找到接口数据，创建一个基本的数据对象
                data = {
                    'device_ip': device['ip'],
                    'device_name': device['name'],
                    'vendor': device['vendor'],
                    'interface': f"Ethernet1/{randint(1,48)}",
                    'qos_enabled': True,
                    'congestion_drops': randint(0, 10000),
                    'max_queue_depth': randint(100, 10000),
                    'max_queue_drops': randint(0, 1000)
                }
                
        elif data_type == 'tcam':
            # Find TCAM data for this device
            device_tcam_data = [t for t in tcam_data if t['device_ip'] == device['ip']]
            if device_tcam_data:
                data = random.choice(device_tcam_data)
            else:
                # 如果没有找到TCAM数据，创建一个基本的数据对象
                data = {
                    'device_ip': device['ip'],
                    'device_name': device['name'],
                    'vendor': device['vendor'],
                    'tcam_resource': 'L3_Routes',
                    'tcam_capacity': device['tcam_capacity'],
                    'tcam_used': int(device['tcam_capacity'] * device['tcam_utilization']),
                    'tcam_utilization': device['tcam_utilization']
                }
                
        elif data_type in ['vxlan', 'vni']:
            # Find VXLAN data for this device
            device_vxlan_data = [v for v in vxlan_data if v['device_ip'] == device['ip']]
            if device_vxlan_data:
                data = random.choice(device_vxlan_data)
            else:
                # 如果没有找到VXLAN数据，创建一个基本的数据对象（确保每个数据类型都有值）
                data = {
                    'device_ip': device['ip'],
                    'device_name': device['name'],
                    'vendor': device['vendor'],
                    'vni_id': randint(10000, 16777215),
                    'is_l2': True,
                    'is_l3': False,
                    'vlan_id': randint(1, 4094),
                    'vrf_name': None,
                    'evpn_type': 2,
                    'evpn_routes': randint(10, 100),
                    'mac_count': randint(10, 2000),
                    'route_count': None,
                    'vtep_count': randint(2, 10),
                    'local_vtep': f"10.{randint(1,254)}.{randint(1,254)}.{randint(1,254)}",
                    'protocol': 'BGP-EVPN',
                    'status': 'Up',
                    'reason': None
                }
                
        elif data_type == 'mpls':
            # Find MPLS data for this device
            device_mpls_data = [m for m in mpls_data if m['device_ip'] == device['ip']]
            if device_mpls_data:
                data = random.choice(device_mpls_data)
            else:
                # 如果没有找到MPLS数据，创建一个基本的数据对象
                data = {
                    'device_ip': device['ip'],
                    'device_name': device['name'],
                    'vendor': device['vendor'],
                    'mpls_service': random.choice(MPLS_SERVICES),
                    'label_count': randint(100, 5000),
                    'label_min': 16,
                    'label_max': 1048575,
                    'tunnels': randint(1, 100),
                    'tunnels_up': randint(1, 100),
                    'status': 'Enabled',
                    'protocol': 'MPLS'
                }
                
        elif data_type == 'mac_table':
            # Use device's MAC table info
            data = {
                'device_ip': device['ip'],
                'device_name': device['name'],
                'vendor': device['vendor'],
                'mac_count': device['mac_table_size'],
                'mac_capacity': device['mac_table_capacity'],
                'mac_utilization': device['mac_table_size'] / device['mac_table_capacity']
            }
        
        # Format gRPC message with vendor-specific structure
        formatted_data = format_grpc_message(device, data_type, data, timestamp_str)
        samples.append(formatted_data)
    
    return samples

def main():
    """Main entrypoint for generating gRPC/gNMI subscription data"""
    parser = argparse.ArgumentParser(description='Generate synthetic gRPC/gNMI subscription data')
    parser.add_argument('--count', type=int, default=1000, help='Number of samples to generate')
    parser.add_argument('--devices', type=int, default=100, help='Number of devices to simulate')
    parser.add_argument('--start-date', type=str, default='2025-02-01', help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, default='2025-03-01', help='End date (YYYY-MM-DD)')
    parser.add_argument('--output', type=str, default='grpc_data.parquet', help='Output parquet filename')
    parser.add_argument('--environment', type=str, default='datacenter', 
                        choices=['datacenter', 'enterprise', 'isp', 'campus', 'complete'],
                        help='Network environment preset (datacenter, enterprise, isp, campus, complete - complete includes all features)')
    
    args = parser.parse_args()
    
    num_samples = args.count
    environment = args.environment
    start_date = args.start_date
    end_date = args.end_date
    output_file = args.output
    
    print(f"Generating {num_samples:,} gRPC/gNMI subscription samples from {start_date} to {end_date}...")
    print(f"Network environment: {environment}")
    print(f"Simulating {args.devices} devices")
    
    # Generate device configurations
    devices = setup_network_devices(environment, args.devices)
    print(f"Generated {len(devices)} device configurations")
    
    # Generate interfaces
    interfaces = generate_interfaces(devices, environment)
    print(f"Generated {len(interfaces)} interfaces")
    
    # Generate VRF and routing info
    vrf_data = generate_vrf_data(devices, environment)
    print(f"Generated {len(vrf_data)} VRF entries")
    
    # Generate VXLAN data
    vxlan_data = generate_vxlan_data(devices, environment)
    print(f"Generated {len(vxlan_data)} VXLAN/VNI entries")
    
    # Generate MPLS data
    mpls_data = generate_mpls_data(devices, environment)
    print(f"Generated {len(mpls_data)} MPLS entries")
    
    # Generate TCAM data
    tcam_data = generate_tcam_data(devices, environment)
    print(f"Generated {len(tcam_data)} TCAM resource entries")
    
    # Generate gRPC subscription samples
    samples = generate_grpc_data(devices, interfaces, vrf_data, vxlan_data, 
                               mpls_data, tcam_data, num_samples, start_date, end_date)
    
    # Convert to DataFrame and save
    if samples:
        df = pd.DataFrame(samples)
        df.to_parquet(output_file)
        
        # Print summary
        print(f"Generated {len(samples):,} gRPC/gNMI subscription samples and saved to {output_file}")
        print(f"Data range: {start_date} to {end_date}")
        print(f"Unique devices: {df['device_ip'].nunique()}")
        print(f"Vendors: {', '.join(df['vendor'].unique())}")
        print(f"Data types: {', '.join(df['data_type'].unique())}")
        print(f"Fields included: {len(df.columns)}")
    else:
        print("No samples were generated. Check your parameters and try again.")

if __name__ == "__main__":
    main() 