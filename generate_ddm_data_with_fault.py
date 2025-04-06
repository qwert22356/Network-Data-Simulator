import pandas as pd
import random
import argparse
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq

# 常量定义
OPTICAL_VENDORS = ["Innolight", "Luxshare", "FS", "HG Genuine", "Finisar", "Accelink"]
SPEEDS = ["1G", "10G", "25G", "40G", "100G", "200G", "400G", "800G"]
DATACENTERS = ["DC1", "DC2", "DC3"]
PODS = ["Pod01", "Pod02", "Pod03", "Pod04"]
RACKS = ["Rack01", "Rack02", "Rack03", "Rack04", "Rack05"]
SWITCHES = [f"SW{i:02d}" for i in range(1, 21)]
INTERFACES = [f"Eth{i}/{j}" for i in range(1, 9) for j in range(1, 9)]

def inject_fault_modules(module_ids, base_time):
    injected = []
    for module_id in module_ids:
        parts = module_id.split("-")
        vendor = parts[0]
        datacenter = parts[1]
        pod = parts[2]
        rack = parts[3]
        device = parts[4]
        interface = parts[5]
        speed = parts[6]
        
        for offset in range(0, 30, 5):  # 每5分钟注入一次
            injected.append({
                "timestamp": (base_time + timedelta(minutes=offset)).strftime("%Y-%m-%d %H:%M:%S"),
                "module_id": module_id,
                "optic_vendor": vendor,
                "device_vendor": random.choice(["Cisco", "Huawei", "Juniper", "Arista"]),
                "speed": speed,
                "temperature": round(random.uniform(80.0, 85.0), 2),
                "voltage": round(random.uniform(3.0, 3.15), 2),
                "current": round(random.uniform(0.0, 5.0), 2),
                "tx_power": round(random.uniform(-7.0, -5.0), 2),
                "rx_power": round(random.uniform(-10.0, -8.0), 2),
                "datacenter": datacenter,
                "room": pod,
                "rack": rack,
                "device_hostname": device,
                "device_ip": f"10.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}",
                "interface": interface,
                "optic_type": f"{speed}-{random.choice(['SR', 'LR', 'PSM4', 'CWDM4', 'LR4', 'SR4', 'AOC', 'DAC'])}",
                "optic_serial": f"{''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=8))}"
            })
    return injected

def generate_ddm(count=1000000, fault_ratio=0.01, output="ddm_fault.parquet"):
    base_time = datetime(2025, 3, 27, 10, 0)

    ddm_list = []
    module_ids = []
    for _ in range(count):
        vendor = random.choice(OPTICAL_VENDORS)
        speed = random.choice(SPEEDS)
        datacenter = random.choice(DATACENTERS)
        pod = random.choice(PODS)
        rack = random.choice(RACKS)
        device = random.choice(SWITCHES)
        interface = random.choice(INTERFACES)
        
        # 按照统一格式创建module_id
        module_id = f"{vendor}-{datacenter}-{pod}-{rack}-{device}-{interface}-{speed}"
        module_ids.append(module_id)
        
        ddm_list.append({
            "timestamp": (base_time + timedelta(minutes=random.randint(0, 10000))).strftime("%Y-%m-%d %H:%M:%S"),
            "module_id": module_id,
            "optic_vendor": vendor,
            "device_vendor": random.choice(["Cisco", "Huawei", "Juniper", "Arista"]),
            "speed": speed,
            "temperature": round(random.uniform(30, 70), 2),
            "voltage": round(random.uniform(3.2, 3.6), 2),
            "current": round(random.uniform(10, 80), 2),
            "tx_power": round(random.uniform(-2.0, 2.0), 2),
            "rx_power": round(random.uniform(-4.0, 1.0), 2),
            "datacenter": datacenter,
            "room": pod,
            "rack": rack,
            "device_hostname": device,
            "device_ip": f"10.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}",
            "interface": interface,
            "optic_type": f"{speed}-{random.choice(['SR', 'LR', 'PSM4', 'CWDM4', 'LR4', 'SR4', 'AOC', 'DAC'])}",
            "optic_serial": f"{''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=8))}"
        })

    # 注入故障模块（默认1%）
    fault_sample = random.sample(module_ids, int(fault_ratio * count))
    if fault_sample:  # Make sure we have modules to inject faults into
        ddm_list.extend(inject_fault_modules(fault_sample, base_time))

    df = pd.DataFrame(ddm_list)
    
    # Ensure all required columns are present according to the schema
    required_fields = [
        'timestamp', 'module_id', 'datacenter', 'room', 'rack', 'device_hostname', 
        'device_ip', 'device_vendor', 'interface', 'speed', 'temperature', 'voltage', 
        'current', 'tx_power', 'rx_power', 'optic_vendor', 'optic_type', 'optic_serial'
    ]
    
    for field in required_fields:
        if field not in df.columns:
            if field in ['datacenter', 'room', 'rack']:
                df[field] = [random.choice(DATACENTERS if field == 'datacenter' else 
                                        PODS if field == 'room' else RACKS) for _ in range(len(df))]
            else:
                df[field] = ''
    
    table = pa.Table.from_pandas(df)
    pq.write_table(table, output)
    
    print(f"Generated {len(ddm_list)} DDM records saved to {output}")
    print(f"Including {len(fault_sample)} fault modules ({fault_ratio*100:.1f}%)")
    print(f"Fields included: {len(df.columns)}")
    
    return output

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate DDM data with fault injection')
    parser.add_argument("--count", type=int, default=1000000, help="Number of records to generate")
    parser.add_argument("--fault_ratio", type=float, default=0.01, help="Ratio of fault modules to inject")
    parser.add_argument("--out", type=str, default="ddm_data.parquet", help="Output file name")
    args = parser.parse_args()
    
    generate_ddm(args.count, args.fault_ratio, args.out)
