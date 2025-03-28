
import pandas as pd
import random
import argparse
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq

def inject_fault_modules(module_ids, base_time):
    injected = []
    for module_id in module_ids:
        for offset in range(0, 30, 5):  # 每5分钟注入一次
            injected.append({
                "timestamp": (base_time + timedelta(minutes=offset)).strftime("%Y-%m-%d %H:%M:%S"),
                "module_id": module_id,
                "vendor": module_id.split("-")[0],
                "speed": module_id.split("-")[-1],
                "temperature": round(random.uniform(80.0, 85.0), 2),
                "voltage": round(random.uniform(3.0, 3.15), 2),
                "bias_current": round(random.uniform(0.0, 5.0), 2),
                "tx_power": round(random.uniform(-7.0, -5.0), 2),
                "rx_power": round(random.uniform(-10.0, -8.0), 2),
                "datacenter": module_id.split("-")[1],
                "device": module_id.split("-")[2],
                "interface": module_id.split("-")[3]
            })
    return injected

def generate_ddm(count=1000000, fault_ratio=0.01, output="ddm_fault.parquet"):
    vendors = ["Innolight", "Luxshare", "FS", "HG Genuine", "Finisar"]
    speeds = ["1G", "10G", "25G", "40G", "100G", "200G", "400G", "800G"]
    datacenters = ["DC-BJ-01", "DC-SH-02", "DC-GZ-03"]
    devices = [f"sw{i:03d}" for i in range(1, 101)]
    interfaces = [f"Ethernet{i}" for i in range(1, 49)]
    base_time = datetime(2025, 3, 27, 10, 0)

    ddm_list = []
    module_ids = []
    for _ in range(count):
        vendor = random.choice(vendors)
        speed = random.choice(speeds)
        datacenter = random.choice(datacenters)
        device = random.choice(devices)
        interface = random.choice(interfaces)
        module_id = f"{vendor}-{datacenter}-{device}-{interface}-{speed}"
        module_ids.append(module_id)
        ddm_list.append({
            "timestamp": (base_time + timedelta(minutes=random.randint(0, 10000))).strftime("%Y-%m-%d %H:%M:%S"),
            "module_id": module_id,
            "vendor": vendor,
            "speed": speed,
            "temperature": round(random.uniform(30, 70), 2),
            "voltage": round(random.uniform(3.2, 3.6), 2),
            "bias_current": round(random.uniform(10, 80), 2),
            "tx_power": round(random.uniform(-2.0, 2.0), 2),
            "rx_power": round(random.uniform(-4.0, 1.0), 2),
            "datacenter": datacenter,
            "device": device,
            "interface": interface
        })

    # 注入故障模块（1%）
    fault_sample = random.sample(module_ids, int(fault_ratio * count))
    ddm_list.extend(inject_fault_modules(fault_sample, base_time))

    df = pd.DataFrame(ddm_list)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, output)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=1000000)
    parser.add_argument("--fault_ratio", type=float, default=0.01)
    parser.add_argument("--out", type=str, default="ddm_fault.parquet")
    args = parser.parse_args()
    generate_ddm(args.count, args.fault_ratio, args.out)
