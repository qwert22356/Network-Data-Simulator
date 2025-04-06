import pandas as pd
import random
import argparse
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq
import json

# 常量定义
OPTICAL_VENDORS = ['Innolight', 'Luxshare', 'Finisar', 'HGTECH', 'Eoptolink', 'Accelink']
SPEEDS = ['1G', '10G', '25G', '100G', '200G', '400G', '800G']
DATACENTERS = ["DC1", "DC2", "DC3"]
PODS = ["Pod01", "Pod02", "Pod03", "Pod04"]
RACKS = ["Rack01", "Rack02", "Rack03", "Rack04", "Rack05"]
SWITCHES = [f"SW{i:02d}" for i in range(1, 21)]
INTERFACES = [f"Eth{i}/{j}" for i in range(1, 9) for j in range(1, 9)]

def generate_prediction(count=1000000, output="predict_data.parquet", start_date="2025-03-01", end_date="2025-04-01"):
    """生成光模块寿命预测数据，包含统一格式的module_id和标准时间戳"""
    rows = []
    
    # 解析开始和结束日期
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    time_window = (end_dt - start_dt).total_seconds()
    
    for i in range(count):
        # 创建统一格式的module_id
        vendor = random.choice(OPTICAL_VENDORS)
        speed = random.choice(SPEEDS)
        datacenter = random.choice(DATACENTERS)
        pod = random.choice(PODS)
        rack = random.choice(RACKS)
        device = random.choice(SWITCHES)
        interface = random.choice(INTERFACES)
        
        # 按照统一格式创建module_id：<光模块厂商>-<数据中心>-<机房/Pod>-<机柜>-<交换机hostname>-<interface>-<speed>
        module_id = f"{vendor}-{datacenter}-{pod}-{rack}-{device}-{interface}-{speed}"
        
        # 生成随机预测数据
        remaining_days = random.randint(30, 1000)
        failure_prob = round(random.uniform(0.001, 0.8), 4)
        
        # 生成标准格式的时间戳
        timestamp = start_dt + timedelta(seconds=random.randint(0, int(time_window)))
        timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")
        
        # 预测日期基于时间戳计算
        predicted_date = (timestamp + timedelta(days=remaining_days)).strftime("%Y-%m-%d")
        
        # 生成一个随机的AI模型名称
        model_name = random.choice(["ARIMA+IF", "LSTM", "GRU", "Prophet", "XGBoost", "RandomForest", "Ensemble"])
        
        # 随机生成命中的规则
        hit_rules = []
        rule_count = random.randint(0, 3)
        possible_rules = [
            "温度波动异常", "功率下降过快", "电压不稳定", "偏差超阈值", 
            "历史故障模式", "厂商批次问题", "接口错误增加", "链路抖动频繁"
        ]
        if rule_count > 0:
            hit_rules = random.sample(possible_rules, rule_count)
        
        rows.append({
            "timestamp": timestamp_str,
            "module_id": module_id,
            "optic_vendor": vendor,
            "device_vendor": random.choice(["Cisco", "Huawei", "Juniper", "Arista"]),
            "speed": speed,
            "datacenter": datacenter,
            "room": pod,
            "rack": rack,
            "device_hostname": device,
            "device_ip": f"10.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}",
            "interface": interface,
            "remaining_days": remaining_days,
            "failure_probability": failure_prob,
            "failure_time_estimate": predicted_date,
            "model_name": model_name,
            "hit_rules": json.dumps(hit_rules, ensure_ascii=False)
        })

    df = pd.DataFrame(rows)
    
    # Ensure all required columns are present according to the schema
    required_fields = [
        'timestamp', 'module_id', 'datacenter', 'room', 'rack', 'device_hostname', 
        'device_ip', 'device_vendor', 'interface', 'speed', 'optic_vendor', 
        'failure_probability', 'failure_time_estimate', 'remaining_days', 'model_name', 'hit_rules'
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
    
    print(f"Generated {count} prediction records saved to {output}")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Fields included: {len(df.columns)}")
    
    return output

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate optical module life prediction data')
    parser.add_argument("--count", type=int, default=1000000, help="Number of records to generate")
    parser.add_argument("--output", type=str, default="predict_data.parquet", help="Output file name")
    parser.add_argument("--start-date", type=str, default="2025-03-01", help="Start date in YYYY-MM-DD format")
    parser.add_argument("--end-date", type=str, default="2025-04-01", help="End date in YYYY-MM-DD format")
    args = parser.parse_args()
    
    generate_prediction(
        count=args.count,
        output=args.output,
        start_date=args.start_date,
        end_date=args.end_date
    )
