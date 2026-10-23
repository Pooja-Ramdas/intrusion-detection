"""
inspect_schemas.py - Inspect cleaned flow and packet schemas on HDFS
and measure flow-packet linkage.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import json
import time

def main():
    spark = (
        SparkSession.builder
        .appName("BDA-Phase3-InspectSchemas")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1100m")
        .config("spark.cores.max", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("======================================================================")
    print("PHASE 3: Inspecting Cleaned Schemas and Flow-Packet Linkage")
    print("======================================================================")

    print("\n[Step 1] Loading Cleaned Datasets from HDFS...")
    t0 = time.time()
    df_flows = spark.read.parquet("hdfs://namenode:9000/bda/clean/flows/")
    df_packets = spark.read.parquet("hdfs://namenode:9000/bda/clean/packets/")
    print(f"Loaded in {time.time() - t0:.2f}s")

    print("\n--- Flow Schema & Identifier Columns ---")
    flow_cols = df_flows.columns
    print(f"Flow Total Columns: {len(flow_cols)}")
    flow_id_cols = [c for c in flow_cols if any(k in c.lower() for k in ["ip", "port", "proto", "time", "flow_id", "label", "split", "day"])]
    print(f"Flow Identifier/Metadata Columns ({len(flow_id_cols)}):")
    for c in flow_id_cols:
        dtype = dict(df_flows.dtypes).get(c)
        print(f"  - {c} ({dtype})")

    print("\n--- Packet Schema & Identifier Columns ---")
    packet_cols = df_packets.columns
    print(f"Packet Total Columns: {len(packet_cols)}")
    packet_id_cols = [c for c in packet_cols if any(k in c.lower() for k in ["ip", "port", "proto", "time", "id", "label"])]
    print(f"Packet Identifier/Metadata Columns ({len(packet_id_cols)}):")
    for c in packet_id_cols:
        dtype = dict(df_packets.dtypes).get(c)
        print(f"  - {c} ({dtype})")

    # Check payload columns in packets
    payload_cols = [c for c in packet_cols if "payload" in c.lower()]
    print(f"Packet Payload Columns Count: {len(payload_cols)}")
    print(f"Sample Payload Columns: {payload_cols[:10]}")

    print("\n--- Row Counts & Distinct Keys ---")
    n_flows = df_flows.count()
    n_packets = df_packets.count()
    print(f"Total Flows: {n_flows:,}")
    print(f"Total Packets: {n_packets:,}")

    # Check flow_id existence
    has_flow_id_in_flows = "flow_id" in flow_cols
    has_flow_id_in_packets = "flow_id" in packet_cols
    print(f"'flow_id' in flows: {has_flow_id_in_flows}")
    print(f"'flow_id' in packets: {has_flow_id_in_packets}")

    # Inspect sample values
    print("\nSample Flow Identifiers:")
    df_flows.select(["flow_id", "source_ip", "source_port", "destination_ip", "destination_port", "protocol", "timestamp"]).show(5, truncate=False)

    print("\nSample Packet Identifiers:")
    df_packets.select(["packet_id", "flow_id", "source_ip", "source_port", "destination_ip", "destination_port", "protocol"]).show(5, truncate=False)

    # Linkage calculation
    print("\n--- Linkage Analysis ---")
    distinct_flow_ids_in_flows = df_flows.select("flow_id").distinct().count()
    distinct_flow_ids_in_packets = df_packets.select("flow_id").distinct().count()
    print(f"Distinct flow_id in Flows: {distinct_flow_ids_in_flows:,}")
    print(f"Distinct flow_id in Packets: {distinct_flow_ids_in_packets:,}")

    # Semi join: flows that have matching packets
    flows_with_packets = df_flows.join(df_packets.select("flow_id").distinct(), on="flow_id", how="left_semi").count()
    flows_without_packets = n_flows - flows_with_packets
    link_rate_flows = (flows_with_packets / n_flows) * 100

    # Packets with matching flows
    packets_with_flows = df_packets.join(df_flows.select("flow_id").distinct(), on="flow_id", how="left_semi").count()
    packets_without_flows = n_packets - packets_with_flows
    link_rate_packets = (packets_with_flows / n_packets) * 100

    print(f"Flows WITH linked packets: {flows_with_packets:,} ({link_rate_flows:.4f}%)")
    print(f"Flows WITHOUT linked packets: {flows_without_packets:,} ({100 - link_rate_flows:.4f}%)")
    print(f"Packets WITH linked flows: {packets_with_flows:,} ({link_rate_packets:.4f}%)")
    print(f"Packets WITHOUT linked flows: {packets_without_flows:,} ({100 - link_rate_packets:.4f}%)")

    # Packets per flow distribution for linked flows
    print("\nPackets per linked flow summary:")
    packets_per_flow = df_packets.groupBy("flow_id").count()
    packets_per_flow.describe("count").show()

    results = {
        "flow_columns_count": len(flow_cols),
        "packet_columns_count": len(packet_cols),
        "total_flows": n_flows,
        "total_packets": n_packets,
        "distinct_flow_ids_in_flows": distinct_flow_ids_in_flows,
        "distinct_flow_ids_in_packets": distinct_flow_ids_in_packets,
        "flows_with_linked_packets": flows_with_packets,
        "flows_without_linked_packets": flows_without_packets,
        "flow_link_rate_percent": round(link_rate_flows, 4),
        "packets_with_linked_flows": packets_with_flows,
        "packets_without_linked_flows": packets_without_flows,
        "packet_link_rate_percent": round(link_rate_packets, 4),
        "flow_id_columns": flow_id_cols,
        "packet_id_columns": packet_id_cols,
        "payload_columns_count": len(payload_cols)
    }

    with open("/tmp/schema_inspection_results.json", "w") as f:
        json.dump(results, f, indent=2)

    spark.stop()

if __name__ == "__main__":
    main()
