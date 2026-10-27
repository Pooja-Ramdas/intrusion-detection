# Feature dictionary (Task 3 draft)

No feature Parquet output has been verified yet. This mapping is a proposed shared subset for CIC/UNSW; units and source differences must be validated before Task 4.

| Shared feature | CIC source | UNSW-NB15 source | Type / definition |
|---|---|---|---|
| duration_seconds | Flow Duration ÷ 1,000,000 | `dur` | double; seconds |
| forward_packet_count | Total Fwd Packets | `spkts` | double; forward packets |
| backward_packet_count | Total Backward Packets | `dpkts` | double; reverse packets |
| forward_bytes | Total Length of Fwd Packets | `sbytes` | double; forward bytes |
| backward_bytes | Total Length of Bwd Packets | `dbytes` | double; reverse bytes |
| flow_bytes_per_second | Flow Bytes/s | derived total bytes / duration | double; definitions may differ |
| flow_packets_per_second | Flow Packets/s | derived total packets / duration | double; definitions may differ |

IP addresses, ports, timestamps, flow IDs, labels, and split assignments are metadata/targets and must never enter `features_scaled`.
