# Phase 3 — Schema Inspection and Flow-Packet Linkage Notes

Generated: 2026-10-24

---

## 1. Cleaned Schema Identification

Inspection of `/bda/clean/flows/` (2,827,083 rows, 90 columns) and `/bda/clean/packets/` (177,693 rows, 1,509 columns) reveals the following identifier and networking metadata attributes:

### Flow-Level Identifiers & Network Metadata (`/bda/clean/flows/`)
- `flow_id` (`bigint`): Unique flow identifier (2,827,083 distinct values across all 2,827,083 rows; 1:1 primary key).
- `source_ip` (`string`): IPv4 source address (e.g. `192.168.10.25`).
- `source_port` (`bigint`): Ephemeral or source transport layer port (e.g. `51147`).
- `destination_ip` (`string`): IPv4 destination server address (e.g. `192.168.10.3`).
- `destination_port` (`bigint`): Target service port (e.g. `53`, `80`, `443`, `22`).
- `protocol` (`string`): Transport layer protocol (`tcp`, `udp`, `other`).
- `timestamp` (`string`): Wall-clock capture timestamp (e.g. `03/07/2017 01:00:01`).
- Supervised Targets & Split Metadata: `label_raw`, `label_multi`, `label_binary`, `is_attack`, `day_of_week`, `split_temporal`, `split_stratified`.

### Packet-Level Identifiers & Network Metadata (`/bda/clean/packets/`)
- `packet_id` (`bigint`): Unique packet index (177,693 distinct values).
- `flow_id` (`bigint`): Direct foreign-key reference linking packet to parent flow.
- `source_ip` (`string`): Packet source IPv4 address.
- `source_port` (`bigint`): Packet source port.
- `destination_ip` (`string`): Packet destination IPv4 address.
- `destination_port` (`bigint`): Packet destination port.
- `protocol` (`string`): Transport layer protocol name (`tcp`, `udp`).
- `attack_label` (`string`): Packet-level label from raw packet capture.
- `payload_length` (`bigint`): Layer 7 payload size in bytes.
- `payload_byte_1` to `payload_byte_1500` (`bigint`): First 1,500 payload byte values [0-255].
- Note: The packet dataset does NOT contain a raw packet timestamp or packet IAT column. Packet inter-arrival times cannot be computed from packets directly; flow-level IAT is already captured in the flow dataset (`flow_iat_mean`, `fwd_iat_mean`, `bwd_iat_mean`, etc.).

---

## 2. Linkage Methodology and Empirical Link Rates

Because both datasets share the identical `flow_id` (`bigint`) primary/foreign key convention from CICFlowMeter / CIC-IDS2017, packets link directly to flows via `flow_id`.

Empirical linkage verification executed on Spark standalone cluster:
- **Total Flow Records**: 2,827,083
- **Total Packet Records**: 177,693
- **Distinct `flow_id` in Flows**: 2,827,083 (100% unique per flow)
- **Distinct `flow_id` in Packets**: 34,548
- **Flows WITH Linked Packets**: 34,548 (**1.2220%** of total flows)
- **Flows WITHOUT Linked Packets**: 2,792,535 (**98.7780%** of total flows)
- **Packets WITH Linked Flows**: 177,693 (**100.0000%** of total packets)
- **Packets WITHOUT Linked Flows**: 0 (**0.0000%**)

### Packet Distribution Per Flow (for the 34,548 linked flows):
- Mean packets per flow: 5.14
- Standard deviation: 21.54
- Min packets: 1
- Max packets: 1,773

### Integration Strategy:
1. Aggregate packet statistics and payload byte features grouped by `flow_id`.
2. Perform a `left` outer join from the full flow dataset to the aggregated packet/payload features on `flow_id`.
3. Add a binary flag column: `has_packet_features` (1 for the 34,548 linked flows, 0 for the 2,792,535 non-linked flows).
4. Fill null values for packet/payload numeric aggregations with standard zero imputation (`0.0`).
