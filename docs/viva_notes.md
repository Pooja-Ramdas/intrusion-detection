# Viva preparation notes (facts and current limitations)

1. **What problem is addressed?** Distributed intrusion detection and traffic profiling over CIC-IDS2017, with UNSW-NB15 reserved for external validation.
2. **Why HDFS?** It provides distributed block storage and replication for large flow/packet datasets.
3. **Why Spark?** Spark distributes Parquet processing and supports MLlib pipelines/classifiers.
4. **What is the storage root?** `D:\bda` (`BDA_ROOT=D:/bda`).
5. **What is the free-space floor?** 12 GB before heavy operations.
6. **What is the physical dataset cap?** 70 GB on D:.
7. **What is the raw replication policy?** Replication factor 1.
8. **What is the clean/features/models policy?** Replication factor 2.
9. **How much packet data was ingested?** Nine verified files totaling 22,158,691,332 bytes in the documented cutoff; packet File 14–17 were incomplete/not ingested.
10. **Were duplicate records synthesized?** No.
11. **Why keep UNSW separate?** It is intended for external validation and must not enter CIC model training.
12. **What EDA scope was used?** Deterministic 10,000-row prefixes as documented in Phase 2.
13. **Are EDA samples representative of all classes?** No; flow and UNSW samples were BENIGN-only, packet sample contained BENIGN and DDoS.
14. **Were full packet files profiled?** No; the 169 MB working slice was the bounded packet source.
15. **Did cleaning and split generation complete in Phase 2?** No; report says they were not run.
16. **Did Phase 3 produce verified features?** No; the feature job exceeded its time window.
17. **What metadata must not enter a model?** IPs, ports, timestamps, flow IDs, labels, and split assignments.
18. **What happened in Task 4?** A bounded seeded run trained binary RF, binary GBT, and multiclass RF after excluding `is_attack` and identifier/label/split leakage candidates.
19. **Are classifier metrics available?** Yes, small-sample held-out metrics are in `docs/model_comparison.md`; do not generalize them to full data.
20. **What is class imbalance?** A skew in label counts; Phase 2 samples were too limited for full-dataset conclusions.
21. **How does replication affect disk use?** Replicated blocks consume more physical storage than logical file size.
22. **What does the dashboard display?** Bounded EDA tables and operational status; model pages must state when verified metrics are absent.
23. **Why keep packet summaries nested but capped?** It limits document size while retaining compact flow-level context; cap is 20 packets per flow when implemented.
24. **Is the dashboard publicly deployed?** No public HTTPS URL is verified; local Docker access is distinct from public hosting.
25. **What is the main limitation?** Phase 2 cleaning/splits and Phase 3 versioned feature generation remain incomplete; MongoDB flow ingestion and UNSW external validation were not performed.
