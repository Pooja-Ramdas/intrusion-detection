# Model comparison status

# Bounded model comparison

Source: existing `/bda/features/flow_features_full`; sampling used 2% per class and seed 20261027, followed by per-class caps. Identifiers, labels, split fields, IP/port/time fields, `is_attack`, and packet-presence flag were excluded. Reduced sample only.

| Model | Train | Test | Accuracy | Weighted precision | Weighted recall | Weighted F1 |
|---|---:|---:|---:|---:|---:|---:|
| Binary Random Forest | 1,799 | 767 | 0.938722 | 0.943060 | 0.938722 | 0.934155 |
| Binary GBT | 1,799 | 767 | 0.930900 | 0.929550 | 0.930900 | 0.927996 |
| Multiclass Random Forest | 1,799 | 766 | 0.908616 | 0.903562 | 0.908616 | 0.901720 |

The output field called `macro_f1` is actually weighted F1 and is not presented as macro. Per-class metrics, AUC, and UNSW validation were not measured. See `docs/phase_4_report.md` for limitations.
