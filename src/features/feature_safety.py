FORBIDDEN = ("flow_id", "packet_id", "source_ip", "destination_ip", "src_ip", "dst_ip", "source_port", "destination_port", "timestamp", "label", "split", "record_key", "dataset")

def safe(names):
    bad = [n for n in names if any(x in n.lower() for x in FORBIDDEN)]
    if bad:
        raise AssertionError("forbidden feature columns: " + repr(bad))
    return True
