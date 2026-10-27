"""Small indexed investigation queries for the MongoDB flow collection."""
from pymongo import ASCENDING, DESCENDING
from datetime import datetime, timezone

def create_indexes(collection):
    collection.create_index([("label", ASCENDING)])
    collection.create_index([("timestamp", DESCENDING)])
    collection.create_index([("src_ip", ASCENDING), ("dst_ip", ASCENDING)])
    collection.create_index([("label", ASCENDING), ("timestamp", DESCENDING)])
    collection.create_index([("payload.entropy", DESCENDING)])

def investigate(collection, label=None, ip=None, start=None, end=None, min_entropy=None, limit=100):
    q={}
    if label: q["label"]=label
    if ip: q["$or"]=[{"src_ip":ip},{"dst_ip":ip}]
    if start or end:
        q["timestamp"]={}
        if start: q["timestamp"]["$gte"]=start
        if end: q["timestamp"]["$lte"]=end
    if min_entropy is not None: q["payload.entropy"]={"$gte":float(min_entropy)}
    return list(collection.find(q, max_time_ms=3000).sort("timestamp",DESCENDING).limit(min(500,int(limit))))

def explain(collection, query=None):
    return collection.find(query or {}).limit(100).explain()

def to_flow_document(flow, packet_summaries=None, prediction=None):
    """Create a bounded nested record; keep no more than 20 packet summaries."""
    packets=list(packet_summaries or [])[:20]
    doc={"_id":str(flow.get("flow_id",flow.get("record_key",""))),
         "label":flow.get("label_multi",flow.get("label")),
         "dataset":flow.get("dataset","cicids2017"),
         "timestamp":flow.get("timestamp"),
         "src_ip":flow.get("source_ip",flow.get("src_ip")),
         "dst_ip":flow.get("destination_ip",flow.get("dst_ip")),
         "metadata":{"flow_id":str(flow.get("flow_id","")),"split":flow.get("split_stratified",flow.get("split"))},
         "features":{k:v for k,v in flow.items() if k not in {"flow_id","record_key","label","label_multi","label_binary","is_attack","split","split_stratified","timestamp","source_ip","destination_ip","src_ip","dst_ip"}},
         "packet_summaries":packets,
         "packet_summary_cap":20,
         "packet_summary_count":len(packets),
         "packet_aggregates":flow.get("packet_aggregates",{}),
         "prediction":prediction or {}}
    return doc

def bulk_upsert(collection, documents, batch_size=500):
    """Bulk upsert prepared documents; caller is responsible for stratified sampling."""
    from pymongo import ReplaceOne
    ops=[]; n=0
    for doc in documents:
        ops.append(ReplaceOne({"_id":doc["_id"]},doc,upsert=True))
        if len(ops)>=batch_size:
            n+=collection.bulk_write(ops,ordered=False).upserted_count; ops=[]
    if ops: n+=collection.bulk_write(ops,ordered=False).upserted_count
    return n
