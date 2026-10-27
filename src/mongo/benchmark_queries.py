import csv, time, os
from pymongo import MongoClient

CASES={
 "label_attack":{"label":{"$regex":"ATTACK","$options":"i"}},
 "ip_lookup":{"$or":[{"src_ip":"192.0.2.1"},{"dst_ip":"192.0.2.1"}]},
 "time_range":{"timestamp":{"$gte":"2017-07-03","$lte":"2017-07-07"}},
 "payload_entropy":{"payload.entropy":{"$gte":0.8}},
 "label_time":{"label":"DDoS","timestamp":{"$gte":"2017-07-03"}},
}
def main():
 c=MongoClient(os.getenv("MONGO_URI","mongodb://mongodb:27017"),serverSelectionTimeoutMS=3000).bda.flows
 n=c.count_documents({}); rows=[]
 for name,q in CASES.items():
  t=time.perf_counter(); found=list(c.find(q).limit(100)); ms=(time.perf_counter()-t)*1000
  try: ex=c.find(q).limit(100).explain(); examined=ex.get("executionStats",{}).get("totalDocsExamined","")
  except Exception: examined=""
  rows.append({"query":name,"elapsed_ms":round(ms,3),"returned":len(found),"docs_examined":examined,"collection_documents":n,"status":"measured_empty_collection" if n==0 else "measured"})
 with open("/logs/mongo_query_benchmarks.csv","w",newline="",encoding="utf-8") as f:
  w=csv.DictWriter(f,fieldnames=rows[0].keys()); w.writeheader(); w.writerows(rows)
 print("documents",n,"rows",rows)
if __name__=="__main__": main()
