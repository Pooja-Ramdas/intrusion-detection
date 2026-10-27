"""Bounded Spark scalability runner; runs through the app container, never local[*]."""
import argparse, csv, os, subprocess, time

LEVELS = (0.10, 0.25, 0.50, 1.00)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--job", required=True, help="Spark feature job path inside app container")
    ap.add_argument("--output", default="D:/bda/logs/scaling/results.csv")
    ap.add_argument("--budget-seconds", type=int, default=180)
    ap.add_argument("--workers", default="1,2")
    a = ap.parse_args(); start = time.monotonic(); rows=[]
    for level in LEVELS:
      for workers in map(int, a.workers.split(",")):
       for repeat in range(1,4):
        if time.monotonic()-start >= a.budget_seconds: break
        # disk_guard must be run by the caller immediately before each config.
        cmd=["docker","exec","app","/opt/spark/bin/spark-submit","--master","spark://spark-master:7077","--conf",f"spark.cores.max={workers}",a.job,"--fraction",str(level),"--repeat",str(repeat)]
        t=time.monotonic(); p=subprocess.run(cmd, text=True, capture_output=True, timeout=max(10,a.budget_seconds-int(time.monotonic()-start)))
        elapsed=time.monotonic()-t
        rows.append({"fraction":level,"workers":workers,"repeat":repeat,"runtime_seconds":round(elapsed,3),"status":"completed" if p.returncode==0 else "failed","input_gb":"","rows":"","throughput_mb_s":"","throughput_rows_s":"","speedup":"","efficiency":"","shuffle_read_bytes":"","shuffle_write_bytes":"","details":(p.stdout+p.stderr)[-1200:]})
       if time.monotonic()-start >= a.budget_seconds: break
      if time.monotonic()-start >= a.budget_seconds: break
    os.makedirs(os.path.dirname(a.output),exist_ok=True)
    with open(a.output,"w",newline="",encoding="utf-8") as f:
      w=csv.DictWriter(f,fieldnames=list(rows[0]) if rows else ["fraction","workers","repeat","runtime_seconds","status"]); w.writeheader(); w.writerows(rows)
if __name__=="__main__": main()
