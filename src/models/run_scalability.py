"""
run_scalability.py - Runner executing scalability benchmarks across cores (1, 2, 4) and data fractions (0.5, 1.0).
"""

import csv
import os
import subprocess
import time
from pathlib import Path

TIMINGS_DIR = Path("/opt/spark/work-dir/artifacts/timings")

def main():
    print("======================================================================")
    print("PHASE 4: Spark MLlib Distributed Scalability Benchmarks")
    print("======================================================================")

    runs_tmp = Path("/tmp/scalability_runs.csv")
    if runs_tmp.exists():
        runs_tmp.unlink()

    configs = [
        (1, 0.5),
        (1, 1.0),
        (2, 0.5),
        (2, 1.0),
        (4, 0.5),
        (4, 1.0),
    ]

    results = []

    for cores, fraction in configs:
        print(f"\n--- Running Benchmark: Cores={cores}, Fraction={fraction*100:.0f}% ---")
        cmd = [
            "/opt/spark/bin/spark-submit",
            "--master", "spark://spark-master:7077",
            "--driver-memory", "1g",
            "--executor-memory", "1100m",
            "--conf", f"spark.cores.max={cores}",
            "/opt/spark/work-dir/src/models/benchmark_worker.py",
            "--cores", str(cores),
            "--fraction", str(fraction)
        ]
        t0 = time.time()
        proc = subprocess.run(cmd, capture_output=True, text=True)
        wall_time = round(time.time() - t0, 2)

        # Parse output line
        fit_time = wall_time
        rows = 0
        for line in proc.stdout.splitlines():
            if line.startswith("RESULT:"):
                # e.g. RESULT: cores=1, fraction=0.5, rows=1130638, fit_time=38.45s
                parts = line.split(",")
                rows = int(parts[2].split("=")[1])
                fit_time = float(parts[3].split("=")[1].replace("s", ""))

        if proc.returncode != 0:
            print(f"  Warning: Run failed with code {proc.returncode}")
            print(proc.stderr[-500:])

        results.append({
            "cores": cores,
            "data_fraction": fraction,
            "training_rows": rows,
            "fit_time_seconds": fit_time,
            "total_wallclock_seconds": wall_time
        })
        print(f"  Completed: Fit Time = {fit_time}s (Total Application Time = {wall_time}s)")

    # Compute Speedup relative to 1 core for each fraction
    for frac in (0.5, 1.0):
        t1 = next((r["fit_time_seconds"] for r in results if r["data_fraction"] == frac and r["cores"] == 1), 1.0)
        for r in results:
            if r["data_fraction"] == frac:
                r["speedup"] = round(t1 / max(r["fit_time_seconds"], 0.001), 2)

    TIMINGS_DIR.mkdir(parents=True, exist_ok=True)
    out_csv = TIMINGS_DIR / "scalability.csv"
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        fieldnames = ["cores", "data_fraction", "training_rows", "fit_time_seconds", "total_wallclock_seconds", "speedup"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    print(f"\nScalability benchmark results saved to {out_csv}")
    print("\nBenchmark Summary Table:")
    print(f"{'Cores':<6} | {'Data Frac':<10} | {'Rows':<10} | {'Fit Time (s)':<14} | {'Speedup':<8}")
    print("-" * 56)
    for r in results:
        print(f"{r['cores']:<6} | {r['data_fraction']*100:<9.0f}% | {r['training_rows']:<10,d} | {r['fit_time_seconds']:<14.2f} | {r.get('speedup', 1.0):<8.2f}")

if __name__ == "__main__":
    main()
