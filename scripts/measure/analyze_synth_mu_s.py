#!/usr/bin/env python3
"""
Analyze synthetic mu_s benchmark results
"""
import os
import glob
import statistics
from collections import defaultdict

RESULTS_DIR = "./results/tpch/synth_mu_s_full"

def parse_mu_s_logs(mu, method):
    """Parse mu_s estimates from stderr or mu_s.log files for a given mu and method"""
    estimates = []
    
    # Try mu_s.log first
    mu_s_log = f"{RESULTS_DIR}/mu_{mu}/{method}/mu_s.log"
    if os.path.exists(mu_s_log):
        with open(mu_s_log, 'r') as f:
            for line in f:
                line = line.strip()
                if '[mu_s' in line and 'mu_s=' in line:
                    # Parse pattern: [mu_s method_name] ... mu_s=value ...
                    parts = line.split()
                    for part in parts:
                        if part.startswith('mu_s='):
                            try:
                                value = float(part.split('=')[1])
                                estimates.append(value)
                            except ValueError:
                                pass
    
    # Fallback to stderr logs if mu_s.log is empty
    if not estimates:
        for run in range(1, 6):
            stderr_file = f"{RESULTS_DIR}/mu_{mu}/{method}/stderr_run{run}.log"
            if not os.path.exists(stderr_file):
                continue
            
            with open(stderr_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if '[mu_s' in line and 'mu_s=' in line:
                        # Parse pattern: [mu_s build_count] rows=2000000 unique=2000000 mu_s=1.000000
                        parts = line.split()
                        for part in parts:
                            if part.startswith('mu_s='):
                                try:
                                    value = float(part.split('=')[1])
                                    estimates.append(value)
                                except ValueError:
                                    pass
    
    return estimates

def parse_timings(mu, method):
    """Parse latency timings from times.txt"""
    times_file = f"{RESULTS_DIR}/mu_{mu}/{method}/times.txt"
    times = []
    
    if os.path.exists(times_file):
        with open(times_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        times.append(float(line))
                    except ValueError:
                        pass
    
    return times

def parse_ground_truth(mu):
    """Parse ground truth mu_s from one of the stderr logs"""
    # Ground truth is printed during DB generation
    ground_truth_file = f"{RESULTS_DIR}/mu_{mu}/.ground_truth"
    
    if os.path.exists(ground_truth_file):
        with open(ground_truth_file, 'r') as f:
            for line in f:
                try:
                    return float(line.strip())
                except ValueError:
                    pass
    
    # If no file, try to extract from any stderr log
    for method in ['none', 'build_count', 'ht_sample', 'probe_sample']:
        stderr_file = f"{RESULTS_DIR}/mu_{mu}/{method}/stderr_run1.log"
        if os.path.exists(stderr_file):
            with open(stderr_file, 'r') as f:
                first_line = f.readline()
                if 'Ground truth mu_s' in first_line:
                    try:
                        return float(first_line.split('=')[-1].strip())
                    except ValueError:
                        pass
    
    return None

def compute_stats(values):
    """Compute mean, std, and coeff of variation"""
    if not values:
        return None, None, None, None, None
    
    mean = statistics.mean(values)
    std = statistics.stdev(values) if len(values) > 1 else 0
    cv = (std / mean * 100) if mean != 0 else 0
    min_v = min(values)
    max_v = max(values)
    
    return mean, std, cv, min_v, max_v

def main():
    print("\n" + "="*120)
    print("SYNTHETIC MUTEX BENCHMARK ANALYSIS")
    print("="*120 + "\n")
    
    mus = [1, 2, 3, 4, 5, 10]
    methods = ['none', 'build_count', 'ht_sample', 'probe_sample']
    
    # Container for results
    all_results = {}
    baseline_times = {}  # Store 'none' method times for overhead calculation
    
    # Phase 1: Collect all data
    print("Collecting data...")
    for mu in mus:
        all_results[mu] = {}
        for method in methods:
            timings = parse_timings(mu, method)
            mu_s_estimates = parse_mu_s_logs(mu, method)
            
            all_results[mu][method] = {
                'timings': timings,
                'mu_s_estimates': mu_s_estimates
            }
            
            if method == 'none':
                baseline_times[mu] = timings
    
    # Phase 2: Print latency results
    print("\n" + "="*120)
    print("LATENCY RESULTS (milliseconds)")
    print("="*120)
    
    print("\n{:<8} {:<15} {:<15} {:<15} {:<15} {:<15}".format(
        "mu", "none", "build_count", "ht_sample", "probe_sample", "none_mean_ms"
    ))
    print("-"*90)
    
    for mu in mus:
        none_times = all_results[mu]['none']['timings']
        none_mean, none_std, none_cv, _, _ = compute_stats(none_times)
        
        row = f"{mu:<8}"
        
        for method in methods:
            times = all_results[mu][method]['timings']
            mean, std, cv, min_v, max_v = compute_stats(times)
            
            if mean is None:
                row += f" {'N/A':<15}"
            else:
                row += f" {mean*1000:>8.4f}±{std*1000:5.2f} "
        
        if none_mean:
            row += f" {none_mean*1000:>8.4f}"
        
        print(row)
    
    # Phase 3: Print overhead analysis
    print("\n" + "="*120)
    print("OVERHEAD vs BASELINE (none method) (%)")
    print("="*120)
    
    print("\n{:<8} {:<18} {:<18} {:<18}".format(
        "mu", "build_count", "ht_sample", "probe_sample"
    ))
    print("-"*65)
    
    for mu in mus:
        none_times = all_results[mu]['none']['timings']
        none_mean, _, _, _, _ = compute_stats(none_times)
        
        if none_mean is None:
            continue
        
        row = f"{mu:<8}"
        
        for method in ['build_count', 'ht_sample', 'probe_sample']:
            times = all_results[mu][method]['timings']
            mean, std, cv, _, _ = compute_stats(times)
            
            if mean is None:
                row += f" {'N/A':<18}"
            else:
                overhead = ((mean - none_mean) / none_mean) * 100
                row += f" {overhead:>6.2f}% (±{cv:>5.1f}%) "
        
        print(row)
    
    # Phase 4: Print variance analysis
    print("\n" + "="*120)
    print("VARIANCE ANALYSIS (Coefficient of Variation %)")
    print("="*120)
    
    print("\n{:<8} {:<15} {:<15} {:<15} {:<15}".format(
        "mu", "none", "build_count", "ht_sample", "probe_sample"
    ))
    print("-"*65)
    
    for mu in mus:
        row = f"{mu:<8}"
        
        for method in methods:
            times = all_results[mu][method]['timings']
            _, _, cv, _, _ = compute_stats(times)
            
            if cv is None:
                row += f" {'N/A':<15}"
            else:
                flag = " ⚠️ HIGH" if cv > 10 else ""
                row += f" {cv:>8.2f}%{flag:<6}"
        
        print(row)
    
    # Phase 5: Print mu_s estimate accuracy (if logs captured)
    print("\n" + "="*120)
    print("mu_s ESTIMATES vs GROUND TRUTH")
    print("="*120)
    
    for mu in mus:
        gt = parse_ground_truth(mu)
        if gt is None:
            print(f"\nmu={mu}: Ground truth not found")
            continue
        
        print(f"\nmu={mu}, Ground Truth={gt:.6f}")
        print("-"*80)
        
        for method in ['build_count', 'ht_sample', 'probe_sample']:
            estimates = all_results[mu][method]['mu_s_estimates']
            
            if not estimates:
                print(f"  {method:<15}: No logs captured")
                continue
            
            mean_est, std_est, cv_est, min_est, max_est = compute_stats(estimates)
            
            error_percent = ((mean_est - gt) / gt) * 100 if gt != 0 else 0
            
            print(f"  {method:<15}: {mean_est:>9.6f} ± {std_est:>8.6f}  " +
                  f"Error: {error_percent:>+7.2f}%  CV: {cv_est:>6.2f}%")
    
    # Phase 6: Summary and recommendations
    print("\n" + "="*120)
    print("SUMMARY & RECOMMENDATIONS")
    print("="*120)
    
    print("""
Based on the synthetic benchmark results:

1. OVERHEAD ASSESSMENT:
   - If all methods show overhead < 1%, performance overhead is negligible ("rounding error")
   - Focus decision on accuracy of mu_s estimates rather than runtime cost
   
2. ACCURACY ASSESSMENT:
   - build_count: Should be exact (mu_s = unique_rows / build_rows, counted during build)
   - ht_sample: Samples HT distribution, may have bias for highly skewed multiplicities
   - probe_sample: Samples only seen keys, may underestimate if probe pattern is biased
   - Compare % error vs ground truth across all mu values
   
3. VARIANCE:
   - CV < 5%: Excellent stability across runs
   - CV 5-10%: Good stability
   - CV > 10%: Concerning variance; may indicate noisy measurement
   
4. RECOMMENDATION:
   - Choose method with lowest % error on accuracy
   - Secondary: choose lowest overhead if choices have similar accuracy
   - If overhead negligible, all three could coexist; pick best for user clarity
""")

if __name__ == '__main__':
    main()
