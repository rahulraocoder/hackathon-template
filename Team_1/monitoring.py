import time
import psutil
import json
import threading
from functools import wraps
from datetime import datetime

class AdaptiveSampler:
    def __init__(self):
        self.cpu_samples = []
        self.mem_samples = []
        self.last_mem = 0
        self.last_sample_time = 0
        self.min_interval = 0.1  # 100ms minimum
    
    def should_sample(self, elapsed_time, current_mem):
        # Calculate memory change percentage
        mem_change = (abs(current_mem - self.last_mem) / self.last_mem) if self.last_mem else float('inf')
        
        # Base sampling interval based on elapsed time
        if elapsed_time < 10:  # First 10 seconds
            base_interval = 0.1
        elif elapsed_time < 60:  # 10s-1m
            base_interval = 1
        else:  # >1m
            base_interval = 10
            
        # Adjust for significant memory changes
        if mem_change > 0.1:  # >10% change
            base_interval = max(self.min_interval, base_interval/2)
            
        return base_interval

def monitor(output_file=None):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            sampler = AdaptiveSampler()
            process = psutil.Process()
            start_time = time.time()
            
            def sampling_loop():
                while not wrapper.stop_sampling:
                    elapsed = time.time() - start_time
                    current_mem = process.memory_info().rss / (1024 * 1024)
                    
                    interval = sampler.should_sample(elapsed, current_mem)
                    if time.time() - sampler.last_sample_time >= interval:
                        sampler.cpu_samples.append(process.cpu_percent(interval=0.05))
                        sampler.mem_samples.append(current_mem)
                        sampler.last_mem = current_mem
                        sampler.last_sample_time = time.time()
                    time.sleep(0.05)  # Minimum sleep time

            wrapper.stop_sampling = False
            sampler_thread = threading.Thread(target=sampling_loop)
            sampler_thread.daemon = True
            sampler_thread.start()

            try:
                result = func(*args, **kwargs)
                status = "success"
            except Exception as e:
                result = None
                status = f"failed: {str(e)}"
                raise
            finally:
                wrapper.stop_sampling = True
                sampler_thread.join()
                
                duration = time.time() - start_time
                metrics = {
                    "timestamp": datetime.now().isoformat(),
                    "function": func.__name__,
                    "duration_sec": round(duration, 4),
                    "cpu_avg": round(sum(sampler.cpu_samples)/len(sampler.cpu_samples), 2),
                    "cpu_max": round(max(sampler.cpu_samples), 2),
                    "memory_avg": round(sum(sampler.mem_samples)/len(sampler.mem_samples), 2),
                    "memory_max": round(max(sampler.mem_samples), 2),
                    "sample_count": len(sampler.cpu_samples),
                    "status": status
                }
                
                if output_file:
                    with open(output_file, 'a') as f:
                        json.dump(metrics, f)
                        f.write('\n')
                
                return result, metrics
        return wrapper
    return decorator
