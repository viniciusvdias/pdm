from time import perf_counter
from typing import Callable, Any, Dict, List


class Workload:
    metrics: List[Dict[str, Any]] = []

    @staticmethod
    def run(title: str, execute_fn: Callable[..., Any], *args, **kwargs) -> Any:
        print(f"[WORKLOAD] {title}")
        start = perf_counter()

        result = execute_fn(*args, **kwargs)

        elapsed = perf_counter() - start
        Workload.metrics.append({
            "title": title,
            "execution_time_sec": round(elapsed, 3)
        })

        print(f"[METRIC] {title} executada em {elapsed:.3f} segundos")
        return result

    @staticmethod
    def report() -> List[Dict[str, Any]]:
        return Workload.metrics

    @staticmethod
    def print_report():
        print("\n[WORKLOAD REPORT]")
        for i, metric in enumerate(Workload.metrics, start=1):
            print(f"- [WORKLOAD-{i}] {metric['title']}: {metric['execution_time_sec']}s")
