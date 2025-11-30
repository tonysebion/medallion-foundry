"""Performance benchmarking harness for Bronze/Silver operations.

Usage:
  python -m scripts.benchmark --scenario api_pagination --async
  python -m scripts.benchmark --scenario rate_limiting --chunk-size 10000
  python -m scripts.benchmark --compare

Scenarios:
  - api_pagination: Compare sync vs async pagination throughput
  - rate_limiting: Test rate limiter overhead and accuracy
"""

import argparse
import time
import statistics
from pathlib import Path
from typing import List, Dict, Any
import json


class BenchmarkResult:
    def __init__(self, name: str, iterations: int = 5):
        self.name = name
        self.iterations = iterations
        self.timings: List[float] = []
        self.metadata: Dict[str, Any] = {}

    def record(self, elapsed: float) -> None:
        self.timings.append(elapsed)

    def summary(self) -> Dict[str, Any]:
        if not self.timings:
            return {"name": self.name, "error": "No data"}

        return {
            "name": self.name,
            "iterations": len(self.timings),
            "mean_seconds": statistics.mean(self.timings),
            "median_seconds": statistics.median(self.timings),
            "stdev_seconds": statistics.stdev(self.timings)
            if len(self.timings) > 1
            else 0.0,
            "min_seconds": min(self.timings),
            "max_seconds": max(self.timings),
            "throughput_ops_per_sec": 1.0 / statistics.mean(self.timings)
            if statistics.mean(self.timings) > 0
            else 0,
            **self.metadata,
        }

    def print_summary(self) -> None:
        summary = self.summary()
        print("\n" + "=" * 60)
        print(f"Benchmark: {summary['name']}")
        print("=" * 60)
        print(f"Iterations:    {summary['iterations']}")
        print(f"Mean:          {summary['mean_seconds']:.4f}s")
        print(f"Median:        {summary['median_seconds']:.4f}s")
        print(f"Std Dev:       {summary['stdev_seconds']:.4f}s")
        print(f"Min:           {summary['min_seconds']:.4f}s")
        print(f"Max:           {summary['max_seconds']:.4f}s")
        print(f"Throughput:    {summary['throughput_ops_per_sec']:.2f} ops/sec")
        if self.metadata:
            print("\nMetadata:")
            for key, value in self.metadata.items():
                print(f"  {key}: {value}")
        print("=" * 60 + "\n")


def benchmark_api_pagination_sync(iterations: int = 5) -> BenchmarkResult:
    """Benchmark synchronous API pagination."""
    result = BenchmarkResult("API Pagination (Sync)", iterations)
    result.metadata["mode"] = "synchronous"

    # Mock pagination scenario: 10 pages, 100 records each
    for i in range(iterations):
        start = time.perf_counter()

        # Simulate sync requests with sleep
        for page in range(10):
            time.sleep(0.01)  # Simulate 10ms network latency

        elapsed = time.perf_counter() - start
        result.record(elapsed)

    return result


def benchmark_api_pagination_async(iterations: int = 5) -> BenchmarkResult:
    """Benchmark async API pagination with prefetch."""
    import asyncio

    result = BenchmarkResult("API Pagination (Async)", iterations)
    result.metadata["mode"] = "asynchronous"
    result.metadata["prefetch"] = "enabled"

    async def run_async_pagination():
        # Simulate async requests
        for page in range(10):
            await asyncio.sleep(0.01)  # Simulate 10ms network latency

    for i in range(iterations):
        start = time.perf_counter()
        asyncio.run(run_async_pagination())
        elapsed = time.perf_counter() - start
        result.record(elapsed)

    return result


def benchmark_rate_limiter(rps: float = 10.0, iterations: int = 20) -> BenchmarkResult:
    """Benchmark rate limiter accuracy and overhead."""
    from core.rate_limit import RateLimiter

    result = BenchmarkResult(f"Rate Limiter ({rps} RPS)", iterations)
    result.metadata["target_rps"] = rps

    limiter = RateLimiter(requests_per_second=rps)

    start = time.perf_counter()
    for i in range(iterations):
        limiter.acquire()
    elapsed = time.perf_counter() - start

    actual_rps = iterations / elapsed if elapsed > 0 else 0
    result.metadata["actual_rps"] = actual_rps
    result.metadata["total_elapsed"] = elapsed
    result.metadata["accuracy_percent"] = (actual_rps / rps * 100) if rps > 0 else 0
    result.record(elapsed / iterations)  # Per-request timing

    return result


def run_benchmarks(args: argparse.Namespace) -> List[BenchmarkResult]:
    """Run selected benchmarks."""
    results = []

    if args.scenario == "api_pagination" or args.scenario == "all":
        print("Running API pagination benchmarks...")
        results.append(benchmark_api_pagination_sync(iterations=args.iterations))
        results.append(benchmark_api_pagination_async(iterations=args.iterations))

    if args.scenario == "rate_limiting" or args.scenario == "all":
        print("Running rate limiter benchmarks...")
        results.append(benchmark_rate_limiter(rps=10.0, iterations=20))
        results.append(benchmark_rate_limiter(rps=50.0, iterations=50))

    return results


def main():
    parser = argparse.ArgumentParser(description="Performance benchmark harness")
    parser.add_argument(
        "--scenario",
        choices=["api_pagination", "rate_limiting", "all"],
        default="all",
        help="Benchmark scenario to run",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=5,
        help="Number of iterations per benchmark",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Save results to JSON file",
    )

    args = parser.parse_args()

    results = run_benchmarks(args)

    # Print all results
    for result in results:
        result.print_summary()

    # Save to file if requested
    if args.output:
        summaries = [r.summary() for r in results]
        args.output.write_text(json.dumps(summaries, indent=2))
        print(f"Results saved to {args.output}")


if __name__ == "__main__":
    main()
