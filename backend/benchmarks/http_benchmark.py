#!/usr/bin/env python3
"""Reproducible HTTP benchmark for cache and seckill resume metrics."""

import argparse
import concurrent.futures
import json
import statistics
import time
import urllib.error
import urllib.request
from pathlib import Path


def percentile(values, percent):
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(round((percent / 100) * (len(ordered) - 1)))))
    return ordered[index]


def request(url, method="GET", token=None, timeout=10):
    headers = {}
    if token:
        headers["authorization"] = token
    req = urllib.request.Request(url, method=method, headers=headers)
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            body = response.read()
            status = response.status
    except urllib.error.HTTPError as error:
        body = error.read()
        status = error.code
    except Exception as error:
        return (time.perf_counter() - started) * 1000, 0, str(error), False
    elapsed_ms = (time.perf_counter() - started) * 1000
    success = 200 <= status < 300
    application_success = False
    try:
        application_success = bool(json.loads(body).get("success"))
    except Exception:
        pass
    return elapsed_ms, status, body.decode("utf-8", errors="replace"), success and application_success


def run_requests(url, total, concurrency, tokens=None, method="GET"):
    started = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = []
        for index in range(total):
            token = tokens[index] if tokens else None
            futures.append(executor.submit(request, url, method, token))
        results = [future.result() for future in futures]
    duration = time.perf_counter() - started
    latencies = [result[0] for result in results]
    transport_errors = sum(1 for result in results if result[1] == 0)
    application_successes = sum(1 for result in results if result[3])
    return {
        "requests": total,
        "concurrency": concurrency,
        "durationSeconds": round(duration, 3),
        "throughputQps": round(total / duration, 2),
        "averageMs": round(statistics.mean(latencies), 2),
        "p50Ms": round(percentile(latencies, 50), 2),
        "p95Ms": round(percentile(latencies, 95), 2),
        "p99Ms": round(percentile(latencies, 99), 2),
        "maxMs": round(max(latencies), 2),
        "transportErrors": transport_errors,
        "applicationSuccesses": application_successes,
    }


def cache_benchmark(args):
    base = args.base_url.rstrip("/")
    direct_url = f"{base}/shop/benchmark/direct/{args.shop_id}"
    redis_url = f"{base}/shop/benchmark/redis/{args.shop_id}"
    cached_url = f"{base}/shop/{args.shop_id}"
    reset_url = f"{base}/shop/cache/metrics/reset"
    metrics_url = f"{base}/shop/cache/metrics"

    request(reset_url, method="POST")
    for _ in range(50):
        request(cached_url)
    request(reset_url, method="POST")

    direct = run_requests(direct_url, args.requests, args.concurrency)
    redis_only = run_requests(redis_url, args.requests, args.concurrency)
    cached = run_requests(cached_url, args.requests, args.concurrency)
    _, _, metrics_body, _ = request(metrics_url)
    try:
        metrics = json.loads(metrics_body).get("data")
    except Exception:
        metrics = None
    return {
        "scenario": "shop-cache",
        "shopId": args.shop_id,
        "directDatabase": direct,
        "redisOnly": redis_only,
        "multiLevelCache": cached,
        "cacheMetrics": metrics,
        "qpsImprovement": round(
            (cached["throughputQps"] - direct["throughputQps"]) / direct["throughputQps"] * 100,
            2,
        ),
        "p99Reduction": round(
            (direct["p99Ms"] - cached["p99Ms"]) / direct["p99Ms"] * 100,
            2,
        ),
        "localVsRedisQpsImprovement": round(
            (cached["throughputQps"] - redis_only["throughputQps"])
            / redis_only["throughputQps"] * 100,
            2,
        ),
        "localVsRedisP99Reduction": round(
            (redis_only["p99Ms"] - cached["p99Ms"]) / redis_only["p99Ms"] * 100,
            2,
        ),
    }


def seckill_benchmark(args):
    tokens = [
        line.strip()
        for line in Path(args.tokens).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    total = min(args.requests, len(tokens))
    route = "benchmark/sync" if args.sync else "seckill"
    url = f"{args.base_url.rstrip('/')}/voucher-order/{route}/{args.voucher_id}"
    result = run_requests(
        url,
        total,
        args.concurrency,
        tokens=tokens[:total],
        method="POST",
    )
    result.update(
        {
            "scenario": "seckill",
            "mode": "sync-database" if args.sync else "async-redis-stream",
            "voucherId": args.voucher_id,
            "availableTokens": len(tokens),
        }
    )
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("scenario", choices=("cache", "seckill"))
    parser.add_argument("--base-url", default="http://127.0.0.1:8081")
    parser.add_argument("--requests", type=int, default=3000)
    parser.add_argument("--concurrency", type=int, default=50)
    parser.add_argument("--shop-id", type=int, default=1)
    parser.add_argument("--voucher-id", type=int, default=90001)
    parser.add_argument("--tokens", default="tokens.txt")
    parser.add_argument("--sync", action="store_true")
    parser.add_argument("--output")
    args = parser.parse_args()

    report = cache_benchmark(args) if args.scenario == "cache" else seckill_benchmark(args)
    rendered = json.dumps(report, ensure_ascii=False, indent=2)
    print(rendered)
    if args.output:
        Path(args.output).write_text(rendered + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
