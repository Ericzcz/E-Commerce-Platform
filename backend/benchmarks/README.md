# Benchmark instructions

Run the backend with JDK 11 and reduced logging before benchmarking.

Cache benchmark:

```bash
python3 benchmarks/http_benchmark.py cache \
  --requests 3000 \
  --concurrency 50 \
  --output benchmarks/results/cache.json
```

The direct endpoint deliberately bypasses both cache levels to provide the
database baseline. The optimized endpoint uses dynamic hot-key detection,
Caffeine, Redis and MySQL fallback.

Seckill benchmark:

1. Create a dedicated active test voucher and initialize its Redis stock.
2. Ensure `tokens.txt` contains distinct logged-in users.
3. Run:

```bash
python3 benchmarks/http_benchmark.py seckill \
  --voucher-id 90001 \
  --requests 1000 \
  --concurrency 100 \
  --tokens tokens.txt \
  --output benchmarks/results/seckill.json
```

After the run, verify MySQL order count, duplicate `(user_id, voucher_id)`
count, Redis remaining stock, MySQL remaining stock, Stream lag and Pending
count. HTTP success count alone is not a correctness proof.
