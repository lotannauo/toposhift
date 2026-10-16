# Evaluation results

`itbench-lite-d0916b08.json` is the output of the blast-radius scorer (`bench/cmd/evalblast`, stage `score`) over the ITBench-Lite dataset at revision `d0916b0`, kept unchanged. Its SHA-256 is `8acdafea5214ad7b63df5a4e3044f91dad98051ac3617e853533eace3cc2e441`.

## How it was made

One run, on the commit of `main` that held the scorer as merged (`c43a292`), with the loader `factload-2` and the policy versions blastradius 1, coresidency 1, extract 1 and interval 1:

```sh
cd bench
CGO_ENABLED=0 go run ./cmd/evalblast -source itbench \
  -data <itbench-lite d0916b08> -out <out> -stage score \
  -table-a  e506d84d31c1677a3239315dad1d90146a5708c7d08c30c7db9f839c5f24f902 \
  -table-b  0e7b9fdf7eaad3a9e5685aeebe087b16f6cc32f40fea7135fe43f15228eed0db \
  -services 8f8d1e05165e27aa2a20ce0804668fd55669882dc429bcd461b8ce93ed06d4d2 \
  -probe <probe.tsv> \
  -probe-sha256 cf1c50be920c00dcf5789f0b5380d75c07bc7dc753fe7fc38a88419232bfd767
```

The four hashes name the frozen inputs: the two classification tables, the set of services the cases are scored on, and the trace-coverage probe. The scorer refuses to run if any of them differs. The acceptance rule is fixed in the scorer: paired bootstrap, 10,000 resamples, seed 20261015, 95 % percentile intervals.

## What it contains

- `meta`, `versions` and `rule`: what ran and under which rule.
- `dataset_check`: the load check (35 cases, 3 uninstrumented seeds excluded, 32 counted, none failed to load).
- `strata`: per stratum (the service call-graph headline stratum and the others), the counts, precision, recall, F1 and lift with their intervals, the differences from the baselines, the ceiling, and the verdict.
- `cases`: each case's outcome and the trace coverage the probe recorded for it.

These are accuracy figures from a replay of recorded cases on one machine. They are not performance measurements.
