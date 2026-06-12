# 009 Remote I/O Tail Experiments

Phase 009 tested whether simple remote-I/O tail optimizations should become
default behavior for the tiered cache. The answer was no: the measured knobs
were mechanically viable, but none satisfied the written keep lines across the
required backends.

The evidence logs are committed in
`benchmark-results/009-remote-io-tail-experiments/`.

## Outcome

| Experiment | Result | Reason |
| --- | --- | --- |
| Exp 1: wider worker/permit ceiling | Killed | More in-flight I/O did not clear queue tails and regressed some query latencies. |
| Exp 2: foreground range GET instead of waiting on optional group prefetch | Killed globally | S3 Express improved, but Fly/Tigris regressed required query rows and queue wait remained high. |
| Exp 3: duplicate/hedged foreground range GETs | Killed | Duplicate requests amplified load and worsened Fly/Tigris tails. |
| Exp 4: frame-granular SEARCH prefetch | Deferred | Depends on phase 008 frame-batch machinery. |
| Exp 5: SEARCH table-tree stopgap | Unscored | Hard-gated on Exp 4 being kept. |
| Exp 6: overlap decode with download | Deferred | Ordered after Exp 4's verdict. |
| Exp 7: auto-tuning | Killed by implication | Exps 1-3 produced no surviving hand-tuned optima to converge to. |

## Performance Read

The main useful signal came from Exp 2. On S3 Express, fetching the needed
foreground frame immediately often beat waiting behind a full-group prefetch.
That suggests frame-level demand reads can help when request overhead and RTT
are low enough.

On Fly/Tigris, the same policy was not safe as a global default. It reduced
some wait counters, but it also added request pressure and regressed required
end-to-end query rows. Exp 3 showed the same shape more sharply: hedging remote
reads made the tail worse when the duplicate work cost more than the saved wait.

The lesson is backend- and planner-shape-sensitive:

- Smaller foreground reads can be valuable on low-RTT, high-throughput storage.
- Wider concurrency alone is not proof that the queue is worker-limited.
- Hedging remote reads should not be enabled without a backend-specific policy
  and a hard byte/request budget.

## What Landed

The failed runtime knobs did not land. The phase kept only durable evidence and
safety coverage:

- benchmark logs for the measured runs;
- exact-length range response validation coverage, including short responses;
- deterministic coverage for the surviving 1s timeout takeover path where a
  foreground frame install wins while a stale optional group prefetch finishes
  late under replay-epoch churn.

Future performance work should revisit the Exp 2 idea only as a bounded,
backend-aware or planner-aware policy, preferably after inheriting the
frame-batch machinery from phase 008.
