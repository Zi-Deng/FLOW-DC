# Pinned Gradient2 decision reference

Nine unchanged Java files and the Apache 2.0 license are retained from Netflix concurrency-limits commit `78a74b9878d38c4c048b0304ce12a162ab7b7222`. `provenance.json` records upstream paths and SHA-256 digests. `ReferenceTrace.java` invokes the actual class and uses reflection only to inspect its floating estimate and long measurement. It contains no second decision implementation.

The Python engine in `bin/flowdc_gradient2.py` preserves the source update order. Defaults are initial/minimum 20, maximum 200, queue 4, smoothing 0.2, long window 600 and tolerance 1.5. The supported finite profile uses integer limits/window in [1,10000], initial within the limit bounds, constant queue in [0,10000], smoothing in [0,1], tolerance in [1,10], positive integer delay nanoseconds up to 2^53-1, and inflight in [0,10000]. Other Java builder configurations/functions, upstream aggregation and the complete limiter are outside this equivalence claim. Deprecated short-window/drift controls are not exposed. The drop flag is ignored by the pinned decision class.

Each Python call consumes one supplied observation. Seconds convert by truncating `seconds * 1e9`; nonfinite, nonpositive, subnanosecond and out-of-profile inputs fail before a decision. No elapsed clock, stale reset, PAARC backoff/probe, sample gate or recovery grace is added. The engine alone does not select a downloader method or provide the required application/completion-delay and aggregate-inflight instrumentation.

Run the live Java comparison with a JDK 17+ installation providing `java` and `javac`:

```bash
python3 -B scripts/verify_gradient2.py
```

It downloads only SLF4J API 1.7.32 from Maven Central into a temporary directory and verifies SHA-256 `3624f8474c1af46d75f98bc097d7864a323c81b3808aa43689a6e1c601c027be`. For offline use, supply `--slf4j-jar PATH` with those exact bytes. `--java PATH` and `--javac PATH` select a private toolchain. Java sources compile with `--release 8`; no Gradle resolution or system installation is required. The temporary classes/JAR are removed when the command ends.

The retained CSV fixture contains 2,006 Java observations in thirteen declared scenarios: warmup/EWMA, fractional utilization gating, strict long-state decay, clipping, truncation, changed parameters, zero smoothing, paired drop flags, long-window convergence and maximum supported delay. Ordinary Python tests compare exact integer limits and last delays plus floating states (relative tolerance 1e-15; absolute 1e-12 for limits and 1e-9 nanoseconds for the average). Live Java execution must reproduce the fixture bytes. `--write-fixture` deliberately regenerates it after source review; update its test digest only with retained actual reference evidence.

These are decision-engine checks. They establish neither full acquisition/shared-authority integration, independent clean-environment reproduction, scientific efficacy nor representative scaling. Reference-only build dependencies add no Java requirement to the downloader.
