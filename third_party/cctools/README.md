# Disclosed TaskVine research patch

The runtime pin is CCTools **7.17.2**, source revision `ce1360061996e547ea14e22a00bc6042a42a13ce`. `zero-completion-debug.patch` applies only the zero-completion division guard from upstream commit `73ead49d394416eb9ff80a2371e7263474135eef`. Other changes in that commit are excluded. `provenance.json` identifies source, patched source and patch hashes. The patch and CCTools retain GPL-2.0; see `COPYING`.

Use a separate build/runtime environment. The unpatched conda release and patched source build must be compared on the same FORSAKEN fixture, with actual binaries and build logs retained. Build and ordinary/failure/cleanup qualification are distinct gates. A successful patch build or synthetic test does not establish distributed execution, provider cleanup or scientific efficacy.

Official sources: [release](https://github.com/cooperative-computing-lab/cctools/releases/tag/release/7.17.2), [fix](https://github.com/cooperative-computing-lab/cctools/commit/73ead49d394416eb9ff80a2371e7263474135eef), [installation](https://cctools.readthedocs.io/en/latest/install/).

After installing a dedicated Python 3.12 build environment with the pinned conda runtime, gcc/gxx, SWIG, make, OpenSSL and zlib, run:

```bash
python3 scripts/build_research_runtime.py --source /absolute/new-source \
  --dependency-prefix /absolute/build-environment --prefix /absolute/research-runtime
```

The script fetches the exact release when the source directory is absent, verifies the patch/source, builds with private tool aliases, installs into the explicitly selected prefix, and records native binary digests. For a separate install prefix, follow upstream's PATH/PYTHONPATH source-install instructions. For a dedicated environment prefix, installation replaces that environment's ndcctools files; preserve the unpatched runtime evidence first. Create TaskVine archives with the pinned `poncho_package_create ENV NEW_ARCHIVE`, which includes its required launcher; plain conda-pack output does not provide that launcher.
