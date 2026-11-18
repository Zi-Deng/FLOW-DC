# FLOW-DC manuscript

This is the current article and supplementary material. Sources, bibliography,
numerical inputs and plotting code are included. No private workspace or image
download is needed to reproduce the presentation.

Install the project research dependencies, a TeX Live installation with
`latexmk`, pdfLaTeX, LuaLaTeX, BibTeX, IEEEtran and the packages declared in the
two TeX sources, and Poppler (`pdfinfo`, `pdftotext`). From this directory:

```sh
make all
make reproduce
```

Outputs appear under `../build/paper/`: the main PDF, supplement, standalone
LuaLaTeX source/PDF, source ZIP and a receipt binding the current inputs and
outputs. The build packages the exact staged input bytes and refuses publication
if those inputs change during compilation. The standalone article embeds its
bibliography and seven PDF figures; compiled citation/reference numbers, page
count, extracted figure hashes and typesetting diagnostics are checked.
The `check`, `single-file` and `source` targets rebuild their prerequisites.

The article retains four matched short CVDF blocks, incomplete historical
TaiBIF validation, and separate distributed engineering qualification. Additional
96/384-thread observations are author-reported and remain outside the matched
statistical analysis. Table 2 remains hidden pending missing measurements.
Long-run efficacy and general scaling claims remain unestablished.

`data/` contains compact projections of retained analyses. Operational host and
VM identities are excluded; measured values and first-attempt dispositions are
preserved. Original private evidence is retained separately. The authors retain
rights to the manuscript; the software license does not relicense third-party
images, datasets or cited works.

PAARC is the controller formerly labeled PAARC-Grad; PAARC-Ratio remains the tested
alternative. The paper calls its sampled feedback quantity the normalized
excess-delay rate. Compact inputs use current names, with `original_method`,
`original_label` and `original_control_reasons` preserving changed source fields.
Stable run identifiers and original source hashes are unchanged. Every build reverses
the relabeling and checks the exact prior packaged bytes as well as current hashes.
This naming revision does not constitute a new acquisition or performance result.
