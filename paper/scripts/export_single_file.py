#!/usr/bin/env python3
"""Export the current article, rendered bibliography and PDF figures as one TeX.

Run after compiling FLOW-DC.tex so build/FLOW-DC.bbl reflects the current sources.
The exported file needs LuaLaTeX and ordinary TeX packages, but no companion files,
Python, BibTeX, shell escape, or network access at compilation time.
"""

import hashlib
import re
import textwrap
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
IMPORT = re.compile(r"^(?P<comment>%\s*)?\\(?:input|tableinput)\{([^}]+)\}\s*$")
GRAPHIC = re.compile(r"(\\includegraphics(?:\[[^\]]*\])?\{)([^}]+)(\})")


def expand(path, parents=()):
    path = path.resolve()
    if not path.is_relative_to(ROOT) or path in parents:
        raise ValueError(f"Invalid or recursive source import: {path}")
    output = []
    for line in path.read_text().splitlines(keepends=True):
        match = IMPORT.fullmatch(line)
        if not match:
            output.append(line)
            continue
        child = ROOT / match.group(2)
        if not child.suffix:
            child = child.with_suffix(".tex")
        content = expand(child, (*parents, path))
        # Include the temporarily hidden table's rows, still fully commented out.
        if match.group("comment"):
            content = "".join("% " + row for row in content.splitlines(keepends=True))
        output.append(f"% BEGIN inline source: {child.relative_to(ROOT)}\n")
        output.append(content)
        if not content.endswith("\n"):
            output.append("\n")
        output.append(f"% END inline source: {child.relative_to(ROOT)}\n")
    return "".join(output)


HEADER = r"""% FLOW-DC: self-contained article source.
% Compile with: latexmk -lualatex -interaction=nonstopmode -halt-on-error FLOW-DC-single-file.tex
% Alternatively run lualatex on this file twice. No BibTeX or shell escape is needed.
% On Overleaf select LuaLaTeX as the compiler and this file as the main document.
% Standard TeX packages (including IEEEtran) must be installed.
% All manuscript text, table rows and bibliography entries are inline below.
% Seven original PDF figures are embedded as hexadecimal data after end{document}.
% Compilation extracts them into a jobname-assets directory beside the build.
% This is an editable snapshot; export_single_file.py regenerates it from the modular source.
\ifdefined\directlua\else
  \errmessage{This self-contained manuscript requires LuaLaTeX; please select that compiler}
\fi
\directlua{
  local sourcefile = assert(io.open(status.filename, "rb"))
  local source = sourcefile:read("*a")
  sourcefile:close()
  local directory = tex.jobname .. "-assets"
  if not lfs.isdir(directory) then assert(lfs.mkdir(directory)) end
  local count = 0
  for name, encoded in source:gmatch("FLOWDC[-]ASSET[-]BEGIN ([a-z0-9-]+[.]pdf)(.-)FLOWDC[-]ASSET[-]END") do
    local hex = encoded:gsub("[^0-9a-f]", "")
    assert(math.fmod(string.len(hex), 2) == 0, "Invalid embedded figure")
    local bytes = hex:gsub("..", function(pair) return string.char(tonumber(pair, 16)) end)
    local target = assert(io.open(directory .. "/" .. name, "wb"))
    assert(target:write(bytes))
    assert(target:close())
    count = count + 1
  end
  assert(count == 7, "Expected seven embedded manuscript figures")
}
% Select the article's existing font encoding before IEEEtran initializes fonts.
\RequirePackage[T1]{fontenc}
\pdfvariable gentounicode=1
"""


def main():
    body = expand(ROOT / "FLOW-DC.tex")
    # These three pdfTeX-only metadata controls are not manuscript content.
    for command in (r"\pdfinfoomitdate=1", r"\pdftrailerid{}", r"\pdfsuppressptexinfo=15"):
        body = body.replace(command + "\n", "")
    body = body.replace("\\bibliographystyle{IEEEtran}\n", "")
    bibliography = (ROOT / "build/FLOW-DC.bbl").read_text()
    if body.count(r"\bibliography{references}") != 1:
        raise ValueError("Expected one article bibliography")
    body = body.replace(r"\bibliography{references}", bibliography.rstrip())
    assets = {}

    def graphic(match):
        path = (ROOT / match.group(2)).resolve()
        if not path.is_relative_to(ROOT) or path.suffix != ".pdf":
            raise ValueError(f"Unsupported graphic: {path}")
        data = path.read_bytes()
        if path.name in assets and assets[path.name] != data:
            raise ValueError(f"Conflicting graphic basename: {path.name}")
        assets[path.name] = data
        return match.group(1) + r"\jobname-assets/" + path.name + match.group(3)

    body = GRAPHIC.sub(graphic, body)
    if len(assets) != 7:
        raise ValueError("Expected seven distinct article figures")
    if re.search(r"\\(?:input|tableinput|bibliography)\{", body):
        raise ValueError("An external manuscript import remains")
    chunks = [HEADER, body, "\n% Embedded original PDF figure data. Keep intact.\n"]
    for name, data in sorted(assets.items()):
        chunks.append(f"% Figure SHA-256: {hashlib.sha256(data).hexdigest()}\n")
        chunks.append(f"% FLOWDC-ASSET-BEGIN {name}\n")
        chunks.extend("% " + line + "\n" for line in textwrap.wrap(data.hex(), 120))
        chunks.append(f"% FLOWDC-ASSET-END {name}\n")
    target = ROOT / "FLOW-DC-single-file.tex"
    target.write_text("".join(chunks))
    print(f"Wrote {target.name}: {target.stat().st_size:,} bytes, {len(assets)} embedded figures")


if __name__ == "__main__":
    main()
