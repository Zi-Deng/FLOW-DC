"""Exercise the pinned normalization path without changing its historical fixture."""

import hashlib
import json
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


class NormalizationTests(unittest.TestCase):
    def test_registry_dispatch_reaches_native_normalization(self):
        fixture = Path(__file__).parent / "fixtures/claude-grep-normalization-2.1.282-v6.mjs"
        raw = fixture.read_bytes()
        self.assertEqual(
            hashlib.sha256(raw).hexdigest(),
            "5c647f32c8662170a4b96d62f60bcd9c9bb61d8ec40d97fd7ac72f3eaf87974a",
        )
        node = shutil.which("node")
        if node is None:
            self.skipTest("Node unavailable; native source exercise requires a separate local check")
        source = raw.decode("utf-8")
        old = "const bn=(name)=>name==='Grep'?descriptor:undefined;"
        self.assertEqual(source.count(old), 1)
        # Only the synthetic registry changes. Every extracted native function
        # and the frozen fixture's existing assertions remain byte-identical.
        source = source.replace(
            old,
            """let registryHits=0;
const bn=(tools,name)=>{
 const tool=tools.find(tool=>tool.name===name);
 if(tool) registryHits++;
 return tool;
};""",
        )
        source += """
const coercible={...input,'-n':'true',head_limit:'10'};
const block={type:'tool_use',name:'Grep',id:'registry-coercion',input:coercible};
const [normalized]=ntt([block],[descriptor],undefined,{},undefined);
assert.deepEqual(normalized,{...block,input});
assert.deepEqual(coercible,{...input,'-n':'true',head_limit:'10'});
assert.equal(registryHits,3); // Two retained fixture calls and this coercion.
// No registry entry must leave the input untouched, not fabricate a tool.
const [unknown]=ntt([block],[],undefined,{},undefined);
assert.deepEqual(unknown,block);
assert.equal(registryHits,3);
console.log(JSON.stringify({registry_hits:registryHits,coercion:true,not_live_evidence:true}));
"""
        with tempfile.TemporaryDirectory() as directory:
            script = Path(directory) / "normalization.mjs"
            script.write_text(source, encoding="utf-8")
            result = subprocess.run([node, str(script)], capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        summary = json.loads(result.stdout.splitlines()[-1])
        self.assertEqual(summary, {"registry_hits": 3, "coercion": True, "not_live_evidence": True})
        self.assertEqual(fixture.read_bytes(), raw)


if __name__ == "__main__":
    unittest.main()
