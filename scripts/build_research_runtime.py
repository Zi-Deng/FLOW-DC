#!/usr/bin/env python3
"""Build the pinned one-line CCTools patch in an explicitly selected environment."""
import argparse
import hashlib
import json
import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REFERENCE = ROOT / 'third_party/cctools'


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def build(args):
    record = json.loads((REFERENCE / 'provenance.json').read_text())
    source, dependency, prefix = args.source.resolve(), args.dependency_prefix.resolve(), args.prefix.resolve()
    if prefix == Path('/') or prefix.is_relative_to('/usr') or prefix.is_relative_to('/etc'):
        raise ValueError('Use an explicitly selected private research prefix')
    if not source.exists():
        subprocess.run(['git', 'clone', '--depth', '1', '--branch', 'release/7.17.2',
                        'https://github.com/cooperative-computing-lab/cctools.git', str(source)], check=True)
    revision = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=source, text=True).strip()
    if revision != record['revision'] or sha(REFERENCE / 'zero-completion-debug.patch') != record['patch_sha256']:
        raise ValueError('Wrong pinned source or patch')
    target = source / 'taskvine/src/manager/vine_manager.c'
    state = sha(target)
    if state == record['source_sha256']:
        if subprocess.check_output(['git', 'diff', '--name-only'], cwd=source):
            raise ValueError('Preserve modified source; use a clean build checkout')
        subprocess.run(['git', 'apply', str(REFERENCE / 'zero-completion-debug.patch')], cwd=source, check=True)
    elif state != record['patched_source_sha256']:
        raise ValueError('Source differs from disclosed patch')
    changed = subprocess.check_output(['git', 'diff', '--name-only'], cwd=source, text=True).splitlines()
    if changed != ['taskvine/src/manager/vine_manager.c'] or sha(target) != record['patched_source_sha256']:
        raise ValueError('Additional source changes are outside the runtime pin')
    # Upstream Makefiles use plain ar/ranlib. Private aliases avoid changes to
    # upstream source or system tool installations on minimal hosts.
    tools = source / '.flowdc-build-tools'
    tools.mkdir(exist_ok=True)
    for name in ('gcc', 'g++', 'ar', 'ranlib', 'ld', 'nm', 'strip'):
        actual = dependency / 'bin' / ('x86_64-conda-linux-gnu-' + name)
        if name == 'gcc': actual = dependency / 'bin/x86_64-conda-linux-gnu-cc'
        alias = tools / name
        if actual.exists() and not alias.exists(): alias.symlink_to(actual)
    env = {**os.environ, 'PATH': f'{tools}:{dependency / "bin"}:{os.environ.get("PATH", "")}',
           'CC': str(tools / 'gcc'), 'CXX': str(tools / 'g++')}
    subprocess.run(['./configure', '--debug', '--with-base-dir', str(dependency), '--prefix', str(prefix),
                    '--with-perl-path', 'no', '--without-system-parrot', '--without-system-prune',
                    '--without-system-umbrella', '--without-system-weaver'], cwd=source, env=env, check=True)
    subprocess.run(['make', '-j', str(args.jobs)], cwd=source, env=env, check=True)
    subprocess.run(['make', 'install'], cwd=source, env=env, check=True)
    binaries = [prefix / 'bin/vine_worker', *prefix.glob('lib/python3.12/site-packages/ndcctools/taskvine/*.so')]
    if len(binaries) < 2: raise ValueError('Native binding/worker absent after build')
    result = {'source_revision': revision, 'patch_sha256': record['patch_sha256'],
              'binaries': {str(path.relative_to(prefix)): sha(path) for path in binaries},
              'qualification': 'Build only; native ordinary/failure/cleanup fixtures remain required.'}
    with (prefix / 'flowdc-runtime-build.json').open('x') as stream:
        json.dump(result, stream, indent=2); stream.write('\n')
    print(json.dumps(result))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--dependency-prefix', type=Path, required=True)
    parser.add_argument('--prefix', type=Path, required=True)
    parser.add_argument('--jobs', type=int, choices=range(1, 9), default=4)
    build(parser.parse_args())
