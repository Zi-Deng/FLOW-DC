"""Reproducible measured-only transfer-study figures and a compact report."""
import argparse
from pathlib import Path

from .common import read_json
from .minimum_study import METHODS

NAMES = {'ratio':'PAARC-Ratio','gradient':'PAARC-Grad',
         'transferred':'img2dataset transferred','target_calibrated':'img2dataset target-calibrated'}
COLORS = {'ratio':'#0072B2','gradient':'#009E73','transferred':'#D55E00','target_calibrated':'#CC79A7'}


def build(study):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_pdf import PdfPages
    study=Path(study); result=read_json(study/'analysis/results.json')
    output=study/'analysis/figures'; output.mkdir(exist_ok=True)
    plt.rcParams.update({'font.size':10,'axes.spines.top':False,'axes.spines.right':False,
                         'savefig.bbox':'tight','figure.constrained_layout.use':True})
    completed=[r for r in result['inventory'] if r['status']=='complete']
    sources=sorted({r['source'] for r in result['inventory']})
    with PdfPages(study/'analysis/figures.pdf') as pdf:
        def save(fig,name):
            fig.savefig(output/(name+'.pdf')); fig.savefig(output/(name+'.png'),dpi=160)
            pdf.savefig(fig); plt.close(fig)
        all_rows = result['development_and_calibration'] + result['inventory']
        if all_rows:
            from collections import Counter
            categories = ['complete', 'stopped', 'dependency_blocked', 'not_started']
            counts = Counter(r['status'] for r in all_rows)
            counts['dependency_blocked'] += sum(r['blocks'] for r in result.get('unavailable_arms', []))
            fig, ax = plt.subplots(figsize=(8,4))
            ax.bar(categories, [counts[c] for c in categories], color=['#009E73','#D55E00','#CC79A7','#999999'])
            ax.set(ylabel='Run dispositions', title='All attempted and frozen work; failures retained')
            save(fig, 'experiment-status')
        if completed:
            fig,axes=plt.subplots(1,len(sources),figsize=(6*len(sources),4),squeeze=False)
            for ax,source in zip(axes[0],sources):
                for index,arm in enumerate(NAMES):
                    rows=[r for r in completed if r['source']==source and (r['arm']==arm or arm in r.get('reference_aliases',[]))]
                    ax.scatter([index+(r['block']-2.5)*.035 for r in rows],[r['goodput_Bps']/1e6 for r in rows],color=COLORS[arm],s=30)
                    ax.annotate(f'{len(rows)}/6 complete',(index,0),xytext=(0,5),textcoords='offset points',ha='center',fontsize=8)
                ax.set(xticks=range(4),xticklabels=[NAMES[a].replace(' ','\n',1) for a in NAMES],ylabel='Verified end-to-end goodput (MB/s)',title=source,ylim=(0,None))
                ax.grid(axis='y',alpha=.2)
            save(fig,'paired-goodput')
            fig,axes=plt.subplots(1,len(sources),figsize=(6*len(sources),4),squeeze=False)
            for ax,source in zip(axes[0],sources):
                for arm in NAMES:
                    rows=[r for r in result['inventory'] if r['source']==source and r.get('arm')==arm and r.get('coverage') is not None]
                    ax.scatter([r['block']+1 for r in rows],[100*r['coverage'] for r in rows],label=NAMES[arm],color=COLORS[arm])
                ax.set(xlabel='Temporal block',ylabel='Original-row coverage (%)',title=source,ylim=(0,101));ax.legend(fontsize=8)
            save(fig,'coverage')
            fig,ax=plt.subplots(figsize=(7,4))
            for arm in NAMES:
                rows=[r for r in completed if r['arm']==arm]
                ax.scatter([r['native_seconds'] for r in rows],[r['elapsed_s'] for r in rows],label=NAMES[arm],color=COLORS[arm])
            ax.set(xlabel='Native acquisition and processing (s)',ylabel='Verified end-to-end elapsed time (s)');ax.legend(fontsize=8)
            save(fig,'timing-overhead')
        calibration=[r for r in result['development_and_calibration'] if r.get('phase')=='calibration']
        if calibration:
            fig,axes=plt.subplots(1,len(set(r['source'] for r in calibration)),figsize=(12,4),squeeze=False)
            for ax,source in zip(axes[0],sorted({r['source'] for r in calibration})):
                for method,label,color in zip(METHODS,['PAARC-Ratio','PAARC-Grad','img2dataset'],['#0072B2','#009E73','#D55E00']):
                    rows=sorted([r for r in calibration if r['source']==source and r['method']==method and r['status']=='complete'],key=lambda r:r['candidate'])
                    ax.plot([r['candidate']+1 for r in rows],[r['goodput_Bps']/1e6 for r in rows],marker='o',label=label,color=color)
                ax.set(title=source,xlabel='Predefined configuration index',ylabel='Verified goodput (MB/s)',xticks=[1,2,3,4],ylim=(0,None));ax.legend(fontsize=8)
            save(fig,'configuration-curves')
        # Sizing failures are diagnostics, never comparative efficacy figures.
        for row in result['development_and_calibration']:
            if row.get('phase') != 'sizing':
                continue
            traces=[read_json(p) for p in (study/'runs'/row['run_id']).glob('*/returned/progress.json')]
            if not any(t['points'] for t in traces):
                continue
            fig,axes=plt.subplots(2,1,figsize=(8,6))
            for trace in traces:
                points=trace['points']
                axes[0].plot([p['second'] for p in points],[p['credited_bytes']/1e9 for p in points],color='#D55E00')
                axes[1].plot([p['second'] for p in points],[p['ttfb_p95_s'] for p in points],color='#D55E00')
            axes[0].set(title=f"Sizing diagnostic: {row['source']} — {row['status']}",
                        ylabel='Post-verified acquired bytes (GB)')
            axes[1].set(xlabel='Seconds since first HTTP request',ylabel='5s-bin p95 first-byte delay (s)')
            fig.supxlabel('Single sizing attempt; no PAARC comparison or confirmed blocking inference',fontsize=9)
            save(fig,row['run_id']+'-diagnostic')
        # Every completed/partial confirmation curve, never a selected best run.
        for source in sources:
            fig,axes=plt.subplots(2,1,figsize=(8,6))
            curves=0
            for row in result['inventory']:
                if row['source']!=source:continue
                for path in (study/'runs'/row['run_id']).glob('*/returned/progress.json'):
                    trace=read_json(path); points=trace['points']; arm=row['arm']
                    axes[0].plot([p['second'] for p in points],[p['credited_bytes']/1e9 for p in points],color=COLORS[arm],alpha=.45)
                    axes[1].plot([p['second'] for p in points],[p['peak_http_inflight'] for p in points],color=COLORS[arm],alpha=.45)
                    curves+=1
            if curves:
                for arm in NAMES:axes[0].plot([],[],label=NAMES[arm],color=COLORS[arm])
                axes[0].set(title=source,ylabel='Post-verified acquired bytes (GB)');axes[0].legend(fontsize=8)
                axes[1].set(xlabel='Seconds since first HTTP request',ylabel='5s peak HTTP inflight')
                save(fig,source+'-progress')
            else:plt.close(fig)
    lines=['# Measured adaptive transfer study','',f"Scientific status: **{result['scientific_status']}**.",
           f"Latency-only automatic stop enabled: {result['latency_stop_enabled']}. Latency remains recorded; no latency-only observation establishes server blocking.",
           f"Attempted jobs across all stages: {result['attempted_jobs']}. Frozen confirmation jobs: {result['planned_jobs']}; complete: {result['complete_jobs']}.",
           'A study that stopped before configuration selection has no frozen confirmation cells; this does not mean its planned comparisons were completed.',
           '', '## Declared scope and actual execution', '',
           '| Source | Stage | Design maximum | Attempted | Complete | Stopped/failed |',
           '|---|---|---:|---:|---:|---:|']
    for row in result['declared_scope']:
        lines.append(f"| {row['source']} | {row['phase']} | {row['maximum_jobs']} | {row['attempted']} | {row['complete']} | {row['stopped_or_failed']} |")
    lines+=['', 'Design maxima include optional sizing corrections and tool arms that may be deduplicated after selection; they are not a frozen run count.',
           '', '## Sizing and calibration attempts', '',
           '| Run | Status | Original rows | Verified coverage | Verified bytes | Stop evidence |',
           '|---|---|---:|---:|---:|---|']
    for row in result['development_and_calibration']:
        coverage='unavailable' if row.get('coverage') is None else f"{100*row['coverage']:.3f}%"
        reasons=sorted({reason for item in row.get('stop_evidence',[]) for reason in item.get('stop_reasons',[])})
        lines.append(f"| {row['run_id']} | {row['status']} | {row.get('original_rows',row['rows'])} | {coverage} | {row.get('credited_bytes','unavailable')} | {', '.join(reasons) or 'none recorded'} |")
    lines+=['', '## Guard-stopped and failed runs', '',
        'A server response and the study guard terminating a whole run are separate events. These are not claims about native img2dataset termination or proof of permanent blocking.', '',
        '| Run | Scope | HTTP statuses | Elapsed (s) | Verified coverage |',
        '|---|---|---|---:|---:|']
    for row in result.get('stopped_runs', []):
        statuses = [e.get('status_counts', {}) for e in row.get('stop_evidence', [])]
        lines.append(f"| {row['run_id']} | {row.get('stop_scope','historical/unknown')} | {statuses} | {row.get('elapsed_s','unavailable')} | {row.get('coverage','unavailable')} |")
    for item in result.get('unavailable_arms', []):
        lines.append(f"\nDependency blocked: {item['source']} / {item['arm']}, {item['blocks']} blocks: {item['reason']}.")
    lines+=['', '## Comparative evidence',
           '', 'Missing and stopped cells remain visible in results.json. Figures include every available observation; no generated performance values.',
           '', '| Target | PAARC method | Comparator | Matched complete blocks | Goodput ratio | Ordinary 95% interval |',
           '|---|---|---|---:|---:|---|']
    for name,value in result['comparisons'].items():
        source,method,reference=name.split(':')
        ratio='unavailable' if value['ratio'] is None else f"{value['ratio']:.3f}"
        lines.append(f"| {source} | {method} | {reference} | {value['blocks']} | {ratio} | {value['interval95']} |")
    lines+=['','## Limitations','',*['- '+v for v in result['limitations']], '',
            'Calibration costs and all first-attempt statuses are retained in results.json. A 429 is rate limiting; a 403 has unknown cause. Absence of either cannot support a blocking-avoidance claim.']
    (study/'analysis/REPORT.md').write_text('\n'.join(lines)+'\n')
    return str(study/'analysis/REPORT.md')


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('study',type=Path)
    print(build(parser.parse_args().study))
