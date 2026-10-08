"""Explicit suite deadline protocol; tiny disposable suites, no live services."""

import copy
import json
import os
import subprocess
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import check_runner as runner
import review_batch_windows_v1 as windows
import review_packet
import test_check_runner as legacy
from workflow import WorkflowError


class SuiteDeadlineTests(unittest.TestCase):
    setUp = legacy.RunnerTests.setUp
    module = legacy.RunnerTests.module
    good = legacy.RunnerTests.good
    helper = legacy.RunnerTests.helper
    evidence = legacy.RunnerTests.evidence

    def test_real_cli_serial_parallel_and_fresh_installed_descriptor(self):
        self.good()
        for jobs in (1, 2):
            result = subprocess.run(
                [
                    sys.executable,
                    "-B",
                    str(self.root / "scripts/agentic/check.py"),
                    "--jobs",
                    str(jobs),
                    "--suite-profile",
                    runner.SUITE_PROFILE,
                ],
                cwd=self.root,
                env={**os.environ, "TMPDIR": str(self.root)},
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=20,
            )
            self.assertEqual(result.returncode, 0, result.stdout.decode())
            directory, summary = self.evidence(result)
            request = json.loads((directory / "request.json").read_bytes())
            self.assertEqual(request["version"], 3)
            self.assertEqual(request["execution_limits"]["seconds"], 1800)
            self.assertEqual(
                request, windows.runner_request(self.root, jobs, suite_profile=runner.SUITE_PROFILE)
            )
            self.assertEqual(summary["execution_limits"], request["execution_limits"])
            windows.suite_summary(directory, request, jobs)
            for index in range(jobs):
                self.assertTrue(
                    runner.reconcile(request, index, directory / f"worker-{index}.jsonl", 0)["successful"]
                )
            for key, value in (
                ("version", True),
                ("process_exits", [True] * jobs),
                ("elapsed_seconds", 1801),
                ("request_digest", "a" * 64),
            ):
                bad = copy.deepcopy(summary)
                bad[key] = value
                (directory / "summary.json").write_text(json.dumps(bad))
                with self.assertRaises(WorkflowError):
                    windows.suite_summary(directory, request, jobs)
            (directory / "summary.json").write_text(json.dumps(summary))
            for key, value in (
                ("seconds", 1801),
                ("seconds", True),
                ("profile", "unknown"),
                ("cap_seconds", 1900),
                ("schema_version", True),
            ):
                bad = copy.deepcopy(request)
                bad["execution_limits"][key] = value
                with self.assertRaises(runner.RunnerError):
                    runner.reconcile(bad, 0, directory / "worker-0.jsonl", 0)
            bad = copy.deepcopy(request)
            bad["execution_limits"]["seconds"] = 1
            with self.assertRaises(runner.RunnerError):
                runner.reconcile(bad, 0, directory / "worker-0.jsonl", 0)
            bad = copy.deepcopy(request)
            bad["source"] = {}
            request_path = directory / "mutated-request.json"
            request_path.write_bytes(runner.canonical(bad))
            worker = subprocess.run(
                [
                    sys.executable,
                    "-B",
                    str(self.root / "scripts/agentic/check_runner.py"),
                    "--worker",
                    "0",
                    str(self.root),
                    str(request_path),
                    str(directory / "rejected.jsonl"),
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=10,
            )
            self.assertNotEqual(worker.returncode, 0)
            self.assertFalse((directory / "rejected.jsonl").exists())

    def test_legacy_default_and_finite_profile_bounds(self):
        self.good()
        result = self.helper()
        self.assertEqual(result.returncode, 0, result.stdout.decode())
        directory, _ = self.evidence(result)
        request = json.loads((directory / "request.json").read_bytes())
        self.assertEqual((runner.SECONDS, runner.VERSION, request["version"]), (840, 2, 2))
        self.assertNotIn("execution_limits", request)
        for kwargs in ({"seconds": 841}, {"suite_profile": True}, {"suite_profile": "unknown"}):
            with self.assertRaises(runner.RunnerError):
                runner.run(self.root, **kwargs)
        for seconds in (True, float("nan"), float("inf"), -1, 0, 1801):
            with self.assertRaises(runner.RunnerError):
                runner.run(self.root, suite_profile=runner.SUITE_PROFILE, seconds=seconds)
        with self.assertRaises(runner.RunnerError):
            runner.request_limits({**request, "version": 3})
        with self.assertRaises(runner.RunnerError):
            runner.request_limits({**request, "execution_limits": {}})

    def test_real_cancellation_reaps_owned_child(self):
        self.module(
            "test_a.py",
            """import os, subprocess, sys, time, unittest
from pathlib import Path
class Tiny(unittest.TestCase):
 def test_child(self):
  child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])
  Path('owned.pid').write_text(str(child.pid))
  time.sleep(30)
""",
        )
        result = self.helper(jobs=1, suite_profile=runner.SUITE_PROFILE, seconds=1)
        self.assertEqual(result.returncode, 1, result.stdout.decode())
        self.assertIn(b"deadline", result.stdout)
        directory, summary = self.evidence(result)
        self.assertEqual(summary["execution_limits"]["seconds"], 1)
        self.assertTrue(all(code is not None for code in summary["process_exits"]))
        pid = int((self.root / "owned.pid").read_text())
        status = Path(f"/proc/{pid}/stat")
        self.assertTrue(not status.exists() or status.read_text().split()[2] == "Z")
        with self.assertRaises(WorkflowError):
            windows.suite_summary(directory, json.loads((directory / "request.json").read_text()), 1)

    def test_makefile_and_case_ci_are_closed(self):
        root = Path(__file__).resolve().parents[2]
        for value, code, expected in (
            ("legacy", 0, "check.py"),
            (runner.SUITE_PROFILE, 0, "--suite-profile issue31-suite1800-v1"),
            ("unknown", 2, "Unsupported AGENTIC_SUITE_PROFILE"),
        ):
            result = subprocess.run(
                ["make", "-n", "test-agentic", f"AGENTIC_SUITE_PROFILE={value}"],
                cwd=root,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=10,
            )
            self.assertEqual(result.returncode, code)
            self.assertIn(expected, result.stdout.decode())
        import yaml

        cfg = yaml.load((root / ".github/workflows/agentic-quality.yml").read_text(), Loader=yaml.BaseLoader)
        job = cfg["jobs"]["agentic-quality"]
        condition = "github.repository == 'Zi-Deng/FLOW-DC' && github.event_name == 'pull_request' && github.event.pull_request.number == 32 && github.event.pull_request.head.ref == 'issue-31-bounded-review-units'"
        self.assertEqual(job["timeout-minutes"], "${{ " + condition + " && 45 || 15 }}")
        self.assertEqual(
            job["env"]["AGENTIC_SUITE_PROFILE"],
            "${{ " + condition + " && 'issue31-suite1800-v1' || 'legacy' }}",
        )
        self.assertEqual(cfg["permissions"], {"contents": "read"})
        product = yaml.load((root / ".github/workflows/flowdc-tests.yml").read_text(), Loader=yaml.BaseLoader)
        self.assertEqual(product["jobs"]["flowdc-tests"]["timeout-minutes"], "10")


# Exact public predecessor body, compressed only for fixture storage.
PUBLIC_G13 = "c$}rZTW=gmmL~YlU*VhCi%K=!>GvC>asZ~JtmKqZQb|hH)D{Y&U&$y1Bcj72D6vKj^wR?Uy0eeFz&z|Pt6#F`YGxj!vZqmv!IUx>;TN-G=lY%ReCu6K(_u(sFZ5o&NfYny|Mq`-+udf`4&8ovH+bD<z27z6{^aDx<>z7YzD?tklW)KE{t%ye7l-}LX1Cnmd(FD{mg}1V{$uF9O}koNH~ZygJ)NAqe)H<}tGCaed9Pl+_|uv9YPY;zuA7y2F~N1cUw*lK7R608jAb0;Wi$k3Hbg;Nwn>nrLs2w!TI6Zr?}qE;v>$eG`K3JZr_Ju;em4wZyKC0njo+<?X6+3_lcZ(SS7Y0k<2dA5mnLJHq-|Z4c{DU#Iizh9``2+C7Ezp)Rh`wznfG$zJ%uOUtcJyU<8{qyHHFjWu<M3!+BbVxikoJAJxpgOCr>w<T@P=u-|W2G!?gFd2YBV_#{1(>uU<TFH~qbT2Wv6H!gTusyevI(*lliahxI<(d|D5?@V41~9CluNzaJ(a{<J;7J=VkLy|-_skJB0b=7X-k-!5T|;X&4L<7=<srT-hPviHODA78zB?k#V(s{!vopPqTIcf&O7U=y1yZ0TLIn!L@r8(^op)d4R9OK|4BFTCw?J?W#qeE#3wc`Mk*U4siStj3^U!fWrt{;*vwJ6MVBs##w?3&+E1rB~*!EIe4b>%%ZjLm&3{>*jXZg}9RNy0k=Hu!k26;kjVxU@IGypfm5y(7~d@W9(oRHhZtx!@BUB;FABrYgU`};7y0^u!GBkDB_x}1`xQHQ@7a;i;EX8yqD>EK+(G!rtoDpbvHwQSizfyFLU@9KD>zk{%`-|B2IK`@g`y0OhecYa51=BINWac_u*l^8=5{Gm$cv^?gKvh68`6UM~@}1#9J^Oc6V^svn(y5G{8;u-h{84-Tnvo(>N|a(?$owXISR&W{K|)KV2<eZsb5rAQ0OL&K~bs565e@%FEL%T)~6yynZ=tH`9{V!#k{57~xxg$h{rzNH^f_uI^!X8#=o0i+<OP`^D=Uc)Ju|J59Y$11|4yhZh{C<u%AKT;$~Bf;K;{!nlT&8TX%H8^WiT-oL=7D93WD=nf0>kRH8`$4eQsn*)gGZs>;Pc0Z9$czSt*iwN7$ecT-O_!4`NJ{C_naPk&`Yz9iyZn#?xpJ4fW*k=*<u;1J^%XN4=?5^P)g5)gsXRL<c;64>OfHS)XA-SeIuQt=P0zvfPa{T~Oe{%A658DA(%d*p0zxwv4i=t`Evd)UJjia*7`=&^XI7;#%smC&khN0`bwn?*WDEew>>V6!?Ebhv-EsCUyI<p#6oMp*&w}gXzfB9?*i!4gxJf;I4#&KEK35en_ohBV9*ooJx*)4}@;eq(?;SD`kDpAySBW$cU!a-WW_k8cu4d^G&$`33zh{bvuJiK%Rt8-W{_wYsDnH^~?6=D*xEvk=e5Va7LCY?i9U(*(dtkVwRF+c(Aw@vpEw_-Zn;tPhGTToDR3iyqK{U+FK@gne4w;MPLbfj6WjU?gxZmQAv)X|m(^v|a6S-it$LeWG`hDZO0VFYF2UA+7gDl&88=$dBFNez$7{cpd|D!eP`3i$PD?3~xdZ@-U|%Gsbi!vEv_so`I=J09p4&s+h21401m@D?_-a()nJk@?Af0}pZLU6NjfCj~_WF9Z6m+w{DdQ+Rz?_PFBsHS<WYegAS;!XA3h-#>rzC)qV~cRbmf9hjHZ0nXEE<*Bj=*TX08ffUjE!HZc1uNS}%;Z*$gdwyk5u<JnnbPJ!OA-iWwbml$(+zkU8ne{!a?>4+$u9vrmTMte!>?RnY4JfLXccoi3%Ud7yKIn2drTb;)UGFxZ_BS9(Yg_|39-#hUA78>Vx6Qu0sacHW34&YHZ%fcVAWECR4x}C8b69{RyBR*iv(uKqa==1?jk`VUn-&hiuK6Ud7lMYn2}#z^U<H0zFLB)iP%bEExScnL^+zzW4ZJs<8eCyq>0jRcSb6{T?W>pGpC{A~A8-x)Vcp9=a63NWS@??&D~_A(!MjcVFu}SE-Y-#9<>4<$lvMQp*I-RSU-slJ=*?JMQ0kZ0hs^=B>o^S{5O{A;mLTu@-GN>jUg6WJT?2e8_+>wA;ekMf-4CCK?tp(ebu16c6M1VQLh^f9czj&Y)u{A9VpgPjK?nS9wP`^@$SQS9SXMZw%_m+-^a4OBOiNhc1+MOZM}LQZZhEx1hxJ|$IjM*BW_Js3%zlW`GpZXvUcGU*`P;CzZxwD<z3#aBFYvDhPx=K29IVC?Z3@_o+a>{#MuEk(qIJ~Qn|}L!8mC#xi(A%o4i{Mk-_N5gD>6Paa4*l^dh&k$<?TDv64rnwKmTwhr-sD5S*1mic;HHcLNC&?Vxea*jU1Ef1BewIVOU{(U%aI2ZaF7aMReRX*Qn6wbx_bZBitHv!|oQpR?x>_mqBSI^luQb{b4s$@TGp)T(38<ecgn=ffX8>+qi&#f)0W&t;yn{f*$tay15f&9`4Ac3U#$Za+B~xPpF%l-UCk+^oDV{RNLUgEj)c<-Xc)wyK%L_BR3u<&=Yun5F=3tJ2XAZi7vC>d>p`ML;uAiK>(|93ob4Agdpi_^dNh%@8HSd?cm?$+G?xeCTQ<Tk%@8l#cKiS04KT^>z|^!!h<}lVYLT%O;B6>nz$tJgNdV%i)YW^^Bp=SAv`La#V%w|;{&Or51(Kw;becfi_f<A-=T+Fhw|BK=&y%e{KS_Z@G^bSW#ykh^wEIs{~)&W!=^=9ptX~0PVjDB<6H+eqUpf*{jes7RlWwsVu_cg@8ON$l|GP1Cl@R5ZuU>O$?%@{AJ{3lfA9q>JrDlW^m0%i{H0qh`7=kX-{+ov4o4`(oxKMO{R}J^>lx2nJAC@$(z|$lIXNEiIh{;0jPMJz>B~L1uJBJdsd-3#DCn2r69gwHZQeA19z(SX7o#WxejniPG3deb#Xn_Py<LiVg>VyY(;ap@*l}?(J#t3jiu>hiCDN>Zs0S7U^a%K4D~KYxdw%R|JnneUxwq<}PDHhXn(OCR&n|!b6IjCy#3o=P4tsIO&s`~l^&~%@jNn4WV70{0McBe^0}q4|(&2WHui&5A5f_~wiY!g$8YluF*Z55^R959J?3U_b!0*r|6!r;Fbn$bRT8!3UVZbT8*@tEy>7^DT=8s=iB^vHOE?19y*;p`s2;O*zKtu5h9QiW$g}q<N%WRk10X04Ty1BmIVW>B_VVJ%VJOBi7_%(1n(3vqd7WC53;A(@3Zc6q>0+1|>%F%I#Jw-Qs=ocQv+w70o=YyDul*ncWPTviz$n}j!X1W*c3*OXn1hHhv<1E4M9#!CK;ER-D#m>O`z%QzchEF?z-b3BwM0h0a6bNwu{JzkuvY62^QNN1LidnhB$5Tc!%<q+-p~+fKB#5eG@0N|H3%h{(@<|%dI*@0H5;gRA33%)d;t)PsCns<=pNTMs<OYH>&nufGVUlXVPP;dOe?5i&6i1$stVI%qN$h<bhHVP=?w=r%d)wR&cNKZ3?E#c78{2-;gbbRA<{I?K79xZ*??-km;2eNqK*t5cPKtr|pEh8IHmhKJ*g@0`<_)eqo%3@!3THPQ=#Qv>1N^}hz(VT*fA{Lui{R-`&!7I@6N$zE4E_gmCev!O59RXU_<TlVZT8YbJGRGvz|P%IChnC)nYwiN2zTNz_T*3DhD`_Z!NSI-*U&#gXG-3cMPtE%|Gh`KCN$*jW`Co%UQ(t?m3LmS9~R2H;<YIc)d#Ch_c7!PhuaOT%zZe)hKFFPF-=A4667WPx%oKY8iY?TLp)HN)-m2G+z;KFJ;~1W5tJ*Z0MKgR8+8xo0{2H$viJJUtM`}Bp1*k;K79q=>&5S$hp%44|Gv9?`McnEzrdfuH_zW+KL2C*_V<@BUcB`*ih{7^-YkRKAIZU>>(Qfgc1Oy-*}BJvZ4Qb9ZiM%Tl42K|yF}?<Pxw>WQ;LYhrkU3UH>5$uhoLS8o=b`RK=+2c(6Ckas|`5K3s4#(o&Y*V4^I)gG1-~-R3%4!Aa*uEDan1vt76~m<b)+3jtWNOu!kVr6MA6epu_jcrF;469YjveM_BXiK7g0CdQA0vjQL%(G!woNyf<zNi-^b0D84g1kVmrOiBsy6e~c>vk0779UTP>#FFmcBtO5y#U}75BF(6-wqFnqR_{7JA-(kAhY(DxwU`+S()7O-j6Nh#^>@n#Js+#n|(1U$}iw`>tg3O`vFo)sYLI?;huiOfBCJ3u~+aVqGP_iTL!-mE}%`L75>gag>2nTU!x*L;##9jKA1Gt=UFBG-C&C0;qvXlIQz2egjeux&a=|4R3c?W+WFVFp(_g#Bfu4o5AGirX*o0<_*x%&Z})9J~X_iQi;TG0U*>2f*%UL0N<Q+KfDpnkhOIpyM<LvXx?{k77=UJ>eJxDP4RHOL6WWn$4_0dIDL6*vF4-$Nv#FFl1to~H$cObh-FW{oC3Dz_+s5*2z@W~@C&3*CGUIlY5&{PV?s^IrY%_W7Il&!2ghFJJ%iPJ@BD^oK1d9ekhV`ff=ftcd>`-aET1&4^inRWrHi4~~d9tMQiH1(53g6@`DCWNgTT@cZ3nyQL5Y!Yj#+$!YO1Wg_~UO$z7>Pyo2E)9@W0HjWBVzcj09;H7R>Pkc8ZdJrxB%K<JFl0lw~6pzhvb=b)zhJ~)_bU*EfTfOjfbo@H=9#lVRc9gUQiJm~UuRBh~fg`-^L4*P+e4ud$!GFtMC%Hxwi?vz|b8L($lx&~%nC9oq*?G=$Ec7^0LZHWN11lE%^O^T<vs)X1-8Z*eDg|jU>b)9A%Fc+K%6SL*2*E*FtD)?gHOILo>q1#1Q4(Zfx$J;?7WMFs{Wx{E^<Y3eNfPwKt9L&crx*T#-j`&|eed=6x6NnOw7rDN@DW6J3s0jw@A>!Luv~o?pXINgoKoZ;`JNc<IKhOYq}|}iE!UWVU8pYdo}cnJBuNrQ-@S%|nBhlH&hqGg_fQ{<d%3Kq#~cFtTU<p=<jhkBnDSx!0Zxm=Gp}D<ynG2J88aIjR)~@eL3P6RDx~O+G*)P>7K<zdk!m6*yCV7=n8BD9-jNn^d|_U7GSn*jR$btxaTW-yD3>ujF)UbcYq3m6No5vkdskUb%=}KS?6F^r$7a}}(~KEtwwmsK(sus#`>MpnU3g|KHD|`+AKQenZ&#T=_0e0}bek3DWPQ3K81-Sb*$#2RJL>c1(=Sj7(b9zW5S!ddXr5Qf5t1Zl9Wjv~>ZDV^dB$aQvg1c?>5`L*6XmjLRV71+PhqQ{9B)Aof4g0BLE^Dukg8RRq$!};Hp`w9!?0re-Tj%zo)APRZ#C2(;YtCz{pfC^<b7(Wj)U>eVSPZsJd<1wI<c_$9B&3D-VAI!8Qc|%k4t(a{i9}1;7`8B2~>0q(l~hZYYTdT&{^Qn3Qh=H1^+eJ$hA20elg(`tI`&|?I~Pu@ETxN-sfSSpjSO{(gXgIz@|&`unzCAZg-c3T^glY_8BIXQ3_8NaTLadoE~xKdOwcCxIBQ&fdF8-+=oTSbF}V__QI_tE35GiS>N*{T27l4FPa#Ul!CX!hpTgmrdcKNSE8oJzMH6y3|7q!3&nvcacEpP`+jP^?+9pC1tzzxzXCCao7(Vd0?mj5fpSPOcYyJoCXXoKgD*^ba%CYxS+FAqk_ZYOtN=*#{1~v|ph^xOoRF6Jfc^&gDWcXeZ~zY)V(kcm)g)u74$(V6`x}C>U$qScTI`lSeR0V~ITUAMvpeH(`1ll3T!LFg;(?2;mX$VNQnq3dsA2|jHT`LIA7H>ETX@E?@A2EQAz^nIqep9&#W{vMYalc?;2J`JKHy-kz)b@e3{4?r43g;L<+CJSaLj@Aza+s=QatQGE<aoU+1vrEa_kPM%5W=wrs&5j0I#Ik1yAJ4DV?=a(DJD?;ID?ysGcsL`M8+;a$F;z3=tzZI&jt4!Ys)%L63|P6|h8)R|`Mga77&isQ0x1w=i*P$aM_Y-I051wG+1{J<lrIGt%>n9^g8NltDymfzh4dX;EZv53Bu>T%J!i5V(Su10mzvpDvQT2(W4f&kSzry^lgSfv0=pqYpXwFQ32gU%U-pzx7GI;;pc_;y-)-VgU~yTt34)Q{ntGCRE_nnq2^vFVF%QxjB^G2j7X8!<<WWwq#iaP9jy>qf(`*hC0<PPjhEB*7ANgtcM*M8cs=AXVSYK_LE$k)Yd}926a5C=`-&KOoy;nCYNN(D{sgJC0fl9tKf4J&e;oVv={S$P(=2(--G=}sg0xG|G>7|H!=tZ0+!Tf^5hX&;8?!5lHv7U@6b?TREKAR9k9b%Zi!jITfQL{S?FgImu4hDBj4VX=Cuqz9a<2%{XtR<KTu9>N+4Ea<qdi|Tc9o-_J{3hl|^Jm!L|OhgL60RPU9*P4T!7s{eLObD*6Pf><qqZJvuZb>i1|eMzxeJ1JGu1@?DEt<gnq%?jiPK|DB!-AI0Yz!xQ*T3kC^%BQT{)2-F;va2iJyI!3(HsKrV7!*QF?==A~m8R~?P8Rz5x+fT3%_(G>C#;zkQ>CI^ob!eeM7I)W}9OYf20B9kpEP`ke_Q^QISpS=BX<FZgHbwLKaD3pwu34brH!Kuhf54g*+pTU`+#KK($d~2jyuI&p8^{&-$`G!uC@DkFtI23u<W}JxU}h+b7|IezMT<A#R5k&Vs}n{<bS1=IfFDW46N<Sgd9XWdxoAh$5Akp-c&ClnG~O3-MKO0H2!RE893Wa~1u^`^XC6UY!h2#3lHvyp*(b?@fxGl0EbF}|zyVBE!-<f>2pvaLaxp$R$D(_(>4Y^pMwJ-2$#RV$I*O)+ml#+V2wSt<S)j};ttnL@iFN(=1%CI`<n6sb5-5jqiULQ`(qYK8qb;sKo)pljpc_HL9T^U<SDO}22Fln(&N1SfIIr$)Mk#Pj>M6kUV2paZMOPexQ@57j#7v}Q7P@$`oAlnIh1;W=hHw}w7ZO}iGUPBeocNH)!8#})d=w15U`y(lS_-zo_*J@9o3ZdcH@4tE_@b~S<$VmH<1d|KG^@*3)5Bp37WEJ7%38ITQCR2+E+F!)MQwp=nAMSQJhHjAVwl8N^_3>wLkUl;0@FjZ-LXF@#T&|Yu(F~Ml`?q-9)<A?DAyW;M0$HrXz+;L&2rVlid9L(O7L!|vl?^}y>MKBRrx-K|6_H_ZYuo=R;R*isZR7BQ;JCqD&o7exOswc-j32-pLtDVgfsDRcX2{&0n#E8#kU~a!QM3KSl`*s1k9*UFH^(AoOxMwmKJH86?I)W<tH>%uU<Z9FKUTsPj(5w#KGC*ljb>fa+X2T!-1pAKW*>@$ah%4Mnaqvk#!US4N%!UOiUP*230|D<v5W^3M??*JmQvU^}&TfG50W|XQMj|K2>MJhJo%n@}P}@J{1TsXi4(qC_*^pLya=ftoWE<CP)sn_;gP0j6G|WDa|^CMxPtAC2HoBt7LHfJ*6b+0P6}j9c5BHwI&SR<iaur=3E8`+x^uOAa$i)poNa{3Tc-d;~M_>%ZnFi8F{rFNg^YN*f24%poh8sX96JWqiBMUSI9d?M^X3+%eBV}jT$JoUT%#Q;dt?6h<*#jbgI~~g%WHO7wMej2qL^ObA)l<fL`|Xl&4m?<Z-rBOBQBDt_n%a=(IquV&*NL@>ymDc1lmVMOZF92Ont&9U@pYJnoqELBt!K?5v_}`R(j_&S^*8u!G};+3+@?-(ZFS_5nkFs_KjwrM(qYFT2ZXlGH#f1X(JkmmgG|Q}S-qOO)KXNqT~aE*0uUi^@(9>&|gG?r-qHG1xxy{y<sm{SxG_=|Q(3!osAj76h9jeGWnMcA}X3fH`$VY_K~N@&jzPB8u!7kTzm>hyrc}cA!tPJFIa%K&lUW&q~k9iTK{LmG!k0?wJEE_=Qj_wI3{U`a6j$zJIqn3{QMGesE3*G>+W>JA;3PQ>F^!)J2d>9n{(wM;%UFR5wis5|CrCVRI0!0R=}z9iW9rw+j{gCkU;8RG>+k8)T1aBFz?tF4%nGJ-rOcFJ<_(W>dhO69-Tbp}54JJhMS^!P{c@0*E5v>*=JDOGsxK^CRuBLQJ4p1Mvn+bQiC8nwmG8%<^kc!3YKCh-lLv;C}e_aFPPrLK7s5uEikD2N!j>4_NN$vI^@FHxFHHEuJi}zzGLcDoq}(xddE-*k*WrB2W=tHHfTCnNIQWgwqhPjQJYj8SjP}9`;!6f$UIN9SRbW3?bLQniU;|Rqq+Eor)MJ`GSjLNj+G5NjS?myvV|f(tBT6ZLem4!As}*h!9$U)x!|a7Rg_x;r@P$C0}X`p?6qQI4>_~AYm90*;8sjk&-H%G@<|>C<!l8MROa-;<;9QOldRxwA-w&xr2tn5OVVMh)9Bpr5;y}p}`Efm7)-)J78J<riC-OcOG6827tv9e9oHC?C0EXAe*wlN<JOn?mmRKT~mrzUx|zkMO4S(kvNMO6M~(F5M&EBRzV+!HQ!j8M$98t;e{>Ri_0*a@6fNXSS=e5PWNlrH;4qZVow>I%|5`A7<;H@Nhd3g3texgOsqv-x9Xtq7Fkj{m|9&8Sz6hb5g%q=z;wC+2#DfHiFw~R3NPZ=ATD}=Q-mVcTTYiqJSC{&0Yn*#O&4*(vAPKtNv7fA1RXxojSTfu6^UmXFQx`g9S2mc3-Ms*SUlUbVpSs*DaK7lMT)D}e|q=RtCz1Y-u?9b5eD^~djsUZyzM=_!8-MVAH3j}IytBqiT~dRLAF_(>g?l(m{k7r<8V~@+XMWmUQ*$DvlsnvS9rnQ)!DHw$PsFBzzT+V7L@%PM+T88GSM|u);>}dL(_fd+)Os~uW1>=H*f?nd%F)uM48jMK|w_&)i1|!L2~r;)z3d)zI#UvhR-K@drvV-UJ(2h-rS2r_#18wNYV<N<d1>^4HODYt%D2PNiAD1C$9R0QZc|V<RghXm$${4S4DPK#TiJ=rW1ugp!8ZKvdndb3=E6Xnfp&0zUl;V4f%uMWj(#**qy8ObW+D9>iKZ`xZH-H26%c50pP*t8StNcuE2pMm;tN{L`;efAlr=gciO@|M_70G1qx^`EoGRol4FKjs|Sq~=T}Y}Df0_-_|chFNib)1I$%Z`)T6BvG9<?=JTLeu)z7&Eh3Cto2M;8@;bB>`?}FJ82}mTt@5E0}=_-yNgAJAqYM9~k(sgI(?kF;9D0pLd7u4=)E2Dzj!l#$0Aut~z5CwYe*Kg=i?=-`$y={n>)65J!k|XV!f0Qmw2dd6NG#?xJtN7fZn;@c6mH5@@<?OW|jxssyI68TB!oRZ@^~ULrQ)sIP4@y4xa(&mpaRC?m<m63!Zu=<(K~Hb?fnk6M0OX3r-5}cI=&x^|zx(Ai2boEXE(!n1lQV#8S4AmE5~kYiF;UQ7e_DQDVL|U5EHqYxFbNKJ6nj(Qp{uryo3@pv!DB>XxBQ?Ervps{x0TJ+6cYT?S*|IRMF{WmK(OAVYSEqa747NaLBN9t!nK^$D$qgML4X67z-SUN1p^_|3<5GB2&DLy@Yw7OYWT^u6+B7U745xD93jS*=x~DNYB!(L$QFbc)2xU#51$?b8kEWF;_v_VKd$3QlUcC&ZxX`TXBokK@F=BameyrdMs>uX&NxouI*PNZDl3)TnA}8I?<|`94Y&}yEQ-UdO3tdHo)#*A=2}%Tzg8Wm1v}N$jEy+mq4z0psD~(thPp_~I4bHS9mj4=qPUBOJcS@2sf)Jj;EHG751{xdQv$A_ddQ%_ZZ=f7g<WLif47+^dvXKL8273-<U1INr(hC<E-d3~@Bp^x#ZPG-pA~r(*F_jt)mdIuRa%jFW?6QY<x!r;VUpBmNtHub4)6T-;^*hyrvW{zq0iDdOv}X6PD%yxOi3A?#YviHMJWHwsuC}oro~y3CJ8K6nwDpIlto1jQ7xRk{mm`<Xq2$BSs({fBd_cvv#{H1kFIuLFvqyk@N%>qn&#{m3PS_khd~**mTZJLN3uoC<<2p4r0%hik}pH5zDFx$<kBj7<6N5LDJH~8&=_WL3wjjccO)7fIKAlMQ=pWTznO}(s&gaLXn|JVr$&80gTecRH^Zh8QM83U3fN&!ZFwk=WSTGu6NP{OxBmxo6@UM?|Ci%X#@Nj}v+`QC`V>l-C&`b$|NH-!V_|oXwd6gUOt3W}Bg+YOOx;IpKi{P1Y}FWCyz4!?n40M`F*HT(v<uYGH&PPM_)P@w#y=R3Jk%T^^`d(uz51aBatm7!67a4eY~V&iSomg#@JPB1^`=nG?b5VRN}Inl+r{eK1a!Y(wcQ7|Q*akk$-ImO=2~D^W@y4ggH07EYJ7(2cKE;BiHgwF-Yk8sj3$$kI2gVS9f6PwqE1sY<uM#@OnorIxBnGBqkefQcX~Gbr2Kn}7zt2#;4pHMXan)m`VMU7r!{#ta9(!eLG2It4@6&UXDGS^Z-2Uo+b(X(evGoVO5;9@qpYivEKTAhD?u@od7Tz*S(HW6cm0@@eVRvU6^%*XWNBJ05Np^|x(M4#;fnQ`_jys(U6~e5Ukpi;56KWmF(}0-N{6~F(<&K~uE~=!AIG%H<1%ZDqOOXzjtWqV;C4)dMLp_4fp`ng*H3o+?1~6b)-VJp@Lv1Omm<wS!vCOFSSoy4MHt&%u`}{8v8lpjyMTbP#Df-Ir5uDjoM{?Lzi`<}&V+G7e0Tzcx@t|poQZd^&rjhrsMHd^ma5#8JNWzm`F~LsSzZ9vm0SXdnaR=z+9`qGrG^Q%<N@PE@JKfM8!96r%-G<cE%X-T0K*KoUUpa$W|?%V@PXXlLR<n~EEt~6Z9x96w5qspTW(Ecz%y>#LQ_rHzO8wJn2t(lT_T^(JYov5#DjKBjuGP@C(>IJEG?WXi}Pj`Vf*Az89Szs&6jrvivX*#0v~jL!-a|&rX054|J$(JkRM0Sv0(K09ia(sQGxQE5N|$p1G_VP^^g<M)DF=PpM5O)?ma$on@{jL2RjDc#?Q=>77Anu5lfWZVL$`vU&)go>{^4KGw;8`>cmG1*rLBsH>$<<xZI0<P@GYwEDpRc^*BrO*Yk^~KXK{F<EQj6w1O3Z@bdMG=kK1&(qU<UwU{j;f8e^Xl%gMKH#)MMDLZvCWzrWT$85CcFCc-zNDky#R+crV?!**QX9?Ifdidj}qS<>)P8M3iAhl9Q%2klu4)=o3XmLauzkGKI!HI34mmjGLvVDEtPXqeQ^1`wvMqVj-7gAwXYdy|FQqqVV`iJ0g&78uo_&}K~_WS2=-d?_XnJ@&Oa^TqNu|q%B#Ou0rp&~8ZbBYk}ozTxb%F^OH&7Tzg848j&*|{g8o5;F5{uT1)$a#GC^Xq4qZ+!HtX&)%u_tWI(<qrfvhzt0a$(m!i*T<5}(@Q!C*ly8KT#KYhva^CQl<WX{1tT0d$?h4zp@G&2;`B#HeTElr-?6)S9a5sjW>`d>`NX$#M<pIkGl*Lr^5GRcw#2y&y2QHVpwFg_9PFdEC-mX5RR-yxq?geEl?mu;0Xp=upg4EvpHeb=Wnd^KIlNnrdy9m2i-2_92?0X<=mrGhw4P9Q1p5FK^dD;J6nhyLqTLWFwp%ki1Etwyf&?jzH?v~FwE@gcLj=|iH_aWyy%da~L4|;X-E6UbvK(ZdcH9xg$BRlTuz9=vgL6xX9jy5_``6&-tbJB)fB*OYiH}2dTcZIbSn)B=IIasYOb0g$)EzvZ6Nor|KE)w{#+=*`Vv*XEhTbo+_l21T5>cYI&OucKS4kP3rgIN_6+?@>yv4G68N>F1TL_jipgg}CUkX&f(_phf9*A*3T<>EcZ;F_lnRAV<xb>GWf**nlp?q3zB-zBj42+{KZzG3BvjeJ%{zJ94aD7rVuckW0MFFfuh>)2!h>yelq)N|uc*jn-1w6CUB9s^qU=C6rhA5;iq;_lS#h6b7^^IB$zDOQoAK(+MSYA5OA*jao5D@I)m*_=<!_#uMWVhSwux4RE?o!3L9@YocIe3dx{i7m^Hn*792BkC(-91FFf$HkpW`&tvOrGG5tJyg7{;01vJK9uH!v20&H(>cT#D_+R<V3eqz0jfEU2&0wN&p5Min>PIZ%rkQ^R1upsWpuQ2RlE<j5oWnllUJj^O|2a8K^#-GY^;s0o^3FuFFZ@<v3r;k$p%>Qc)C|58yVr=bC#683v^l4b2jA4u<<)XK{eZ2|0Z)>~JA)gr7FcLd)pG@Hn-8WOp9PG82sUsKHg7V9Cr<Vm6#{g1LFqP($;hJgp?nO{P|es|;MvSZ~wKhg(&g%RYI`!=}`4zW0^>3W<;{GRJLc<pSW{6lk9k*WY2I^(oG>B*w;bma~<hk%}^sQ*lbCP&WU%+4|YoLKry8pZzS8)%2^jl@F;s9-Gp3%a$Fi3fny(()5(34KzOIjEVURu|RlsOkOI)r_+8&1GJY8DH3xod}*PJ&zPWFOo?VWeFWV^o_T<Y8D?OTRVTCJsP-9k-y9|2H9ez~WouB&Fk-+Y>x~n{;hSNi+@S>T8F|Dfz*nIrHo@>$00t!7?|;^l!AW&Ee4sNJD~d2AO{Gu7iiCA9SSGHI&r-+S!og;8v$MM4+iywGydQ`}(=<20)9gVRAgsV4TJz08A&M+;?@vyY8#7DFq~uH&QTQQCT~;<(U5!mtcX1y@Z4<Xc*|yP`b=gqnRn=B?+m%J!k6G3vQB(AF6DLX1)^!$@qi^vnMHDAlouqLUQr-)$-i$-kW>wjxbv71p79~T`rhVP^bum^s=5_0_@AJBC>aHHDqDuR=8@p&o#x^SlpJC}zU~pY)1e_S_)jK{lzclzpsD_r4Hl|XPtV?I&Qc1V#3HDh2P-$Y-icxUbRDdq4o3AsfJ$z%=+v6^C`Z-3}M0sfdiCjGqwWWAklkP>&5T0|5;kb!onJmw3ixbG>q!2o6{{JKpE9U+5jYbw!-N0;BpsP`on3mNOURF<tSp^Yo3bdZ~rg?X{D^Gij7zRr%VcY`N<1;Q-Cv7wdK_a+qpajnC3;yT0T0I8H;&Z@uGn{7T{Wp6q4~YIl9tM0|G(Mig^Q!!=uzfL=f^=2s+pcPGYs<<R8Cd!Z!gluL$r0w6Hj=C&!Luo`GUra6mEV?Y(S~QU)p~jvXeG_u3XWDB7fF&Q26sLtRat?<Q4VoE<W(G(S(aB>SC(;IM0wRman;myo~CVASJhArU7aR<JtQ4`DXzWmt}8>}8kc3;WMf%INjbpTj>aU9qp`2y8hud=?HG5(*pF~3^C4>5W-Q94iidh^<ECl5Cx$q7&;ngJmkj|o8Jb?$Jx$WX%SC0GCq*1ymxdU)s*0lS^Qf)6tZw?gu7)zslcCO&svp}96bUx7jza^g30|_U(-OZ3Zw^lzH<<cQ%QVipl?YYcP~=12RQP&DnP*ws4^`TgU0#n-ofpy2!Mb&Ak;O$E_c^R=i_4ovU6)09=GH7=b6Gnm!-?@aT{XdgK166a_QZpiq<+A=%|{S1s$Rs>Fi1Thsxb_QBcE><(%u*jp}8fag0Mr+;$0Ucny^rHRpfD98u<ZPfela7K1%8;hCj7&-4tz>RB=~FutsS*rpQbh6=j-_eVbqr4VGpchO!)5-#NyrKJiw0T@-MUq^NzXP~iSCmd4`T_iW#5;=$)tRAb#n+A#D_DgLRB%A$hHHj^npdcQ}%<+=b#2|<1(MZ)yM#Fk4ezFf)t4vshCfs9vuU5D3k^aPYJ={CDj|Er|EqOAjPvK|olnzn_nQTZ~~F<`Kxwb;s+gca(glV~+MSv0X}R95fb0mKV~H0m2lRynW607ryz;9sRliQuW^Sj3oby(L&6D|GP`_0(EHGgkN(G%URM$;ms=mVaj1oP`*?FnFp{u<%jwm+x-&`|WfdhS$se&7nPmr3(L#W$<i(<@@o)t3L+Mp29*wV3H=^QlqpGS_k$TPmEKDkp~AYciF6qv)O}yfxjJ{dB3CXn5(33;-Y91P=$HbfNxurSqf?^jWX~viXqGMIPRJXl|WLANmrEd*u>cgeoX_e&lQyq8|gAY83RSn4c&1erA6T!z;&Wj1eduv!E*JBEdU%HR5i4m9K9lVg5&|Xl)zlWoD#L(rN?0w**^-BHBpLOcqWs>44UlWNCf2D^owuVb{uvBL{D%S$y9rjebtg)bxr+O&5jtQ@M&E-0OkDBHgU@uKrh1ZR{fF&Itra`OoGg4Fz?w$ynir}9#szL8ou<J`$m~IYwg)b;w=0e%uihSMBlJN%P&S<XgA(xLY<{)F50Q}CPt+kS03?0lYX}x;wL8%fhBol<5h1CkhD~ClC_og+VHMq>NLka73XQQTO5705=KJiDFxwndV4iB<osAx(Lx(ZeJm#tDf>v$w?aq4;YC-rI^AYTs2b)qw_SyN*U%|P9CfRR5HdvG5XOjQ6!0wd3%cQsc-M|s4|WT!A^0Yy3cNE=Z9Q4w<PjE|&pK}(Zn5H|=^6QKkm}p)S|;d_PMm>AN@W|FwM$61hNE3oG%Q15m>A0AkZ?pWEXbI!!$=vLj}?`rnHXt)EZOuDL6kBYF(1kXjD)hluJEmA_(+HV<}8xNK`^v4LPs&6bYl-Q)iqbW<vo48$bhHV7;z9d$y0z3fqEmPln;JVSxLkDtW3kz(*Si9`c@doXl>@IE9zv#NeMj!czIp+DJa>Z>7%ravMx#UGB3(8Yr8ZqAik`MdK~+t>9eZq8VCbL9lCChS`~t5_)deFOgq3WavIhZSO)>iRa%uTxFg+=$9+@f-55g@T!K4Yl(3<F+1GWERNWXg;0@(f0WK~WojgstKIx05gr~XOJ5d>*Bzf1SMG%p-lDG`b&H=^E4vM1CER`N}SGAh6vnZ7YY&E&h+APBrFY$v|R#dWx^Ja64igdH*LM0r^+f7f|dJkhAJT|Ve#PEVWfP^{;DU+bJFs}REX%n(-uRl5YWnzUYJ_y{VX|ZG6yZX;irsP*loXC5{3rZv^SC7%7<X7^Y|9~JxpC5e8hb|JU+1%Z#BL(PP{VGXIeubg<dR*>qaRrq7(bD?37^CBf8HVYP@rTm^e0{K~R}P1+;oH5-&$jnh9(97D0K-9se~3AEGKZX|pmu&C3tNCHA;G{45zGmn6dAV2Yc{bkzdyYE7Sr}uvQ<p~9Q+l_!^v5E20nanSHapl)aXZki?V6^W-*<{Rm`!MzH7w<4iuRw0*?~pCG=ui6LbLPdN+1&hEGzMCF&bu-gDhvOqS5sEo(2sL*4QQB{L+7@UE1(=_)k;`~Y#lzPWNpHNeQ-gS?$nquX*q?iXrVF|<#bAnk=^Z$TCvmy7|>3vbOh7`d&Ba|*@jmk46zQE3PMGdeq~qX?q*I?1y#i|RV|srWM>FNOuxAvHFkE49pO$+?!p3jf>GLWrK;*QW}VzG_ZyNPr6-QzJkd>6z06*!$8%SLid%21p174&*n^jq}IsbW&o8)87!9Y&(ckX4-bJW<c^dXM*?nJp>B4(g;(KgVU0_l=cIjy*<QC#?pNuGFxtlM=rM(-sB7DJ;}nU2Kb_se*KKP=Zp6vA;}zKhA(i*APBri{(txj_>o^wogC2}T?w*J&N31MhQEV1AnJUdSyr`aVRmkW7g`8jj0QVq6ZE-9!Un}0K3=aWEhF9sDtZuLY@i7+6Glo{cMD&Y^5A@fyL=vDNnX??cQVl^EEqleMCi&d-YcY#;nr3k9X07;_5UgHOp_}wR*j4G71k$Mvbn1{!S|+-!eOBonzu31gKNjzEH7@t4QwEnok5odQ378)9aQr*o+gJe#YrQ7-n&A9>!So8I1J!_c@a*Y%a&o8#|;!6vMITYbz}VkE@<iM%%wBu(!>+FFyLL4yq}S9ZoL9G!b#g<*LA?PW`s0NTLxi83ofu&(zk{8&;N{cViGIe<^TN8>ezr*CO#?e)8?>3Yv^)hx%c7&Cq6IM{W$T>Y*!S@O>%jS)9$k0II#joY?U@qgM(&{8dVRey2a^S&>|ZwZF`+|Uc&;m1Gj{LVP2B9bi5sq2zpVUd3px@0)dqA^lgrQfhvp-C{-!dLaOhLJsTWHSF@6-`s|ZudAEhLM?n3$<J_)nrYR`vDgw^}1$6Lt7S7gdgE{@~7W{UejI~<c(i|VW(hY*!gxqvaY#=xj?vcPKMjQe*&HS|J^<SmUI8=F>cTv}+=@<=3HkNIY_f1<i=xDc9o7YK<<({lAnhKo$C~2D{F5+S6Q*bX{Z4Xm^mV*n?FAukVM#n@@l|<p_6T8yPXV8em27Y$o`3=3R|Naegr+#xzJeB@8I(UL+u)xPLe@yO&ewGM5;y2RxHvg)!MC!)`9sgH&2h>JpLV@sVvUkE(OpBqyTPev-GEvPoKJrK`Kc+fCR6e)V6>^NTD26RuBMR%9_+3<B=*qsQP%U3Li%phg0#(@Em>eJd)pv0C$dfz~M=}FZ#rVQ{Th3%3?Dnu)DRnaIVoJ}D{a{vU96Ysdw$sgq0w{+`2?Ta~j}IrMytyspxg&>12y@llX092^MZGaQPlY%60X73WJ?{o&CGDXUwm(Ez8QfTHl{K-;^UyKyV-T`Tq9<oVJAwWRPyN+yAR8puR0l@5R-Gvyo9pg6tYk})Xa15-MgU>;zqxHbt1zZXSycRqkbU82`~i2U;v<>UfFF}8`@pW@H|N#AUM(EnVw7JD8!7u{FTg4IBqa}ty9#`v9vi^3vh4^;VobRk@jlIH1_Pd%5G=+LmHhm}3_VxDe``*~r?D{N62c=UtP<#e9~~do2mD0f+$$G{);*kMJ%4ObWB(h8jJ`Sl=2t_bZ~Sj4_JHrH_YWUY#Nd3yAq4z^OdBsY{P{RcfG=?L0-wkVeslikZ!lJXKS>Y72ju4d<}a{$JUQ!|bKXe)_i=>quU81#plqVe+)%K>PPC;<@`UVZAXa%hih}5HXd!j=#Byi@M8F_V*g>kBV~GOiK~2iY?wPQNY9t06IECOQ9T=Od6yWeU2l_+1gj*pY8pYQj{W7r1>Mtd47>Ll<J?9pDFI?9y2H)~ttN0B>QEg))?Q`}X4hnKI!zLAEXt6kzk{*vPwQL2P<dKMva|G<ENP6ew<Ynw#<72^1X;datH@HPVZH_<JNu|D4LZn{eAqyq_&4cgZV319JFAy*;s9Zkd&mQE@IGsfb9OHJ%F6x1?0f!kADtY>aC*w#7z%$(}w;_`sigz`eT*WP#^O2z#pQ7S5kPdkv@~pMLRe8m<t4PwH%=e4+kQ+*oV0V3h6-Wb*vz2<63`oo&O$@aOaVg27M8UpcrOrI6C~F9YI8&wAbJH{~Q&=_UBtP?oJx*E{nPh|#u(Bg8R+`jhA=lIhDlW#&6E&+N;)=#|52~+@J)<0?_9$p$mtJYcUl<^Qh%ysSqm$9ll8T@f2_$$BmK>!8BifYDjw>Is--XFik@qFn#Jq8XdVxhsUYf>{h-0sZ<Uoj0YZ5tu4N3tO&2)Zp5<BpLHQMh7$*|QbPBO_{o9pwKt#CHk7SUjIWzrcZ$(i>){>>Cu!N@os{v{_)jr8lrF2Emnq|n8f>b-+ka?_Rba$M+C<A~aglV%Tdnu{3Q;|$yKnfD8o(q>&moH989?ejkQ&6Fqn5}M4ndWiJDc?mgc2zuIq^v0>AW(?ya7jf>AdA3dnI1JUMOEdQH@dXTJV?Y<;C{1GCD<y6}id+9FI*ZFH&65iKAE#@A=lD|OLSCgtcCi0}N)wOHJjq~DQd|_4iZHFCGR;jvL)Pf3>=2<=^kbQ4eHW+M&^LXWw_uTrYRKC-ZTqNBo4jtasPFqWhtHB`D3Ug5qsC;Yy{jSZ>Nf3?u86au$kM*A(kO4EvFzhwNQ=B1nr2LgD5XxlHZ9{cs=B_(x*{#J0<QGPZmFh^Guf@2cgsI2J%vY`^s9TLUFKXnacV7W0}y~wWLXXpNTFVt^h4SdaZCYTQNtgKE-tbpug9iNtExpLQeTW+TSRS?CUrCB2`pyXc6ri_S0=FS#p9de`mCx-kiD}aO4Q$$;K;<W*wBEY5S4YAhw!fmQA%myJ3^9VRiRF{n{g0)D=}6|aAXO-;ygP`Q}}5d##wn*<yD#)uiuQ`;UYYzJ<m>ZtRVkJa%^$&@Tfgnv@)F&Trc8+lC#am)S{pNj{<Y9{3FPZ`UF~@iz|@wBI`lSqX9zqzK+Ibzye(v4OI!-2=X3B*o``jaawglMHr|$Dbg-VN4bHyUXj)CL}iVSGW7M(#Ca7pY1JoV2ZyOI;;M+MF-zJsD&Qo=Sq}GWiaJfwF{?6+pGD5?HS?h<`;r!5EX%TqhoY#GzJ!w!<zt++MVTaUD7&;6vpgS%wrSd->dP^SlC(!U8EQ(HA%c{bTr;Hmo5rejondZAW!pcAR#_NuC0|EPzKv@+%sui12)};g;vr?OrFL|2mFT26^PZ*==ERZpr~yBXgUl?-i)t=vrqtRsT@GvB)??n5{g6~yQo{NUO<v;Ord^w5X;t?%Xs<S^hA|tHv55vm_T_bT<)<mlskcgzHHz_ZtqPJ+bYf4rBIh>Q8QsIeuOlafV~x!T1vV3Y|GbF8vMk(DoN+xP7ckS7ovK9?#kHSU`|H|R2omFpcPJ2~igOXpjXT(^xqAv)I;ySdc6Uo?@wJxDB|OcA6Mio)Wy%6_?IRz42yO_e3_`q4rZ7iCRmu4wCKUJ^SF#@K^?INGhT*+x(stYd$#H|!izm*Dn%$y$Jhd{h??+&74IF$8<I(SzFo_es+CMA}$b{@&*-mi7CQh@A?lLLI&I-zNx8VwlN>{egQ?exwf9I6KP1#RMp8DR5rGn4S?5=!=!uI6K*Kw>Egd7#1jQj4r=f7X+76x%~rP6Gwz{h@1=3VK!U70w?k+!(N3T|UYa}qr5ToO5%&qb!V@E=rxc>1#}1yAqc)9j1OEwXDmEPeU_*~I;gttW)4rBcJYi{mkHv6gECTAQ)siSN!-Uov`XH%Ri*(h*TIe<x#iWw0Z9!^>OjzPR0DQJO;~xCXzyneB-3$1pJ12xOUlu;bxu4Sloz1UlGHHb#)}g0@M}!_VCbi>!hPet^06(Q~%f`<lCbRG*%Yo_GXOpkYO6QUeB?MiXT4EMLi&zl)0}SF#l3S80V7hdEI4agMN~)Fl({Goo*1BCw{@SVUc50u5{@<P0yv>LdX0;TfGsVF8PYJ7~LJMn~0R#Hnty?Zy;@74;byHOvb|HGx2aCKefUD><&l<=2x0S6%9yZc$#u^dm}_=p}jgY!tP>SL19lIY%y)0#q$?qXvxDQmMoT6v1gfB`6CB6qv%qj^mToGVElRv@68O*YLYrscQ1L!bkYCl%QRn3==CBu*!`qjUaQV5~Xz2wu7gE^G@|JaNT<DWs-Ec5Qu^W!7{)-4y$+SzCT@Pn@3~vn@Ih%L+GUGp4IAs&cB#wxFv<<G}eUTTOz_SPh?SCVn48lloBn6wO&q=AjAEJ9*Bz`n7frkB-hPA*f~KjmSn%-VdgK$ojz*#k_4YpnkrmVtTkq5;3R#-q8@e|Kj;h7#Gr4AS?F(Z4rkUf_InuwHtnb5Xo2^MK=`vpJR#1PrZ4UQqnwCLZ=g~1O}pbhBpZuL{E2Bnh(Vu8hSYQk_%isCNHtyPJ7M<;@FfDo+`9Y~IyeNyC9*y1z;LRRq#R%W>d#H^w<xIpBL5rw<-bRMS!1Qj?cL!7kIC(L9itjG#k2Q_X``^E5LSnK?-o|DDl+E})PkrPFNPVKQMDw>CIPK@HWo8*(_Lo(4XW7@7gR2z1<7GI9OIIdJf;^n<WWLAfxuIjmRE({R_=W}u5?3iPMM}ak!mY57#Jww`Yt2@KI6WHRSLlc1}-JzVlF%+aV7p-i;8wG!_XI1^b1pBmigd_*b<~C=-*!OcQQJW<`+2CYo~+2ol1qK#$lg7wu+SKw`2j$UUau9X<7)NbqOUI&!At1SLndfIi=hTwkMmye$SEf$0^c*C_ae~kZA!J4Qi1NhIwMiC4TKYS8v0c<tTSF_{TiJsUyw{;(Sx^&m(wt2eP3}BW6m=G4R7CE?p#y3ZBA3C^PCPLTBcKFFYUW_Og)cYzq~7gWgvlh-PqHg{WgGS)@GWJ;(JFeFlomx18mK1Cf?&8pk0zR0gc{w)sSoxwg(?9_JX{Z(S}k^3}}womAZ!1hs;5j99s0G#p@GwTR5w#Cf)Z1~J}aX)Z#7VYp=vof|81>?MAVj;@O<Uqi7+CTh#7N{PbB;68$8e3;Qa#&<DZ$z<*cl>+~X`o}?PY0N-%J8h((JQFgB-fmzyP_KHN%A2H$V!%q8Hmpn$#p+jxwppt#;9v7MrGfw>`k0A8+M{^7v8lI9OY4uqzc3q;@IVZhKOFPEk4&UB@qOqN+zj=mWw>Vk1}Ok30X9!o+RaJh$>UN^9#-S`)^#WGNoXSKf#8Vbjaog5e;~!%kelxdWWw<@7`?%5+NjSxGtb6F?y&{n`Vlo3Sks~n1&>s2*elQjb%V89@=n-gFfO9{*)o}c4~Wda`jN+@DOgjj{xMIRP9QQIwQ4&#Y%>w4h&axDVl2rq64>X)h5xAjq*FZ{4Q;mDQu8=NTC@fzYV>Z|I2Zz24(WszVlPtG;#GC5gJnCB><r#1pw<m%MzO5|Eb-K79>%AQ2lK$Z9Ye2D=aap;XSd8Lp_6&XYY)ygTzK~7z1d_gLHf)kMPwJvgF%eLYA;)A<kna1AD_b_(^QPykj|#QH5`3~{auPPvtuFLAx~n$=X1<F$8oY2ZZ?8NsD^}LYqf#DgbD*P$#&|;M>HBLU7Z+mjFNv1OA)wTlJa;|!QC%cE87xlpT+7rLE&j3RUVA4)Q1mU(%&sn$s^+j(R#?-2OP95ofnRe=Fgl_o;MDbQIr0<6}ZaGzh>0@%%*Fzs1TTn-;e+HO@Qf6ff7Q0cjA@9$7~iC#JeGn5!~ao@k`KzuelFoSecrO@TJBzW(~#o8Wt8^L6dL{Y?%}hULUjitSE{)%Z=kt<1@j2H@jPo^k_=>f)g6cNlSHj(rDNmP2iRSYjBr2CkR{8cuC!1oW?Uy44XD*IasoWc~0D1AWAWKdVwee$5*2?u$j_@rp46cjl1iBcnz1<BT7l_u#R{*^DakfS!{?Eg-g(n3>=k!+V+MLwVF??Mg(;=YvLO^W+h?wu{X|8NO^d~cZ12Bq7IT#J3J}}o5waAb`m*AoBPO;E@a%4=~2PJFz7K^K$q<|9CWh7C`EmuRChcoceRbH4vLUtV4S~vfJL<lb#bte69(fdtSEsO*1J7GwkJj;QhH;2_dB+)NIPemStHR0r^klBAWh_G@v(*2{NADEv2#Y4O4p(9TX@qgXH3Nm-Q}FE3vL^Xz6X<;l6Y)OwM+^KLHRXo5wPh%aDsP+=izSxmQ&VmjM~sWp_@+}QxYmtQ!zx|=X2aXanl=Fs9EY%$~w}qV>(E+kwE<s4q)kw;$Rx#@Pt6!2G&fCrd?vDL^~-Z8dH{=^tqX9Bu&n^7)zxGqA`?2tgT%*+T)7fQu<NhW#`5R?uGKM4C?B;Cs!!)^pMib<N|#dF-;S6&rKoXE)U_^*6;uajNA9RQQA1l$wyxix{VwTEy(F8iCIT)73?>`U8#OMy9N#k7a3o-q=Ce^;F0`oc%)Gc+rk{_*3UoOWhlGYTs!+Ac9_n8(yf;AWj5%NKe7TVbk7hjQ#!>hnV6^GbS*{ROP&Oz{r5ERU8)>2a$r3w-2O<}UgZGMTr)icmrH1HitOlNjc_DJ{Ufi_!Esa{TMK$O_${71ZtL@HpmXu^SzOx=!1;_#Ya*6}g(r{cS8fSwNjGz|zxQII2Vxjw&Yw`@S$e7kS~<JkSMo<RcR`?qp)YZHMGXPh*org9J#i;IT~J3)m~q&ib9eScxTq(N2d<|yI1x;CwnZisjti4;^py44;FnCjNW$wtLwsR=Sj44CNJ)pZh9Y`V%uKPZa28Wp2WdrHkx6MY(lU_E#^9FNE_D-7+jx?Px7b=aCAQ@r7MJ!&@@X-P@riKoN(!88Tp0E1%&W09wDbi|Z$UuMRfXB4RPF(C$F+rKT977WcFbspuBH(ZBiw=M!c{b_Hh{}X1{csF>@Wcx9fSav%}LnZd&?12Ai75l@<P0lkzQPapRtn|c_5jRGz+tLu6PXSz1}RqzA$qbaY!V=8D?GUCJgDxt3X6HhOqj$W=%{?PD5O%Cgdm~Ryq13q;%Nf=nROEdA5eZ2Rf~vF8vLqT(&|gV8|P|A|ZSJfp^<_4i9#JCb#ySLbQi_+TgBsJ4z_4aj<R;uRVqwVAf_Erwqr~Sc+g)v@#->SY#0=!O$^St_h|G5uq0!(zVxmf<zyy6S2Rsj=mPH?L{482`(&6`$J0$evh*@sF=9srdx9Wi9F!}_@14}Jhm(lSP`Zu$u!YoKsq;L7VpZe5$vY_hwj;Td0_=iFa~tPC(d($Wi~T5JBT8>DKtqJLR1jEt;xG68|r#YhAdCpGL4F)LFTKvXvVb4(g+86G-=xA`Ow5!(bh#fL}{N#B?8c{3?yU@mvp%v8I@=Hmnmf;@2gzC>@jZ0N=o9$&d-t2H~1ClKH19v8$qYPK_({T62*Bjn{Km8xHk#TRT3Xf&he?+kmFO^RA`8#J%>r!vsBXFoJ%^+Z<p(*m;NkIXc;e@iP)Bi-Y|nHdG@JE)XfRqe4DZ`m_;F5MNVj+Q`s}FY9XhlfJ*%QUmxr9^wPBOe6@Xt`>NmPNUtxHA*3YfWKf{%GzqAB!1%{DolZA9ZiFyYnRK|Q8;r-o$`*tBeSLtVNY948-I_WUdH9c~^WbO(5DPwh8#YkR5u=c(Ig`|_1ln<-)Yft7U<5*JqI4?q<b=a%lcE`eM>t0iYSJb|p`{rbfhpwi`Qr{T7sDVOLr&$|$)W_rl`H`;zYs+#lEQwOc(32QdVl%s`J1;6N+0cI!@S^B%|`k@UU3Slwf7x1H{@&D?Zn?UI->Pbr#UJ43p}rx<SmOy(-cID=}47+1IL&|z+i&-1lgHqW)3qQ2}v&Ms58&ZIzyKI?qhRJ2~XwkLQ4N51K$s$x+=#MxOQ;Yl7~DKJ8uheeS>Wsm>56wmXOpPGeyZ1Oy^38#}RwYQzDU!4OPk8;v-6tB8T+_C2i0tWT?n|uBru6l9T0hW6uZ=k<JHYtSqDN%dA%S^E575VV!1jW^ju?Rk1c6zYO0vTAtlsZZZ*iUqobthIiS*Ldqx(CmMp;tOA=+0(qEC7^4|W$WsU#R$9(;od)@o+j3=|106R5ACxXbPM;Y3#mpBsdO%G%8fn{oL1KpygG!T3N#>D03$h=^_oKOZ5}BDPx|o}_oYOo#LLvj+VLYtFjHJmV4pW0>3h3%GVe!oS1ysfcbdq3w0$u`#Si_`c#q>37<~heh$?6S4GNlt6E@7Hwj5RT1>YV0x!bsYN1vuXqZ{OjpB%}`4%-78Ed&+e|osdbaoVaHanqD2vp*-ST@qg_LA8ZZ$@$Wv!Q62r=2%7l@xts2^O$sP9jrARaI-1>#lhl_)oaE|s{hnaS2TR?K?!de&PQ1bGbz_3_S@J+4Ic~Y;=6x#5JR})3#aCvOAkSBkOa?;+p#l1sgw<o4rP4(vle_={7;rMECs*3!LQVLiPb&?FlEjeQ1@U{CQOR4A(N<1Ob!(Lxo=4tdG(lp-;?2WamNfRUJzR5oiaWs2^Rjln9j>*%$9ZSNQD$GBt7d{Mbpy|b_`^rAhPZD2gMkJs8FoBSo_)Wg2a>fpvwS5?IWH#c{NyAMreU;UW-sThK-4Q|Du3F85FV8VwRB-?f5SKup&O^%q$-T1c12VEdbgSu(qw!A;v=z(^uRy_ts865MIC}hl%$QA<{}t+X386d8t-cwk%&r=V_(}X4G~-~G1VC?(wl+OBDY+(bAl{R7s>1;uJJf!f4UqUmW7!#WgP%VARe;Du~#*(<Wh-Ob13Fn1PLJ$<FLvR;Sttmx&0shv}&e(aJ#v6O1odhLkS`mQBhisbt<%OO`RN2^-Yv!ki0XO;4YNl!HBl0h8YWoME=X$gOF5lfO$)6@u|7DtA3oMma-}>RzPc~Z8##`LUQvEFTvX*NJOe|@i7U_W6(En_beX|Loft}j!+GL@naAOU+GsHjV+u(M<>i%k0<6y=F=h!5i-}BHqW;lMxlD-GUFNn*l_6dnBej#IzrGP<OWz6#hLfM5~eJdT)?5qGBJzqvZGPe5ILee<;}nf+u<|1pB#{FWlHEA@gl%#N58o}%zF04tcib!8;T*&u%}jWM9a;JuP!u8`cQe~hzmLKe-#cC8q!m>M|=g%Z91`z>CaK?uaL-4F(#oRs+>v})2;ZPJA|Ewz<m_dDdzsR?79DFJuVmeyj+9O5~sifvYA9Cr9dqsM2f@5P%4OrIW|jtdA#Zoo(mWF)e+Mp)$z9$Wc)~1`of#dp7~0fi0Wgfqro+PJ|=z!ce<yXH@i(bz$7j)T_G&($JlCb-b7`Zt5h)|HKC(~!FfcYe0x`rQlc*q?bCBV2dZ?#)WalD#7tIr7SQHW**dv=X7Hva2_`qyF;BvY!qjI8Hf+YJ;XR8p`XIMNXtKe>&U*w%_IS3ILuJnlI_KDLNgcwksG{OPnB|q*f#Lbc=YjI@)~t+^(io`Bh#3Xz>9A8sR53!y#6=fN{l$ibbr(E<;u4zGBbXB#ERd2F>xo$fv8Q&&bXJKhEU&cnN0qV#cMC*^-6tkkGFebx4>K3#cxn9^%?d|7rh=&cuoe=LSr?;M3S%5J5AK}B<{%d6*U&lPqp%tSQlXkYV_p=a5-K)D7fe(zU5EmO8G2{NI<5r>Pz=EeHh-n12$}OU&(u3)@>aqy>NDgt_(G@=m`T$B-Gph0-^y1eW!RX_G%*GoV{xoS2V&T@xh?QC0pmDb%C|f7SMhL2vR{0E%J2G4>@o9KXU({hbP1@z$j!XCyc8yuB4<r!_lA@lV24ekXdUgrEZv4Q;q1Xt5*gxUzIQ0gU3e?cpJqEt=StZ%AozHho1S3}Y6O%dp>3$ChR_)mb!tXM(a=3EkU6vWLK;=?jFA%On=6bDW<^aK!VesejAHgb+TOpyIWke0j$(4>Ko9G?CD?{r9iT{ankJ&yKc(*>=@&)pI;BT;(>~<Uj3&xF;(K`7EusUqR4_w#PLd~sdIZpH#&p;!h9c>|{njhbyr-Dk+F==PzX?nc&2g4AOhQgc^JQX))UHr43k~eXdhgOn(qsXX?0A$t5<a_#y$5bP{29w_whUq@fcM~dS~EzuHmwKFq+T%c%1L2MyEF`52cb9`0K-MnhWZ%fA?M|3W`D~@mIqLCoyLwV!3L;0t>QdUiV&1q9A8o;d732RVp6WoHps%(?e5Se_~QNGx_5fCb?+q?3oo>=OfjF-NuVNy!w0P-MaSLc3-o}@+h~H;1j2kZ@;Gj_CrmQFsNA$458W}IkBvONw0nu8lqh%g#lkqvfXGlA2`P;n=<St@4L55$&uLn^p<ka;K^@{cs^Vxog*$M<?6!nYpe1SEwJp{lbEUZ{alflpW&I|82mC+QKA3-8csf2Uz=2@_F=3O#L@!1xSfChpyX9bS&n5%6kg9}eqiL0$QXgz$hY(SMgK|c4j`E?UPu<RLcirciwKCMAuWLHxGi=Txwl<oL#%r%-3+W;k2Lcf{mMG1=GxxRd%nbt!^cWGynI@*9bt>s!G3c_?0#}s0vRFb9$78qLhL-k$P11ik*x-SIfKH>3U<Tz(xi4gZthCITqhbu0_A3g?seC#L=3!Y~us{Ug(3UMpEtxav<TTvjy%vS87h5o_&w|`UR2dQX1tG>d&R$NcRv#Wt64f%04LTJ2X66ZN(g~4wMq3aK=WINLLTq$q!{XR`F<y)C*3)9=+M*}EB)%uDHN6aZ%SbAVZQopsE$BZoYCZH5t$F)DH=mUNmcqG<*O!rxHNu5Sp;^jg!!XK1MLYe)dq>?TGVw^eIJlx{n=>AEXb~kBD@vAF)7vK;mlOk!!U{w)H+g*R+JrDTm9TioZb{YCW9Cv`w!(Dn5t`ltTfQ9e8P77b259j}V%CBr$?4~-FXS*9Z}m89pk5%)FwxwE)2pZO7FgcRj<EPDagsye0i?5xocKwBn*}lWEMDZeGARy|N-(L7M*~%x8#Sy@g(2kD;Fs36R8+^Dy2Meqc`UrSpH@m=;@wq<xfD!bUU-6|eGGWWDMt$3O)+dC_(LZumL$_xf@8s{IrYac-cNW!n?q0Jw*;t#m*!cBBzH=nbWC32jJXo+IHR_5vN!A=Jc5u?AQi`&W-P;u-;9@6sOW96*f@+%G{&5k6Arw0qF4sD_{O}krS!iJWplg?jxRLJCn%K}3;!BqlP7!5SyjJ?^FOP@*(t=DyPQW>>kW^hM>=-nSvlp*M@l4|^|Z`?=6%9nA$gT(lb5!pSWj<3V?N>OpB`7act8XS;a#o-xo(WAbCd=i_Zx6;B3o~Xu{HR1CDO1}%uuS5PMP;)Jql43ui^3eG@+9qH$^GVhHzre3};ZY<qrDbW((u#$|g+~dlR}TjDS6w@R0-rX@1#GC#!??TI5ACs-FX$NZVIn4<*>6DC!QI<SDgCD2~aW0iFNzV3;EuOc<90{}k|tfqYQ4h$Bt6Vy1=TFH%jjH)mrZ#075DY+x<g&uc<i_LC)0<JAAm_%orU9DSY3@ECqYmN4d%t6e)z(c}vPmdY}+xeYUz*}||UnSwlpM+pvt9aA8Lx}SVZ*)OG>?n0yoLw_cF(EB(6{8w@jGDLd)a6UCtt|2n(3swc@^PNrDuRVzdb2g5lL0T*zBo(gBY1(v4!|dTeQMIq<k$KV1JWs<(!AVrf!k8AeSxAo~5cR6fwh@Q_sL7FJ_KRidFrZPaoIfNP3DpXMG-Gp(ASkg)Pa!gI0O_+(v3$-T2KDtVs}3&k>o@u!*y>@Hg*$^F26qy(PA>Jh6lOc`Fj=G!J~?U($6EDH0$zh<$NRO&UX8{LU)QOd(0q`dL|moKLV&FBbVcs=Q>XvS$;kyKFYM)&kPlT1bte$(NPF1tQAOFX7SRcR)?n4LBGS+<OmQSdS;o~rHrSA_@x<vgL9~~BE6fEj^}(N|jLa&Y0b&2hy`!>7@~DoVh?B>`QL|68KB-&q7XR#yE&qq^RHt<nrRDLRT6luAP2f&aAQg&j-JpNyo>ft#Ssgv`bTW-t-Q0GR+cp94rp#fMRE6fTX>78D0E#jEvx6=9#~;;Bw<NL`aW<O0jvJ&Wc@Rq)kR4_|nFeD5t1ji~X|uY+QZ9SoC@i}jA5lt#4i8xkP!V83`-R5E78YX?8KN%C2{F}WJYv(vI+JvAoDs&(G*He9{$uOK*LdQ%W&)c?lc>vwIxNbl%=?t{jDME=V_>bG^FWf4{D&S$uHilqY#+~`=EJ)}o3fi@I+=HJZJ0}l@SWN}agfJ~+VY5(z=Vyg#qDb>94n0&SmvZA`XW)>3bO756HTmbrcpql*TT=sy4yF~II%Y&{!eJ1PpSlHyANA)ZRx#*Pw}VoXSM-{i%GzhnSs-+SRXS`h75nQ9cVLyst&J4#?~S~n7QD+Zs@^4MC_RtiiCdI>qtFnp3vqvR<@#sm1~#H!g1HMV;_<vOVT?3ha^j`;b^g6&BGhiiZiNgG(amZrc+Lvbc$rjsqrC3!q6s#_w3pziU&|i{g}r^))ZaTL~RC#E{^iL%A2T6ifA1BA<NRTiSwo!iuQ`(@wAw-G{mh!z<8bUZGiCTPq0#=>aeSf6i{v$Fu(oQtB`j`HdNVsHF9D3a&oIKs#s^h8%It<s;nrmfP;{oybzZkTxG{EK2^UsE-+eppM^vYs`Z$L2`cCOo+juF*%#&{{R3JJgQ7Pn81a*3JgN!p5LYFP8cgLwGAzyeNu;r&!^`xEC>ynL7TiiOtkQFJ)IV}GnH{W1spNE%)1bwE@4}2gGAl`NONzCphmNogaZNI-2+yTH3CkR2Rdg1|NmV72ujNYoJgLjG90$BtlTQ}M=@)V4&ke>T!`HYJn#8^dHqtaql1gGH=0ihQ)D1x{-o6WYYQ=-fC>J@mF4_oNJZLz9nROoi5MCJNYLv^u<64ZWy$ehA7J|4fG1zSSzaFMN)mKnuOH%mEdn;?mNLM2nvzoBGz7B}o4V|n!qV`Br@UW6|JqEa>$oKqRUPh;|-cKa{!ni8(Q`nRwON#<F1<r?UVo^d+jtDq7imB`?E5n6Ny4C{13JkgAmQ<mtk}@vhq8@SXYu49IT1IgR4ro6nO`XDb$27|0e$Lj>)^!`leba-6>A)2olA)-QuFlJ0Y|ExAh9qgzvc^I_^*p3ymZo)4r)3(_GJu4|WgMqTTqhxVm?P44$KZ^Db}I{JG#}%k?AtDnqNc0sB5LubaaT8ekyS-q=XsswafL&xsve#Nlww<@W8T7(Wkr`SOtdTaui)~t`zKYARP*~6MV>|p-oL1de5}X5$n!Mq;U;-oC&N%>ebE<V4}L7H9xU3}G+CZCZ92jY;JxE99cyqrkWs-fN?I|n@i9pgA>i>-IQ!VX@@b7n16vV}qT|2+`~TE=6%r9I*q@M)m!fHGP?;pJK{4k{9KmgMmn2EzIM2ZC7Qva~FcEu;>yxH=mKRA8(tlKW0o#>%QoJWWikiz7+T&ubnCEtp@gfCyd?Jnu7ND^ydazzYn^bMm4B#6^8T?<-fdpn{5|s^zVOMkn9RNGE>$Q)}GC3+W7<vnQakyC_42W_NoF}WxvMRI4K3QHC803_`yCU?hqB#Cm6#4YrU7qzR2zlNmO^jm#B0S1%)W_M77e!W&Nj*gM=sS($Vg*GZT|>;Mqp?vr7F{)zLzH%7TlZa-W<}EGRoWB{NL^D`RShCl^sS+VAto0?^D^UzK|6Gq9M4BkXePrTIiU7nXSYxfgFMhe$Y6vyM-_#V!%!?^VOI}Lk+QTgniaK$rK#m=7*0)g##eIS9vEahAyBArPOBZzKhf|?qmJ9vCgd3yTe-|b#H7e<u|mXX*d$qjVeOlz@%c8H5zb4*f&>spSBF&+eQM*|!g9FTvLhF+n>#7qhwAoorP;RwmNq&cjU0G`d-E+u6?2JHoA-nlGBgw(A;oHzC1<jc-`B~woFPK8qn_c_ag$JnWCt0ZwgkU+Xy{fv=*jSrauNy)(rosql4i=;<b17%S&3SZt7hQ6Ti2r#PRVNGYLiwt+*T=hxA!v5nMQ9)qKiR=Rm`h}I@BbG>?8mxal}7|uy4HfpzePUw*XIMgS1@`H0_+l{N>$`LFMV#F9tw62|ZTVGz)C`HwK8I{}(8J1LuiSVFnxKI~tuegAP+1=g<vV8X}it(h|`N9z8^s-q4;pI8wK_jZWh<%yJKCg^=GtQp<C6;0(fW)E8#X3RQLk2B#ipBjj*N+KrgUr3KD++}mG2(-v&w&L>c&IvE3OtGX7^NMJBBos3&lRA}g68l|cvEXU4o31|)89Ov{++XPpBv=(6D1(x%FVKCkd-Z5O`h8C4E^CE&yALVR#4w!Y_Ud9&AJjjJ6?q!VV)Gcu_M=W-Zhz0GK&TzxfFVApb6ZJQk8q{vP<@9kn$5b`vDX75)a~U2w&;zM!gPJ37yCu1@<J)lH^TKeVi8e7zT({g3awiisENPZAW#f^~75bf~>3C?1CJc1lW~9@P&PtV%y+stjQ}h;UaONpXoO1sf*2yJP+u;dp`i;Q+*<6_~n<UxNVp~nR;?oUwE%AUZ>JvZgDD2X1CGDG`IAbte)OainVH>%B)N6Faq_M|F`D@0C8{&;0<qn!TVdt)K=Z_MfA*1OUE(JLKnLQ3bU-AemN@v-o11AZ=`kY7Oifur$AF2wT(bzkqC1~!QDSf&P0F8EYP<BL1(Y9snXfB;mf-{0a!V##`VFK4r&BPNI+$f8uBVSi0FJxvHd+&4R6-a4NEF%b{<g3ImI&Q|L5)dA@DV#wtIdOobrW8~sad?db)eqdnVdl{pO!(7F+g*jz7;zI<t2t^(ZRP3Q=}=|!u$D3}rHI${ctu5xHa++IpS9<l`)j1l9Da0QgjQ&EOsIMVZH=M2QjjUg!EgV!AOM|1`aLkrhUZP(wL{w$L0tD^kR?$XRN%G-ZBauYmBw+Grd|;I7fKerJ$bA1e)@sYgw$FHFUkP#I6+7XYgKgK*oJ>lJwYwS>I)J-2~HpP1VbTT#)m%Ja=Z4Gu;~5vd+$38=}!$^->LWJ`Hyd&zy0aM&lhigcli=Nc>3bi)8Bu%c=6)ZA1_`$eg4GzXAj2;Ux5zCW0>bob(x2isa2rvdGfVGVI~X>2bzrC<E}5-+B5HR?foHj5P2W&iqLc?7ou>d4|g~~k~EY#bFgeQqLZW-Uj5a}=jzzf;nWd~rf9i8zzKt=S&f0r4M3;yc$^bYA))k5oCL_kdT(?Vrx+F{tvSIq!7+fX+7Py$I6Uw_FJ51U#;Fy~EBqYIkMoE|!7-5Z9^0@w2UaZ<wf7<OyV~z-BM7%K8BL&7OU`ndB%it;V#FwY$G;SymZsl1zf9XhK7bCoBn^3Y%$h_t`W#){G@2+w9^yYt#HGVh!F{7%m$*W@7HM~f4;S3Or7E2eaO_^#@y=W<w(ren8~jRV^LbLal15|8H1}+olgSfarN}eE+U~mpY(54*XzUhM7C9YMT9*3yK~LJIXwy0mMu>sopeISt^vMvUdDr2Hvbw7pI_UpDJ@-0$"


class PredecessorTests(unittest.TestCase):
    def test_exact_public_predecessor_identity_and_complete_material(self):
        import base64
        import tempfile
        import zlib

        raw = zlib.decompress(base64.b85decode(PUBLIC_G13))
        row = {
            "id": 6045434332,
            "issue_url": "https://api.github.com/repos/Zi-Deng/FLOW-DC/issues/31",
            "user": {"login": "Zi-Deng"},
            "body": raw.decode(),
        }
        context = {
            "designated_plan_comment": {"id": 6061320190, "body": "T"},
            "issue": {"title": "issue31", "body": "acceptance"},
            "issue_comments": [row],
            "reviews": [],
            "inline_comments": [],
            "pr_comments": [],
            "pull_request": {"head": {"sha": "a" * 40}},
            "commit_statuses": [],
            "check_runs": [],
        }
        repo = SimpleNamespace(name="Zi-Deng/FLOW-DC", root=Path.cwd())
        self.assertEqual(review_packet.predecessor_contract(repo, context), raw)
        for change in (
            lambda c: c.update(issue_comments=[]),
            lambda c: c["issue_comments"].append(copy.deepcopy(row)),
            lambda c: c["issue_comments"][0].update(id=1),
            lambda c: c["issue_comments"][0].update(issue_url="other"),
            lambda c: c["issue_comments"][0]["user"].update(login="other"),
            lambda c: c["issue_comments"][0].update(body=row["body"] + "\n"),
        ):
            bad = copy.deepcopy(context)
            change(bad)
            with self.assertRaises(WorkflowError):
                review_packet.predecessor_contract(repo, bad)
        old = copy.deepcopy(context)
        old["designated_plan_comment"]["id"] = 6045434332
        self.assertIsNone(review_packet.predecessor_contract(repo, old))
        with tempfile.TemporaryDirectory() as tmp:
            packet = Path(tmp)
            for name in ("repository-policy.txt", "review-policy.txt", "domain-policy.txt"):
                (packet / name).write_text("policy\n")
            with patch.object(review_packet, "run", return_value=SimpleNamespace(stdout="")):
                review_packet.build(
                    repo,
                    packet,
                    "a" * 40,
                    "b" * 40,
                    [],
                    [],
                    context,
                    {"required_checks": [], "max_snapshot_bytes": 1000000},
                )
            name = "contract-predecessor-6045434332.txt"
            self.assertEqual((packet / name).read_bytes(), raw)
            inventory = json.loads((packet / "required-material.json").read_text())["required"]
            entries = [i for i in inventory if i["path"] == name]
            self.assertTrue(entries)
            self.assertTrue(all(i["kind"] == "contract" and not i.get("omitted") for i in entries))
            lines = raw.decode().splitlines(keepends=True)
            covered = []
            for item in entries:
                covered.extend(range(item["start_line"], item["end_line"] + 1))
            self.assertEqual(covered, list(range(1, len(lines) + 1)))
            self.assertEqual(len({i["id"] for i in entries}), len(entries))


class WorkerChildImportTests(unittest.TestCase):
    setUp = legacy.RunnerTests.setUp
    module = legacy.RunnerTests.module

    def test_absent_path_child_import(self):
        self.module(
            "test_child.py",
            "import subprocess, sys, unittest\n"
            "class Child(unittest.TestCase):\n"
            " def test_import(self):\n"
            "  subprocess.run([sys.executable, '-B', '-c', 'import check_runner'], check=True)\n",
        )
        env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
        result = subprocess.run(
            [sys.executable, "-B", str(self.root / "scripts/agentic/check.py"), "--jobs", "1"],
            cwd=self.root,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=20,
        )
        self.assertEqual(result.returncode, 0, result.stdout.decode())


PUBLIC_T = "c$}45?QY!0lKroz=!Gv38yQZ|w<K$`fMUyua2-oml5h3{3#o@}&M@MT44c$=^6S3C?i1c8*;7^BBqe(<c7p`5C9=D^>(r@JReYr`oonrUs<gVkna|YU|MuVN?ya(4syvIcM(wv{ZX?%bja6l3Ytv+9VRDsNljK=pkB&ZMU#wHF=F_92>$<E;XA_l{wY;gSc8!JjEoF*CZEWEenJ%!1%Qgl7?MxM=%63(5ceZHcc`RbT*tnIK+h$wVS#!YRd0y_FYPOkERh>N<*p}2LZN}<dSu|PE!qz(5WMt^MYigrDf4+P_nVN}BkEhY{cw(dDg`Gs}<Mk|>&FyMsPUoxTe5JBNWsOs{-DIw@HO`Xx3)+|UV^do@Qm!#IcG;*nx2BlS(_}fHFPE!$alBlc`E<En+v#MIt|#fq`Xo)v@qE58<D;WjuhiXitS*bK#Ys#NTfP%V7Zq<!vB7r3OEz>+dhF=vjt*;ZHwoPj`KPwd<p&rXmvvR*9ZOeY)w`=-u0LMfUHGe4o9SFtxhd3ttR{=)V!oKqXaD(n+ccGXtMw*pw(WWxmplFMS@hl(8~x$q)vwX}cbX6G^nCh?|Aa?q>!@Gl&B#0X^WBFNwJwvx^7Qm{u|B{Qef#tIY`IborzSPa`Ql`euFNz(Ihikx?dkeBJxx}V)oGk8X7eP8lXaTls$wi~d^(%VXRArPI-N{T7Y`#9!>P528}%@m+2a#qr}I@ZS*@n4WIa1Qj?>k0W#)EzoUW3UwF`K2hIJ+hesHonHA!kuXD7+(boww>=Xs7hV+Y{2|Dtd7HN5$wzEviUZN*+0sWdARc-V~;?!77N1H4|gb!;`x*66A(%T(JhFe)nBrfTU&0AhU5J2;}wOs;E02m-`8_VKpenIhWRdIN02FB|r;<Fhef?HAfz-F@f!;FGWmp%zT=kMirZG>mbTjhpNVC~E7P@X<$0l@$(<1E#x>AWy;0`q9=-n+oqgnVc}cGk||vcqon)gAV_eh3!@}yibqBJpjZHq~L)hfDY(PY>dN~^$3v-gSto!=%yG|$)PYi@IwwHJDcMaSfe){mSn|PUDKj&tL{ABnr&USn=Qk=Gj}z?KM{dXZCX<&U=;ggYaFYR*L73lVKRZ<8(7FhQ{)D|(d$EH90ihTWSKF=o>g=;4gRzBcWyq3cJSE?M(l!B;Y#xi(RlUg<G-n<<n0hW;5NKMG?L;;b$j*U?$`613w3vKd#7$cU*25|co$AdGaU87(~nT_^aMwInj$7D!+uUO7neBYL3d0E-~m)|mS@7E0Y3DuqhBRJH95>NPK9LwWO$(2m$WcWVskbW01@oiBF;-koagP)oGlFKyvgF|_txa(uXmT~w{neV7E8Gyy#C&%rp+6j+YREwW12jU#o-*f)RRT9EC9MG<1(L}sqm8g_GB>y`DTdV+NQ0e)jyT&CQ~#P?nuKu<KS-<L;Fic5t$#Rvy-riju9~~l4zPu{{FZBnt5}XC<{6!qMTnO_zQkJ5ALjSEoBD42@f%jw<WSg5~c7Qcp1kXtDl)T2-`TGN==sIB{?8*wqNWw7*SC4Jz87!A!?eY&erTt0Py_dN0n39<=IZM6Gtk$?XBX=PeYP*dD%EQ)EuG5;X0p8G{|(M@>0whQB>sS08D^YY|~c!>lDVQeT{n~X86-=F~T08-t+5AU70MwBQb>yj%{)*`u%Exz^Gv|Ahic&mV2!8J#Qd%up!Sl9Wdyy4EYUct+G1v*I_mr<Xc?!avu5J^`Dl|2eAc;!NTn3!tQZ#DRMH!7@pYyia7Cz!B8ah|AvTqS0*+mihjxxtVC?0eK_fNA1{5T#ZgSWB~pFNup=}Jyt8G#UYi=WF)g?_qtoJck?c*jBTlsbR>_Td>+mPgK@NstboJ~^I^lrTz(^6h$}4~wWJ=C5sjQkP6J3PlQvyzAjhLqZt`m23^k>2ZjEo=efi-Z?`1a!6)u;EjKM<towZC56++JRN`T?QnLa@MFxUusD>lX-Wam0|R0=ImNT6ootg}i~_Fzm0{l5urVuOA|LDf&lCJ-j(nrFRmAk(kH6H1#?IuIdAb<teMnf>TLA30=JC#2oa_n8yhy)R1e%1Ku#7ZOE%8-_vc!+Y(pn$OL&3v~4NE=mc~%0PH~lgNSH{EQ4PvXx5A!imXrMRJ6#`ST=AHvLg~SY1#xK3)~e5Z9)XGeV$9)kuG3j#g$V|^g^;rYT>40Q=K6Lmn0pXF3LhS)qmK!)CGkBEYZa2cv82;V^Qvl@V4{Br`8tcfWj-YM}EBcLI#1gJOaW2)^=;2GT6-s18v&yAY1!&#y>jx><E9qx1@5m@CO3OwYwd;3MP>&=aSdxI#_{=E5_=_j5blRK)Xvv_nxz$_2W9L8mbIPGB}`VXQqKD`~fK{D{|fl5z(~dHZcZ|bji#ArU5s}>H=B{cQ{0T1kr$aw)K-89{Ni&pZ${ksKc|<75)Z9P?G2`=?P%eFslJ!Ye)MRB>ZYnIMCu?HYGj8*tH~Kdhz1?-OoZHK5=ps7brg3Se_ut$MdWPE<W8|BJ%@4WkDGtoKh%-lqZ2MOAMX6zqwJLguEK<^FLGzYAKGRszgvA(0&y|#Eoie+}3(!!*mojJyXHM>x|$Y+#fj8rm0t#U4YVg$~9jv2PO;#rNhl*b>r0sOl&LEB_uraB!FkugH1{wq&Zj;pmnA^;PsABp(#!@4qk*ONX5cQQkkv6m`Ml;2#u06b!|E1q3@c0SHp}zFQ;ZrNtsk2+s!~G3q9+O)olx|e9D-%Btk?1!&ebus$npB0Y<Duei={!T^tF^Xakj6DPaubrJ1Sd<p4D==vinwE`d4U2JIyoXls*}fG78c82!aOTILdy;P{@j9?pLvRK2@rC3$~+bM@i!<HZAf#`4Qk!<i72=LszT?Hs_BkWEMt)O*-QUM^MO|FQZIf<HFk3nHY49gLDEBCiV7H!b?%o2kM_2pm9Hh1Cb^{eRXulbW33WJgO5ikW9=hgTO~$0-mPc^(OD%AAAF6ReLyl3^rnh0UeTe#Tp5|6&=+0@A=;taNM%$wi3<#iV*sYD3Pqd43;A0Q9}K^?Wws&?S6}n<AW+F%P9#gTm}SYT<hThp<$_s9MMi3ip}54*m`iJ?$)gDd1r_-xQsIgwbdqv`{O?f@0CVP<0imK#=3XV-uE8X_Lg!Fu%Cz#1{kXWw9KlhDyi{y>!br>=naMw8WGtH>0koqec2kajfF(KCpTS<vvkx{$MacLBNYt<WNxn#x8g2`bI8VtdoEr=0p;0bIP(rn&8_?2=J>~`{?KsSCj;>uS)f|N?07XD0i-(3A>6q4BvWye-k2pq{h;)(cR_g7cLwhb~$&HaJoBGP%jrH6ms|Kp1k?Q*L(l8;%)lh(G5~sx%cf4d1Bzd`?YD}?Y)S8cmFh{U;Ayn^oM|V!JFEz<K-n^WSKVL78`B2D}bAWoeGpm27JqI-nKIFqiw$0tn27RU9%`OVuDQP5cV!h%Gj?Qg^Sd;7k8hp$2)<+)%C^A`Q7EGpQ4{WU%tPfACqn*VNCIaPy^?6?Mh10tTAHg%Ivp936%&C;`?n^3rZOw6^%r!&*X@<Ja-z{YdCfX(M1U*AsHb8QbG-((q#r`kt>l0Hk9^2V*@du5eQh!s~N>9DP$muqyy(ZZfb>RxP>w-F~{nPEL|?CFc&o|Vw4u~wUBQ}_Bk`W+SMUU37tFXZ}&{y_kk(zJ);MpQ6X3)QU5YEXZgJzr_X9%0jYG7yAB_{Kpn44s!o_Moi`2dkvfqJ>TVUqc1ksYrw-f@mhbvnlEnvYYYlI4r!W9`L`iwbFooIS^`8(7h)ynP0zx4z^E67b4PQPIKxoA&`Pm>=*hZU7=MioXJ`2$#nhs!MQlyC8)2R`AGlfF&?&kd69amG+x%zbRuXl<iDV5|S^>AO1fc8s0j1=c|o7F?K(jVY5>UmIGWT2`;tHNRNsm6wyHYvG#+N)SGpME3WBpCs~O=r&!(nqEz(?9&}`s(SvyEFO&%+~j7>CgsZLB>PelpD3KA;n1Oh9VX|AU4|+*&kMr*g4fZ8B8@4o{sulB0a(Nc#ajQPTGnQVD%ma>n0N1^u5ZiCG~-;03X(!;lWOBM{rw2qg09?yRc)ncic4S@Pi=7OH8J6ztAG4%Tb-q<*vaRnGG_OB(THnx%4nNpX$5O<YsK|QdsZXGa+#@E6M1C8Yv<o`Xh2n8;qM9<lz**T!V~}oahv=CA6Vsa<hSy;Zab_rxQqzeNSV6FarL)oLx&U>~s({GB^nQ6*-Y(!YN&k!l&U#PWdd0D8sP~v-9V`l2zGHjd70=E}fc^2o9S16r_`%U)rv9&9jQGD~7$XFtiDMR4z40@kxhu)b~`S`6mpY@X@6o>Rz-9byv9f6S<eJr3#lTZiz6jcVWf4eDYHz$$k&j0SAYn(U1SDz)(>H!RORe{h#_eXF*o>^=o&ihz{lFg}GVd%oUPvK%#U7?C=syy?LUl@%J{X?N{|}s6{1J`O4b&Od*)snDiF5R4<y|bO9`teQhi3pAA}(0F}+D-0K*P-O(FXnL}QhL<`aBFus7I6E&e(bPF*9aC_g&l!K;O?V89mjokr`(jDeh^u5$4q&M{%#gpz-X)RYkN!!VoiZU}_r<6gVITUm?CpzCr8bH;xYe<nM^p2@i15Uh{pbkTb(vio`*CU&L#3#Tf?lv`(JLTXYd_5PB86@|9KIo*q0|5?~Gy)(EVuc8~e6MNr#98Z11)}vjTbtcF+q7ltI{6+7`)AVcJ2%jm6z5iP&U4&u;}h`|lBZm(WgI0PgMut!`c9sWemc2GBi$5#7@?e%%;mdAzTyi0Nuo|z##<x}dc>dDLzhQ-{v<3acw$Txefi#L--eNT*YAx*q(G%|q4pvITNZXNgTJ!s#_qc^Fs$#|jMZN$pB)DCJrQsnS%rX*v}_fP6T~ucso^m?cYubYNu<+a<B%CThu_IeSPw>_DEWL1ph5<&?YhkXUC)^!Ub*kJ^T%$faYBC`HXqerX4L!(6OzVmr1lFbe`wpF<;jg=z?VNR@L5uj(zidQ5kIIhufKvk4-Ya+>I;P@r&*B+qWSy~v+E!9X}{~hLBkkqOh2Q)^NbIaUP)gk6%b)ZpjS!DX|dq?UD4DCioM-cK4z)lTtG-jX9o${OKTu$V?rHGYpWXiKS<VrER_MDZ&CSn6kTp*lX*94vy94&;Yjz*28KK1o@C<t7NG+hvcgkl`uKSF@lsP?y|%vvUZJ}Zc^&3n+>0;RDj>pSvUc1$S#@CFbQuhMtFV42VkDjEW?x1=4Rk3u^zYr-08PH{^(>jvwMi?4CW%ZhCI=Il9OvAHeq-O0@7nQg5m+F!Q2aE)Pvp-0U28GzW#@d*`)<sxveJ;}7H8MkJQJA`_IcJ&WWVOjq9|~;6kMD}+Um@6cxVvlu1Uq;)QH6CbK1ZVCTLfcg|tkB^Z;gDCUX@{Id+`MM0=feT7J=KS;SjJ@#d=p9i$kr9kI0R@7SlBLBPG_|6X5vZVI<A#UHD_FsAWxkQ$F*2AS_9C4!nMEk6(hm9Lt^$3xvjklUMXnE8!Qwg^I7H{y*zmNjnkiH_1kOJu6JN{3-Jw?dsC{Sb&Lejx(zo|K)0cnJof1wK|N#i{97OK_j{!RP~6zCu!A8L!eQiZqWrRbG}fJ61HKWz~+&@5+X}_cF%(M0I-7e+z)Cim!cS(<0z5M1zq=nve=O>8364WKEEeIu)y4v$WJVZJHtwXlOzYH2VDZPWRnTon&dMeK*9@lUE_4x#;&i+MrQRTy8C&+IP@6W3c{V$a^BW`gAc+zF9+^AOC{ay`cjvf|xUO7zMIEe?H5AjHst4t4ZXCxO?huC8bOUp2gyz$=HyJrd%wD^NmR)Z5mdw0w$Iqam#Ai<lUUd=`i=#t^sfx`k$?9{nc0}jG^!hPQ<*^4O@nVwvVR4_YV{aJ^lV;EOLq(0Gv^UllQQMB6%{QMC4Cos6P(OQ%!g4%T?B%JTK#f=37u<Dk{Yolm=@Tj=@n^mRZgX19oR*@XoKrJEtd;!Fd^OJ;vUQhTMfcD70+UGaqT5?$aA_4rAh3c41%y;JofYan>;?nJ;JM`F`K!@ww|{@@DjGjKQa1_-YaUvg+)ZCY;Xftm-N&yeU-+tomm8z$M!27wumD&}J04jyvsdppHzu^{4dR)rK4K*ygPJzyplQnVJ}sBv-?}?P%C_-IwWf&JBI#cIBh(3k+6L9f!(AhC-AYV-58~y06wm_>MxEMRN-NuCDiy2-p!(@d?Cz8B0C^Q1tQXsoGgU1uGxV`cH6><um?XXID}y6!(KdCC_YqyzD>W3H=g^DxOZi8S&R7CL|QUvN(W80hE<n7bXY%*<RFDYJ*-cz<<Tq?oKZ?K*~a#Key>xx=F~7G)ZErANl9{(!on;-}JD9)(6jF8Xu7Xd42am#_@w;H-F1RT$cIL;TQAeTb~M@1&8z{BAJ6|ESm2JGJH@t`fsobYZK=`a_9mf=$AAUO){tH8Wqp_s2s7GSM>f9AO6ls22I_Uz5`{;&&g+${qP6bsg!OMPxD9^n?!0RUz>5$z2oj~8bROmQOQ@<!!!5^6<ZHIAWMjpVz-5UtF3AJ8NTe&JR8>XF_L24Oq*Cy7SDQUT&R!LWy)o#&qH#lw3-`rQ0<G_;YVX#4w89H7`P8U1kI%lj%`bX07@Oajh&rUzSN;h`{#d(qHY4%&D!2BR&g?0r?bdRm#b*8NKd2FX>6k9Y;rt1jaTVOGEq_VFGv3aPT1H^"


class WorkerOriginTests(unittest.TestCase):
    setUp = legacy.RunnerTests.setUp
    module = legacy.RunnerTests.module
    evidence = legacy.RunnerTests.evidence

    def test_real_profiles_and_relocated_child_origins(self):
        original = dict(os.environ)
        runtime = self.root / "scripts/agentic"
        self.module(
            "test_origin.py",
            "import subprocess, sys, unittest\nfrom pathlib import Path\n"
            "class Origin(unittest.TestCase):\n"
            " def test_child(self):\n"
            "  actual = subprocess.check_output([sys.executable, '-B', '-c', "
            "'import check_runner; print(check_runner.__file__)'], text=True).strip()\n"
            f"  self.assertEqual(Path(actual).resolve(), Path({str(runtime / 'check_runner.py')!r}))\n",
        )
        for jobs, supplied, profile in ((1, None, False), (2, "", True), (1, "/unrelated/nonexistent", True)):
            env = dict(os.environ)
            env.pop("PYTHONPATH", None)
            if supplied is not None:
                env["PYTHONPATH"] = supplied
            cmd = [sys.executable, "-B", str(runtime / "check.py"), "--jobs", str(jobs)]
            if profile:
                cmd += ["--suite-profile", runner.SUITE_PROFILE]
            result = subprocess.run(
                cmd, cwd=self.root, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=20
            )
            self.assertEqual(result.returncode, 0, result.stdout.decode())
            directory, summary = self.evidence(result)
            request = json.loads((directory / "request.json").read_text())
            self.assertEqual(request["version"], 3 if profile else 2)
            self.assertEqual(summary["process_exits"], [0] * jobs)
            if profile:
                self.assertEqual(request["execution_limits"]["seconds"], 1800)
            else:
                self.assertNotIn("execution_limits", request)
        self.assertEqual(dict(os.environ), original)

    def test_original_window_case_once_under_real_worker(self):
        source = Path(__file__).resolve().parents[2]
        self.module(
            "test_original.py",
            "import sys, unittest\n"
            f"sys.path.insert(0, {str(source / 'scripts/agentic')!r})\n"
            f"sys.path.insert(0, {str(source / 'tests/agentic')!r})\n"
            "import test_review_windows_v1 as original\n"
            "class Selected(unittest.TestCase):\n"
            " test_original = original.WindowTests.test_actual_runner_journals_and_fresh_external_ci_adapter\n",
        )
        # Execute the repository harness over the disposable selected fixture.
        env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
        code = (
            "import sys; from pathlib import Path; "
            f"sys.path.insert(0, {str(source / 'scripts/agentic')!r}); "
            "import check_runner; "
            f"raise SystemExit(check_runner.run(Path({str(self.root)!r}), 1))"
        )
        result = subprocess.run(
            [sys.executable, "-B", "-c", code],
            cwd=self.root,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=30,
        )
        self.assertEqual(result.returncode, 0, result.stdout.decode())
        directory, summary = self.evidence(result)
        self.assertEqual(summary["occurrences"], 1)
        self.assertTrue(summary["successful"])


class Generation15PredecessorTests(unittest.TestCase):
    def test_both_fixed_predecessors_and_primary_ranges(self):
        import base64
        import tempfile
        import zlib

        bodies = [zlib.decompress(base64.b85decode(v)) for v in (PUBLIC_T, PUBLIC_G13)]
        numbers = [6061320190, 6045434332]
        rows = [
            {
                "id": n,
                "issue_url": "https://api.github.com/repos/Zi-Deng/FLOW-DC/issues/31",
                "user": {"login": "Zi-Deng"},
                "body": raw.decode(),
            }
            for n, raw in zip(numbers, bodies, strict=True)
        ]
        context = {
            "designated_plan_comment": {"id": 6062530466, "body": "U"},
            "issue": {"title": "31", "body": "acceptance"},
            "issue_comments": rows,
            "reviews": [],
            "inline_comments": [],
            "pr_comments": [],
            "pull_request": {"head": {"sha": "a" * 40}},
            "commit_statuses": [],
            "check_runs": [],
        }
        repo = SimpleNamespace(name="Zi-Deng/FLOW-DC", root=Path.cwd())
        self.assertEqual(
            review_packet.g15_predecessors(repo, context), list(zip(numbers, bodies, strict=True))
        )
        for index in range(2):
            for key, value in (
                ("id", True),
                ("id", float(numbers[index])),
                ("body", None),
                ("body", rows[index]["body"] + "\n"),
                ("user", None),
                ("user", {"login": "wrong"}),
                ("issue_url", "wrong"),
            ):
                bad = copy.deepcopy(context)
                bad["issue_comments"][index][key] = value
                with self.assertRaises(WorkflowError):
                    review_packet.g15_predecessors(repo, bad)
            for duplicate in (False, True):
                bad = copy.deepcopy(context)
                if duplicate:
                    bad["issue_comments"].append(copy.deepcopy(rows[index]))
                else:
                    bad["issue_comments"].pop(index)
                with self.assertRaises(WorkflowError):
                    review_packet.g15_predecessors(repo, bad)
        with tempfile.TemporaryDirectory() as tmp:
            packet = Path(tmp)
            for name in ("repository-policy.txt", "review-policy.txt", "domain-policy.txt"):
                (packet / name).write_text("policy\n")
            with patch.object(review_packet, "run", return_value=SimpleNamespace(stdout="")):
                review_packet.build(
                    repo,
                    packet,
                    "a" * 40,
                    "b" * 40,
                    [],
                    [],
                    context,
                    {"required_checks": [], "max_snapshot_bytes": 1000000},
                )
            inventory = json.loads((packet / "required-material.json").read_text())["required"]
            for number, raw in zip(numbers, bodies, strict=True):
                name = f"contract-predecessor-{number}.txt"
                self.assertEqual((packet / name).read_bytes(), raw)
                entries = [v for v in inventory if v["path"] == name]
                self.assertTrue(entries)
                self.assertTrue(all(v["kind"] == "contract" and not v.get("omitted") for v in entries))
                covered = [i for v in entries for i in range(v["start_line"], v["end_line"] + 1)]
                self.assertEqual(covered, list(range(1, len(raw.decode().splitlines()) + 1)))
                self.assertEqual(len({v["id"] for v in entries}), len(entries))
