"""Single-actor pilot lifecycle state machine, also exercised by fake providers."""

import time
from dataclasses import asdict, replace
from pathlib import Path

import flowdc_ops as ops
from flowdc_pilot import AccountingError, ClockSample
from flowdc_pilot_journal import allowance, failure, fresh_network

POLL_SECONDS = 2
# Allow three 20s observation + 20s mutation steps, a preceding 20s call,
# polling and journal overhead before the 600s reserve. Provider outages can
# still prevent completion; this is scheduling slack, not a billing guarantee.
ACTION_LEAD_SECONDS = 180
HEARTBEAT_SECONDS = 30
OBSERVATION_SECONDS = 120


def sample_clock():
    return ClockSample(
        Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
        time.clock_gettime(time.CLOCK_BOOTTIME),
        time.time(),
    )


def heartbeat_fresh(record, now, *, maximum_age=HEARTBEAT_SECONDS):
    try:
        saved = ClockSample(**record["heartbeat"])
        return (
            saved.boot_id == now.boot_id
            and 0 <= now.boottime - saved.boottime <= maximum_age
            and 0 <= now.utc - saved.utc <= maximum_age
        )
    except (TypeError, ValueError):
        return False


def request(journal, command, *, window=1800, inspection=True, clock=sample_clock):
    now = clock()
    if command == "start" and not journal.supervisor_locked():
        raise failure("supervisor_not_ready")

    def update(record):
        if command == "start":
            if not heartbeat_fresh(record, now) or record["service"] is None:
                raise failure("supervisor_not_ready")
            if record["desired"] == "run":
                if record["window"] != {"seconds": window, "inspection": inspection}:
                    raise failure("run_already_requested")
                return
            if record["desired"] != "idle" or any(
                allowance(vm["account"]).obligation for vm in record["vms"].values()
            ):
                raise failure("cleanup_outstanding")
            for vm in record["vms"].values():
                allowance(vm["account"]).activation_intent(now, window_seconds=window, inspection=inspection)
            if record["network"]["rolled_back"]:
                journal.event(record, "network_history", record["network"])
                record["network"] = fresh_network()
            record["window"] = {"seconds": window, "inspection": inspection}
            record["desired"] = "run"
            record["checkpoint"] = None
            for vm in record["vms"].values():
                vm["phase"] = "pending"
                vm["activation_seen"] = False
            journal.event(record, "start_requested", record["window"])
        elif command in ("stop", "reconcile"):
            if (
                command == "stop"
                and record["desired"] == "idle"
                and record["network"]["rolled_back"]
                and all(
                    vm["phase"] == "offloaded" and not allowance(vm["account"]).obligation
                    for vm in record["vms"].values()
                )
            ):
                journal.event(record, "stop_requested", {})
                return
            # Reconciliation is conservative: never resumes activation.
            if record["desired"] == "idle":
                for vm in record["vms"].values():
                    vm["phase"] = "verify_offload"
            record["desired"] = "stop"
            journal.event(record, command + "_requested", {})
        else:
            raise failure("invalid_request", invalid=True)

    try:
        return journal.change(update)
    except AccountingError:
        raise failure("allowance_refused") from None


class Supervisor:
    """Provider methods may block for at most 20 seconds, never inside change().

    Hold journal.supervisor_lock for this object's whole lifetime. The CLI cannot
    call provider mutations. Any restart with obligations forces shutdown rather
    than replaying an uncertain activation.
    """

    def __init__(self, journal, provider, *, clock=sample_clock):
        self.journal, self.provider, self.clock = journal, provider, clock

    def recover(self):
        def update(record):
            now = self.clock()
            for vm in record["vms"].values():
                vm["account"] = asdict(allowance(vm["account"]).account(now))
            if record["desired"] == "run" or any(
                allowance(vm["account"]).obligation for vm in record["vms"].values()
            ):
                record["desired"] = "stop"
                record["checkpoint"] = "supervisor_restarted_cleanup_required"
            record["heartbeat"] = asdict(now)

        self.journal.change(update)

    def account(self):
        def update(record):
            now = self.clock()
            record["heartbeat"] = asdict(now)
            for vm in record["vms"].values():
                account = allowance(vm["account"]).account(now)
                vm["account"] = asdict(account)
                if account.obligation and (
                    account.uncertain
                    or account.consumed + ACTION_LEAD_SECONDS >= account.shutdown_at_consumed
                ):
                    record["desired"] = "stop"
            if record["desired"] == "stop":
                record["network"]["ready"] = False

        return self.journal.change(update)

    def before_activation(self):
        if self.account()["desired"] != "run":
            raise failure("stop_or_deadline_requested")

    def checkpoint(self, code):
        def update(record):
            record["desired"] = "stop"
            record["checkpoint"] = code
            self.journal.event(record, "checkpoint", {"code": code})

        self.journal.change(update)

    def tick(self):
        """At most one mutating provider step per tick; stop wins at each boundary."""
        record = self.account()
        if record["desired"] == "idle":
            return
        try:
            if record["desired"] == "stop":
                return self.cleanup(record)
            # Charge setup as well as activation, for all three selected VMs.
            if all(vm["phase"] == "pending" for vm in record["vms"].values()) and not any(
                allowance(vm["account"]).obligation for vm in record["vms"].values()
            ):
                # Fresh provider context and selected states are mandatory before intent.
                self.provider.preflight(record)

                def intent(current):
                    if current["desired"] != "run":
                        return
                    for vm in current["vms"].values():
                        vm["account"] = asdict(
                            allowance(vm["account"]).activation_intent(
                                self.clock(),
                                window_seconds=current["window"]["seconds"],
                                inspection=current["window"]["inspection"],
                            )
                        )
                    self.journal.event(current, "activation_intent", {"ids": list(current["vms"])})

                self.journal.change(intent)
                return
            if not record["network"]["ready"]:
                self.provider.network_step(self.journal, rollback=False)
                return
            ordered = sorted(
                record["vms"].items(),
                key=lambda item: (
                    item[1]["phase"] != "pending",
                    (item[1]["observed"] or {"clock": {"utc": 0}})["clock"]["utc"],
                ),
            )
            for vm_id, vm in ordered:
                record = self.account()
                if record["desired"] != "run":
                    return
                if vm["phase"] == "pending":
                    # Persist intent separately from the result. Never replay after crash.
                    def intent(current, vm_id=vm_id):
                        if current["desired"] == "run":
                            current["vms"][vm_id]["phase"] = "unshelve_intent"
                            self.journal.event(current, "unshelve_intent", {"id": vm_id})

                    record = self.journal.change(intent)
                    if record["desired"] != "run":
                        return
                    self.provider.lifecycle(record, vm_id, "unshelve")
                    self.journal.change(
                        lambda current, vm_id=vm_id: current["vms"][vm_id].update(phase="requested")
                    )
                    return
                state = self.provider.observe(record, vm_id)
                self.save_observation(vm_id, state, settle=False)
                if state not in ("ACTIVE", "BUILD", "SHELVED_OFFLOADED"):
                    self.checkpoint("activation_state_requires_cleanup")
                    return
                return
        except (ops.OpsError, OSError, AccountingError) as exc:
            if isinstance(exc, ops.OpsError) and exc.code == "pilot_state_busy":
                raise
            self.checkpoint(exc.code if isinstance(exc, ops.OpsError) else "pilot_operation_failed")

    def save_observation(self, vm_id, state, *, settle):
        def update(record):
            now = self.clock()
            vm = record["vms"][vm_id]
            vm["observed"] = {"state": state, "clock": asdict(now)}
            # Offloaded can still be the pre-unshelve state after an accepted or
            # lost reply. Only a later settled activation state proves that the
            # activation has left that initial state. Keep watching indefinitely
            # if no such evidence arrives; elapsed time is not cancellation.
            if state in ("ACTIVE", "SHUTOFF", "ERROR", "PAUSED", "SUSPENDED", "SHELVED"):
                vm["activation_seen"] = True
            unresolved = vm["phase"] in ("unshelve_intent", "requested") and not vm.get(
                "activation_seen", False
            )
            can_settle = settle and not unresolved
            if settle and unresolved:
                record["checkpoint"] = "activation_completion_unresolved"
            account = allowance(vm["account"])
            if settle and state != "SHELVED_OFFLOADED" and not account.obligation:
                # Activity outside the last observed interval has unknown age.
                # Retain a cleanup obligation and refuse any new allowance.
                account = replace(
                    account,
                    consumed=max(account.consumed, account.limit),
                    obligation=True,
                    sample=now,
                    shutdown_at_consumed=0,
                    uncertain=True,
                )
            vm["account"] = asdict(account.observe(now, state=state) if can_settle else account.account(now))
            if can_settle and state == "SHELVED_OFFLOADED":
                vm["phase"] = "offloaded"

        self.journal.change(update)

    def cleanup(self, record):
        # Rotate across resources even when one fails; do not starve later VMs.
        candidates = [
            key
            for key, vm in record["vms"].items()
            if allowance(vm["account"]).obligation or vm["phase"] != "offloaded"
        ]
        if candidates:
            vm_id = min(
                candidates, key=lambda key: record["vms"][key].get("cleanup_attempt", {}).get("order", 0)
            )
            now = self.clock()
            previous = record["vms"][vm_id].get("cleanup_attempt")
            if previous is not None:
                saved = ClockSample(**previous["clock"])
                if now.boot_id == saved.boot_id and 0 <= now.boottime - saved.boottime < 10:
                    return

            def intent(current):
                order = (
                    max(vm.get("cleanup_attempt", {}).get("order", 0) for vm in current["vms"].values()) + 1
                )
                current["vms"][vm_id]["cleanup_attempt"] = {"order": order, "clock": asdict(now)}

            self.journal.change(intent)
            state = self.provider.observe(record, vm_id)
            self.save_observation(vm_id, state, settle=True)
            if state == "SHELVED_OFFLOADED":
                return
            action = "offload" if state == "SHELVED" else "shelve"
            self.journal.change(
                lambda current: current["vms"][vm_id].update(
                    cleanup_intent={"action": action, "clock": asdict(self.clock())}
                )
            )
            self.provider.lifecycle(record, vm_id, action)
            # Acknowledgement is not completion; observe on the next tick.
            return
        if not record["network"]["rolled_back"]:
            self.provider.network_step(self.journal, rollback=True)
            return

        now = self.clock()
        stale = [
            key
            for key, vm in record["vms"].items()
            if vm["observed"] is None
            or not heartbeat_fresh(
                {"heartbeat": vm["observed"]["clock"]}, now, maximum_age=OBSERVATION_SECONDS
            )
        ]
        if stale:

            def refresh(current):
                for key in stale:
                    current["vms"][key]["phase"] = "verify_offload"

            self.journal.change(refresh)
            return

        def finished(current):
            current["desired"] = "idle"
            current["checkpoint"] = None
            self.journal.event(current, "cleanup_verified", {})

        self.journal.change(finished)
