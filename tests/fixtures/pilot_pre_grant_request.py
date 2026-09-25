# Frozen request and heartbeat functions from e2c8324fc7b9916f199f44002d95e97982202b18.
# Imports resolve unchanged primitives; request function is retained verbatim.
from flowdc_pilot import AccountingError, ClockSample
from flowdc_pilot_journal import allowance, failure, fresh_network
from flowdc_pilot_supervisor import HEARTBEAT_SECONDS, sample_clock


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
