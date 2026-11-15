"""Native manager helper, isolated from the portable acquisition runtime."""

import ctypes

from .protocol import require


def pidfd(pid):
    libc = ctypes.CDLL(None, use_errno=True)
    function = getattr(libc, "pidfd_open", None)
    require(function is not None, "native child cleanup requires Linux pidfd")
    function.argtypes, function.restype = [ctypes.c_int, ctypes.c_uint], ctypes.c_int
    fd = function(pid, 0)
    if fd < 0:
        raise OSError(ctypes.get_errno(), "pidfd unavailable")
    return fd


async def execute(settings):
    import asyncio
    import shlex
    import time
    from pathlib import Path

    import ndcctools.taskvine as vine

    from ..integrity import parse
    from .cohort import CATEGORY, NATIVE_RETRIES, cohort
    from .ownership import OwnedWorkers
    from .protocol import write_new
    from .runtime import verify_runtime

    runtime = verify_runtime(vine, settings["runtime_receipt"], settings.get("worker_binary"))
    root = Path(settings["root"])
    manager = vine.Manager(
        port=settings["port"],
        run_info_path=str(root / "run-info"),
        staging_path=str(root / "staging"),
        ssl=True,
        shutdown=True,
        init_fn=lambda m: m.set_password_file(settings["password"]),
    )
    workers = None
    watch = None
    cleanup = {"all_stopped": False}
    results = []
    try:
        manager.disable_peer_transfers()
        manager.enable_disconnect_slow_workers(0)
        manager.enable_disconnect_slow_workers_category(CATEGORY, 0)
        require(manager.set_category_mode(CATEGORY, "fixed") == 1, "native fixed allocation failed")
        contract = cohort(settings["workers"])
        package = manager.declare_file(settings["package"], cache=True, peer_transfer=False)
        environment = (
            manager.declare_poncho(settings["environment_archive"], cache=True, peer_transfer=False)
            if settings["environment_archive"]
            else None
        )
        if settings["start_local_workers"]:
            workers = OwnedWorkers(
                root,
                settings["worker_binary"],
                settings["workers"],
                cores=settings["cores"],
                memory=settings["memory_mb"],
                disk=settings["disk_mb"],
                wall_time=settings["timeout_s"] + 30,
            )
            workers.bind_cohort(contract)
            for slot in contract["slots"]:
                workers.start(manager.port, Path(settings["password"]), slot["feature"])

            async def observe_owned_tasks():
                while not workers.stopping:
                    workers.capture()
                    for request in (root / "registration").glob("*.request.json"):
                        acknowledgement = request.with_name(
                            request.name.replace(".request.json", ".accepted.json")
                        )
                        if acknowledgement.exists():
                            continue
                        try:
                            registration = parse(request.read_bytes())
                        except (OSError, ValueError):
                            continue
                        handle = workers.handles.get(registration.get("pid"))
                        if handle is not None and not workers.dead(handle):
                            write_new(acknowledgement, registration)
                    await asyncio.sleep(0.01)

            watch = asyncio.create_task(observe_owned_tasks())
        else:
            # Prepared remote cohorts must use the printed, unique task features.
            write_new(root / "worker-cohort.json", contract)
            cleanup = {
                "all_stopped": True,
                "scope": "no local workers created; remote lifecycle remains operator-owned",
            }
        write_new(
            root / "endpoint.json",
            {
                "port": manager.port,
                "ssl": True,
                "features": [s["feature"] for s in contract["slots"]],
                "runtime": runtime,
            },
        )
        for i, path in enumerate(settings["tasks"]):
            record = parse(Path(path).read_bytes())
            executable = settings["python_executable"] if environment is None else "python"
            task = vine.Task(f"{shlex.quote(executable)} -B -m flowdc.distributed.worker task.json")
            task.set_env_var("PYTHONPATH", "application.zip")
            task.set_cores(settings["cores"])
            task.set_memory(settings["memory_mb"])
            task.set_disk(settings["disk_mb"])
            task.set_category(CATEGORY)
            task.set_retries(NATIVE_RETRIES)
            task.set_max_forsaken(0)
            task.set_time_max(settings["timeout_s"])
            task.add_feature(contract["slots"][i]["feature"])
            task.add_input(package, "application.zip")
            task.add_input(
                manager.declare_file(record["partition"], cache=False, peer_transfer=False), "input.parquet"
            )
            task.add_input(manager.declare_file(path, cache=False, peer_transfer=False), "task.json")
            task.add_output(
                manager.declare_file(str(root / f"part-{i}"), cache=False, peer_transfer=False), "return"
            )
            if environment is not None:
                task.add_execution_context(environment)
            manager.submit(task)
        deadline = time.monotonic() + settings["timeout_s"]
        while not manager.empty() and time.monotonic() < deadline:
            task = await asyncio.to_thread(manager.wait, 1)
            if task:
                results.append(
                    {
                        "task_id": task.id,
                        "result": task.result,
                        "exit_code": task.exit_code,
                        "successful": task.successful(),
                        "stdout": (task.std_output or "")[-4096:],
                    }
                )
        if not manager.empty():
            manager.cancel_all()
    finally:
        manager.cancel_all()
        if workers is not None:
            cleanup = await workers.close()
        if watch is not None:
            watch.cancel()
            import contextlib

            with contextlib.suppress(asyncio.CancelledError):
                await watch
        manager.__exit__(None, None, None)
        write_new(root / "native-result.json", {"tasks": results, "cleanup": cleanup})
    return 0 if len(results) == settings["workers"] and all(r["successful"] for r in results) else 1


def main():
    import asyncio
    import sys
    from pathlib import Path

    from ..integrity import parse

    return asyncio.run(execute(parse(Path(sys.argv[1]).read_bytes())))


if __name__ == "__main__":
    raise SystemExit(main())
