import asyncio
import sys
from pathlib import Path

base_path = Path(__file__).parent
sys.path.append(base_path.parent.as_posix())

from gaterpc.global_settings import Settings
from gaterpc.core import (
    Context, Worker, Service, AMajordomo, Client,
)
from gaterpc.utils import interface


FRONTEND_ADDR = "ipc:///tmp/gate-rpc/run/test_e2e_frontend"
BACKEND_ADDR = "inproc://test_e2e_backend"


class EchoWorker(Worker):
    @interface
    async def aecho(self, *args, **kwargs):
        return {"args": args, "kwargs": kwargs, "kind": "async"}

    @interface("thread")
    def secho(self, *args, **kwargs):
        return {"args": args, "kwargs": kwargs, "kind": "thread"}

    @interface
    def gen(self, n: int):
        for i in range(n):
            yield i

    @interface
    async def agen(self, n: int):
        for i in range(n):
            await asyncio.sleep(0)
            yield i

    @interface
    async def boom(self):
        raise ValueError("boom!")


async def main():
    Settings.configure("BASE_PATH", Path("/tmp/gate-rpc"))
    Settings.setup()
    ctx = Context()

    # majordomo
    md = AMajordomo(context=ctx)
    md.bind_backend(BACKEND_ADDR)
    md.bind_frontend(FRONTEND_ADDR)
    md.run()

    # worker
    svc = Service()
    worker = svc.create_worker(EchoWorker, BACKEND_ADDR, context=ctx)
    worker.run()

    # let worker register
    await asyncio.sleep(0.5)

    # client
    cli = Client(broker_addr=FRONTEND_ADDR)
    await cli.connect()
    await asyncio.sleep(0.3)
    print(f"[ready] services: {cli._remote_services}")

    failures = []

    # ---- 1. async call ----
    r = await cli.GateRPC.aecho("a", "b", k=1)
    ok = r["args"] == ["a", "b"] and r["kwargs"] == {"k": 1} and r["kind"] == "async"
    print(f"[1] async aecho: {'OK' if ok else 'FAIL'} -> {r}")
    if not ok:
        failures.append("async aecho")

    # ---- 2. sync via thread ----
    r = await cli.GateRPC.secho("x", k="y")
    ok = r["args"] == ["x"] and r["kwargs"] == {"k": "y"} and r["kind"] == "thread"
    print(f"[2] thread secho: {'OK' if ok else 'FAIL'} -> {r}")
    if not ok:
        failures.append("thread secho")

    # ---- 3. generator (StreamReply) ----
    stream = await cli.GateRPC.gen(5)
    collected = []
    async for v in stream:
        collected.append(v)
    ok = collected == [0, 1, 2, 3, 4]
    print(f"[3] gen: {'OK' if ok else 'FAIL'} -> {collected}")
    if not ok:
        failures.append("gen")

    # ---- 4. async generator ----
    stream = await cli.GateRPC.agen(4)
    collected = []
    async for v in stream:
        collected.append(v)
    ok = collected == [0, 1, 2, 3]
    print(f"[4] agen: {'OK' if ok else 'FAIL'} -> {collected}")
    if not ok:
        failures.append("agen")

    # ---- 5. exception propagation ----
    try:
        await cli.GateRPC.boom()
        print("[5] boom: FAIL - no exception")
        failures.append("boom")
    except Exception as e:
        ok = "boom" in str(e)
        print(f"[5] boom: {'OK' if ok else 'FAIL'} -> {type(e).__name__}: {e}")
        if not ok:
            failures.append("boom")

    # ---- 6. concurrent independent proxy chains ----
    async def call_a():
        return await cli.GateRPC.aecho("from_a")

    async def call_b():
        return await cli.GateRPC.aecho("from_b")

    results = await asyncio.gather(*(call_a() if i % 2 == 0 else call_b() for i in range(20)))
    a_count = sum(1 for r in results if r["args"] == ["from_a"])
    b_count = sum(1 for r in results if r["args"] == ["from_b"])
    ok = a_count == 10 and b_count == 10
    print(f"[6] concurrent proxy (P8): {'OK' if ok else 'FAIL'} -> a={a_count}, b={b_count}")
    if not ok:
        failures.append("concurrent proxy")

    # ---- 7. unknown method ----
    try:
        await cli.GateRPC.does_not_exist()
        print("[7] missing method: FAIL")
        failures.append("missing method")
    except Exception as e:
        print(f"[7] missing method: OK -> {type(e).__name__}")

    # ---- 8. sequential many calls (smoke) ----
    n = 100
    results = []
    for i in range(n):
        r = await cli.GateRPC.aecho(i)
        results.append(r["args"][0])
    ok = results == list(range(n))
    print(f"[8] sequential x{n}: {'OK' if ok else 'FAIL'}")
    if not ok:
        failures.append("sequential")

    # cleanup
    cli._recv_task.cancel()
    worker.stop()
    md.stop()
    await asyncio.sleep(0.3)

    if failures:
        print(f"\n=== FAILED: {failures} ===")
        return 1
    print("\n=== ALL E2E TESTS PASSED ===")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
