import sys
from pathlib import Path

sys.path.append(Path(__file__).resolve().parent.parent.as_posix())

from gaterpc.utils import msg_pack, to_bytes
from gaterpc.global_settings import Settings


def emit(name, value: bytes) -> None:
    print(f"{name:24} {value.hex()}")


emit("V_STR_HELLO", msg_pack("hello"))
emit("V_INT_42", msg_pack(42))
emit("V_EMPTY_ARRAY", msg_pack([]))
emit("V_EMPTY_MAP", msg_pack({}))
emit(
    "V_SINGLETON_OK",
    msg_pack({"result": 42, "exception": None}),
)
emit(
    "V_SINGLETON_EXC",
    msg_pack(
        {
            "result": None,
            "exception": [
                "ValueError",
                "ValueError('boom!')",
                "Traceback (most recent call last):\n  File ...\n"
                "ValueError: boom!\n",
            ],
        }
    ),
)
emit(
    "V_READY_BODY",
    (
        f"Echo{Settings.MDP_DESCRIPTION_SEP}A test service"
        f"{Settings.MDP_DESCRIPTION_SEP}32"
    ).encode("utf-8"),
)

emit("C_MDP_CLIENT", Settings.MDP_CLIENT)
emit("C_MDP_WORKER", Settings.MDP_WORKER)
emit("C_GATE_MEMBER", Settings.GATE_MEMBER)
emit("C_STREAM_GEN", Settings.STREAM_GENERATOR_TAG)
emit("C_STREAM_END", Settings.STREAM_END_TAG)
emit("C_STREAM_EXC", Settings.STREAM_EXCEPT_TAG)
emit("C_HUGE_TAG", Settings.STREAM_HUGE_DATA_TAG)
emit("C_HUGE_END", Settings.HUGE_DATA_END_TAG)
emit("C_HUGE_EXC", Settings.HUGE_DATA_EXCEPT_TAG)

# ---- Multicast NOTICE (43-byte UDP datagram) -----------------------------
GATE_ID_FIXTURE = b"\x11" * 16
CLUSTER_ID_FIXTURE = b"\x42" * 16
GATE_PORT_FIXTURE = 9777
emit(
    "V_MCAST_NOTICE",
    GATE_ID_FIXTURE
    + Settings.GATE_MEMBER
    + Settings.GATE_COMMAND_NOTICE
    + CLUSTER_ID_FIXTURE
    + to_bytes(GATE_PORT_FIXTURE, fmt="!i"),
)
