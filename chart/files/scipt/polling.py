import os
import logging
import json
import base64
import signal
import subprocess
import threading
import time
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, asdict
from functools import partial


GRPC_PROTO_IMPORT_PATH = os.getenv("GRPC_IMPORT_PATH", "./proto")
GRPC_HOST = os.getenv("GRPC_HOST", "rpc.mainnet.succinct.xyz:443")
OUTPUT_PATH = Path(os.getenv("OUTPUT_PATH", "./data"))
TOKEN_UNIT_FACTOR = int(os.getenv("TOKEN_UNIT_FACTOR", 18))
BILLION_DECIMALS = 9


@dataclass
class TaskConfig:
    func: partial
    interval: int


class FulfillmentStatus(str, Enum):
    REQUESTED = "REQUESTED"
    ASSIGNED = "ASSIGNED"
    FULFILLED = "FULFILLED"
    UNFULFILLABLE = "UNFULFILLABLE"


class ExecutionStatus(str, Enum):
    UNEXECUTED = "UNEXECUTED"
    EXECUTED = "EXECUTED"
    UNEXECUTABLE = "UNEXECUTABLE"
    VALIDATION_FAILED = "VALIDATION_FAILED"


@dataclass
class ProofRequest:
    request_id: str
    strategy: str
    version: str
    mode: str
    fulfillment: str
    requester: str
    tx_hash: str
    cycle_limit: int
    gas_limit: int
    timeout: int
    base_fee: float
    max_price_per_billion_pgu: float
    whitelist_count: int
    fulfiller: Optional[str] = None # exist in status assigned, fulfilled, unfulfillable
    cycles: Optional[int] = None # exist in status fulfilled
    gas_used: Optional[int] = None # exist in status fulfilled
    gas_price: Optional[int] = None # exist in status assigned, fulfilled, unfulfillable
    turnaround: Optional[int] = None # exist in status fulfilled


@dataclass
class ProverStat:
    name: str
    address: str
    created_at: int
    owner: str
    tx_hash: str
    total_auction_requests: int
    successful_requests: int
    total_billion_gas_proved: float
    last_active_at: int
    lifetime_rewards: float
    staker_fee_bips: int


@dataclass
class SuspendableProver:
    address: str
    failure_count: int


@dataclass
class ProverBidHistory:
    bidder: str
    amount_billion: float
    created_at: int


def b64_to_hex(b64: Optional[str]) -> str:
    if b64 is None:
        return ""
    try:
        raw = base64.b64decode(b64)
        return "0x" + raw.hex()
    except Exception:
        raise


def hex_to_b64(hex_str: Optional[str]) -> str:
    if hex_str is None:
        return ""
    try:
        if hex_str.startswith("0x"):
            hex_str = hex_str[2:]
        hex_bytes = bytes.fromhex(hex_str)
        return base64.b64encode(hex_bytes).decode()
    except Exception:
        raise


def call_grpcurl(import_path: str, host: str, method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    args = [
        "grpcurl",
        "-import-path", import_path,
        "-proto", "network.proto",
        "-proto", "types.proto",
        "-d", json.dumps(payload),
        host,
        method,
    ]
    proc = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if proc.returncode != 0:
        raise RuntimeError(
            f"grpcurl failed (code {proc.returncode}).\nSTDERR:\n{proc.stderr.strip()}"
        )
    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Failed to parse grpcurl JSON response: {e}\nOUTPUT:\n{proc.stdout}")


def write_output(filename: str, data: Dict[str, Any]) -> None:
    file_path = OUTPUT_PATH / filename

    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))


def get_filtered_proof_requests(fulfillment: FulfillmentStatus) -> List[ProofRequest]:
    grpc_method = os.getenv("GRPC_METHOD_FILTERED_PROOF_REQUESTS", "network.ProverNetwork.GetFilteredProofRequests")
    query_limit = int(os.getenv("QUERY_FILTERED_PROOF_REQUESTS_LIMIT", 100))
    query_page = 1

    all_reqs: List[ProofRequest] = []
    max_proof_requests = 1000

    while True:
        if query_limit * query_page > max_proof_requests:
            break

        params = {
            "fulfillment_status": fulfillment.value,
            "limit": query_limit,
            "page": query_page,
        }
        payload = {k: v for k, v in params.items() if v}
        resp = call_grpcurl(GRPC_PROTO_IMPORT_PATH, GRPC_HOST, grpc_method, payload)

        reqs = resp.get("requests") or []
        if not isinstance(reqs, list):
            raise RuntimeError("Unexpected response format: 'requests' is not a list")
        if len(reqs) == 0:
            break

        for req in reqs:
            execution_status = req.get("executionStatus")
            if execution_status and execution_status in ExecutionStatus.UNEXECUTABLE:
                continue

            pr = ProofRequest(
                request_id=b64_to_hex(req.get("requestId")) or "",
                strategy=str(req.get("strategy")) or "",
                version=str(req.get("version")) or "",
                mode=str(req.get("mode")) or "",
                fulfillment=str(req.get("fulfillmentStatus")) or "",
                requester=b64_to_hex(req.get("requester")) or "",
                tx_hash=b64_to_hex(req.get("txHash")) or "",
                cycle_limit=int(req.get("cycleLimit")),
                gas_limit=int(req.get("gasLimit")),
                timeout=( int(req.get("deadline")) - int(req.get("createdAt")) ),
                base_fee=int(req.get("baseFee")) / 10**TOKEN_UNIT_FACTOR,
                max_price_per_billion_pgu=int(req.get("maxPricePerPgu")) / 10**TOKEN_UNIT_FACTOR * 10**BILLION_DECIMALS,
                whitelist_count=len(req.get("whitelist") or []),
            )

            if FulfillmentStatus(pr.fulfillment) in (
                    FulfillmentStatus.ASSIGNED, FulfillmentStatus.FULFILLED, FulfillmentStatus.UNFULFILLABLE
            ):
                pr.fulfiller = b64_to_hex(req.get("fulfiller")) or ""
                pr.gas_price = int(req.get("gasPrice")) or 0

            if FulfillmentStatus(pr.fulfillment) in FulfillmentStatus.FULFILLED:
                pr.cycles = int(req.get("cycles")) or 0
                pr.gas_used = int(req.get("gasUsed")) or 0
                pr.turnaround = ( int(req.get("fulfilledAt")) - int(req.get("createdAt")) )

            all_reqs.append(pr)

        query_page += 1

    write_output(
        filename=f"{fulfillment.value.lower()}_proof_requests.json",
        data={"requests": [asdict(x) for x in all_reqs]},
    )
    return all_reqs


def get_filtered_prover_stats(is_featured: bool) -> List[ProverStat]:
    grpc_method = os.getenv("GRPC_METHOD_FILTERED_PROVER_STATS", "network.ProverNetwork.GetFilteredProverStats")
    query_limit = int(os.getenv("QUERY_FILTERED_PROVER_STATS_LIMIT", 100))
    query_page = 1

    all_stats: List[ProverStat] = []
    while True:
        params = {
            "is_featured": is_featured,
            "limit": query_limit,
            "page": query_page,
        }
        payload = {k: v for k, v in params.items() if v}
        resp = call_grpcurl(GRPC_PROTO_IMPORT_PATH, GRPC_HOST, grpc_method, payload)

        stats = resp.get("stats") or []
        if not isinstance(stats, list):
            raise RuntimeError("Unexpected response format: 'stats' is not a list")
        if len(stats) == 0:
            break

        for stat in stats:
            all_stats.append(ProverStat(
                name=str(stat.get("name")) or "",
                address=b64_to_hex(stat.get("address")) or "",
                created_at=int(stat.get("createdAt")) or 0,
                owner=b64_to_hex(stat.get("owner")) or "",
                tx_hash=b64_to_hex(stat.get("txHash")) or "",
                total_auction_requests=int(stat.get("totalAuctionRequests")),
                successful_requests=int(stat.get("successfulRequests")),
                total_billion_gas_proved=int(stat.get("totalGasProved")) / 10**BILLION_DECIMALS,
                last_active_at=int(stat.get("lastActiveAt")),
                lifetime_rewards=int(stat.get("lifetimeRewards")) / 10**TOKEN_UNIT_FACTOR,
                staker_fee_bips=int(stat.get("stakerFeeBips")),
            ))

        query_page += 1

    write_output(
        filename="prover_stats.json",
        data={"stats": [asdict(x) for x in all_stats]},
    )
    return all_stats


def get_filtered_suspendable_provers(recent_failure_window: int, total_failure_window: int) -> List[SuspendableProver]:
    grpc_method = os.getenv("GRPC_METHOD_FILTERED_SUSPENDABLE_PROVERS", "network.ProverNetwork.GetFilteredSuspendableProvers")
    query_limit = int(os.getenv("QUERY_FILTERED_SUSPENDABLE_PROVERS_LIMIT", 100))
    query_page = 1

    all_provers: List[SuspendableProver] = []
    while True:
        params = {
            "lookback": recent_failure_window,
            "outer_lookback": total_failure_window,
            "limit": query_limit,
            "page": query_page,
        }
        payload = {k: v for k, v in params.items() if v}
        resp = call_grpcurl(GRPC_PROTO_IMPORT_PATH, GRPC_HOST, grpc_method, payload)

        provers = resp.get("provers") or []
        if not isinstance(provers, list):
            raise RuntimeError("Unexpected response format: 'provers' is not a list")
        if len(provers) == 0:
            break

        for prover in provers:
            all_provers.append(SuspendableProver(
                address=b64_to_hex(prover.get("prover")) or "",
                failure_count=int(prover.get("failureCount")),
            ))

        query_page += 1

    write_output(
        filename="suspendable_provers.json",
        data={"provers": [asdict(x) for x in all_provers]},
    )
    return all_provers


def get_filtered_prover_bid_history(is_featured: bool) -> List[ProverBidHistory]:
    grpc_method = os.getenv("GRPC_METHOD_FILTERED_PROVER_BID_HISTORY", "network.ProverNetwork.GetFilteredProverBidHistory")
    query_limit = int(os.getenv("QUERY_FILTERED_PROVER_BID_HISTORY_LIMIT", 100))

    prover_stats = get_filtered_prover_stats(is_featured=is_featured)
    provers = list(set(stat.address for stat in prover_stats))

    all_bids: List[ProverBidHistory] = []
    max_bids = 100

    for prover in provers:
        query_page = 1
        while True:
            if query_limit * query_page > max_bids:
                break

            params = {
                "prover_address": hex_to_b64(prover),
                "limit": query_limit,
                "page": query_page,
            }
            payload = {k: v for k, v in params.items() if v}
            resp = call_grpcurl(GRPC_PROTO_IMPORT_PATH, GRPC_HOST, grpc_method, payload)

            bids = resp.get("bids") or []
            if not isinstance(bids, list):
                raise RuntimeError("Unexpected response format: 'bids' is not a list")
            if len(bids) == 0:
                break

            for bid in bids:
                all_bids.append(ProverBidHistory(
                    bidder=b64_to_hex(bid.get("bidder")) or "",
                    amount_billion=int(bid.get("amount")) / 10**TOKEN_UNIT_FACTOR * 10**BILLION_DECIMALS,
                    created_at=int(bid.get("createdAt")) or 0,
                ))

            query_page += 1

    write_output(
        filename="prover_bid_stats.json",
        data={"bids": [asdict(x) for x in all_bids]},
    )
    return all_bids


def run_periodically(func: partial, interval: int) -> None:
    while True:
        try:
            logging.info(f"Running task: {func.func.__name__} with {func.keywords}")
            func()
        except Exception:
            logging.error(f"Task failed: {func.func.__name__} with {func.keywords}", exc_info=True)
        time.sleep(interval)


def main() -> None:
    shutdown_event = threading.Event()
    shutdown_signals = [signal.SIGINT, signal.SIGTERM]
    for sig in shutdown_signals:
        signal.signal(sig, lambda signum, frame: shutdown_event.set())

    task_configs: List[TaskConfig] = [
        TaskConfig(
            func=partial(get_filtered_proof_requests, fulfillment=FulfillmentStatus("REQUESTED")),
            interval=15,
        ),
        TaskConfig(
            func=partial(get_filtered_proof_requests, fulfillment=FulfillmentStatus("ASSIGNED")),
            interval=15,
        ),
        TaskConfig(
            func=partial(get_filtered_proof_requests, fulfillment=FulfillmentStatus("FULFILLED")),
            interval=15,
        ),
        TaskConfig(
            func=partial(get_filtered_proof_requests, fulfillment=FulfillmentStatus("UNFULFILLABLE")),
            interval=15,
        ),
        TaskConfig(
            func=partial(get_filtered_prover_stats, is_featured=True),
            interval=15,
        ),
        TaskConfig(
            func=partial(get_filtered_suspendable_provers, recent_failure_window=60, total_failure_window=60),
            interval=15,
        ),
        TaskConfig(
            func=partial(get_filtered_prover_bid_history, is_featured=True),
            interval=30,
        ),
    ]

    for task_config in task_configs:
        threading.Thread(
            target=run_periodically,
            args=(task_config.func, task_config.interval),
            daemon=True,
        ).start()

    shutdown_event.wait()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s %(threadName)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    main()
