import os
import queue
import random
import statistics
import threading
import time
from collections import Counter
from dataclasses import dataclass

try:
    import requests
except ImportError:  # pragma: no cover - optional dependency
    requests = None


@dataclass
class Event:
    """Synthetic mobility event produced by sensors or cameras."""

    comuna: str
    tipo_evento: str
    velocidad: float
    timestamp: str
    corrupted: bool = False
    source: str = "sensor"


class UrbanMobilityPipeline:
    """Simulates a streaming ETL pipeline with synthetic load and failures."""

    def __init__(self, name: str = "urban-pipeline", batch_size: int = 25):
        self.name = name
        self.batch_size = batch_size
        self.event_queue: "queue.Queue[Event]" = queue.Queue(maxsize=500)
        self.lock = threading.Lock()
        self.stop_event = threading.Event()

        self.total_events = 0
        self.successful_events = 0
        self.failed_events = 0
        self.accidents_by_comuna = Counter()
        self.batch_durations = []
        self.queue_depth_samples = []

    def _build_event(self, scenario: str) -> Event:
        comunas = ["santiago", "las_condes", "ñuñoa", "maipu", "providencia"]
        tipos = ["vehiculo", "semaforo", "accidente"]

        if scenario == "accident_spike":
            if random.random() < 0.55:
                return Event(
                    comuna="santiago",
                    tipo_evento="accidente",
                    velocidad=max(0.0, random.gauss(12, 5)),
                    timestamp=time.strftime("%Y-%m-%dT%H:%M:%S"),
                    corrupted=False,
                    source="camera",
                )

        if scenario == "error":
            # Inject a high rate of corrupted events for the error scenario.
            # Increase probability to aggressively force failures during testing.
            if random.random() < 0.75:
                # Return an event that will always fail validation (None fields, invalid speed)
                return Event(
                    comuna=None,
                    tipo_evento=None,
                    velocidad=-999,
                    timestamp="",
                    corrupted=True,
                    source="sensor",
                )

        if random.random() < 0.10:
            tipo = "accidente"
            comuna = random.choice(["santiago", "maipu", "ñuñoa"])
            velocidad = max(0.0, random.gauss(15, 6))
        else:
            tipo = random.choice(tipos)
            comuna = random.choice(comunas)
            velocidad = max(0.0, random.gauss(40, 12))

        if scenario == "high_load" and random.random() < 0.08:
            velocidad = max(0.0, random.gauss(60, 20))

        return Event(
            comuna=comuna,
            tipo_evento=tipo,
            velocidad=velocidad,
            timestamp=time.strftime("%Y-%m-%dT%H:%M:%S"),
            corrupted=False,
            source="sensor",
        )

    def _is_valid_event(self, event: Event) -> bool:
        if event.corrupted:
            return False
        if not event.comuna or not event.tipo_evento or not event.timestamp:
            return False
        if event.velocidad is None or event.velocidad < 0 or event.velocidad > 180:
            return False
        return True

    def _process_event(self, event: Event) -> None:
        self.total_events += 1

        # Simulate ETL processing time and occasional latency spikes.
        processing_delay = random.uniform(0.001, 0.004)
        if event.tipo_evento == "accidente":
            processing_delay += 0.002
        # Base processing time
        time.sleep(processing_delay)

        # If the event is explicitly marked corrupted, add extra latency to
        # simulate retry/backoff or additional validation cost, then mark as failed.
        if event.corrupted:
            time.sleep(0.005)
            # Corrupted events always fail validation path
            self.failed_events += 1
            return

        # Small chance of hard failure for otherwise-valid events to simulate
        # random downstream errors (network, DB contention, etc.). 5% is a
        # realistic small-failure probability under stress.
        if random.random() < 0.05:
            self.failed_events += 1
            return

        if self._is_valid_event(event):
            self.successful_events += 1
            if event.tipo_evento == "accidente":
                self.accidents_by_comuna[event.comuna] += 1
        else:
            self.failed_events += 1

    def _process_batch(self, batch: list[Event]) -> None:
        batch_start = time.perf_counter()
        for event in batch:
            self._process_event(event)
        self.batch_durations.append(time.perf_counter() - batch_start)

    def _producer(self, scenario: str, target_rate_per_sec: float, duration_seconds: int) -> None:
        end_time = time.time() + duration_seconds
        while time.time() < end_time and not self.stop_event.is_set():
            self.event_queue.put(self._build_event(scenario))
            interval = max(0.0, 1.0 / target_rate_per_sec)
            time.sleep(interval)

    def _consumer(self) -> None:
        while not self.stop_event.is_set() or not self.event_queue.empty():
            batch = []
            while len(batch) < self.batch_size:
                try:
                    batch.append(self.event_queue.get(timeout=0.1))
                except queue.Empty:
                    break

            if not batch:
                time.sleep(0.005)
                continue

            self._process_batch(batch)
            self.queue_depth_samples.append(self.event_queue.qsize())

    def _emit_metrics(self, scenario: str, started_at: float, final: bool = False) -> None:
        elapsed = max(time.time() - started_at, 1e-6)
        total = self.total_events
        success_rate = (self.successful_events / total * 100.0) if total else 0.0
        throughput = (self.total_events / (elapsed / 60.0)) if elapsed else 0.0
        etl_duration = statistics.mean(self.batch_durations) if self.batch_durations else 0.0

        send_metric(
            "pipeline.success_rate_pct",
            round(success_rate, 2),
            ["env:test", f"scenario:{scenario}"],
        )
        send_metric(
            "pipeline.throughput.events_per_min",
            round(throughput, 2),
            ["env:test", f"scenario:{scenario}"],
        )
        send_metric(
            "etl_duration_seconds",
            round(etl_duration, 4),
            ["env:test", f"scenario:{scenario}"],
        )
        send_metric(
            "events_failed",
            self.failed_events,
            ["env:test", f"scenario:{scenario}"],
        )

        for comuna, count in sorted(self.accidents_by_comuna.items()):
            send_metric(
                "movilidad.accidentes_por_comuna",
                count,
                ["env:test", f"scenario:{scenario}", f"comuna:{comuna}"],
            )

        print(
            f"[{scenario}] processed={total} success_rate={success_rate:.2f}% "
            f"failed={self.failed_events} throughput={throughput:.2f} evt/min "
            f"etl_batch={etl_duration:.4f}s queue={self.event_queue.qsize()}"
        )

        if final:
            print(f"[{scenario}] summary -> accidents={dict(self.accidents_by_comuna)}")

    def run(self, duration_seconds: int, scenario: str, target_rate_per_sec: float = 120.0) -> dict:
        started_at = time.time()
        self.stop_event.clear()

        producer = threading.Thread(
            target=self._producer,
            args=(scenario, target_rate_per_sec, duration_seconds),
            daemon=True,
        )
        consumer = threading.Thread(target=self._consumer, daemon=True)

        producer.start()
        consumer.start()

        while time.time() < started_at + duration_seconds:
            self._emit_metrics(scenario, started_at)
            time.sleep(1.0)

        self.stop_event.set()
        producer.join(timeout=2)
        consumer.join(timeout=2)
        self._emit_metrics(scenario, started_at, final=True)

        return {
            "scenario": scenario,
            "total_events": self.total_events,
            "successful_events": self.successful_events,
            "failed_events": self.failed_events,
            "success_rate_pct": round((self.successful_events / self.total_events * 100.0) if self.total_events else 0.0, 2),
            "throughput_events_per_min": round((self.total_events / (max(time.time() - started_at, 1e-6) / 60.0)) if time.time() - started_at else 0.0, 2),
            "etl_duration_seconds": round(statistics.mean(self.batch_durations) if self.batch_durations else 0.0, 4),
            "accidents_by_comuna": dict(self.accidents_by_comuna),
        }


def send_metric(name: str, value: float, tags: list[str] | None = None) -> None:
    """Print the metric and optionally forward it to a Datadog-like endpoint."""

    tags = tags or []
    print(f"[METRIC] {name}={value} tags={tags}")

    if not os.getenv("DATADOG_URL"):
        return

    if requests is None:
        print("[DATADOG] requests package is not installed; skipping API send")
        return

    try:
        requests.post(
            os.getenv("DATADOG_URL"),
            json={"series": [{"metric": name, "points": [[int(time.time()), value]], "tags": tags}]},
            timeout=0.2,
        )
    except Exception as exc:  # pragma: no cover - network simulation
        print(f"[DATADOG] send failed: {exc}")


def run_scenario(name: str, duration_seconds: int = 30, batch_size: int = 25, target_rate_per_sec: float = 120.0) -> dict:
    pipeline = UrbanMobilityPipeline(name=name, batch_size=batch_size)
    return pipeline.run(duration_seconds=duration_seconds, scenario=name.lower().replace(" ", "_"), target_rate_per_sec=target_rate_per_sec)


if __name__ == "__main__":
    print("Starting urban mobility pipeline simulation...")
    result = run_scenario("NORMAL LOAD", duration_seconds=10, batch_size=20, target_rate_per_sec=80)
    print("Completed.")
    print(result)
