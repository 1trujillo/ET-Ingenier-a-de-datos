import argparse
import time
from typing import Any

from main import run_scenario


def print_header(title: str) -> None:
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def dump_summary(summary: dict[str, Any]) -> None:
    print("\n--- Scenario summary ---")
    for key in [
        "scenario",
        "total_events",
        "successful_events",
        "failed_events",
        "success_rate_pct",
        "throughput_events_per_min",
        "etl_duration_seconds",
        "accidents_by_comuna",
    ]:
        print(f"{key}={summary.get(key)}")


def report_result(label: str, failures: list[str]) -> bool:
    if failures:
        print(f"[FAIL] {label}")
        for reason in failures:
            print(f"  - {reason}")
        return False

    print(f"[OK] {label}")
    return True


def validate_normal(summary: dict[str, Any]) -> bool:
    failures = []
    if summary["total_events"] < 100:
        failures.append("El pipeline no procesó suficientes eventos en carga normal")
    if summary["success_rate_pct"] < 85.0:
        failures.append(f"Tasa de éxito baja: {summary['success_rate_pct']}% < 85%")
    if summary["throughput_events_per_min"] < 5000:
        failures.append(f"Throughput insuficiente en normal: {summary['throughput_events_per_min']}")
    return report_result("NORMAL LOAD", failures)


def validate_high_load(summary: dict[str, Any], target_rate_per_sec: float) -> bool:
    failures = []
    expected_throughput_min = target_rate_per_sec * 60 * 0.70
    if summary["throughput_events_per_min"] < expected_throughput_min:
        failures.append(
            f"Throughput no alcanzó el 70% del objetivo: {summary['throughput_events_per_min']} < {expected_throughput_min:.0f}"
        )
    if summary["success_rate_pct"] < 70.0:
        failures.append(f"Tasa de éxito demasiado baja en alta carga: {summary['success_rate_pct']}% < 70%")
    if summary["etl_duration_seconds"] > 1.2:
        failures.append(f"Duración de lote ETL excesiva: {summary['etl_duration_seconds']}s > 1.2s")
    return report_result("HIGH LOAD", failures)


def validate_error_scenario(summary: dict[str, Any]) -> bool:
    failures = []
    if summary["failed_events"] <= 0:
        failures.append("No se detectaron eventos fallidos en ERROR SCENARIO")
    if summary["success_rate_pct"] >= 95.0:
        failures.append(f"Tasa de éxito demasiado alta en ERROR SCENARIO: {summary['success_rate_pct']}% >= 95%")
    if summary["total_events"] < 100:
        failures.append("No se generaron suficientes eventos para validar el escenario de error")
    return report_result("ERROR SCENARIO", failures)


def validate_accident_spike(summary: dict[str, Any]) -> bool:
    failures = []
    total_accidents = sum(summary.get("accidents_by_comuna", {}).values())
    if total_accidents <= 0:
        failures.append("No se detectaron accidentes durante el spike")
    if not any(count >= 20 for count in summary.get("accidents_by_comuna", {}).values()):
        failures.append("No hay un pico lo suficientemente alto de accidentes por comuna")
    if summary["throughput_events_per_min"] < 2000:
        failures.append(f"Throughput bajo en ACCIDENT SPIKE: {summary['throughput_events_per_min']}")
    return report_result("ACCIDENT SPIKE", failures)


def validate_max_stress(summary: dict[str, Any], target_rate_per_sec: float) -> bool:
    failures = []
    expected_throughput_min = target_rate_per_sec * 60 * 0.65
    if summary["throughput_events_per_min"] < expected_throughput_min:
        failures.append(
            f"No se alcanzó la presión esperada: {summary['throughput_events_per_min']} < {expected_throughput_min:.0f} evt/min"
        )
    if summary["etl_duration_seconds"] > 1.5:
        failures.append(f"Procesamiento de lote demasiado lento bajo estrés máximo: {summary['etl_duration_seconds']}s")
    if summary["success_rate_pct"] < 60.0:
        failures.append(f"Tasa de éxito excesivamente baja en estrés máximo: {summary['success_rate_pct']}% < 60%")
    if summary["failed_events"] < 5:
        failures.append("No se introdujeron suficientes errores reales bajo estrés máximo")
    return report_result("MAXIMUM STRESS", failures)


def run_scenario_and_validate(
    label: str,
    duration: int,
    batch_size: int,
    target_rate_per_sec: float,
    validator: Any,
) -> bool:
    print_header(f"RUNNING SCENARIO: {label}")
    summary = run_scenario(
        label,
        duration_seconds=duration,
        batch_size=batch_size,
        target_rate_per_sec=target_rate_per_sec,
    )
    dump_summary(summary)
    return validator(summary) if validator else True


def main() -> None:
    parser = argparse.ArgumentParser(description="Stress test runner for urban mobility pipeline")
    parser.add_argument("--duration", type=int, default=45, help="Seconds per scenario")
    parser.add_argument("--batch-size", type=int, default=50, help="Events per ETL batch")
    parser.add_argument("--normal-rate", type=float, default=180.0, help="Events/sec for NORMAL LOAD")
    parser.add_argument("--high-rate", type=float, default=480.0, help="Events/sec for HIGH LOAD")
    parser.add_argument("--error-rate", type=float, default=340.0, help="Events/sec for ERROR SCENARIO")
    parser.add_argument("--accident-rate", type=float, default=240.0, help="Events/sec for ACCIDENT SPIKE")
    parser.add_argument("--max-rate", type=float, default=620.0, help="Events/sec for MAXIMUM STRESS")
    args = parser.parse_args()

    scenarios = [
        ("NORMAL LOAD", args.normal_rate, validate_normal),
        ("HIGH LOAD", args.high_rate, lambda summary: validate_high_load(summary, args.high_rate)),
        ("ERROR SCENARIO", args.error_rate, validate_error_scenario),
        ("ACCIDENT SPIKE", args.accident_rate, validate_accident_spike),
        ("MAXIMUM STRESS", args.max_rate, lambda summary: validate_max_stress(summary, args.max_rate)),
    ]

    print_header("URBAN MOBILITY PIPELINE STRESS TEST RUNNER")
    passed = 0
    failed = 0

    for label, rate, validator in scenarios:
        ok = run_scenario_and_validate(label, duration=args.duration, batch_size=args.batch_size, target_rate_per_sec=rate, validator=validator)
        if ok:
            passed += 1
        else:
            failed += 1
        time.sleep(3)

    print_header("FINAL TEST RESULT")
    print(f"passed={passed} failed={failed}")
    if failed == 0:
        print("✅ ALL SCENARIOS PASSED")
    else:
        print("❌ AT LEAST ONE SCENARIO FAILED")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
'@; Set-Content -Path tmp_test.py -Value $content -Encoding UTF8"