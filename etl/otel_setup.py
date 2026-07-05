import logging
import os
from typing import Optional, Tuple

from opentelemetry import metrics, trace
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor


def _build_otlp_exporter(exporter_cls, endpoint: str, timeout: int = 10):
    try:
        return exporter_cls(endpoint=endpoint, insecure=True, timeout=timeout)
    except TypeError:
        return exporter_cls(endpoint=endpoint, timeout=timeout)


def configure_observability(
    service_name: str,
    log_level: int = logging.INFO,
    service_version: Optional[str] = None,
    environment: Optional[str] = None,
) -> Tuple[object, object, object]:
    """Configure OpenTelemetry exporters for traces, metrics, and logs."""
    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://datadog-agent:4318")
    endpoint = endpoint.rstrip("/")

    # Control whether to send logs via OTLP. Many local Datadog agent
    # installations don't expose an OTLP logs HTTP endpoint, so keep
    # this disabled by default and rely on the Datadog agent's
    # container log collection instead.
    send_logs = os.getenv("OTEL_EXPORTER_OTLP_SEND_LOGS", "false").lower() in (
        "true",
        "1",
        "yes",
    )

    resource = Resource.create(
        {
            "service.name": service_name,
            "service.version": service_version or os.getenv("SERVICE_VERSION", "dev"),
            "deployment.environment": environment or os.getenv("OTEL_ENVIRONMENT", "local"),
        }
    )

    metric_reader = PeriodicExportingMetricReader(
        _build_otlp_exporter(OTLPMetricExporter, f"{endpoint}/v1/metrics"),
        export_interval_millis=10000,
    )
    metrics.set_meter_provider(MeterProvider(resource=resource, metric_readers=[metric_reader]))

    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(
        BatchSpanProcessor(
            _build_otlp_exporter(OTLPSpanExporter, f"{endpoint}/v1/traces")
        )
    )
    trace.set_tracer_provider(tracer_provider)

    logger_provider = None
    if send_logs:
        logger_provider = LoggerProvider(resource=resource)
        logger_provider.add_log_record_processor(
            BatchLogRecordProcessor(
                _build_otlp_exporter(OTLPLogExporter, f"{endpoint}/v1/logs")
            )
        )
        set_logger_provider(logger_provider)

        root_logger = logging.getLogger()
        root_logger.setLevel(log_level)
        root_logger.addHandler(LoggingHandler(level=log_level, logger_provider=logger_provider))

    meter = metrics.get_meter(service_name)
    tracer = trace.get_tracer(service_name)
    return meter, tracer, logger_provider
