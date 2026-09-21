import json
import runpy

from src.aiops_pipeline import load_data, run_pipeline
from src.anomaly_detector import AnomalyDetector
from src.calculations import area_of_circle, get_nth_fibonacci
from src.event_producer import EventProducer
from src.event_topic import EventTopic


def test_load_data(tmp_path):
    data = [{"service": "test-service"}]

    file_path = tmp_path / "data.json"
    file_path.write_text(json.dumps(data), encoding="utf-8")

    assert load_data(file_path) == data


def test_run_pipeline_with_normal_and_anomalous_records(tmp_path):
    data = [
        {
            "timestamp": "2026-09-20T10:00:00",
            "service": "payment-service",
            "response_time_ms": 120,
            "cpu_percent": 42,
            "memory_percent": 51,
            "log_level": "INFO",
            "message": "OK"
        },
        {
            "timestamp": "2026-09-20T10:05:00",
            "service": "payment-service",
            "response_time_ms": 610,
            "cpu_percent": 75,
            "memory_percent": 70,
            "log_level": "ERROR",
            "message": "Timeout"
        }
    ]

    file_path = tmp_path / "data.json"
    file_path.write_text(json.dumps(data), encoding="utf-8")

    result = run_pipeline(file_path)

    assert result["records_processed"] == 2
    assert len(result["anomalies_detected"]) == 1
    assert result["anomalies_detected"][0]["type"] == "ANOMALY"
    assert result["events_consumed"] == []


def test_anomaly_cpu_memory_and_warning():
    detector = AnomalyDetector()

    record = {
        "timestamp": "2026-09-20T10:00:00",
        "service": "test-service",
        "response_time_ms": 100,
        "cpu_percent": 90,
        "memory_percent": 90,
        "log_level": "WARNING",
        "message": "Warning"
    }

    event = detector.detect(record)

    assert event is not None
    assert "High CPU utilization" in event["reasons"]
    assert "High memory utilization" in event["reasons"]
    assert "Error log detected" in event["reasons"]


def test_area_negative_radius():
    import pytest

    with pytest.raises(ValueError):
        area_of_circle(-1)


def test_fibonacci_negative():
    import pytest

    with pytest.raises(ValueError):
        get_nth_fibonacci(-1)


def test_fibonacci_recursive_loop():
    assert get_nth_fibonacci(10) == 55


def test_producer_empty_event():
    topic = EventTopic("test")
    producer = EventProducer(topic)

    assert producer.publish(None) is False
    assert topic.get_messages() == []


def test_topic_clear():
    topic = EventTopic("test")
    topic.publish({"type": "TEST"})

    assert len(topic.get_messages()) == 1

    topic.clear()

    assert topic.get_messages() == []


def test_main_block():
    runpy.run_path("src/aiops_pipeline.py", run_name="__main__")
