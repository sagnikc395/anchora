from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def test_dockerfile_exposes_runnable_targets() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")

    for target in ("api", "worker", "starter", "tests"):
        assert re.search(rf"^FROM\s+\S+\s+AS\s+{target}$", dockerfile, re.MULTILINE)

    assert 'CMD ["uvicorn", "flowforge.api.app:app"' in dockerfile
    assert 'CMD ["python", "-m", "flowforge.worker.worker"]' in dockerfile
    assert 'CMD ["python", "-m", "flowforge.api.starter"]' in dockerfile
    assert 'CMD ["pytest", "-q"]' in dockerfile


def test_compose_wires_local_services_to_temporal() -> None:
    compose = (ROOT / "compose.yaml").read_text(encoding="utf-8")

    for service in ("temporal", "api", "worker", "starter", "tests"):
        assert re.search(rf"^  {service}:$", compose, re.MULTILINE)

    assert "temporalio/temporal:latest" in compose
    assert "target: api" in compose
    assert "target: worker" in compose
    assert "target: starter" in compose
    assert "target: tests" in compose
    assert "TEMPORAL_HOST: temporal:7233" in compose
    assert "condition: service_healthy" in compose
