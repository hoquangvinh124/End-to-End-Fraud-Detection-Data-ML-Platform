from __future__ import annotations

from pathlib import Path

LAKEHOUSE_ROOT = Path(__file__).resolve().parents[2] / "lakehouse"


def test_trino_memory_fits_the_local_full_stack_and_restarts_after_oom():
    jvm = (LAKEHOUSE_ROOT / "trino" / "etc" / "jvm.config").read_text(
        encoding="utf-8"
    )
    config = (LAKEHOUSE_ROOT / "trino" / "etc" / "config.properties").read_text(
        encoding="utf-8"
    )
    compose = (LAKEHOUSE_ROOT / "docker-compose.lakehouse.yml").read_text(
        encoding="utf-8"
    )

    assert "-Xmx4G" in jvm
    assert "-Xmx8G" not in jvm
    assert "G1UsePreventiveGC" not in jvm
    assert "-agentpath:/usr/lib/trino/bin/libjvmkill.so" in jvm
    assert "query.max-memory-per-node=2GB" in config
    assert "query.max-memory=3GB" in config
    assert "query.max-total-memory-per-node" not in config
    assert "spiller-spill-path=/mnt/trino-spill" in config
    assert "query-max-spill-per-node=20GB" in config
    assert "memory.heap-headroom-per-node=512MB" in config
    assert "./trino/etc/jvm.config:/etc/trino/jvm.config:ro" in compose
    assert "./trino/etc/config.properties:/etc/trino/config.properties:ro" in compose
    assert "./trino/etc/node.properties:/etc/trino/node.properties:ro" in compose
    assert "./trino/etc:/usr/lib/trino/etc:ro" not in compose
    assert "  trino-spill-init:" in compose
    assert "chown -R 1000:1000 /mnt/trino-spill" in compose
    assert "trino-spill-init:\n        condition: service_completed_successfully" in compose
    assert 'test: ["CMD-SHELL", "/usr/lib/trino/bin/health-check"]' in compose
    assert "start_period: 90s" in compose
    assert "retries: 12" in compose
    assert "restart: unless-stopped" in compose.split("  clickhouse:", 1)[0]
