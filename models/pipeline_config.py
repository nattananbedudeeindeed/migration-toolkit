"""
PipelineConfig — domain model for a data pipeline.

A pipeline chains multiple MigrationConfig names into an ordered execution
plan with optional inter-step dependencies, a shared error strategy, and
shared source/target datasource settings.

Follows the same from_dict / to_dict pattern as migration_config.py.
No Streamlit imports — pure domain model.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
import uuid


@dataclass
class PipelineStep:
    order: int
    config_name: str
    depends_on: list[str] = field(default_factory=list)
    enabled: bool = True

    @classmethod
    def from_dict(cls, d: dict) -> "PipelineStep":
        return cls(
            order=d.get("order", 0),
            config_name=d.get("config_name", ""),
            depends_on=d.get("depends_on", []),
            enabled=d.get("enabled", True),
        )

    def to_dict(self) -> dict:
        return {
            "order": self.order,
            "config_name": self.config_name,
            "depends_on": self.depends_on,
            "enabled": self.enabled,
        }


@dataclass
class PipelineConfig:
    id: str
    name: str
    description: str = ""
    steps: list[PipelineStep] = field(default_factory=list)
    source_datasource_id: int | None = None
    target_datasource_id: int | None = None
    error_strategy: str = "fail_fast"   # fail_fast | continue_on_error | skip_dependents
    batch_size: int = 1000
    truncate_targets: bool = False
    created_at: str = ""
    updated_at: str = ""

    @classmethod
    def new(cls, name: str, **kwargs) -> "PipelineConfig":
        """Convenience factory: auto-generates UUID and ISO timestamps."""
        now = datetime.now().isoformat()
        return cls(id=str(uuid.uuid4()), name=name, created_at=now, updated_at=now, **kwargs)

    @classmethod
    def from_dict(cls, d: dict) -> "PipelineConfig":
        return cls(
            id=d.get("id", str(uuid.uuid4())),
            name=d.get("name", ""),
            description=d.get("description", ""),
            steps=[PipelineStep.from_dict(s) for s in d.get("steps", [])],
            source_datasource_id=d.get("source_datasource_id"),
            target_datasource_id=d.get("target_datasource_id"),
            error_strategy=d.get("error_strategy", "fail_fast"),
            batch_size=d.get("batch_size", 1000),
            truncate_targets=d.get("truncate_targets", False),
            created_at=d.get("created_at", ""),
            updated_at=d.get("updated_at", ""),
        )

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "steps": [s.to_dict() for s in self.steps],
            "source_datasource_id": self.source_datasource_id,
            "target_datasource_id": self.target_datasource_id,
            "error_strategy": self.error_strategy,
            "batch_size": self.batch_size,
            "truncate_targets": self.truncate_targets,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }
