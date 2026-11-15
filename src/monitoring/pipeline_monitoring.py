from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text

from src.loading.load_silver_to_postgres import (
    get_postgres_engine,
)
from src.utils.run_context import (
    get_git_branch,
    get_git_commit_sha,
    is_git_worktree_dirty,
)


MONITORING_SCHEMA = "monitoring"
PIPELINE_RUNS_TABLE = "pipeline_runs"


def _normalize_datetime(
    value: datetime | str | None,
) -> datetime | None:
    """
    Normalise une date en datetime timezone-aware.

    Le monitoring travaille exclusivement
    avec des timestamps timezone-aware.
    """

    if value is None:
        return None

    if isinstance(value, str):
        value = datetime.fromisoformat(
            value.replace(
                "Z",
                "+00:00",
            )
        )

    if value.tzinfo is None:
        raise ValueError(
            "Le timestamp de monitoring "
            "doit être timezone-aware."
        )

    return value.astimezone(
        timezone.utc
    )


def _clean_error_message(
    error_message: str | None,
) -> str | None:
    """
    Évite d'enregistrer des traces gigantesques
    dans la table de monitoring.
    """

    if not error_message:
        return None

    cleaned = str(
        error_message
    ).strip()

    if not cleaned:
        return None

    return cleaned[:4000]


def start_pipeline_run(
    *,
    dag_id: str,
    dag_run_id: str,
    logical_date: datetime | str | None,
    started_at: datetime | str | None = None,
) -> dict[str, Any]:
    """
    Enregistre le démarrage d'un DAG Run.

    L'opération est idempotente grâce à
    l'unicité :

        dag_id + dag_run_id

    Un retry ne crée donc pas un nouveau run.
    """

    if not dag_id:
        raise ValueError(
            "dag_id est obligatoire."
        )

    if not dag_run_id:
        raise ValueError(
            "dag_run_id est obligatoire."
        )

    logical_date_normalized = (
        _normalize_datetime(
            logical_date
        )
    )

    started_at_normalized = (
        _normalize_datetime(
            started_at
        )
        or datetime.now(
            timezone.utc
        )
    )

    git_commit_sha = (
        get_git_commit_sha()
    )

    git_branch = (
        get_git_branch()
    )

    git_worktree_dirty = (
        is_git_worktree_dirty()
    )

    engine = get_postgres_engine()

    query = text(
        f"""
        INSERT INTO
            {MONITORING_SCHEMA}.
            {PIPELINE_RUNS_TABLE}
        (
            dag_id,
            dag_run_id,
            logical_date,
            status,
            started_at,
            git_commit_sha,
            git_branch,
            git_worktree_dirty
        )
        VALUES
        (
            :dag_id,
            :dag_run_id,
            :logical_date,
            'RUNNING',
            :started_at,
            :git_commit_sha,
            :git_branch,
            :git_worktree_dirty
        )

        ON CONFLICT (
            dag_id,
            dag_run_id
        )
        DO UPDATE SET
            status = 'RUNNING',
            logical_date = EXCLUDED.logical_date,
            started_at = LEAST(
                {MONITORING_SCHEMA}.
                {PIPELINE_RUNS_TABLE}.started_at,
                EXCLUDED.started_at
            ),
            finished_at = NULL,
            duration_seconds = NULL,
            failed_task_id = NULL,
            error_message = NULL,
            dbt_status = NULL,
            git_commit_sha = EXCLUDED.git_commit_sha,
            git_branch = EXCLUDED.git_branch,
            git_worktree_dirty =
                EXCLUDED.git_worktree_dirty,
            updated_at = NOW()

        RETURNING
            pipeline_run_id,
            dag_id,
            dag_run_id,
            status,
            started_at,
            git_commit_sha,
            git_branch,
            git_worktree_dirty;
        """
    )

    with engine.begin() as connection:
        result = connection.execute(
            query,
            {
                "dag_id": dag_id,
                "dag_run_id": dag_run_id,
                "logical_date": (
                    logical_date_normalized
                ),
                "started_at": (
                    started_at_normalized
                ),
                "git_commit_sha": (
                    git_commit_sha
                ),
                "git_branch": (
                    git_branch
                ),
                "git_worktree_dirty": (
                    git_worktree_dirty
                ),
            },
        )

        row = (
            result
            .mappings()
            .one()
        )

    return dict(
        row
    )


def complete_pipeline_run(
    *,
    dag_id: str,
    dag_run_id: str,
    batch_id: str,
    processing_run_id: str,
    raw_object_key: str,
    bronze_object_key: str,
    silver_object_key: str,
    raw_row_count: int,
    bronze_row_count: int,
    silver_row_count: int,
    postgres_row_count: int,
    dbt_status: str = "SUCCESS",
    finished_at: datetime | str | None = None,
) -> dict[str, Any]:
    """
    Marque un DAG Run comme terminé avec succès
    et enregistre son résumé fonctionnel.
    """

    if dbt_status != "SUCCESS":
        raise ValueError(
            "Un pipeline SUCCESS doit avoir "
            "dbt_status='SUCCESS'."
        )

    finished_at_normalized = (
        _normalize_datetime(
            finished_at
        )
        or datetime.now(
            timezone.utc
        )
    )

    engine = get_postgres_engine()

    query = text(
        f"""
        UPDATE
            {MONITORING_SCHEMA}.
            {PIPELINE_RUNS_TABLE}

        SET
            status = 'SUCCESS',

            finished_at = :finished_at,

            duration_seconds =
                EXTRACT(
                    EPOCH FROM (
                        :finished_at
                        - started_at
                    )
                ),

            batch_id = :batch_id,

            processing_run_id =
                :processing_run_id,

            raw_object_key =
                :raw_object_key,

            bronze_object_key =
                :bronze_object_key,

            silver_object_key =
                :silver_object_key,

            raw_row_count =
                :raw_row_count,

            bronze_row_count =
                :bronze_row_count,

            silver_row_count =
                :silver_row_count,

            postgres_row_count =
                :postgres_row_count,

            dbt_status =
                :dbt_status,

            failed_task_id = NULL,

            error_message = NULL,

            updated_at = NOW()

        WHERE
            dag_id = :dag_id
            AND dag_run_id = :dag_run_id

        RETURNING
            pipeline_run_id,
            dag_id,
            dag_run_id,
            status,
            started_at,
            finished_at,
            duration_seconds,
            batch_id,
            processing_run_id,
            raw_row_count,
            bronze_row_count,
            silver_row_count,
            postgres_row_count,
            dbt_status;
        """
    )

    with engine.begin() as connection:
        result = connection.execute(
            query,
            {
                "dag_id": dag_id,
                "dag_run_id": dag_run_id,
                "finished_at": (
                    finished_at_normalized
                ),
                "batch_id": batch_id,
                "processing_run_id": (
                    processing_run_id
                ),
                "raw_object_key": (
                    raw_object_key
                ),
                "bronze_object_key": (
                    bronze_object_key
                ),
                "silver_object_key": (
                    silver_object_key
                ),
                "raw_row_count": (
                    raw_row_count
                ),
                "bronze_row_count": (
                    bronze_row_count
                ),
                "silver_row_count": (
                    silver_row_count
                ),
                "postgres_row_count": (
                    postgres_row_count
                ),
                "dbt_status": dbt_status,
            },
        )

        row = (
            result
            .mappings()
            .one_or_none()
        )

    if row is None:
        raise RuntimeError(
            "Impossible de terminer le monitoring : "
            "aucun pipeline RUNNING correspondant "
            f"à dag_id={dag_id!r}, "
            f"dag_run_id={dag_run_id!r}."
        )

    return dict(
        row
    )


def fail_pipeline_run(
    *,
    dag_id: str,
    dag_run_id: str,
    failed_task_id: str | None,
    error_message: str | None,
    finished_at: datetime | str | None = None,
) -> dict[str, Any]:
    """
    Marque un DAG Run comme FAILED.

    Cette fonction ne modifie pas les données
    métier déjà enregistrées pour le run.
    """

    finished_at_normalized = (
        _normalize_datetime(
            finished_at
        )
        or datetime.now(
            timezone.utc
        )
    )

    cleaned_error = (
        _clean_error_message(
            error_message
        )
    )

    engine = get_postgres_engine()

    query = text(
        f"""
        UPDATE
            {MONITORING_SCHEMA}.
            {PIPELINE_RUNS_TABLE}

        SET
            status = 'FAILED',

            finished_at = :finished_at,

            duration_seconds =
                EXTRACT(
                    EPOCH FROM (
                        :finished_at
                        - started_at
                    )
                ),

            failed_task_id =
                :failed_task_id,

            error_message =
                :error_message,

            dbt_status =
                CASE
                    WHEN :failed_task_id = 'dbt_build'
                    THEN 'FAILED'
                    ELSE dbt_status
                END,

            updated_at = NOW()

        WHERE
            dag_id = :dag_id
            AND dag_run_id = :dag_run_id

        RETURNING
            pipeline_run_id,
            dag_id,
            dag_run_id,
            status,
            started_at,
            finished_at,
            duration_seconds,
            failed_task_id,
            error_message,
            dbt_status;
        """
    )

    with engine.begin() as connection:
        result = connection.execute(
            query,
            {
                "dag_id": dag_id,
                "dag_run_id": dag_run_id,
                "finished_at": (
                    finished_at_normalized
                ),
                "failed_task_id": (
                    failed_task_id
                ),
                "error_message": (
                    cleaned_error
                ),
            },
        )

        row = (
            result
            .mappings()
            .one_or_none()
        )

    if row is None:
        raise RuntimeError(
            "Impossible d'enregistrer l'échec : "
            "aucun pipeline RUNNING correspondant "
            f"à dag_id={dag_id!r}, "
            f"dag_run_id={dag_run_id!r}."
        )

    return dict(
        row
    )

ALLOWED_QUALITY_LAYERS = {
    "RAW",
    "BRONZE",
    "SILVER",
    "POSTGRES",
    "DBT",
}

ALLOWED_METRIC_UNITS = {
    "count",
    "percent",
    "boolean",
}


def record_data_quality_metrics(
    *,
    dag_id: str,
    dag_run_id: str,
    metrics: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    Historise les métriques de qualité
    associées à un DAG Run.

    Une métrique est identifiée par :

        pipeline_run_id
        + layer
        + metric_name

    L'écriture est idempotente :
    une métrique rejouée est mise à jour
    au lieu d'être dupliquée.
    """

    if not dag_id:
        raise ValueError(
            "dag_id est obligatoire."
        )

    if not dag_run_id:
        raise ValueError(
            "dag_run_id est obligatoire."
        )

    if not metrics:
        raise ValueError(
            "La liste metrics ne peut pas "
            "être vide."
        )

    normalized_metrics = []

    for metric in metrics:

        layer = str(
            metric.get(
                "layer",
                "",
            )
        ).strip().upper()

        metric_name = str(
            metric.get(
                "metric_name",
                "",
            )
        ).strip()

        metric_unit = str(
            metric.get(
                "metric_unit",
                "",
            )
        ).strip().lower()

        source_task_id = str(
            metric.get(
                "source_task_id",
                "",
            )
        ).strip()

        metric_value = metric.get(
            "metric_value"
        )

        if layer not in ALLOWED_QUALITY_LAYERS:
            raise ValueError(
                "Layer de monitoring invalide : "
                f"{layer!r}."
            )

        if not metric_name:
            raise ValueError(
                "metric_name est obligatoire."
            )

        if metric_unit not in ALLOWED_METRIC_UNITS:
            raise ValueError(
                "metric_unit invalide : "
                f"{metric_unit!r}."
            )

        if not source_task_id:
            raise ValueError(
                "source_task_id est obligatoire."
            )

        if metric_value is None:
            raise ValueError(
                f"metric_value manquant pour "
                f"{layer}.{metric_name}."
            )

        if metric_unit == "boolean":

            if metric_value not in {
                True,
                False,
                1,
                0,
            }:
                raise ValueError(
                    "Une métrique boolean doit "
                    "valoir True, False, 1 ou 0."
                )

            normalized_value = (
                1
                if bool(metric_value)
                else 0
            )

        elif metric_unit == "count":

            if isinstance(
                metric_value,
                bool,
            ):
                raise ValueError(
                    "Une métrique count ne peut "
                    "pas être un booléen."
                )

            numeric_value = float(
                metric_value
            )

            if (
                numeric_value < 0
                or not numeric_value.is_integer()
            ):
                raise ValueError(
                    "Une métrique count doit être "
                    "un entier positif ou nul."
                )

            normalized_value = int(
                numeric_value
            )

        else:

            normalized_value = float(
                metric_value
            )

            if not (
                0
                <= normalized_value
                <= 100
            ):
                raise ValueError(
                    "Une métrique percent doit "
                    "être comprise entre 0 et 100."
                )

        normalized_metrics.append(
            {
                "layer": layer,
                "metric_name": metric_name,
                "metric_value": (
                    normalized_value
                ),
                "metric_unit": metric_unit,
                "source_task_id": (
                    source_task_id
                ),
            }
        )

    engine = get_postgres_engine()

    pipeline_run_query = text(
        f"""
        SELECT
            pipeline_run_id
        FROM
            {MONITORING_SCHEMA}.
            {PIPELINE_RUNS_TABLE}
        WHERE
            dag_id = :dag_id
            AND dag_run_id = :dag_run_id;
        """
    )

    metric_query = text(
        """
        INSERT INTO
            monitoring.data_quality_metrics
        (
            pipeline_run_id,
            layer,
            metric_name,
            metric_value,
            metric_unit,
            source_task_id
        )
        VALUES
        (
            :pipeline_run_id,
            :layer,
            :metric_name,
            :metric_value,
            :metric_unit,
            :source_task_id
        )

        ON CONFLICT (
            pipeline_run_id,
            layer,
            metric_name
        )
        DO UPDATE SET
            metric_value =
                EXCLUDED.metric_value,

            metric_unit =
                EXCLUDED.metric_unit,

            source_task_id =
                EXCLUDED.source_task_id,

            updated_at = NOW()

        RETURNING
            metric_id,
            pipeline_run_id,
            layer,
            metric_name,
            metric_value,
            metric_unit,
            source_task_id;
        """
    )

    recorded_metrics = []

    with engine.begin() as connection:

        pipeline_run_id = (
            connection.execute(
                pipeline_run_query,
                {
                    "dag_id": dag_id,
                    "dag_run_id": dag_run_id,
                },
            )
            .scalar_one_or_none()
        )

        if pipeline_run_id is None:
            raise RuntimeError(
                "DAG Run introuvable dans "
                "monitoring.pipeline_runs : "
                f"dag_id={dag_id!r}, "
                f"dag_run_id={dag_run_id!r}."
            )

        for metric in normalized_metrics:

            row = (
                connection.execute(
                    metric_query,
                    {
                        "pipeline_run_id": (
                            pipeline_run_id
                        ),
                        **metric,
                    },
                )
                .mappings()
                .one()
            )

            recorded_metrics.append(
                dict(
                    row
                )
            )

    return {
        "pipeline_run_id": pipeline_run_id,
        "dag_id": dag_id,
        "dag_run_id": dag_run_id,
        "metrics_recorded": len(
            recorded_metrics
        ),
        "metrics": recorded_metrics,
    }
