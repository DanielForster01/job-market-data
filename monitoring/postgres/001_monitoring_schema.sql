CREATE SCHEMA IF NOT EXISTS monitoring;


-- ============================================================
-- Pipeline runs
-- Une ligne = un DAG Run Airflow complet.
-- ============================================================

CREATE TABLE IF NOT EXISTS monitoring.pipeline_runs (
    pipeline_run_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    dag_id TEXT NOT NULL,
    dag_run_id TEXT NOT NULL,
    logical_date TIMESTAMPTZ,

    status TEXT NOT NULL
        CHECK (
            status IN (
                'RUNNING',
                'SUCCESS',
                'FAILED'
            )
        ),

    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,

    duration_seconds DOUBLE PRECISION
        CHECK (
            duration_seconds IS NULL
            OR duration_seconds >= 0
        ),

    batch_id TEXT,
    processing_run_id TEXT,

    raw_object_key TEXT,
    bronze_object_key TEXT,
    silver_object_key TEXT,

    raw_row_count BIGINT
        CHECK (
            raw_row_count IS NULL
            OR raw_row_count >= 0
        ),

    bronze_row_count BIGINT
        CHECK (
            bronze_row_count IS NULL
            OR bronze_row_count >= 0
        ),

    silver_row_count BIGINT
        CHECK (
            silver_row_count IS NULL
            OR silver_row_count >= 0
        ),

    postgres_row_count BIGINT
        CHECK (
            postgres_row_count IS NULL
            OR postgres_row_count >= 0
        ),

    dbt_status TEXT
        CHECK (
            dbt_status IS NULL
            OR dbt_status IN (
                'SUCCESS',
                'FAILED'
            )
        ),

    failed_task_id TEXT,
    error_message TEXT,

    git_commit_sha TEXT,
    git_branch TEXT,
    git_worktree_dirty BOOLEAN,

    created_at TIMESTAMPTZ
        NOT NULL
        DEFAULT NOW(),

    updated_at TIMESTAMPTZ
        NOT NULL
        DEFAULT NOW(),

    CONSTRAINT uq_monitoring_pipeline_run
        UNIQUE (
            dag_id,
            dag_run_id
        )
);


CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status
ON monitoring.pipeline_runs (status);


CREATE INDEX IF NOT EXISTS idx_pipeline_runs_started_at
ON monitoring.pipeline_runs (started_at DESC);


CREATE INDEX IF NOT EXISTS idx_pipeline_runs_batch_id
ON monitoring.pipeline_runs (batch_id);


-- ============================================================
-- Data quality metrics
-- Format long :
-- une ligne = une métrique pour un pipeline_run.
-- ============================================================

CREATE TABLE IF NOT EXISTS monitoring.data_quality_metrics (
    metric_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    pipeline_run_id BIGINT NOT NULL,

    layer TEXT NOT NULL
        CHECK (
            layer IN (
                'RAW',
                'BRONZE',
                'SILVER',
                'POSTGRES',
                'DBT'
            )
        ),

    metric_name TEXT NOT NULL,

    metric_value NUMERIC NOT NULL,

    metric_unit TEXT NOT NULL
        CHECK (
            metric_unit IN (
                'count',
                'percent',
                'boolean'
            )
        ),

    source_task_id TEXT NOT NULL,

    created_at TIMESTAMPTZ
        NOT NULL
        DEFAULT NOW(),

    updated_at TIMESTAMPTZ
        NOT NULL
        DEFAULT NOW(),

    CONSTRAINT fk_quality_metrics_pipeline_run
        FOREIGN KEY (pipeline_run_id)
        REFERENCES monitoring.pipeline_runs (
            pipeline_run_id
        )
        ON DELETE CASCADE,

    CONSTRAINT uq_quality_metric
        UNIQUE (
            pipeline_run_id,
            layer,
            metric_name
        )
);


CREATE INDEX IF NOT EXISTS idx_quality_metrics_pipeline_run
ON monitoring.data_quality_metrics (
    pipeline_run_id
);


CREATE INDEX IF NOT EXISTS idx_quality_metrics_layer_metric
ON monitoring.data_quality_metrics (
    layer,
    metric_name
);
