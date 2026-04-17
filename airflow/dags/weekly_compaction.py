from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    "weekly_compaction",
    start_date=datetime(2024, 1, 1),
    schedule="@weekly",
    catchup=False,
    tags=["maintenance"],
) as dag:
    compact_note = BashOperator(
        task_id="document_compaction_strategy",
        bash_command=(
            "echo \"Local-first portfolio mode: compaction is documented in RUNBOOK.md and executed "
            "manually during demos because OPTIMIZE/Z-ORDER requires a heavier cluster than this $0 setup.\""
        ),
    )

    compact_note
