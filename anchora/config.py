from __future__ import annotations

import os

TEMPORAL_HOST = os.getenv("TEMPORAL_HOST", "localhost:7233")
TASK_QUEUE = os.getenv("TASK_QUEUE", "fulfillment-queue")
MAX_CONCURRENT_ACTIVITIES = int(os.getenv("MAX_CONCURRENT_ACTIVITIES", "100"))
MAX_CONCURRENT_WORKFLOW_TASKS = int(os.getenv("MAX_CONCURRENT_WORKFLOW_TASKS", "100"))
DATABASE_URL = os.getenv("DATABASE_URL", "")
ANCHORA_DB_PATH = os.getenv("ANCHORA_DB_PATH", ".anchora/anchora.sqlite3")
