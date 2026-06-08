import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Service packages (text_chunker, vector_indexer, embedder, ...).
sys.path.insert(0, _ROOT)
# Shared package (ragflow_contracts) without needing an install.
sys.path.insert(0, os.path.join(_ROOT, "contracts"))

# Settings objects instantiate at import; provide harmless defaults.
os.environ.setdefault("RABBITMQ_USER", "user")
os.environ.setdefault("RABBITMQ_PASSWORD", "password")
os.environ.setdefault("MINIO_ACCESS_KEY", "x")
os.environ.setdefault("MINIO_SECRET_KEY", "x")
os.environ.setdefault("MINIO_BUCKET", "bucket")
