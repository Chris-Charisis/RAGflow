"""Smoke test for the deletion race guard in batched upsert (mocked Weaviate)."""
from vector_indexer.settings import Settings
from vector_indexer.vector_indexer.database_indexer import WeaviateIndexBackend


class _BatchCtx:
    def __init__(self, sink):
        self.sink = sink

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def add_object(self, properties, uuid, vector):
        self.sink.append(uuid)


class FakeBatch:
    def __init__(self):
        self.added = []
        self.failed_objects = []

    def dynamic(self):
        return _BatchCtx(self.added)


class FakeCollection:
    def __init__(self):
        self.batch = FakeBatch()


class _Obj:
    def __init__(self, deleted_at):
        self.properties = {"deleted_at": deleted_at}


class FakeTombstones:
    def __init__(self, deleted_at=None):
        self.deleted_at = deleted_at
        self.query = self

    def fetch_object_by_id(self, uid):
        return _Obj(self.deleted_at) if self.deleted_at is not None else None


def _backend(tomb_deleted_at=None):
    b = WeaviateIndexBackend(Settings())
    b.collection = FakeCollection()
    b.tombstones = FakeTombstones(tomb_deleted_at)
    return b


def _payload(published_at):
    return {
        "doc_id": "b/doc",
        "published_at": published_at,
        "chunk": {"index": 0, "text": "x"},
        "source": {"bucket": "b", "object": "doc.pdf"},
        "embedding": {"embedding_vector": [[0.1, 0.2]]},
    }


def test_upsert_many_indexes_when_no_tombstone():
    b = _backend(tomb_deleted_at=None)
    n = b.upsert_many([_payload(published_at=100)])
    assert n == 1
    assert len(b.collection.batch.added) == 1


def test_upsert_many_skips_chunk_from_deleted_generation():
    # Tombstone deleted_at=200; a chunk published at 100 belongs to the deleted gen.
    b = _backend(tomb_deleted_at=200)
    n = b.upsert_many([_payload(published_at=100)])
    assert n == 0
    assert b.collection.batch.added == []


def test_upsert_many_keeps_newer_reingestion():
    # Re-ingestion published after the deletion must survive.
    b = _backend(tomb_deleted_at=200)
    n = b.upsert_many([_payload(published_at=300)])
    assert n == 1
    assert len(b.collection.batch.added) == 1
