from minio import Minio
from ..settings import Settings

def init_minio(cfg: Settings):
    return Minio(
        endpoint=cfg.minio_endpoint,
        access_key=cfg.minio_access_key,
        secret_key=cfg.minio_secret_key,
        secure=cfg.minio_secure,
    )
