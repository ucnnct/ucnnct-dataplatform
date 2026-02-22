"""Utilitaire bookmark — suivi incremental des fichiers traités dans MinIO.

Algorithme :
  - Les chemins raw sont triés alphabétiquement (= tri chronologique YYYY/MM/DD/).
  - Le bookmark stocke la dernière clé traitée dans datalake/_bookmarks/{name}.json.
  - Le prochain run ne traite que les fichiers avec clé > last_key.
  - La pagination S3 est gérée automatiquement (pas de limite à 1000 objets).
"""
import json
import logging
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger("bookmark")


def get_s3_client(endpoint, user, password):
    return boto3.client(
        "s3",
        endpoint_url=f"http://{endpoint}",
        aws_access_key_id=user,
        aws_secret_access_key=password,
    )


def read_bookmark(s3, bucket, name):
    """Lit le bookmark depuis MinIO. Retourne last_key ou None (premier run)."""
    key = f"_bookmarks/{name}.json"
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        data = json.loads(obj["Body"].read())
        logger.info("Bookmark '%s' : last_key=%s", name, data.get("last_key"))
        return data.get("last_key")
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            logger.info("Bookmark '%s' : premier run, traitement intégral.", name)
            return None
        raise


def write_bookmark(s3, bucket, name, last_key):
    """Sauvegarde le bookmark après un run réussi."""
    key = f"_bookmarks/{name}.json"
    payload = json.dumps({
        "last_key":   last_key,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }).encode()
    s3.put_object(Bucket=bucket, Key=key, Body=payload)
    logger.info("Bookmark '%s' mis à jour : last_key=%s", name, last_key)


def list_new_files(s3, bucket, prefix, last_key=None, end_key=None, suffix=".jsonl"):
    """
    Liste les fichiers nouveaux depuis last_key jusqu'à end_key (exclu).
    Tri alphabétique = tri chronologique (chemins contiennent YYYY/MM/DD/YYYYMMDD_HHMMSS).
    Gère automatiquement la pagination (> 1000 objets).
    end_key=None signifie pas de borne supérieure (traitement jusqu'au dernier fichier).
    """
    paginator = s3.get_paginator("list_objects_v2")
    all_keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(suffix):
                all_keys.append(obj["Key"])

    all_keys.sort()
    new_keys = [
        k for k in all_keys
        if (last_key is None or k > last_key)
        and (end_key is None or k <= end_key or k.startswith(end_key))
    ]

    logger.info(
        "Prefix '%s' — total: %d, nouveaux: %d (last_key=%s, end_key=%s)",
        prefix, len(all_keys), len(new_keys), last_key, end_key,
    )
    return new_keys
