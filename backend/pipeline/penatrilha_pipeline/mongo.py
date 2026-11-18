"""Acesso ao MongoDB com o driver oficial (pymongo)."""

from __future__ import annotations

from pymongo import MongoClient
from pymongo.database import Database

from . import config


def conectar() -> tuple[MongoClient, Database]:
    cliente = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=15_000, tz_aware=True)
    return cliente, cliente[config.MONGO_DB]
