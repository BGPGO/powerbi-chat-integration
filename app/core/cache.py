"""
Cache layer — implementação in-memory simples com TTL.
Substitui Redis para desenvolvimento local sem dependências externas.
"""

import logging
import time
from typing import Any, Optional

logger = logging.getLogger(__name__)


class InMemoryCache:
    """Cache em memória com suporte a TTL por chave"""

    def __init__(self) -> None:
        self._store: dict[str, Any] = {}
        self._expiry: dict[str, float] = {}

    async def get(self, key: str) -> Optional[Any]:
        """Retorna valor do cache, ou None se expirado/ausente"""
        if key not in self._store:
            return None

        if key in self._expiry and time.time() > self._expiry[key]:
            del self._store[key]
            del self._expiry[key]
            logger.debug(f"Cache miss (expirado): {key}")
            return None

        logger.debug(f"Cache hit: {key}")
        return self._store[key]

    async def set(self, key: str, value: Any, ttl: int = 300) -> None:
        """Armazena valor com TTL em segundos"""
        self._store[key] = value
        self._expiry[key] = time.time() + ttl
        logger.debug(f"Cache set: {key} (TTL={ttl}s)")

    async def delete(self, key: str) -> None:
        """Remove uma chave do cache"""
        self._store.pop(key, None)
        self._expiry.pop(key, None)

    async def clear(self) -> None:
        """Limpa todo o cache"""
        self._store.clear()
        self._expiry.clear()

    def stats(self) -> dict:
        """Retorna estatísticas do cache"""
        now = time.time()
        active = sum(1 for k in self._store if self._expiry.get(k, float("inf")) > now)
        return {"total_keys": len(self._store), "active_keys": active}


# Singleton global
_cache = InMemoryCache()


def get_cache() -> InMemoryCache:
    """Retorna a instância global do cache"""
    return _cache
