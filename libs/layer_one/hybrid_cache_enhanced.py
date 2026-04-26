from __future__ import annotations
import json
import math
import time
from dataclasses import dataclass, field
from typing import Optional

import redis
from rapidfuzz import fuzz

from hybrid_cache import HybridCache, CacheConfig


# ============ CONFIG ============

@dataclass
class EnhancedCacheConfig(CacheConfig):
    # --- Eviction (max_entries cap) ---
    max_entries: int = 1000
    
    recency_weight: float = 1.0
    frequency_weight: float = 3600.0
    eviction_batch: int = 10  

    # --- TTL adaptive ---
    hot_hits_threshold: int = 10           
    hot_ttl_multiplier: float = 3.0        
    cold_ttl_divisor: float = 2.0          
    cold_idle_seconds: int = 3600          

    # --- IDF weighting ---
    idf_enabled: bool = True
    idf_recompute_every: int = 50          
    idf_smoothing: float = 1.0             

    # --- Early exit ---
    early_exit_score: float = 95.0


# ============ ENHANCED CACHE ============

class HybridCacheEnhanced(HybridCache):

    
    _LRFU_KEY = "faq:lrfu"
    _HITS_KEY = "faq:hits"

    # IDF cache
    _IDF_KEY = "faq:idf"            # hash trigram -> idf
    _IDF_META_KEY = "faq:idf_meta"  # {set_count_at_last_recompute, total_docs}

    def __init__(
        self,
        redis_client: redis.Redis,
        config: Optional[EnhancedCacheConfig] = None,
    ):
        super().__init__(redis_client, config or EnhancedCacheConfig())
        self.config: EnhancedCacheConfig  

    # ====== GET (override) ======

    def get(self, query: str) -> Optional[dict]:
        
        norm = self.normalize(query)
        if not norm:
            return None

        # === L1a EXACT ===
        exact = self.r.get(self._key(norm))
        if exact:
            self._track_hit(norm)
            self._maybe_refresh_ttl(norm)  
            return self._hit(exact, source="L1a_exact", score=100.0)

        # === L1b FUZZY ===
        ngrams = self._ngrams(norm)
        if not ngrams:
            return None

        if self.config.idf_enabled:
            scored = self._collect_candidates_idf(ngrams)
        else:
            
            counter = self._collect_candidates(ngrams)
            scored = {k: float(v) for k, v in counter.items()}

        if not scored:
            return None

        if self.config.idf_enabled:
            #
            min_weight = float(self.config.min_shared_ngrams)
            candidates = [c for c, w in scored.items() if w >= min_weight]
        else:
            candidates = [
                c for c, w in scored.items() if w >= self.config.min_shared_ngrams
            ]

        if not candidates:
            return None

        candidates.sort(key=lambda c: scored[c], reverse=True)
        candidates = candidates[: self.config.max_candidates]

        best_key: Optional[str] = None
        best_score: float = 0.0
        cutoff = self.config.fuzzy_threshold
        early = self.config.early_exit_score

        for cand in candidates:
            score = fuzz.ratio(norm, cand)
            if score < cutoff:
                continue
            if score > best_score:
                best_score = score
                best_key = cand
                if score >= early:
                
                    break

        if best_key is None:
            return None

        raw = self.r.get(self._key(best_key))
        if raw is None:
            self._cleanup_seed(best_key)
            self._cleanup_metadata(best_key)
            return None

        self._track_hit(best_key)
        self._maybe_refresh_ttl(best_key)
        return self._hit(raw, source="L1b_fuzzy", score=best_score)

    # ====== SET (override) ======

    def set(self, query: str, answer: str, faq_id: int) -> str:
        norm = super().set(query, answer, faq_id)

        
        now = time.time()
        pipe = self.r.pipeline()
        pipe.zadd(self._LRFU_KEY, {norm: now})
        pipe.hsetnx(self._HITS_KEY, norm, 0)
        pipe.execute()

      
        self._evict_if_needed()

    
        self._maybe_recompute_idf()

        return norm

    # ====== INVALIDATE (override) ======

    def invalidate(self, query: str) -> bool:
        norm = self.normalize(query)
        deleted = super().invalidate(query)
        if deleted:
            self._cleanup_metadata(norm)
        return deleted

    # ============ TTL ADAPTIVE ============

    def _maybe_refresh_ttl(self, norm: str) -> None:
        
        if not self.config.refresh_ttl_on_hit:
            return

        base = self.config.expire_seconds

       
        pipe = self.r.pipeline()
        pipe.hget(self._HITS_KEY, norm)
        pipe.zscore(self._LRFU_KEY, norm)
        hits_raw, last_access = pipe.execute()

        hits = int(hits_raw) if hits_raw is not None else 0
        now = time.time()
        idle = now - float(last_access) if last_access is not None else 0.0

        if hits >= self.config.hot_hits_threshold:
            ttl = int(base * self.config.hot_ttl_multiplier)
        elif idle > self.config.cold_idle_seconds:
            ttl = int(base / self.config.cold_ttl_divisor)
        else:
            ttl = base

        self.r.expire(self._key(norm), ttl)

    def _track_hit(self, norm: str) -> None:
        
        now = time.time()
        pipe = self.r.pipeline()
        pipe.hincrby(self._HITS_KEY, norm, 1)
        pipe.execute()

        
        new_hits = int(self.r.hget(self._HITS_KEY, norm) or 0)
        score = (
            self.config.recency_weight * now
            + self.config.frequency_weight * new_hits
        )
        self.r.zadd(self._LRFU_KEY, {norm: score})

    # ============ EVICTION ============

    def _evict_if_needed(self) -> None:
        
        size = self.r.zcard(self._LRFU_KEY)
        if size <= self.config.max_entries:
            return

        overflow = size - self.config.max_entries
        # Rimuovi almeno overflow + un piccolo batch per ammortizzare
        to_remove = overflow + self.config.eviction_batch
        victims = self.r.zpopmin(self._LRFU_KEY, count=to_remove)

        if not victims:
            return

        pipe = self.r.pipeline()
        evicted_norms: list[str] = []
        for member, _score in victims:
            norm = member.decode("utf-8") if isinstance(member, bytes) else member
            evicted_norms.append(norm)
            pipe.delete(self._key(norm))
            pipe.hdel(self._HITS_KEY, norm)
            for ng in self._ngrams(norm):
                pipe.srem(self._ngram_key(ng), norm)
        pipe.execute()

        print(f"[evict] rimosse {len(evicted_norms)} entry: {evicted_norms[:5]}...")

    def _cleanup_metadata(self, norm: str) -> None:
        
        pipe = self.r.pipeline()
        pipe.zrem(self._LRFU_KEY, norm)
        pipe.hdel(self._HITS_KEY, norm)
        pipe.execute()

    # ============ IDF WEIGHTING ============

    def _collect_candidates_idf(self, ngrams: set[str]) -> dict[str, float]:
        
        pipe = self.r.pipeline()
        ngram_list = list(ngrams)
        for ng in ngram_list:
            pipe.smembers(self._ngram_key(ng))
        raw_sets = pipe.execute()

        idf_raw = self.r.hmget(self._IDF_KEY, ngram_list)
        idf_map: dict[str, float] = {}
        for ng, val in zip(ngram_list, idf_raw):
            if val is None:
                idf_map[ng] = 1.0  
            else:
                idf_map[ng] = float(val)

        scored: dict[str, float] = {}
        for ng, members in zip(ngram_list, raw_sets):
            weight = idf_map[ng]
            for m in members:
                k = m.decode("utf-8") if isinstance(m, bytes) else m
                scored[k] = scored.get(k, 0.0) + weight
        return scored

    def _maybe_recompute_idf(self) -> None:
        
        if not self.config.idf_enabled:
            return

        count = self.r.hincrby(self._IDF_META_KEY, "set_count", 1)
        if count % self.config.idf_recompute_every != 0:
            return

        n_docs = self.r.zcard(self._LRFU_KEY)
        if n_docs == 0:
            return

   
        cursor = 0
        smoothing = self.config.idf_smoothing
        log_num = math.log(n_docs + 1)

        all_ngram_keys: list[bytes] = []
        while True:
            cursor, batch = self.r.scan(
                cursor=cursor, match="ngram:*", count=500
            )
            all_ngram_keys.extend(batch)
            if cursor == 0:
                break

        if not all_ngram_keys:
            return

        pipe = self.r.pipeline()
        for k in all_ngram_keys:
            pipe.scard(k)
        cards = pipe.execute()

        idf_updates: dict[str, float] = {}
        for k, df in zip(all_ngram_keys, cards):
            key_str = k.decode("utf-8") if isinstance(k, bytes) else k
            ng = key_str[len("ngram:"):]
            idf = log_num - math.log(df + smoothing)
            idf_updates[ng] = max(idf, 0.0)

        pipe = self.r.pipeline()
        pipe.delete(self._IDF_KEY)
        if idf_updates:
            pipe.hset(self._IDF_KEY, mapping={k: v for k, v in idf_updates.items()})
        pipe.hset(self._IDF_META_KEY, "total_docs", n_docs)
        pipe.execute()

        print(
            f"[idf] ricalcolati {len(idf_updates)} trigrammi su {n_docs} docs"
        )