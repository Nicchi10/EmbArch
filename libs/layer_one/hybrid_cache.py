from __future__ import annotations
import redis
import json
from rapidfuzz import fuzz, process
import unicodedata
import re
from dataclasses import dataclass
from typing import Optional
import time
import logging

logger = logging.getLogger(__name__)

# ============ CONFIG ============ 

@dataclass
class CacheConfig:
    fuzzy_threshold: float = 85.0     
    max_candidates: int = 50          
    expire_seconds: int = 86400
    ngram_size: int = 3               
    min_shared_ngrams: int = 2        
    refresh_ttl_on_hit: bool = True 

    max_entries: int = 1000

    
# ============ MASTER CLASS ============ 

class HybridCache:

    _PUNCT_RE = re.compile(r"[^\w\s]", flags=re.UNICODE) # Catch not alphanumeric
    _WS_RE = re.compile(r"\s+") # Catch consecutive sequences of white spaces

    _LRFU_KEY = "faq:lrfu"
    _HITS_KEY = "faq:hits"


    def __init__(self, redis_client: redis.Redis, config: Optional[CacheConfig] = None):
        logger.info(" ====== Inizialized HybridCache ====== ")
        self.r = redis_client
        self.config = config or CacheConfig()

    # ====== NORMALIZATION ======

    def normalize(self, text: str) -> str:
        """
        Lowercase + removal punctuation + removal mark + collapse whitespace
        e.g: '  WhY??? ? ' --> 'why'
        Args:
            text (str): user query
        Returns:
            (str): Computed string
        """

        if not text:
            return ""
        
        t = text.lower().strip()

        t = unicodedata.normalize("NFKD", t) # find the mutual characters 'è' --> 'e'
        t = "".join(ch for ch in t if not unicodedata.combining(ch)) # substitutes it

        t = self._PUNCT_RE.sub(" ", t)
        t = self._WS_RE.sub(" ", t).strip()

        logger.debug(f"normalize() - input string: {text}, normalized string: {t}")

        return t

    def _ngrams(self, text: str) -> set[str]:
        """
        Padding trigrams to capture start/end of words
        Args:
            text (str): user query

        Returns:
            set[str]: set with query's substrings
        """
        n = self.config.ngram_size
        padded = f" {text} "
        if len(padded) < n:
            return {padded}
        
        return {padded[i : i + n] for i in range(len(padded) - n + 1)}
    
    # ====== REDIS KEYS ======

    @staticmethod
    def _key(norm: str) -> str:
        return f"faq:{norm}"
 
    @staticmethod
    def _ngram_key(ngram: str) -> str:
        return f"ngram:{ngram}"

    # ====== GET ======

    def get(self, query: str) -> Optional[dict]:
        """
        Two-stage lookup: exact match first, fuzzy fallback via trigrams.

        L1a - exact:
            Normalize and GET 'faq:{norm}' directly. On hit, refresh TTL

        L1b - fuzzy:
            Split 'norm' into trigrams, fetch every candidate sharing at
            least one trigram (one pipelined round-trip via
            '_collect_candidates'), drop anyone below 'min_shared_ngrams',
            sort by overlap count and cap at 'max_candidates'. Score the
            survivors with rapidfuzz and return the winner if it clears
            'fuzzy_threshold'.

            If the winner's main key has been evicted but its trigram
            buckets still point to it, that's an orphan: clean it up and
            report a miss.

        Args:
            query: _description_

        Returns:
            Dict with contextual info, None on miss --> the caller should fall through to L2
        """

        norm = self.normalize(query)
        if not norm:
            return None

        # === L1a EXACT MATCH ===

        # TODO() : updates LRFU & HITS
        exact = self.r.get(self._key(norm))
        if exact:
            self._maybe_refresh_ttl(norm)
            return self._hit(exact, source="L1a_exact", score=100.0)
        
        # === L1b FUZZY BUCKET ===
        ngrams = self._ngrams(norm)
        if not ngrams:
            return None
        
        counter = self._collect_candidates(ngrams)
        if not counter:
            return None
        
        # Keep only candidates sharing enough ngrams, then cap to top-K
        candidates = [c for c, ctn in counter.items() if ctn >= self.config.min_shared_ngrams]
        if not candidates:
            return None
        candidates.sort(key=lambda c: counter[c], reverse=True)
        candidates = candidates[: self.config.max_candidates]

        best = process.extractOne(norm, candidates, scorer=fuzz.ratio, processor=None, score_cutoff=self.config.fuzzy_threshold)
        if best is None:
            return None
        
        best_key, score, _ = best
        raw = self.r.get(self._key(best_key))

        if raw is None:
            self._cleanup_seed(best_key)
            self._cleanup_metadata(best_key)
            return None
        
        # updates LRFU & HITS
        now = time.time()
        pipe = self.r.pipeline()
        pipe.zadd(self._LRFU_KEY, {best_key: now})
        pipe.hincrby(self._HITS_KEY, best_key, 1)
        pipe.execute()
        
        self._maybe_refresh_ttl(best_key)
        return self._hit(raw, source="L1b_fuzzy", score=score)
    
    # ====== SET ======

    def set(self, query: str, answer: str, faq_id: int) -> str:
        """
        Persist a query --> answer mapping and index it by trigrams, in a
        single pipelined round-trip to Redis.

        Flow:
        1. Normalize the incoming query (same pipeline used at read time,
            so lookups stay consistent).
        2. Reject empty normalizations (e.g. queries made of punctuation
            only) storing them would create unretrievable keys.
        3. SETEX the main record at 'faq:{norm}' with TTL.
        4. For every trigram of 'norm', SADD 'norm' into the
            'ngram:{trigram}' bucket and refresh that bucket's TTL.
            Rolling TTL means hot buckets stay warm, cold ones vanish on
            their own, no sweeper needed.

        Args:
            query:  _description_
            answer: Answer to associate with the query.
            faq_id: ID of the canonical FAQ row (Postgres), kept in the
                    payload so the API layer can trace the source

        Returns:
            The normalized string used as the storage key

        Raises:
            ValueError: The query is empty after normalization
        """
        norm = self.normalize(query)
        if not norm:
            raise ValueError("Empty query")
 
        payload = json.dumps(
            {"answer": answer, "faq_id": faq_id, "query": norm},
            ensure_ascii=False,
        )

        if not self.r.exists(self._key(norm)):
            self._evict_if_needed()

        now = time.time()    
        pipe = self.r.pipeline()
        pipe.setex(self._key(norm), self.config.expire_seconds, payload)
        pipe.zadd(self._LRFU_KEY, {norm: now}) # upsert: creates or updates timestamp
        pipe.hsetnx(self._HITS_KEY, norm, 0)
        for ng in self._ngrams(norm):
            ng_key = self._ngram_key(ng)
            pipe.sadd(ng_key, norm)
            # TTL rolling: idle buckets go to sleep, the active ones are renewed
            pipe.expire(ng_key, self.config.expire_seconds)
        pipe.execute()
 
        logger.info(f"set() - Cached norm={norm} ({len(self._ngrams(norm))}trigrams)")
        return norm

    def _evict_if_needed(self):
        """_summary_
        If _LRFU_KEY is == max_entries:
            - scrolls through all keys
            - for each withdraws last_access and hit numbers
            - computed the least hitted and oldest one
            - removes it from cache
        Else
            - return
        """
        current_size = self.r.zcard(self._LRFU_KEY)

        if current_size < self.config.max_entries:
            return
        
        # Recovers all items with last access
        lrfu_items = self.r.zrange(self._LRFU_KEY, 0, -1, withscores=True)

        now = time.time()
        max_score = float('-inf')
        victim_key = None

        hits_raw = self.r.hgetall(self._HITS_KEY) or {}

        hits = {
            (k.decode() if isinstance(k, bytes) else k): int(v)
            for k, v in hits_raw.items()
        }


        for key, last_access in lrfu_items:

            key = key.decode() if isinstance(key, bytes) else key

            age = now - float(last_access) # further last_access is, bigger age becomes

            score = age / (hits.get(key, 0) + 1) # to avoid division by 0 

            if score > max_score:
                max_score = score
                victim_key = key
        
        logger.debug(f"_evict_if_needed() - found victim: {victim_key}")

        pipe = self.r.pipeline()
        pipe.delete(self._key(victim_key))
        pipe.zrem(self._LRFU_KEY, victim_key)
        pipe.hdel(self._HITS_KEY, victim_key)
        pipe.execute()
        
        self._cleanup_seed(victim_key)
        self._cleanup_metadata(victim_key)

    # ============ INVALIDATION ============ 

    def invalidate(self, query: str) -> bool:
        """
        Explicit removal
        Args:
            query (str): _description_

        Returns:
            bool: _description_
        """
        norm = self.normalize(query)
        deleted = self.r.delete(self._key(norm)) > 0
        if deleted:
            self._cleanup_seed(norm)
            self._cleanup_metadata(norm)
        return deleted
    
    # ============ TTL ============ 

    def _maybe_refresh_ttl(self, norm: str):
        """_summary_

        Args:
            norm (str): normalized query
        """
        if self.config.refresh_ttl_on_hit:
            self.r.expire(self._key(norm), self.config.expire_seconds)

    def _compute_refresh_ttl(self) -> None:
        """
        Adaptive TTL based on hit frequency.
        Reads the hit counter for every tracked key in a single HGETALL,
        computes the mean, and updates TTLs:
        - hits below mean      -> halved TTL (cold keys leave faster)
        - hits above 2x mean   -> full TTL  (hot keys stay)
        - in between           -> untouched (their current TTL keeps running)

        TODO(): Add LRFU logic
        """
        
        lrfu_items = self.r.zrange(self._LRFU_KEY, 0, -1, withscores=False)
        if not lrfu_items:
            return

        
        hits_raw = self.r.hgetall(self._HITS_KEY)  
        if not hits_raw:
            return

        hits = {
            (k.decode() if isinstance(k, bytes) else k): int(v)
            for k, v in hits_raw.items()
        }

        if not hits:
            return

        mean = sum(hits.values()) / len(hits)
        hot_threshold = mean * 2  

        full_ttl = self.config.expire_seconds
        short_ttl = full_ttl // 2

        pipe = self.r.pipeline()
        for key in lrfu_items:
            key = key.decode() if isinstance(key, bytes) else key
            count = hits.get(key, 0)

            if count < mean:
                pipe.expire(self._key(key), short_ttl)
            elif count > hot_threshold:
                pipe.expire(self._key(key), full_ttl)

        pipe.execute()

    # ============ UTILS ============ 

    def _collect_candidates(self, ngrams: set[str]) -> dict[str, int]:
        """
        Fetch all cached queries that share at least one ngram with the input
        Args:
            ngrams (set[str]): _description_

        Returns:
            dict[str, int]: {normalized_query: shared_ngram_count}
        """
        pipe = self.r.pipeline()
        for ng in ngrams:
            pipe.smembers(self._ngram_key(ng))
        raw_sets = pipe.execute()

        counter: dict[str, int] = {}
        for members in raw_sets:
            for m in members:
                k = m.decode("utf-8") if isinstance(m, bytes) else m
                counter[k] = counter.get(k, 0) + 1
        return counter
    
    def _cleanup_seed(self, norm: str):
        """
        Removes a normalized keys from its triagrams bucket
        Args:
            norm (str): normalized query
        """

        pipe = self.r.pipeline()
        for ng in self._ngrams(norm):
            pipe.srem(self._ngram_key(ng), norm)
        pipe.execute()
        logger.info(f"_cleanup_seed() - Seed cleanup norm={norm}")

    def _cleanup_metadata(self, norm: str):
        """
        Removes a normalized keys from its LRFU e HITS data structures
        Args:
            norm (str): _description_
        """
        pipe = self.r.pipeline()
        pipe.zrem(self._LRFU_KEY, norm)
        pipe.hdel(self._HITS_KEY, norm)
        pipe.execute()

    @staticmethod
    def _hit(raw: bytes, source: str, score: float) -> dict:
        data = json.loads(raw)
        data["source"] = source
        data["match_score"] = round(float(score), 2)
        logger.info(f"_hit() - hitted: {data}")
        return data
