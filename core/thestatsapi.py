"""
Minimal client for TheStatsAPI (https://www.thestatsapi.com).

Auth is a Bearer token read from the THE_STATS_API_API_KEY environment variable
(override with the api_key argument). Handles pagination and 429 backoff.

Docs: https://www.thestatsapi.com/docs/api-reference/overview
"""

import os
import sys
import time
from datetime import datetime, timezone

import requests

BASE_URL = "https://api.thestatsapi.com/api/football"
API_KEY_ENV = "THE_STATS_API_API_KEY"

# Pacing is driven by the API's own rate-limit headers (X-Ratelimit-Remaining /
# X-Ratelimit-Reset), so we don't guess a fixed throttle. Confirmed live 2026-07-29
# (direct header check, current key): limit=120/min, matching the documented Starter
# tier — an earlier version of this comment claimed ~12/min observed in practice,
# which did not match a live check and should not be trusted. The client
# proactively sleeps until the window resets when the bucket is empty, so requests
# almost never get a 429.
MAX_RETRIES = 4            # for network/5xx errors
MAX_RATE_WAITS = 30        # how many times to wait out a full rate-limit window
RATE_LIMIT_BUFFER = 1.0    # seconds added after a reset to avoid edge races


# Off by default (existing scripts/tests print their own progress and don't need
# per-HTTP-call noise) -- a long unattended backfill sets THESTATSAPI_VERBOSE=1 so
# every request/wait is individually timestamped and immediately flushed. Added
# 2026-08-20 after a backfill run went silent for 11+ minutes mid-request with no
# way to tell, after the fact, whether it was stuck making a request, stuck waiting
# out a rate limit, or just never printed anything because a completed process
# hadn't flushed stdout yet -- see BUGS.md.
VERBOSE = bool(os.environ.get("THESTATSAPI_VERBOSE"))


def _log(msg):
    if VERBOSE:
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
        print(f"[thestatsapi {ts}] {msg}", file=sys.stderr, flush=True)


class TheStatsAPIError(RuntimeError):
    pass


def get_api_key(explicit=None):
    key = explicit or os.environ.get(API_KEY_ENV)
    if not key:
        raise TheStatsAPIError(
            f"No API key. Set the {API_KEY_ENV} environment variable or pass --api-key.")
    return key


class RequestCapExceeded(TheStatsAPIError):
    """Raised when a client hits its configured request ceiling."""


class Client:
    def __init__(self, api_key=None, base_url=BASE_URL, max_requests=None):
        self.base_url = base_url.rstrip("/")
        self.max_requests = max_requests
        self.requests_made = 0   # successful HTTP requests issued
        # Rate-limit state, learned from response headers.
        self._rl_remaining = None
        self._rl_reset = 0.0
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {get_api_key(api_key)}",
            "Accept": "application/json",
        })

    def _wait_for_window(self):
        """If the rate-limit bucket is empty, sleep until it resets."""
        if self._rl_remaining is not None and self._rl_remaining <= 0:
            delay = self._rl_reset - time.time() + RATE_LIMIT_BUFFER
            if delay > 0:
                _log(f"rate bucket empty, waiting {delay:.1f}s for reset")
                time.sleep(delay)
            self._rl_remaining = None   # assume refilled after the reset

    def _read_rate_headers(self, resp):
        rem = resp.headers.get("X-Ratelimit-Remaining")
        rst = resp.headers.get("X-Ratelimit-Reset")
        if rem is not None:
            try:
                self._rl_remaining = int(rem)
            except ValueError:
                pass
        if rst is not None:
            try:
                self._rl_reset = float(rst)
            except ValueError:
                pass

    def get(self, path, params=None):
        """GET {base}/{path}; returns the parsed JSON body (or None on 404).

        Pacing is header-driven: before each call we wait out an empty rate-limit
        window, so 429s are rare. Network/5xx errors retry with backoff. The
        optional max_requests cap guards against runaway loops.
        """
        url = f"{self.base_url}/{path.lstrip('/')}"
        network_attempts = 0
        rate_waits = 0
        while True:
            if self.max_requests is not None and self.requests_made >= self.max_requests:
                raise RequestCapExceeded(
                    f"Request cap reached ({self.max_requests}). "
                    f"Raise --max-requests if this is expected.")
            self._wait_for_window()
            _log(f"GET {path} {params or ''}")
            t0 = time.time()
            try:
                resp = self.session.get(url, params=params, timeout=30)
            except requests.exceptions.RequestException as exc:
                network_attempts += 1
                elapsed = time.time() - t0
                if network_attempts >= MAX_RETRIES:
                    _log(f"  -> network error after {elapsed:.1f}s, giving up "
                         f"({network_attempts}/{MAX_RETRIES}): {exc}")
                    raise TheStatsAPIError(f"GET {url} failed: {exc}") from exc
                backoff = 2 ** network_attempts
                _log(f"  -> network error after {elapsed:.1f}s ({network_attempts}/{MAX_RETRIES}): "
                     f"{exc} -- retrying in {backoff}s")
                time.sleep(backoff)
                continue

            elapsed = time.time() - t0
            self._read_rate_headers(resp)

            if resp.status_code == 429:
                # Bucket drained faster than expected — wait until reset and retry.
                rate_waits += 1
                delay = max(self._rl_reset - time.time() + RATE_LIMIT_BUFFER, 1.0)
                if rate_waits > MAX_RATE_WAITS:
                    _log(f"  -> 429 after {elapsed:.1f}s, giving up ({rate_waits}/{MAX_RATE_WAITS} waits)")
                    raise TheStatsAPIError(f"GET {url}: persistently rate-limited")
                _log(f"  -> 429 after {elapsed:.1f}s ({rate_waits}/{MAX_RATE_WAITS}) -- waiting {delay:.1f}s")
                time.sleep(delay)
                continue
            if resp.status_code >= 500:
                network_attempts += 1
                if network_attempts >= MAX_RETRIES:
                    _log(f"  -> {resp.status_code} after {elapsed:.1f}s, giving up "
                         f"({network_attempts}/{MAX_RETRIES})")
                    raise TheStatsAPIError(f"GET {url} -> {resp.status_code}")
                backoff = 2 ** network_attempts
                _log(f"  -> {resp.status_code} after {elapsed:.1f}s ({network_attempts}/{MAX_RETRIES}) "
                     f"-- retrying in {backoff}s")
                time.sleep(backoff)
                continue
            if resp.status_code == 404:
                self.requests_made += 1
                _log(f"  -> 404 in {elapsed:.1f}s")
                return None
            if not resp.ok:
                _log(f"  -> {resp.status_code} in {elapsed:.1f}s (fatal): {resp.text[:200]}")
                raise TheStatsAPIError(
                    f"GET {url} -> {resp.status_code}: {resp.text[:200]}")
            self.requests_made += 1
            _log(f"  -> 200 in {elapsed:.1f}s (rl_remaining={self._rl_remaining})")
            return resp.json()

    def get_data(self, path, params=None):
        """GET a single-resource endpoint and return its `data` object (or None)."""
        body = self.get(path, params)
        return body.get("data") if body else None

    def paginate(self, path, params=None, per_page=100, max_items=None):
        """Yield items across all pages of a list endpoint.

        List responses look like {"data": [...], "meta": {"page", "total_pages"}}.
        """
        params = dict(params or {})
        params["per_page"] = per_page
        page = 1
        yielded = 0
        while True:
            params["page"] = page
            body = self.get(path, params)
            if not body:
                return
            for item in body.get("data", []):
                yield item
                yielded += 1
                if max_items is not None and yielded >= max_items:
                    return
            meta = body.get("meta", {})
            total_pages = meta.get("total_pages", page)
            _log(f"paginate {path}: page {page}/{total_pages}, {yielded} items so far")
            if page >= total_pages:
                return
            page += 1
