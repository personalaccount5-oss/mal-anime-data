#!/usr/bin/env python3
"""
MyAnimeList Offline Database & Delta Sync Script ("Git Scraping")
================================================================
Generates and maintains a normalized SQLite database (mal_catalog.db) containing:
1. ID mappings (MAL <-> AniList, Kitsu, AniDB) and metadata via manami-project/anime-offline-database
2. Official MAL rankings, popularity, scores, genres, and studios via MAL API v2
3. Fast daily delta patches (airing_delta.json) for currently airing & upcoming shows

Usage:
    python scripts/mal_db_sync.py --mode full     # Complete rebuild from upstream sources (~70-90s)
    python scripts/mal_db_sync.py --mode airing   # Fast daily delta update for airing/upcoming (<2s)
    python scripts/mal_db_sync.py --mode both     # Airing update if DB exists, else full rebuild
"""

import argparse
import gzip
import hashlib
import json
import os
import re
import sqlite3
import sys
import time
import urllib.parse
import urllib.request
import urllib.error

# Upstream anime-offline-database (weekly releases mapping MAL, AniList, Kitsu, AniDB)
AODB_URL = (
    "https://github.com/manami-project/anime-offline-database/releases/latest/download/"
    "anime-offline-database-minified.json"
)

# MAL API v2
MAL_API_BASE = "https://api.myanimelist.net/v2"
DEFAULT_CLIENT_ID = "18e5087eaeef557833a075a4d30d2afe"
USER_AGENT = "AnimetailMalDbSync/1.0 (+https://github.com)"

FIELDS = (
    "id,title,main_picture,alternative_titles,start_date,mean,rank,popularity,"
    "num_scoring_users,media_type,status,num_episodes,start_season,source,studios,genres"
)

SCHEMA_VERSION = 2
DB_PATH = "mal_catalog.db"
DB_GZ_PATH = "mal_catalog.db.gz"
MANIFEST_PATH = "manifest.json"
DELTA_PATH = "airing_delta.json"

# Canonical MAL Studio Names and Aliases for Normalization
MAL_STUDIO_NAMES = [
    "MAPPA", "Madhouse", "Kyoto Animation", "Bones", "ufotable", "Wit Studio",
    "A-1 Pictures", "CloverWorks", "Production I.G", "J.C.Staff", "Studio Pierrot",
    "White Fox", "Trigger", "Shaft", "David Production", "Sunrise", "Studio Ghibli",
    "TMS Entertainment", "Studio Deen", "P.A. Works", "Toei Animation", "Studio Bind",
    "Science SARU", "Kinema Citrus", "8bit", "SILVER LINK.", "Lerche", "LIDENFILMS",
    "Doga Kobo", "OLM", "feel.", "Brain's Base", "CoMix Wave Films", "Nexus",
    "Passione", "Orange", "Gonzo", "Gainax", "Satelight", "Diomedéa", "Manglobe",
    "TNK", "Artland", "Telecom Animation Film", "Shin-Ei Animation",
    "Tatsunoko Production", "CygamesPictures", "Pierrot Films", "KONAMI animation"
]

CANONICAL_MAP = {}
for name in MAL_STUDIO_NAMES:
    CANONICAL_MAP[name.lower()] = name

ALIASES = {
    "a-1 pictures inc.": "A-1 Pictures",
    "a1 pictures": "A-1 Pictures",
    "cloverworks inc.": "CloverWorks",
    "clover works": "CloverWorks",
    "toei animation co., ltd.": "Toei Animation",
    "toei animation co.,ltd.": "Toei Animation",
    "j.c. staff": "J.C.Staff",
    "j.c.staff co., ltd.": "J.C.Staff",
    "j.c. staff co., ltd.": "J.C.Staff",
    "production i.g": "Production I.G",
    "production i.g.": "Production I.G",
    "production i.g., inc.": "Production I.G",
    "production ig": "Production I.G",
    "pierrot": "Studio Pierrot",
    "pierrot co., ltd.": "Studio Pierrot",
    "studio pierrot": "Studio Pierrot",
    "studio pierrot co., ltd.": "Studio Pierrot",
    "pierrot films": "Pierrot Films",
    "sunrise inc.": "Sunrise",
    "madhouse inc.": "Madhouse",
    "bones inc.": "Bones",
    "bones film": "Bones",
    "bones film inc.": "Bones",
    "ufotable, inc.": "ufotable",
    "ufotable inc.": "ufotable",
    "wit studio, inc.": "Wit Studio",
    "wit studio inc.": "Wit Studio",
    "kyoto animation co., ltd.": "Kyoto Animation",
    "mappa co., ltd.": "MAPPA",
    "silver link": "SILVER LINK.",
    "silver link.": "SILVER LINK.",
    "silverlink": "SILVER LINK.",
    "shin-ei animation co., ltd.": "Shin-Ei Animation",
    "tms entertainment co., ltd.": "TMS Entertainment",
    "tokyo movie shinsha": "TMS Entertainment",
    "p.a.works": "P.A. Works",
    "p.a. works co., ltd.": "P.A. Works",
    "8-bit": "8bit",
    "eight bit": "8bit",
    "liden films": "LIDENFILMS",
    "liden films inc.": "LIDENFILMS",
    "lidenfilms": "LIDENFILMS",
    "liden films kyoto studio": "LIDENFILMS",
    "lidenfilms kyoto studio": "LIDENFILMS",
    "dogakobo": "Doga Kobo",
    "doga kobo inc.": "Doga Kobo",
    "studio deen co., ltd.": "Studio Deen",
    "tatsunoko prod.": "Tatsunoko Production",
    "tatsunoko production co., ltd.": "Tatsunoko Production",
    "telecom animation film co., ltd.": "Telecom Animation Film",
    "kinema citrus co., ltd.": "Kinema Citrus",
    "science saru inc.": "Science SARU",
    "cygames pictures": "CygamesPictures",
    "brains base": "Brain's Base",
    "comix wave": "CoMix Wave Films",
    "comix wave films inc.": "CoMix Wave Films",
    "diomedea": "Diomedéa",
    "white fox inc.": "White Fox",
    "trigger inc.": "Trigger",
    "shaft inc.": "Shaft",
    "david production inc.": "David Production",
    "olm, inc.": "OLM",
    "olm inc.": "OLM",
    "orange co., ltd.": "Orange",
    "aic": "AIC",
    "anime international co., inc.": "AIC",
    "anime international co.": "AIC",
    "nippon animation co., ltd.": "Nippon Animation",
    "group tac co., ltd.": "Group TAC",
}

for k, v in ALIASES.items():
    CANONICAL_MAP[k.lower()] = v

SUFFIX_RE = re.compile(
    r"(?:\s*,\s*|\s+)(?:co\.?,\s*ltd\.?|co\.?|ltd\.?|inc\.?|corp(?:oration)?\.?|llc\.?|k\.k\.?)$",
    re.IGNORECASE
)


def canonicalize_studio_name(name: str) -> str:
    """Canonicalizes studio names across AODB and MAL API variations."""
    if not name:
        return ""
    cleaned = name.strip()
    lowered = cleaned.lower()

    if lowered in CANONICAL_MAP:
        return CANONICAL_MAP[lowered]

    stripped = SUFFIX_RE.sub("", cleaned).strip().rstrip(",").strip()
    stripped_lowered = stripped.lower()

    if stripped_lowered in CANONICAL_MAP:
        return CANONICAL_MAP[stripped_lowered]

    for can_low, can_name in CANONICAL_MAP.items():
        if len(can_low) > 4 and (can_low == stripped_lowered):
            return can_name

    if cleaned.islower():
        return stripped.title()
    return stripped


def get_headers(client_id: str) -> dict:
    return {
        "X-MAL-CLIENT-ID": client_id,
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
    }


def fetch_url_json(url: str, headers: dict = None, max_retries: int = 4):
    """Fetch URL returning parsed JSON with exponential backoff on 429/5xx."""
    req_headers = {"User-Agent": USER_AGENT}
    if headers:
        req_headers.update(headers)

    for attempt in range(max_retries):
        try:
            req = urllib.request.Request(url, headers=req_headers)
            with urllib.request.urlopen(req, timeout=30) as resp:
                if resp.status == 200:
                    data = resp.read()
                    return json.loads(data.decode("utf-8"))
                else:
                    print(f"Warning: Unexpected status {resp.status} for {url}")
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 504):
                retry_after = int(e.headers.get("Retry-After", 2 ** (attempt + 1)))
                print(f"HTTP {e.code} for {url}, retrying in {retry_after}s (attempt {attempt + 1}/{max_retries})...")
                time.sleep(retry_after)
            else:
                print(f"HTTP Error {e.code} for {url}: {e.reason}")
                raise
        except Exception as e:
            print(f"Network error for {url}: {e}, retrying in {2 ** (attempt + 1)}s...")
            time.sleep(2 ** (attempt + 1))

    raise RuntimeError(f"Failed to fetch {url} after {max_retries} retries")


def init_database(conn: sqlite3.Connection):
    """Creates tables, virtual tables, and indexes."""
    cursor = conn.cursor()
    cursor.executescript(f"""
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;

        CREATE TABLE IF NOT EXISTS db_metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS anime_catalog (
            mal_id INTEGER PRIMARY KEY,
            anilist_id INTEGER,
            kitsu_id INTEGER,
            anidb_id INTEGER,
            title TEXT NOT NULL,
            english_title TEXT,
            synonyms_csv TEXT,
            image_url TEXT,
            score REAL,
            scored_by INTEGER DEFAULT 0,
            rank INTEGER,
            popularity INTEGER,
            favorite_rank INTEGER,
            media_type TEXT,
            status TEXT,
            episodes INTEGER,
            season_year INTEGER,
            season_name TEXT,
            start_date_epoch INTEGER,
            source_material TEXT,
            country_of_origin TEXT,
            primary_studio TEXT COLLATE NOCASE,
            studios_csv TEXT,
            genres_csv TEXT,
            tags_csv TEXT,
            updated_at INTEGER NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_anime_rank ON anime_catalog(rank ASC) WHERE rank IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_anime_popularity ON anime_catalog(popularity ASC) WHERE popularity IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_anime_score ON anime_catalog(score DESC, scored_by DESC);
        CREATE INDEX IF NOT EXISTS idx_anime_status_score ON anime_catalog(status, score DESC);
        CREATE INDEX IF NOT EXISTS idx_anime_media_type_rank ON anime_catalog(media_type, rank ASC);
        CREATE INDEX IF NOT EXISTS idx_anime_favorite_rank ON anime_catalog(favorite_rank ASC) WHERE favorite_rank IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_anime_year_season ON anime_catalog(season_year DESC, season_name);
        CREATE INDEX IF NOT EXISTS idx_anime_anilist_id ON anime_catalog(anilist_id) WHERE anilist_id IS NOT NULL;

        CREATE TABLE IF NOT EXISTS anime_studios (
            studio_name TEXT COLLATE NOCASE NOT NULL,
            mal_id INTEGER NOT NULL,
            PRIMARY KEY (studio_name, mal_id),
            FOREIGN KEY (mal_id) REFERENCES anime_catalog(mal_id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_anime_studios_name ON anime_studios(studio_name COLLATE NOCASE);

        CREATE TABLE IF NOT EXISTS studios_summary (
            studio_id INTEGER PRIMARY KEY AUTOINCREMENT,
            studio_name TEXT COLLATE NOCASE UNIQUE NOT NULL,
            total_count INTEGER NOT NULL,
            popular_count INTEGER NOT NULL,
            airing_count INTEGER NOT NULL,
            upcoming_count INTEGER NOT NULL,
            avg_score REAL,
            sample_covers_json TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_studios_popular ON studios_summary(popular_count DESC, avg_score DESC);

        CREATE TABLE IF NOT EXISTS anime_relations (
            source_mal_id INTEGER NOT NULL,
            target_mal_id INTEGER NOT NULL,
            relation_type TEXT NOT NULL,
            PRIMARY KEY (source_mal_id, target_mal_id)
        );
        CREATE INDEX IF NOT EXISTS idx_relations_source ON anime_relations(source_mal_id);
    """)

    cursor.execute("""
        INSERT INTO db_metadata (key, value) VALUES ('schema_version', ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
    """, (str(SCHEMA_VERSION),))
    conn.commit()


def setup_fts5(conn: sqlite3.Connection):
    """Rebuilds the FTS5 virtual table for full-text search."""
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS anime_fts")
    cursor.execute("""
        CREATE VIRTUAL TABLE anime_fts USING fts5(
            title,
            english_title,
            synonyms_csv,
            primary_studio,
            content='anime_catalog',
            content_rowid='mal_id',
            tokenize='unicode61 remove_diacritics 2'
        );
    """)
    cursor.execute("""
        INSERT INTO anime_fts(rowid, title, english_title, synonyms_csv, primary_studio)
        SELECT mal_id, title, english_title, synonyms_csv, primary_studio
        FROM anime_catalog;
    """)
    conn.commit()


def ingest_anime_offline_database(conn: sqlite3.Connection):
    """
    Ingests manami-project/anime-offline-database to populate:
    - Base anime records
    - Cross-mappings: mal_id <-> anilist_id, kitsu_id, anidb_id
    - Synonyms, studios, tags, and season metadata
    """
    print(f"Downloading anime-offline-database from {AODB_URL}...")
    data = fetch_url_json(AODB_URL)
    entries = data.get("data", [])
    print(f"Downloaded {len(entries)} entries. Ingesting into SQLite...")

    cursor = conn.cursor()
    now_epoch = int(time.time())

    mal_id_regex = re.compile(r"myanimelist\.net/anime/(\d+)")
    anilist_id_regex = re.compile(r"anilist\.co/anime/(\d+)")
    kitsu_id_regex = re.compile(r"kitsu\.(?:io|app)/anime/(\d+)")
    anidb_id_regex = re.compile(r"anidb\.net/anime/(\d+)")

    batch = []
    studios_batch = set()
    relations_batch = set()

    for item in entries:
        sources = item.get("sources", [])
        mal_id = None
        anilist_id = None
        kitsu_id = None
        anidb_id = None

        for s in sources:
            if not mal_id:
                m = mal_id_regex.search(s)
                if m:
                    mal_id = int(m.group(1))
            if not anilist_id:
                m = anilist_id_regex.search(s)
                if m:
                    anilist_id = int(m.group(1))
            if not kitsu_id:
                m = kitsu_id_regex.search(s)
                if m:
                    kitsu_id = int(m.group(1))
            if not anidb_id:
                m = anidb_id_regex.search(s)
                if m:
                    anidb_id = int(m.group(1))

        if not mal_id:
            continue

        title = item.get("title", "").strip()
        if not title:
            continue

        synonyms = item.get("synonyms", [])
        synonyms_csv = "|".join(s.strip() for s in synonyms if s.strip())

        image_url = item.get("picture") or item.get("thumbnail")
        media_type = (item.get("type") or "").lower()
        raw_status = (item.get("status") or "").upper()
        status = (
            "finished_airing" if raw_status == "FINISHED"
            else "currently_airing" if raw_status == "CURRENTLY"
            else "not_yet_aired" if raw_status == "UPCOMING"
            else "finished_airing"
        )
        episodes = item.get("episodes")

        season_info = item.get("animeSeason", {})
        season_year = season_info.get("year")
        season_name = (season_info.get("season") or "").lower()

        studios = item.get("studios", [])
        canonical_studios = [canonicalize_studio_name(st) for st in studios if st.strip()]
        primary_studio = canonical_studios[0] if canonical_studios else None
        studios_csv = "|".join(canonical_studios)

        tags = item.get("tags", [])
        tags_csv = "|".join(t.strip() for t in tags if t.strip())

        batch.append((
            mal_id, anilist_id, kitsu_id, anidb_id,
            title, None, synonyms_csv, image_url,
            media_type, status, episodes,
            season_year, season_name,
            primary_studio, studios_csv, tags_csv, now_epoch
        ))

        for st in canonical_studios:
            if st:
                studios_batch.add((st, mal_id))

        related_anime = item.get("relatedAnime", [])
        for rel in related_anime:
            m_rel = mal_id_regex.search(rel)
            if m_rel:
                target_mal_id = int(m_rel.group(1))
                if target_mal_id != mal_id:
                    relations_batch.add((mal_id, target_mal_id, "related"))

        if len(batch) >= 2000:
            cursor.executemany("""
                INSERT INTO anime_catalog (
                    mal_id, anilist_id, kitsu_id, anidb_id,
                    title, english_title, synonyms_csv, image_url,
                    media_type, status, episodes,
                    season_year, season_name,
                    primary_studio, studios_csv, tags_csv, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(mal_id) DO UPDATE SET
                    anilist_id = COALESCE(excluded.anilist_id, anime_catalog.anilist_id),
                    kitsu_id = COALESCE(excluded.kitsu_id, anime_catalog.kitsu_id),
                    anidb_id = COALESCE(excluded.anidb_id, anime_catalog.anidb_id),
                    synonyms_csv = CASE WHEN anime_catalog.synonyms_csv IS NULL OR anime_catalog.synonyms_csv = ''
                                        THEN excluded.synonyms_csv ELSE anime_catalog.synonyms_csv END,
                    image_url = COALESCE(anime_catalog.image_url, excluded.image_url),
                    media_type = COALESCE(anime_catalog.media_type, excluded.media_type),
                    status = COALESCE(anime_catalog.status, excluded.status),
                    episodes = COALESCE(anime_catalog.episodes, excluded.episodes),
                    season_year = COALESCE(anime_catalog.season_year, excluded.season_year),
                    season_name = COALESCE(anime_catalog.season_name, excluded.season_name),
                    primary_studio = COALESCE(anime_catalog.primary_studio, excluded.primary_studio),
                    studios_csv = COALESCE(anime_catalog.studios_csv, excluded.studios_csv),
                    tags_csv = COALESCE(anime_catalog.tags_csv, excluded.tags_csv),
                    updated_at = excluded.updated_at
            """, batch)
            batch = []

    if batch:
        cursor.executemany("""
            INSERT INTO anime_catalog (
                mal_id, anilist_id, kitsu_id, anidb_id,
                title, english_title, synonyms_csv, image_url,
                media_type, status, episodes,
                season_year, season_name,
                primary_studio, studios_csv, tags_csv, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(mal_id) DO UPDATE SET
                anilist_id = COALESCE(excluded.anilist_id, anime_catalog.anilist_id),
                kitsu_id = COALESCE(excluded.kitsu_id, anime_catalog.kitsu_id),
                anidb_id = COALESCE(excluded.anidb_id, anime_catalog.anidb_id),
                synonyms_csv = CASE WHEN anime_catalog.synonyms_csv IS NULL OR anime_catalog.synonyms_csv = ''
                                    THEN excluded.synonyms_csv ELSE anime_catalog.synonyms_csv END,
                image_url = COALESCE(anime_catalog.image_url, excluded.image_url),
                media_type = COALESCE(anime_catalog.media_type, excluded.media_type),
                status = COALESCE(anime_catalog.status, excluded.status),
                episodes = COALESCE(anime_catalog.episodes, excluded.episodes),
                season_year = COALESCE(anime_catalog.season_year, excluded.season_year),
                season_name = COALESCE(anime_catalog.season_name, excluded.season_name),
                primary_studio = COALESCE(anime_catalog.primary_studio, excluded.primary_studio),
                studios_csv = COALESCE(anime_catalog.studios_csv, excluded.studios_csv),
                tags_csv = COALESCE(anime_catalog.tags_csv, excluded.tags_csv),
                updated_at = excluded.updated_at
        """, batch)

    if studios_batch:
        cursor.executemany("""
            INSERT OR IGNORE INTO anime_studios (studio_name, mal_id) VALUES (?, ?)
        """, list(studios_batch))

    if relations_batch:
        cursor.executemany("""
            INSERT OR IGNORE INTO anime_relations (source_mal_id, target_mal_id, relation_type) VALUES (?, ?, ?)
        """, list(relations_batch))

    conn.commit()
    print(f"Base anime-offline-database ingestion complete with {len(relations_batch)} relations.")


def sync_mal_ranking(
    conn: sqlite3.Connection,
    client_id: str,
    ranking_type: str = "all",
    max_pages: int = None,
    delay_sec: float = 1.0,
) -> list:
    """
    Syncs ranking from MAL API v2 (/anime/ranking).
    Returns list of updated items.
    """
    cursor = conn.cursor()
    headers = get_headers(client_id)
    offset = 0
    limit = 500
    page_count = 0
    updated_items = []
    now_epoch = int(time.time())

    print(f"Starting MAL sync for ranking_type='{ranking_type}'...")

    while True:
        url = f"{MAL_API_BASE}/anime/ranking?ranking_type={ranking_type}&limit={limit}&offset={offset}&fields={FIELDS}"
        data = fetch_url_json(url, headers=headers)
        items = data.get("data", [])
        if not items:
            break

        upsert_batch = []
        studios_batch = set()

        for item in items:
            node = item.get("node", {})
            ranking_info = item.get("ranking", {})

            mal_id = node.get("id")
            if not mal_id:
                continue

            title = node.get("title")
            alt_titles = node.get("alternative_titles", {})
            english_title = alt_titles.get("en") or None
            synonyms_list = alt_titles.get("synonyms") or []
            synonyms_csv = "|".join(s.strip() for s in synonyms_list if s.strip())

            main_pic = node.get("main_picture", {})
            image_url = main_pic.get("large") or main_pic.get("medium")

            score = node.get("mean")
            scored_by = node.get("num_scoring_users", 0)
            rank = ranking_info.get("rank") or node.get("rank")
            popularity = node.get("popularity")
            media_type = node.get("media_type")
            status = node.get("status")
            episodes = node.get("num_episodes")

            start_season = node.get("start_season", {})
            season_year = start_season.get("year")
            season_name = start_season.get("season")

            start_date_str = node.get("start_date")
            start_date_epoch = None
            if start_date_str:
                try:
                    parts = start_date_str.split("-")
                    y = int(parts[0])
                    m = int(parts[1]) if len(parts) > 1 else 1
                    d = int(parts[2]) if len(parts) > 2 else 1
                    start_date_epoch = int(time.mktime((y, m, d, 0, 0, 0, 0, 0, -1)))
                except Exception:
                    pass

            source_material = node.get("source")
            studios_data = node.get("studios", [])
            primary_studio = canonicalize_studio_name(studios_data[0].get("name")) if studios_data else None
            studios_csv = "|".join(canonicalize_studio_name(s.get("name", "")) for s in studios_data if s.get("name"))

            genres_data = node.get("genres", [])
            genres_csv = "|".join(g.get("name", "").strip() for g in genres_data if g.get("name"))

            favorite_rank = rank if ranking_type == "favorite" else None

            upsert_batch.append((
                mal_id, title, english_title, synonyms_csv, image_url,
                score, scored_by, rank, popularity, favorite_rank,
                media_type, status, episodes, season_year, season_name,
                start_date_epoch, source_material, primary_studio,
                studios_csv, genres_csv, now_epoch
            ))

            updated_items.append({
                "mal_id": mal_id,
                "title": title,
                "score": score,
                "scored_by": scored_by,
                "rank": rank,
                "popularity": popularity,
                "status": status,
                "episodes": episodes,
            })

            for s in studios_data:
                s_name = canonicalize_studio_name(s.get("name", ""))
                if s_name:
                    studios_batch.add((s_name, mal_id))

        cursor.executemany("""
            INSERT INTO anime_catalog (
                mal_id, title, english_title, synonyms_csv, image_url,
                score, scored_by, rank, popularity, favorite_rank,
                media_type, status, episodes, season_year, season_name,
                start_date_epoch, source_material, primary_studio,
                studios_csv, genres_csv, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(mal_id) DO UPDATE SET
                title = excluded.title,
                english_title = COALESCE(excluded.english_title, anime_catalog.english_title),
                synonyms_csv = CASE WHEN excluded.synonyms_csv != '' THEN excluded.synonyms_csv ELSE anime_catalog.synonyms_csv END,
                image_url = COALESCE(excluded.image_url, anime_catalog.image_url),
                score = COALESCE(excluded.score, anime_catalog.score),
                scored_by = excluded.scored_by,
                rank = COALESCE(excluded.rank, anime_catalog.rank),
                popularity = COALESCE(excluded.popularity, anime_catalog.popularity),
                favorite_rank = COALESCE(excluded.favorite_rank, anime_catalog.favorite_rank),
                media_type = COALESCE(excluded.media_type, anime_catalog.media_type),
                status = COALESCE(excluded.status, anime_catalog.status),
                episodes = COALESCE(excluded.episodes, anime_catalog.episodes),
                season_year = COALESCE(excluded.season_year, anime_catalog.season_year),
                season_name = COALESCE(excluded.season_name, anime_catalog.season_name),
                start_date_epoch = COALESCE(excluded.start_date_epoch, anime_catalog.start_date_epoch),
                source_material = COALESCE(excluded.source_material, anime_catalog.source_material),
                primary_studio = COALESCE(excluded.primary_studio, anime_catalog.primary_studio),
                studios_csv = CASE WHEN excluded.studios_csv != '' THEN excluded.studios_csv ELSE anime_catalog.studios_csv END,
                genres_csv = CASE WHEN excluded.genres_csv != '' THEN excluded.genres_csv ELSE anime_catalog.genres_csv END,
                updated_at = excluded.updated_at
        """, upsert_batch)

        if studios_batch:
            cursor.executemany("""
                INSERT OR IGNORE INTO anime_studios (studio_name, mal_id) VALUES (?, ?)
            """, list(studios_batch))

        conn.commit()
        page_count += 1
        offset += len(items)
        print(f"Synced {offset} anime for '{ranking_type}' (page {page_count})...")

        if "next" not in data.get("paging", {}):
            break
        if max_pages and page_count >= max_pages:
            break

        time.sleep(delay_sec)

    print(f"Completed sync for '{ranking_type}': {offset} items total.")
    return updated_items


def build_studios_summary(conn: sqlite3.Connection):
    """Pre-aggregates studio statistics into studios_summary for instant Studio Explorer."""
    print("Building pre-aggregated studios_summary...")
    cursor = conn.cursor()
    cursor.execute("DELETE FROM studios_summary;")

    # Find studios with anime counts and average score
    cursor.execute("""
        INSERT INTO studios_summary (
            studio_name, total_count, popular_count, airing_count, upcoming_count, avg_score, sample_covers_json
        )
        SELECT 
            s.studio_name COLLATE NOCASE,
            COUNT(DISTINCT a.mal_id) as total_count,
            COUNT(DISTINCT CASE WHEN a.popularity <= 5000 THEN a.mal_id END) as popular_count,
            COUNT(DISTINCT CASE WHEN a.status = 'currently_airing' THEN a.mal_id END) as airing_count,
            COUNT(DISTINCT CASE WHEN a.status = 'not_yet_aired' THEN a.mal_id END) as upcoming_count,
            AVG(CASE WHEN a.score > 0 THEN a.score END) as avg_score,
            '[]' as sample_covers_json
        FROM anime_studios s
        JOIN anime_catalog a ON s.mal_id = a.mal_id
        WHERE s.studio_name IS NOT NULL AND TRIM(s.studio_name) != ''
        GROUP BY s.studio_name COLLATE NOCASE
        HAVING COUNT(DISTINCT a.mal_id) >= 1;
    """)

    # Populate top sample anime per studio for card previews
    cursor.execute("""
        SELECT studio_name FROM studios_summary WHERE popular_count > 0 ORDER BY popular_count DESC LIMIT 250;
    """)
    top_studios = [row[0] for row in cursor.fetchall()]

    for s_name in top_studios:
        cursor.execute("""
            SELECT a.mal_id, a.title, a.english_title, a.image_url, a.score, a.rank, a.popularity, a.status
            FROM anime_studios s
            JOIN anime_catalog a ON s.mal_id = a.mal_id
            WHERE s.studio_name = ? COLLATE NOCASE
            ORDER BY COALESCE(a.popularity, 999999) ASC
            LIMIT 3;
        """, (s_name,))
        samples = []
        for row in cursor.fetchall():
            samples.append({
                "id": row[0],
                "title": row[1],
                "englishTitle": row[2],
                "coverUrl": row[3],
                "score": row[4],
                "rank": row[5],
                "popularity": row[6],
                "status": row[7],
            })
        cursor.execute("""
            UPDATE studios_summary SET sample_covers_json = ? WHERE studio_name = ? COLLATE NOCASE
        """, (json.dumps(samples), s_name))

    conn.commit()
    print("studios_summary build complete.")


def compress_database(db_path: str, gz_path: str) -> tuple:
    """Gzips SQLite database and returns (file_size, sha256)."""
    print(f"Compressing {db_path} -> {gz_path}...")
    sha256 = hashlib.sha256()
    with open(db_path, "rb") as f_in:
        with gzip.open(gz_path, "wb", compresslevel=9) as f_out:
            while chunk := f_in.read(1024 * 1024):
                f_out.write(chunk)

    with open(gz_path, "rb") as f_gz:
        while chunk := f_gz.read(1024 * 1024):
            sha256.update(chunk)

    gz_size = os.path.getsize(gz_path)
    hash_hex = sha256.hexdigest()
    print(f"Compressed DB size: {gz_size / (1024*1024):.2f} MB, SHA-256: {hash_hex}")
    return gz_size, hash_hex


def run_full_build(client_id: str, repo_raw_url: str):
    """Complete full catalog build."""
    start_time = time.time()
    conn = sqlite3.connect(DB_PATH)
    init_database(conn)

    # 1. Base mappings & synonyms from anime-offline-database
    ingest_anime_offline_database(conn)

    # 2. MAL rankings
    sync_mal_ranking(conn, client_id, ranking_type="airing", max_pages=5, delay_sec=1.0)
    sync_mal_ranking(conn, client_id, ranking_type="all", delay_sec=1.0)
    sync_mal_ranking(conn, client_id, ranking_type="bypopularity", max_pages=10, delay_sec=1.0)
    sync_mal_ranking(conn, client_id, ranking_type="favorite", max_pages=5, delay_sec=1.0)
    sync_mal_ranking(conn, client_id, ranking_type="upcoming", max_pages=5, delay_sec=1.0)

    # 3. Aggregations & FTS5
    build_studios_summary(conn)
    setup_fts5(conn)

    # Sanity checks
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM anime_catalog;")
    total_count = cursor.fetchone()[0]
    cursor.execute("PRAGMA integrity_check;")
    integrity = cursor.fetchone()[0]

    if integrity != "ok":
        raise RuntimeError(f"Database integrity check failed: {integrity}")
    if total_count < 20000:
        raise RuntimeError(f"Sanity check failed: total anime count too low ({total_count} < 20000)")

    now_epoch = int(time.time())
    cursor.execute("""
        INSERT INTO db_metadata (key, value) VALUES ('full_sync_epoch', ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
    """, (str(now_epoch),))
    cursor.execute("""
        INSERT INTO db_metadata (key, value) VALUES ('daily_sync_epoch', ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
    """, (str(now_epoch),))
    cursor.execute("""
        INSERT INTO db_metadata (key, value) VALUES ('total_anime_count', ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
    """, (str(total_count),))
    conn.commit()
    conn.close()

    # Compress & generate manifest
    gz_size, gz_sha256 = compress_database(DB_PATH, DB_GZ_PATH)

    manifest = {
        "schema_version": SCHEMA_VERSION,
        "full_sync_epoch": now_epoch,
        "daily_sync_epoch": now_epoch,
        "total_anime_count": total_count,
        "full_db_url": f"{repo_raw_url}/{DB_GZ_PATH}",
        "full_db_sha256": gz_sha256,
        "full_db_size_bytes": gz_size,
        "airing_delta_url": f"{repo_raw_url}/{DELTA_PATH}",
        "airing_delta_count": 0,
    }
    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    delta = {
        "base_full_sync_epoch": now_epoch,
        "delta_epoch": now_epoch,
        "items": [],
    }
    with open(DELTA_PATH, "w", encoding="utf-8") as f:
        json.dump(delta, f, indent=2)

    elapsed = time.time() - start_time
    print(f"Full build completed successfully in {elapsed:.1f}s. Total anime: {total_count}")


def run_airing_update(client_id: str, repo_raw_url: str):
    """Daily fast update for currently airing & upcoming shows (<2s)."""
    start_time = time.time()
    if not os.path.exists(DB_PATH):
        if os.path.exists(DB_GZ_PATH):
            print(f"Decompressing {DB_GZ_PATH} -> {DB_PATH}...")
            with gzip.open(DB_GZ_PATH, "rb") as f_in:
                with open(DB_PATH, "wb") as f_out:
                    f_out.write(f_in.read())
        else:
            remote_gz_url = f"{repo_raw_url}/{DB_GZ_PATH}"
            print(f"Checking for existing DB at {remote_gz_url}...")
            try:
                req = urllib.request.Request(remote_gz_url, headers={"User-Agent": USER_AGENT})
                with urllib.request.urlopen(req, timeout=60) as resp:
                    if resp.status == 200:
                        print("Downloading existing database from remote...")
                        with gzip.GzipFile(fileobj=resp) as gz_stream:
                            with open(DB_PATH, "wb") as f_out:
                                while chunk := gz_stream.read(1024 * 1024):
                                    f_out.write(chunk)
            except Exception as e:
                print(f"Could not download existing database from remote ({e}).")

    if os.path.exists(DB_PATH):
        try:
            test_conn = sqlite3.connect(DB_PATH)
            c = test_conn.cursor()
            c.execute("SELECT COUNT(*) FROM anime_catalog")
            count_row = c.fetchone()
            existing_count = count_row[0] if count_row else 0
            test_conn.close()
            if existing_count < 20000:
                print(f"Existing database only has {existing_count} anime (< 20000). Falling back to full rebuild...")
                return run_full_build(client_id, repo_raw_url)
        except Exception as e:
            print(f"Error checking existing database ({e}). Falling back to full rebuild...")
            return run_full_build(client_id, repo_raw_url)
    else:
        print("No database available. Initiating full build...")
        return run_full_build(client_id, repo_raw_url)

    conn = sqlite3.connect(DB_PATH)
    init_database(conn)

    airing_items = sync_mal_ranking(conn, client_id, ranking_type="airing", max_pages=3, delay_sec=1.0)
    upcoming_items = sync_mal_ranking(conn, client_id, ranking_type="upcoming", max_pages=2, delay_sec=1.0)

    # Deduplicate items for delta
    merged_items = {}
    for item in airing_items + upcoming_items:
        merged_items[item["mal_id"]] = item

    delta_items = list(merged_items.values())
    now_epoch = int(time.time())

    build_studios_summary(conn)
    setup_fts5(conn)

    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO db_metadata (key, value) VALUES ('daily_sync_epoch', ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
    """, (str(now_epoch),))
    cursor.execute("SELECT value FROM db_metadata WHERE key = 'full_sync_epoch';")
    row = cursor.fetchone()
    full_sync_epoch = int(row[0]) if row else now_epoch
    cursor.execute("SELECT COUNT(*) FROM anime_catalog;")
    total_count = cursor.fetchone()[0]
    conn.commit()
    conn.close()

    gz_size, gz_sha256 = compress_database(DB_PATH, DB_GZ_PATH)

    delta = {
        "base_full_sync_epoch": full_sync_epoch,
        "delta_epoch": now_epoch,
        "items": delta_items,
    }
    with open(DELTA_PATH, "w", encoding="utf-8") as f:
        json.dump(delta, f, indent=2)

    manifest = {
        "schema_version": SCHEMA_VERSION,
        "full_sync_epoch": full_sync_epoch,
        "daily_sync_epoch": now_epoch,
        "total_anime_count": total_count,
        "full_db_url": f"{repo_raw_url}/{DB_GZ_PATH}",
        "full_db_sha256": gz_sha256,
        "full_db_size_bytes": gz_size,
        "airing_delta_url": f"{repo_raw_url}/{DELTA_PATH}",
        "airing_delta_count": len(delta_items),
    }
    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    elapsed = time.time() - start_time
    print(f"Airing delta update complete in {elapsed:.1f}s. Delta items: {len(delta_items)}")


def main():
    parser = argparse.ArgumentParser(description="Sync MyAnimeList catalog to offline SQLite DB")
    parser.add_argument("--mode", choices=["full", "airing", "both"], default="airing",
                        help="Sync mode: 'full' for complete scrape, 'airing' for daily delta, 'both' for auto-select")
    parser.add_argument("--client-id", default=os.getenv("MAL_CLIENT_ID", DEFAULT_CLIENT_ID),
                        help="MyAnimeList API Client ID")
    parser.add_argument("--repo-url", default=os.getenv("REPO_DATA_URL", "https://raw.githubusercontent.com/user/repo/mal-db-data"),
                        help="Base raw URL for downloading published artifacts")
    args = parser.parse_args()

    print(f"Starting MAL DB Sync in mode='{args.mode}'...")

    if args.mode == "full":
        run_full_build(args.client_id, args.repo_url)
    elif args.mode == "airing":
        run_airing_update(args.client_id, args.repo_url)
    elif args.mode == "both":
        if os.path.exists(DB_PATH) or os.path.exists(DB_GZ_PATH):
            run_airing_update(args.client_id, args.repo_url)
        else:
            run_full_build(args.client_id, args.repo_url)


if __name__ == "__main__":
    main()
