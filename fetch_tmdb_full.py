"""
TMDB Top 500 Movies by Vote Count per Year (1980-2025)
Outputs 3 CSV files:
  - tmdb_movies.csv       — one row per movie
  - tmdb_cast.csv         — cast members, joinable by tmdb_id
  - tmdb_crew.csv         — crew members, joinable by tmdb_id
"""

import requests
import csv
import time
import os
import sys

API_KEY = "6e2307437dc8bf4e637ad5ae53875510"
BASE_URL = "https://api.themoviedb.org/3"
START_YEAR = 1980
END_YEAR = 2025
TARGET_PER_YEAR = 500
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))

SESSION = requests.Session()
SESSION.params = {"api_key": API_KEY}  # type: ignore


def get(path, **params):
    """GET with simple retry logic."""
    for attempt in range(4):
        try:
            r = SESSION.get(f"{BASE_URL}{path}", params=params, timeout=15)
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", 5))
                print(f"\n    Rate limited — waiting {wait}s...", end="", flush=True)
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as e:
            if attempt == 3:
                print(f"\n    Request failed: {e}")
                return None
            time.sleep(2 ** attempt)
    return None


def discover_page(year, page):
    return get(
        "/discover/movie",
        sort_by="vote_count.desc",
        primary_release_year=year,
        include_adult=False,
        include_video=False,
        language="en-US",
        page=page,
    )


def movie_details(tmdb_id):
    return get(f"/movie/{tmdb_id}", append_to_response="external_ids,credits")


def collect_movie_ids(year, target=500):
    ids = []
    page = 1
    while len(ids) < target:
        data = discover_page(year, page)
        if not data:
            break
        results = data.get("results", [])
        if not results:
            break
        for m in results:
            ids.append(m["id"])
            if len(ids) >= target:
                break
        if page >= min(data.get("total_pages", 1), 25):
            break
        page += 1
        time.sleep(0.15)
    return ids


def safe(v):
    """Clean a value for CSV: stringify, strip newlines."""
    if v is None:
        return ""
    return str(v).replace("\n", " ").replace("\r", "")


def process_movie(tmdb_id, year):
    d = movie_details(tmdb_id)
    if not d:
        return None, [], []

    # ── Movie row ──────────────────────────────────────────────
    genres = "|".join(g["name"] for g in (d.get("genres") or []))
    prod_cos = "|".join(c["name"] for c in (d.get("production_companies") or []))
    external = d.get("external_ids") or {}

    movie = {
        "tmdb_id":              d.get("id"),
        "imdb_id":              safe(external.get("imdb_id")),
        "wikidata_id":          safe(external.get("wikidata_id")),
        "title":                safe(d.get("title")),
        "release_date":         safe(d.get("release_date")),
        "year":                 year,
        "genres":               genres,
        "runtime":              d.get("runtime"),
        "vote_average":         d.get("vote_average"),
        "vote_count":           d.get("vote_count"),
        "popularity":           d.get("popularity"),
        "revenue":              d.get("revenue"),
        "production_companies": prod_cos,
    }

    credits = d.get("credits") or {}

    # ── Cast rows ──────────────────────────────────────────────
    cast_rows = []
    for c in (credits.get("cast") or []):
        cast_rows.append({
            "tmdb_id":      tmdb_id,
            "person_id":    c.get("id"),
            "name":         safe(c.get("name")),
            "character":    safe(c.get("character")),
            "cast_order":   c.get("order"),
            "gender":       c.get("gender"),
            "known_for":    safe(c.get("known_for_department")),
            "profile_path": f"https://image.tmdb.org/t/p/w185{c['profile_path']}" if c.get("profile_path") else "",
        })

    # ── Crew rows ──────────────────────────────────────────────
    crew_rows = []
    for c in (credits.get("crew") or []):
        crew_rows.append({
            "tmdb_id":      tmdb_id,
            "person_id":    c.get("id"),
            "name":         safe(c.get("name")),
            "department":   safe(c.get("department")),
            "job":          safe(c.get("job")),
            "gender":       c.get("gender"),
            "known_for":    safe(c.get("known_for_department")),
            "profile_path": f"https://image.tmdb.org/t/p/w185{c['profile_path']}" if c.get("profile_path") else "",
        })

    return movie, cast_rows, crew_rows


def main():
    movie_path = os.path.join(OUTPUT_DIR, "tmdb_movies.csv")
    cast_path  = os.path.join(OUTPUT_DIR, "tmdb_cast.csv")
    crew_path  = os.path.join(OUTPUT_DIR, "tmdb_crew.csv")

    movie_fields = [
        "tmdb_id", "imdb_id", "wikidata_id", "title", "release_date", "year",
        "genres", "runtime", "vote_average", "vote_count", "popularity",
        "revenue", "production_companies",
    ]
    cast_fields  = ["tmdb_id", "person_id", "name", "character", "cast_order", "gender", "known_for", "profile_path"]
    crew_fields  = ["tmdb_id", "person_id", "name", "department", "job", "gender", "known_for", "profile_path"]

    years = list(range(START_YEAR, END_YEAR + 1))
    total_movies = total_cast = total_crew = 0

    with open(movie_path, "w", newline="", encoding="utf-8") as mf, \
         open(cast_path,  "w", newline="", encoding="utf-8") as cf, \
         open(crew_path,  "w", newline="", encoding="utf-8") as kf:

        mw = csv.DictWriter(mf, fieldnames=movie_fields)
        cw = csv.DictWriter(cf, fieldnames=cast_fields)
        kw = csv.DictWriter(kf, fieldnames=crew_fields)
        mw.writeheader(); cw.writeheader(); kw.writeheader()

        for year in years:
            print(f"\n[{year}] Collecting IDs...", end="", flush=True)
            ids = collect_movie_ids(year, TARGET_PER_YEAR)
            print(f" {len(ids)} movies. Fetching details:", end="", flush=True)

            for i, tmdb_id in enumerate(ids, 1):
                movie, cast_rows, crew_rows = process_movie(tmdb_id, year)
                if movie:
                    mw.writerow(movie)
                    cw.writerows(cast_rows)
                    kw.writerows(crew_rows)
                    total_movies += 1
                    total_cast   += len(cast_rows)
                    total_crew   += len(crew_rows)

                if i % 50 == 0:
                    print(f" {i}", end="", flush=True)
                    # Flush to disk periodically
                    mf.flush(); cf.flush(); kf.flush()

                time.sleep(0.25)

    print(f"\n\n✅ Complete!")
    print(f"   Movies : {total_movies:,}  →  {movie_path}")
    print(f"   Cast   : {total_cast:,}  →  {cast_path}")
    print(f"   Crew   : {total_crew:,}  →  {crew_path}")


if __name__ == "__main__":
    main()
