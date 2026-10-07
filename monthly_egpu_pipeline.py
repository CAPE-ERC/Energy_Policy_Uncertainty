"""Automated Energy Policy Uncertainty (EgPU) computation pipeline.

This pipeline deliberately does not scrape. It reads the exact article files
selected by the production EPU workflow, applies the EgPU term dictionary, and
uses the same frozen-baseline country-index and GDP-weighted aggregation logic.
An article qualifies only when Energy, Policy, and Uncertainty terms are all present.

Stage 1 - build and freeze the baseline:
    python -X utf8 monthly_egpu_pipeline.py baseline 2025-12

Stage 2 - compute post-baseline months with frozen parameters:
    python -X utf8 monthly_egpu_pipeline.py update 2026-09
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import math
import os
import re
import sys
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urlsplit

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd


# Production inputs shared with the EPU workflow. Nothing in these folders is
# modified by this script.
EPU_ROOT = Path(r"G:\My Drive\News_Articles_Africa_Workbook\Main_Monthly_Scrapper")
ARTICLE_ROOT = EPU_ROOT / "Scrapped_Top_Articles"
GDP_WIDE_CSV = EPU_ROOT / "EPU_Computations" / "gdp_ppp_monthly_wide.csv"
EgPU_ROOT = Path(r"G:\My Drive\News_Articles_Africa_Workbook\EgPU")
EPTU_FRAMEWORK = EPU_ROOT / "EPU_Computations" / "EPTU_Framework"
TERMS_JSON = EgPU_ROOT / "energy_policy_uncertainty_keywords.json"
if str(EPTU_FRAMEWORK) not in sys.path:
    sys.path.insert(0, str(EPTU_FRAMEWORK))
from eptu_text_matcher import satisfies_index

# Keep every EgPU program file and generated product under the EgPU folder.
OUTPUT_ROOT = EgPU_ROOT / "egpu_outputs"
CACHE_DIR = OUTPUT_ROOT / "_egpu_baseline_cache"
BASELINE_AGGREGATES_DIR = CACHE_DIR / "aggregate_history"
AGGREGATE_PARAMS_JSON = CACHE_DIR / "aggregate_params.json"
REGIONAL_DIR = OUTPUT_ROOT / "Regional_Output"
REPORTS_DIR = OUTPUT_ROOT / "reports"
METHODOLOGY_ID = "EgPU Unicode whole-word/phrase matcher v1 strict ENERGY-P-U components"
ARTICLE_SCAN_WORKERS = max(2, min(8, os.cpu_count() or 4))
ARTICLE_SCAN_BATCH_FILES = 25
_ARTICLE_WORKER_TERMS: Dict[str, Dict[str, List[str]]] = {}

BASELINE_START = "2015-01"
BASELINE_END = "2025-12"

COUNTRY_NAME_ALIASES = {
    "Cabo Verde": "Cape Verde",
    "Congo, The Democratic Republic of the": "Congo DRC",
}

REGION_COUNTRIES: Dict[str, List[str]] = {
    "central_africa": [
        "Cameroon", "Central African Republic", "Chad", "Congo", "Congo DRC",
        "Equatorial Guinea", "Gabon",
    ],
    "east_africa": [
        "Burundi", "Comoros", "Djibouti", "Eritrea", "Ethiopia", "Kenya",
        "Rwanda", "Seychelles", "Sudan", "South Sudan", "Tanzania", "Uganda",
        "Somalia",
    ],
    "north_africa": [
        "Algeria", "Egypt", "Libya", "Mauritania", "Morocco", "Tunisia",
    ],
    "southern_africa": [
        "Angola", "Botswana", "Lesotho", "Madagascar", "Malawi", "Mauritius",
        "Mozambique", "Namibia", "Sao Tome and Principe", "South Africa",
        "Eswatini", "Zambia", "Zimbabwe",
    ],
    "west_africa": [
        "Benin", "Burkina-Faso", "Cape Verde", "Ivory Coast", "Gambia", "Ghana",
        "Guinea", "Guinea-Bissau", "Liberia", "Mali", "Niger", "Nigeria",
        "Senegal", "Sierra Leone", "Togo",
    ],
}

REGION_LABELS = {
    "central_africa": "Central Africa",
    "east_africa": "East Africa",
    "north_africa": "North Africa",
    "southern_africa": "Southern Africa",
    "west_africa": "West Africa",
}

COUNTRY_COLUMNS = ["country", "year_month", "EgPU", "M_t", "mu", "n_sites"]
PANEL_COLUMNS = ["country", "site", "year_month", "egpu_count", "total_articles"]


def log(message: str) -> None:
    print(message, flush=True)


def clean_list(values: Optional[Sequence[object]]) -> List[str]:
    seen = set()
    result: List[str] = []
    for value in values or []:
        text = str(value).strip() if value is not None else ""
        key = text.casefold()
        if text and key not in seen:
            seen.add(key)
            result.append(text)
    return result


def normalize_ym(value: str) -> str:
    value = str(value).strip()
    if re.fullmatch(r"\d{4}-\d{2}", value):
        return str(pd.Period(value, freq="M"))
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        return str(pd.Timestamp(value).to_period("M"))
    raise ValueError(f"Invalid month {value!r}; expected YYYY-MM or YYYY-MM-DD")


def month_range(start_ym: str, end_ym: str) -> List[str]:
    start = pd.Period(normalize_ym(start_ym), freq="M")
    end = pd.Period(normalize_ym(end_ym), freq="M")
    if start > end:
        raise ValueError(f"Start month {start} is after end month {end}")
    return [str(period) for period in pd.period_range(start, end, freq="M")]


def in_window(value: str, start_ym: str, end_ym: str) -> bool:
    try:
        period = pd.Period(value, freq="M")
    except Exception:
        return False
    return pd.Period(start_ym, freq="M") <= period <= pd.Period(end_ym, freq="M")


def atomic_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    df.to_csv(temp, index=False, encoding="utf-8")
    temp.replace(path)


def atomic_json(payload: object, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    temp.replace(path)


def methodology_fingerprint() -> str:
    digest = hashlib.sha256(METHODOLOGY_ID.encode("utf-8"))
    matcher_path = EPTU_FRAMEWORK / "eptu_text_matcher.py"
    for path in (TERMS_JSON, matcher_path):
        if not path.exists():
            raise FileNotFoundError(f"Methodology input not found: {path}")
    term_document = json.loads(TERMS_JSON.read_text(encoding="utf-8"))
    term_content = term_document.get("countries", term_document)
    digest.update(json.dumps(
        term_content, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8"))
    digest.update(matcher_path.read_bytes())
    return digest.hexdigest()


def canonical_article_url(value: object) -> str:
    try:
        parts = urlsplit(str(value).strip())
    except Exception:
        return ""
    host = (parts.hostname or "").casefold()
    if host.startswith("www."):
        host = host[4:]
    path = re.sub(r"/{2,}", "/", parts.path or "/")
    if path != "/":
        path = path.rstrip("/")
    return f"{host}{path}".casefold() if host else ""


def load_terms(path: Path) -> Dict[str, Dict[str, List[str]]]:
    if not path.exists():
        raise FileNotFoundError(f"EgPU terms file not found: {path}")
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw = raw.get("countries", raw)
    terms: Dict[str, Dict[str, List[str]]] = {}
    for original_country, payload in raw.items():
        country = COUNTRY_NAME_ALIASES.get(str(original_country).strip(), str(original_country).strip())
        normalized = {
            "ENERGY_TERMS": clean_list(payload.get("ENERGY_TERMS", [])),
            "POLICY_TERMS": clean_list(payload.get("POLICY_TERMS", [])),
            "UNCERTAINTY_TERMS": clean_list(payload.get("UNCERTAINTY_TERMS", [])),
        }
        missing = [key for key, values in normalized.items() if not values]
        if missing:
            raise ValueError(f"{original_country}: empty EgPU term categories: {missing}")
        if country in terms:
            raise ValueError(f"Country alias collision in EgPU terms: {country}")
        terms[country] = normalized
    return terms


def is_egpu_article(
    title: object,
    text: object,
    energy_terms: Sequence[str],
    policy_terms: Sequence[str],
    uncertainty_terms: Sequence[str],
) -> bool:
    full_text = f"{'' if pd.isna(title) else title} {'' if pd.isna(text) else text}"

    # EgPU requires Energy AND Policy AND Uncertainty; Economic is not required.
    return satisfies_index(
        "", full_text,
        {"ENERGY": energy_terms, "P": policy_terms, "U": uncertainty_terms},
        ("ENERGY", "P", "U"),
    )


def iter_article_files(
    root: Path,
    allowed_countries: Sequence[str],
    start_ym: str,
    end_ym: str,
) -> Iterable[Tuple[str, str, str, Path]]:
    allowed = set(allowed_countries)
    if not root.exists():
        raise FileNotFoundError(f"EPU top-article folder not found: {root}")
    for country_dir in sorted(root.iterdir(), key=lambda p: p.name.casefold()):
        if not country_dir.is_dir() or country_dir.name not in allowed:
            continue
        for site_dir in sorted(country_dir.iterdir(), key=lambda p: p.name.casefold()):
            if not site_dir.is_dir():
                continue
            for month_dir in sorted(site_dir.iterdir(), key=lambda p: p.name):
                if not month_dir.is_dir() or not in_window(month_dir.name, start_ym, end_ym):
                    continue
                for path in sorted(month_dir.glob("articles_*.csv*")):
                    if ".backup_" not in path.name and not path.name.startswith("."):
                        yield country_dir.name, site_dir.name, month_dir.name, path


def configure_csv_field_limit() -> int:
    """Allow unusually long article bodies without overflowing the C API."""
    limit = sys.maxsize
    while True:
        try:
            csv.field_size_limit(limit)
            return limit
        except OverflowError:
            limit //= 10


def safe_read_article_rows(path: Path) -> Optional[List[Tuple[str, str, str]]]:
    """Stream only URL, title, and text from one article CSV."""
    try:
        opener = gzip.open if path.name.casefold().endswith(".gz") else open
        with opener(path, "rt", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            if not reader.fieldnames or not {"title", "text"}.issubset(reader.fieldnames):
                return None
            rows: List[Tuple[str, str, str]] = []
            has_content = False
            for row in reader:
                url = row.get("url") or ""
                title = row.get("title") or ""
                text = row.get("text") or ""
                has_content = has_content or bool(title.strip() or text.strip())
                rows.append((url, title, text))
    except MemoryError:
        raise
    except (EOFError, OSError, gzip.BadGzipFile, UnicodeError, csv.Error):
        return None
    except Exception:
        return None
    if not rows or not has_content:
        return None
    return rows


def init_article_worker(terms: Dict[str, Dict[str, List[str]]]) -> None:
    """Load the shared dictionary once in each worker process."""
    global _ARTICLE_WORKER_TERMS
    configure_csv_field_limit()
    _ARTICLE_WORKER_TERMS = terms


def process_country_month_group(group_item):
    """Read, deduplicate, and classify one country-month in a worker process."""
    (country, ym), inputs = group_item
    group_records: List[dict] = []
    group_skipped: List[dict] = []
    group_duplicates = 0
    seen = set()
    site_counts: Dict[str, Dict[str, int]] = {}
    dictionary = _ARTICLE_WORKER_TERMS[country]
    for site, path in sorted(inputs, key=lambda item: (item[0].casefold(), str(item[1]).casefold())):
        rows = safe_read_article_rows(path)
        if rows is None:
            group_skipped.append({
                "country": country, "site": site, "year_month": ym,
                "file": str(path), "reason": "Unreadable, empty, or missing title/text",
            })
            continue
        counts = site_counts.setdefault(site, {"egpu_count": 0, "total_articles": 0})
        for url, title, text in rows:
            key = canonical_article_url(url)
            if not key:
                fallback = f"{title.strip().casefold()}\n{text.strip().casefold()}"
                key = f"text:{fallback}" if fallback.strip() else ""
            if key and key in seen:
                group_duplicates += 1
                continue
            if key:
                seen.add(key)
            counts["total_articles"] += 1
            if is_egpu_article(
                title,
                text,
                dictionary["ENERGY_TERMS"],
                dictionary["POLICY_TERMS"],
                dictionary["UNCERTAINTY_TERMS"],
            ):
                counts["egpu_count"] += 1
    for site, counts in sorted(site_counts.items()):
        group_records.append({
            "country": country,
            "site": site,
            "year_month": ym,
            "egpu_count": counts["egpu_count"],
            "total_articles": counts["total_articles"],
        })

    return group_records, group_skipped, group_duplicates, len(inputs)


def process_country_month_batch(batch):
    """Process a balanced batch without splitting a country-month group."""
    return [process_country_month_group(group_item) for group_item in batch]


def build_panel(
    terms: Dict[str, Dict[str, List[str]]],
    countries: Sequence[str],
    start_ym: str,
    end_ym: str,
) -> Tuple[pd.DataFrame, pd.DataFrame, int]:
    # Keep only paths during discovery. Holding every article DataFrame here can
    # exhaust memory on the 25,000+ file historical archive.
    grouped: Dict[Tuple[str, str], List[Tuple[str, Path]]] = {}
    skipped: List[dict] = []
    discovered = 0
    for country, site, ym, path in iter_article_files(ARTICLE_ROOT, countries, start_ym, end_ym):
        discovered += 1
        grouped.setdefault((country, ym), []).append((site, path))
        if discovered % 1000 == 0:
            log(f"Discovered {discovered:,} EPU-selected article files...")

    records: List[dict] = []
    duplicate_rows = 0
    processed = 0
    last_reported = 0
    groups = sorted(grouped.items())
    batches = []
    current_batch = []
    current_files = 0
    for group_item in groups:
        group_files = len(group_item[1])
        if current_batch and current_files + group_files > ARTICLE_SCAN_BATCH_FILES:
            batches.append(current_batch)
            current_batch = []
            current_files = 0
        current_batch.append(group_item)
        current_files += group_files
    if current_batch:
        batches.append(current_batch)

    log(
        f"Scanning {len(groups):,} country-month groups with "
        f"{ARTICLE_SCAN_WORKERS} process workers in {len(batches):,} balanced batches..."
    )
    with ProcessPoolExecutor(
        max_workers=ARTICLE_SCAN_WORKERS,
        initializer=init_article_worker,
        initargs=(terms,),
    ) as executor:
        futures = [executor.submit(process_country_month_batch, batch) for batch in batches]
        for future in as_completed(futures):
            for group_records, group_skipped, group_duplicates, file_count in future.result():
                records.extend(group_records)
                skipped.extend(group_skipped)
                duplicate_rows += group_duplicates
                processed += file_count
            if processed - last_reported >= 250 or processed == discovered:
                log(f"Processed {processed:,} of {discovered:,} EPU-selected article files...")
                last_reported = processed

    panel = pd.DataFrame(records, columns=PANEL_COLUMNS).sort_values(
        ["country", "year_month", "site"], ignore_index=True
    )
    skipped_df = pd.DataFrame(
        skipped, columns=["country", "site", "year_month", "file", "reason"]
    ).sort_values(["country", "year_month", "site", "file"], ignore_index=True)
    log(
        f"Article scan complete: files={processed:,}, panel rows={len(panel):,}, "
        f"cross-source duplicate rows removed={duplicate_rows:,}"
    )
    return panel, skipped_df, duplicate_rows


def compute_baseline_country(group: pd.DataFrame) -> Tuple[Optional[dict], pd.DataFrame, pd.DataFrame]:
    country = str(group["country"].iloc[0])
    frame = group.copy()
    numerator = pd.to_numeric(frame["egpu_count"], errors="coerce").astype(float)
    denominator = pd.to_numeric(frame["total_articles"], errors="coerce").astype(float)
    frame["S_it"] = numerator.div(denominator.where(denominator.gt(0)))
    valid_months = frame.groupby("site")["S_it"].count()
    keep_sites = set(valid_months[valid_months >= 2].index)
    dropped = [
        {"country": country, "site": site, "reason": "Fewer than 2 valid baseline months"}
        for site in sorted(set(frame["site"]) - keep_sites)
    ]
    frame = frame[frame["site"].isin(keep_sites)].copy()
    site_std = frame.groupby("site")["S_it"].std(ddof=0)
    valid_std = site_std[(site_std.notna()) & (site_std > 0)]
    for site in sorted(keep_sites - set(valid_std.index)):
        dropped.append({"country": country, "site": site, "reason": "Baseline sigma_i is zero or missing"})
    frame = frame[frame["site"].isin(valid_std.index)].copy()
    if frame.empty:
        dropped.append({"country": country, "site": "*ALL*", "reason": "No usable baseline sites"})
        return None, pd.DataFrame(columns=COUNTRY_COLUMNS), pd.DataFrame(dropped)

    frame = frame.merge(valid_std.rename("sigma_i"), left_on="site", right_index=True)
    frame["N_it"] = frame["S_it"] / frame["sigma_i"]
    monthly = frame.groupby("year_month").agg(
        M_t=("N_it", "mean"), n_sites=("N_it", lambda values: int(values.notna().sum()))
    ).reset_index()
    mu = float(monthly["M_t"].mean())
    if not math.isfinite(mu) or mu == 0:
        dropped.append({"country": country, "site": "*ALL*", "reason": "Baseline mu is zero or missing"})
        return None, pd.DataFrame(columns=COUNTRY_COLUMNS), pd.DataFrame(dropped)
    monthly["EgPU"] = monthly["M_t"] / mu * 100.0
    monthly["country"] = country
    monthly["mu"] = mu
    result = monthly[COUNTRY_COLUMNS].sort_values("year_month")
    params = {
        "baseline_start_ym": BASELINE_START,
        "baseline_end_ym": BASELINE_END,
        "mu": mu,
        "keep_sites": sorted(valid_std.index.tolist()),
        "sigma_i": {site: float(value) for site, value in valid_std.items()},
    }
    return params, result, pd.DataFrame(dropped)


def build_baseline(panel: pd.DataFrame) -> Tuple[dict, pd.DataFrame, pd.DataFrame]:
    params: Dict[str, dict] = {}
    outputs: List[pd.DataFrame] = []
    drops: List[pd.DataFrame] = []
    for country, group in panel.groupby("country", sort=True):
        country_params, country_output, country_drops = compute_baseline_country(group)
        if country_params is not None:
            params[country] = country_params
            outputs.append(country_output)
        if not country_drops.empty:
            drops.append(country_drops)
    if not outputs:
        raise RuntimeError("No country EgPU series could be calibrated in the baseline window")
    return (
        params,
        pd.concat(outputs, ignore_index=True),
        pd.concat(drops, ignore_index=True) if drops else pd.DataFrame(columns=["country", "site", "reason"]),
    )


def compute_updates(panel: pd.DataFrame, params: dict) -> Tuple[pd.DataFrame, pd.DataFrame]:
    results: List[pd.DataFrame] = []
    dropped: List[dict] = []
    for country, group in panel.groupby("country", sort=True):
        config = params.get(country)
        if not config:
            dropped.append({"country": country, "site": "*ALL*", "reason": "No frozen baseline parameters"})
            continue
        frame = group.copy()
        numerator = pd.to_numeric(frame["egpu_count"], errors="coerce").astype(float)
        denominator = pd.to_numeric(frame["total_articles"], errors="coerce").astype(float)
        frame["S_it"] = numerator.div(denominator.where(denominator.gt(0)))
        keep_sites = set(config["keep_sites"])
        for site in sorted(set(frame["site"]) - keep_sites):
            dropped.append({"country": country, "site": site, "reason": "Site not in frozen baseline"})
        frame = frame[frame["site"].isin(keep_sites)].copy()
        frame["sigma_i"] = frame["site"].map(config["sigma_i"])
        frame = frame.dropna(subset=["sigma_i", "S_it"])
        if frame.empty:
            dropped.append({"country": country, "site": "*ALL*", "reason": "No usable update sites"})
            continue
        frame["N_it"] = frame["S_it"] / frame["sigma_i"]
        monthly = frame.groupby("year_month").agg(
            M_t=("N_it", "mean"), n_sites=("N_it", lambda values: int(values.notna().sum()))
        ).reset_index()
        monthly["EgPU"] = monthly["M_t"] / float(config["mu"]) * 100.0
        monthly["country"] = country
        monthly["mu"] = float(config["mu"])
        results.append(monthly[COUNTRY_COLUMNS])
    output = pd.concat(results, ignore_index=True) if results else pd.DataFrame(columns=COUNTRY_COLUMNS)
    return output, pd.DataFrame(dropped, columns=["country", "site", "reason"])


def read_gdp_long() -> pd.DataFrame:
    if not GDP_WIDE_CSV.exists():
        raise FileNotFoundError(f"GDP file not found: {GDP_WIDE_CSV}")
    wide = pd.read_csv(GDP_WIDE_CSV, parse_dates=["date"])
    if "date" not in wide.columns:
        raise ValueError("GDP file must contain a date column")
    wide["date"] = wide["date"].dt.to_period("M").dt.to_timestamp()
    long = wide.melt(id_vars=["date"], var_name="country", value_name="gdp_ppp_musd")
    long["country"] = long["country"].astype(str).str.strip()
    long["gdp_ppp_musd"] = pd.to_numeric(long["gdp_ppp_musd"], errors="coerce")
    return long.dropna(subset=["gdp_ppp_musd"]).sort_values(["date", "country"])


def gdp_snapshot(gdp_long: pd.DataFrame, ym: str, policy: str) -> Tuple[pd.DataFrame, str, str]:
    target = pd.Period(ym, freq="M").to_timestamp()
    exact = gdp_long[gdp_long["date"] == target]
    if not exact.empty:
        return exact[["country", "gdp_ppp_musd"]].copy(), ym, "current"
    if policy != "carry-forward":
        raise RuntimeError(f"No GDP data for {ym}; rerun with --gdp-policy carry-forward if appropriate")
    available = gdp_long.loc[gdp_long["date"] <= target, "date"]
    if available.empty:
        raise RuntimeError(f"No GDP data at or before {ym}")
    source_date = available.max()
    snapshot = gdp_long[gdp_long["date"] == source_date]
    return snapshot[["country", "gdp_ppp_musd"]].copy(), str(source_date.to_period("M")), "carry-forward"


def country_z_scores(
    country_long: pd.DataFrame,
    frozen_stats: Optional[Dict[str, Dict[str, float]]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    frame = country_long.copy()
    if frozen_stats is None:
        baseline = frame[frame["year_month"].map(lambda ym: in_window(ym, BASELINE_START, BASELINE_END))]
        stats = baseline.groupby("country")["EgPU"].agg(
            egpu_mean="mean", egpu_std=lambda values: values.std(ddof=0)
        ).reset_index()
        stats = stats[(stats["egpu_std"].notna()) & (stats["egpu_std"] > 0)]
    else:
        stats = pd.DataFrame(
            [
                {
                    "country": country,
                    "egpu_mean": float(values["egpu_mean"]),
                    "egpu_std": float(values["egpu_std"]),
                }
                for country, values in frozen_stats.items()
            ]
        )
    if stats.empty:
        raise RuntimeError("No valid country EgPU baseline moments for aggregation")
    frame = frame.merge(stats, on="country", how="inner")
    frame["z_egpu"] = (frame["EgPU"] - frame["egpu_mean"]) / frame["egpu_std"]
    return frame, stats


def aggregate_group(
    z_scores: pd.DataFrame,
    gdp_long: pd.DataFrame,
    universe: Sequence[str],
    z_name: str,
    index_name: str,
    gdp_policy: str,
    fixed_universe: Optional[Sequence[str]] = None,
    scale_mean: Optional[float] = None,
    scale_std: Optional[float] = None,
    months: Optional[Sequence[str]] = None,
) -> Tuple[pd.DataFrame, dict]:
    if fixed_universe is None:
        baseline_countries = set(
            z_scores.loc[
                z_scores["year_month"].map(lambda ym: in_window(ym, BASELINE_START, BASELINE_END)), "country"
            ]
        )
        gdp_countries = set(gdp_long["country"])
        fixed = [country for country in universe if country in baseline_countries and country in gdp_countries]
    else:
        fixed = list(fixed_universe)
    if not fixed:
        raise RuntimeError(f"No usable countries for aggregate {z_name}")
    target_months = sorted(months if months is not None else z_scores["year_month"].unique())
    if not target_months:
        raise RuntimeError(f"No months supplied for aggregate {z_name}")
    rows: List[dict] = []
    for ym in target_months:
        month_scores = z_scores[(z_scores["year_month"] == ym) & (z_scores["country"].isin(fixed))]
        score_map = month_scores.set_index("country")["z_egpu"].to_dict()
        snapshot, gdp_source_ym, gdp_mode = gdp_snapshot(gdp_long, ym, gdp_policy)
        snapshot = snapshot[snapshot["country"].isin(fixed)].copy()
        snapshot["z_egpu"] = snapshot["country"].map(score_map).fillna(0.0)
        snapshot = snapshot[snapshot["gdp_ppp_musd"] > 0]
        gdp_sum = float(snapshot["gdp_ppp_musd"].sum())
        if not math.isfinite(gdp_sum) or gdp_sum <= 0:
            raise RuntimeError(f"No positive GDP coverage for {z_name} in {ym}")
        weighted_z = float((snapshot["z_egpu"] * snapshot["gdp_ppp_musd"] / gdp_sum).sum())
        rows.append({
            "year_month": ym,
            "date": pd.Period(ym, freq="M").to_timestamp(),
            z_name: weighted_z,
            "n_countries": int(snapshot["country"].nunique()),
            "n_countries_with_egpu": int(month_scores["country"].nunique()),
            "gdp_covered_musd": gdp_sum,
            "gdp_source_ym": gdp_source_ym,
            "gdp_mode": gdp_mode,
        })
    aggregate = pd.DataFrame(rows).sort_values("date")
    if scale_mean is None or scale_std is None:
        baseline = aggregate[aggregate["year_month"].map(lambda ym: in_window(ym, BASELINE_START, BASELINE_END))]
        mean = float(baseline[z_name].mean())
        std = float(baseline[z_name].std(ddof=0))
    else:
        mean = float(scale_mean)
        std = float(scale_std)
    if not math.isfinite(std) or std <= 0:
        raise RuntimeError(f"Invalid baseline standard deviation for {z_name}")
    aggregate[index_name] = 100.0 + 10.0 * ((aggregate[z_name] - mean) / std)
    config = {
        "universe": fixed,
        "z_name": z_name,
        "index_name": index_name,
        "scale_mean": mean,
        "scale_std": std,
    }
    return aggregate, config


def save_plot(frame: pd.DataFrame, value_col: str, title: str, ylabel: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    plot_frame = frame.dropna(subset=[value_col]).copy()
    if plot_frame.empty:
        return
    plt.figure(figsize=(10, 4.5))
    plt.plot(pd.to_datetime(plot_frame["date"]), plot_frame[value_col], linewidth=1.6)
    plt.axhline(100.0, color="gray", linewidth=0.8, linestyle="--")
    plt.title(title)
    plt.xlabel("Date")
    plt.ylabel(ylabel)
    plt.grid(alpha=0.25)
    plt.tight_layout()
    temp = path.with_name(path.stem + ".tmp" + path.suffix)
    plt.savefig(temp, dpi=150)
    plt.close()
    temp.replace(path)


def build_aggregates(country_long: pd.DataFrame, gdp_policy: str, mode: str) -> dict:
    gdp_long = read_gdp_long()
    if mode == "baseline":
        z_scores, stats = country_z_scores(country_long)
        aggregate_params = {
            "baseline_start_ym": BASELINE_START,
            "baseline_end_ym": BASELINE_END,
            "methodology_fingerprint": methodology_fingerprint(),
            "country_z_params": {
                row.country: {"egpu_mean": float(row.egpu_mean), "egpu_std": float(row.egpu_std)}
                for row in stats.itertuples(index=False)
            },
            "aggregates": {},
        }
        target_months = month_range(BASELINE_START, BASELINE_END)
    else:
        if not AGGREGATE_PARAMS_JSON.exists():
            raise FileNotFoundError(
                f"Frozen aggregate parameters not found: {AGGREGATE_PARAMS_JSON}. Run baseline mode first."
            )
        aggregate_params = json.loads(AGGREGATE_PARAMS_JSON.read_text(encoding="utf-8"))
        if aggregate_params.get("baseline_end_ym") != BASELINE_END:
            raise RuntimeError("Frozen aggregate baseline end does not match the configured baseline")
        z_scores, stats = country_z_scores(country_long, aggregate_params["country_z_params"])
        target_months = sorted(
            ym for ym in z_scores["year_month"].unique()
            if pd.Period(ym, freq="M") > pd.Period(BASELINE_END, freq="M")
        )
        if not target_months:
            raise RuntimeError("No post-baseline country EgPU months are available for aggregation")

    BASELINE_AGGREGATES_DIR.mkdir(parents=True, exist_ok=True)
    produced: Dict[str, str] = {}
    for region_key, universe in REGION_COUNTRIES.items():
        frozen = aggregate_params["aggregates"].get(region_key) if mode == "update" else None
        calculated, config = aggregate_group(
            z_scores,
            gdp_long,
            universe,
            "region_z",
            "region_index",
            gdp_policy,
            fixed_universe=frozen["universe"] if frozen else None,
            scale_mean=frozen["scale_mean"] if frozen else None,
            scale_std=frozen["scale_std"] if frozen else None,
            months=target_months,
        )
        baseline_cache = BASELINE_AGGREGATES_DIR / f"{region_key}_egpu_aggregate_baseline.csv"
        if mode == "baseline":
            aggregate = calculated
            aggregate_params["aggregates"][region_key] = config
            atomic_csv(aggregate, baseline_cache)
        else:
            if not baseline_cache.exists():
                raise FileNotFoundError(f"Frozen regional history not found: {baseline_cache}")
            trusted = pd.read_csv(baseline_cache, dtype={"year_month": str}, parse_dates=["date"])
            aggregate = merge_keyed(trusted, calculated, ["year_month"])
        csv_path = REGIONAL_DIR / f"{region_key}_egpu_aggregate_long.csv"
        png_path = REGIONAL_DIR / f"{region_key}_egpu_aggregate_plot.png"
        atomic_csv(aggregate, csv_path)
        save_plot(
            aggregate, "region_index", f"{REGION_LABELS[region_key]} Aggregate EgPU (GDP-weighted)",
            f"{REGION_LABELS[region_key]} EgPU Index", png_path,
        )
        produced[region_key] = str(csv_path)

    africa_universe = sorted(stats["country"].unique())
    frozen = aggregate_params["aggregates"].get("africa") if mode == "update" else None
    calculated, config = aggregate_group(
        z_scores,
        gdp_long,
        africa_universe,
        "africa_z",
        "africa_index",
        gdp_policy,
        fixed_universe=frozen["universe"] if frozen else None,
        scale_mean=frozen["scale_mean"] if frozen else None,
        scale_std=frozen["scale_std"] if frozen else None,
        months=target_months,
    )
    africa_baseline_cache = BASELINE_AGGREGATES_DIR / "africa_egpu_aggregate_baseline.csv"
    if mode == "baseline":
        africa = calculated
        aggregate_params["aggregates"]["africa"] = config
        atomic_csv(africa, africa_baseline_cache)
        atomic_json(aggregate_params, AGGREGATE_PARAMS_JSON)
    else:
        if not africa_baseline_cache.exists():
            raise FileNotFoundError(f"Frozen Africa history not found: {africa_baseline_cache}")
        trusted = pd.read_csv(africa_baseline_cache, dtype={"year_month": str}, parse_dates=["date"])
        africa = merge_keyed(trusted, calculated, ["year_month"])
    africa_csv = REGIONAL_DIR / "africa_egpu_aggregate_long.csv"
    africa_png = REGIONAL_DIR / "africa_egpu_aggregate_plot.png"
    atomic_csv(africa, africa_csv)
    save_plot(africa, "africa_index", "Africa Aggregate EgPU (GDP-weighted)", "Africa EgPU Index", africa_png)
    produced["africa"] = str(africa_csv)
    return produced


def merge_keyed(existing: pd.DataFrame, new: pd.DataFrame, keys: Sequence[str]) -> pd.DataFrame:
    if existing.empty:
        return new.copy()
    combined = pd.concat([existing, new], ignore_index=True)
    return combined.drop_duplicates(list(keys), keep="last").sort_values(list(keys)).reset_index(drop=True)


def load_existing(path: Path, columns: Sequence[str], dtypes: Optional[dict] = None) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=list(columns))
    frame = pd.read_csv(path, dtype=dtypes)
    missing = set(columns) - set(frame.columns)
    if missing:
        raise ValueError(f"{path} is missing columns: {sorted(missing)}")
    return frame[list(columns)].copy()


def validate_inputs(terms: dict, selected: Sequence[str], start_ym: str, end_ym: str) -> dict:
    source_countries = {
        path.name for path in ARTICLE_ROOT.iterdir()
        if path.is_dir() and not path.name.startswith("_") and path.name != "old_outputs"
    }
    term_countries = set(terms)
    selected_set = set(selected)
    return {
        "source_countries_without_egpu_terms": sorted(source_countries - term_countries),
        "egpu_term_countries_without_source_folder": sorted(term_countries - source_countries),
        "selected_countries": sorted(selected_set),
        "window": {"start": start_ym, "end": end_ym},
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute EgPU from the EPU-selected article folders without scraping."
    )
    parser.add_argument("mode", choices=["baseline", "auto", "month", "update", "validate"])
    parser.add_argument("target_month", help="Last month to compute (YYYY-MM or YYYY-MM-DD)")
    parser.add_argument(
        "--countries", default="ALL",
        help="ALL (default) or comma-separated exact source country folder names",
    )
    parser.add_argument(
        "--gdp-policy", choices=["current", "carry-forward"], default="carry-forward",
        help="Use exact-month GDP or carry forward the last available GDP snapshot",
    )
    parser.add_argument(
        "--reuse-panel", action="store_true",
        help="Baseline only: reuse the saved monthly site panel instead of rescanning article files",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    target_month = normalize_ym(args.target_month)
    if args.mode == "baseline" and target_month != BASELINE_END:
        raise ValueError(
            f"Baseline mode is locked to {BASELINE_START} through {BASELINE_END}; "
            f"use target month {BASELINE_END}."
        )
    if args.mode in {"update", "auto", "month"} and pd.Period(target_month, freq="M") <= pd.Period(BASELINE_END, freq="M"):
        raise ValueError(f"Update target must be after the frozen baseline end {BASELINE_END}")

    terms = load_terms(TERMS_JSON)
    current_methodology = methodology_fingerprint()
    requested_all_countries = args.countries.strip().upper() == "ALL"
    if requested_all_countries:
        selected = sorted(terms)
    else:
        requested = [value.strip() for value in args.countries.split(",") if value.strip()]
        unknown = sorted(set(requested) - set(terms))
        if unknown:
            raise ValueError(f"Countries not found in EgPU dictionary after aliasing: {unknown}")
        selected = sorted(set(requested))

    params_path = CACHE_DIR / "baseline_params.json"
    baseline_long_path = CACHE_DIR / "baseline_egpu_long.csv"
    mode = args.mode
    if mode in {"auto", "month"}:
        cache_is_complete = False
        if params_path.exists() and baseline_long_path.exists() and AGGREGATE_PARAMS_JSON.exists():
            cached_params = json.loads(params_path.read_text(encoding="utf-8"))
            cached_aggregates = json.loads(AGGREGATE_PARAMS_JSON.read_text(encoding="utf-8"))
            cached_baseline = pd.read_csv(baseline_long_path, usecols=["country"])
            cached_countries = set(cached_baseline["country"].dropna().astype(str))
            cache_is_complete = (
                bool(cached_params)
                and set(cached_params) == cached_countries
                and cached_countries <= set(terms)
                and cached_aggregates.get("baseline_end_ym") == BASELINE_END
                and cached_aggregates.get("methodology_fingerprint") == current_methodology
                and set(cached_aggregates.get("aggregates", {})) == set(REGION_COUNTRIES) | {"africa"}
            )
        if not cache_is_complete:
            raise RuntimeError(
                "The frozen EgPU baseline is missing or incomplete. First run: "
                f"python -X utf8 monthly_egpu_pipeline.py baseline {BASELINE_END}"
            )
        mode = "update"

    if mode == "update":
        if not AGGREGATE_PARAMS_JSON.exists():
            raise RuntimeError(
                f"The frozen EgPU baseline is missing. First run baseline {BASELINE_END}."
            )
        frozen_meta = json.loads(AGGREGATE_PARAMS_JSON.read_text(encoding="utf-8"))
        if frozen_meta.get("methodology_fingerprint") != current_methodology:
            raise RuntimeError(
                "The frozen EgPU baseline belongs to an older matcher/dictionary. "
                f"Rebuild it with: python -X utf8 monthly_egpu_pipeline.py baseline {BASELINE_END}"
            )

    if mode == "baseline" and not requested_all_countries:
        raise ValueError(
            "Baseline mode must use all 54 countries. Remove --countries and rerun. "
            "Country subsets are allowed only for update mode."
        )
    if args.reuse_panel and args.mode != "baseline":
        raise ValueError("--reuse-panel can be used only with baseline mode")

    if mode == "baseline":
        selected = sorted(terms)

    if mode == "baseline" or args.mode == "validate":
        scan_start = BASELINE_START
    elif args.mode == "month":
        scan_start = target_month
    else:
        scan_start = str(pd.Period(BASELINE_END, freq="M") + 1)
    log(
        f"Effective mode={mode}; countries={len(selected)}; "
        f"window={scan_start} through {target_month}"
    )
    diagnostics = validate_inputs(terms, selected, scan_start, target_month)
    log(json.dumps(diagnostics, indent=2))
    if args.mode == "validate":
        log("Validation complete. No article data was read and no outputs were changed.")
        return

    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    REGIONAL_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    panel_path = OUTPUT_ROOT / "egpu_monthly_panel_by_site.csv"
    panel_manifest_path = OUTPUT_ROOT / "egpu_monthly_panel_by_site.manifest.json"
    if args.reuse_panel:
        if not panel_path.exists():
            raise FileNotFoundError(f"Saved EgPU panel not found: {panel_path}")
        old_manifest = (
            panel_manifest_path
            if panel_manifest_path.exists()
            else OUTPUT_ROOT / "latest_run_manifest.json"
        )
        previous = json.loads(old_manifest.read_text(encoding="utf-8")) if old_manifest.exists() else {}
        if previous.get("methodology_fingerprint") != current_methodology:
            raise RuntimeError(
                "The saved EgPU panel was produced by a different matcher/dictionary. "
                "Do not use --reuse-panel; rebuild the baseline from article files."
            )
        panel = load_existing(panel_path, PANEL_COLUMNS, {"year_month": str})
        panel = panel[
            panel["country"].isin(selected)
            & panel["year_month"].map(lambda ym: in_window(ym, scan_start, target_month))
        ].copy()
        skipped = pd.DataFrame(columns=["country", "site", "year_month", "file", "reason"])
        duplicates_removed = 0
        log(f"Reusing saved EgPU panel: {panel_path} ({len(panel):,} rows)")
    else:
        panel, skipped, duplicates_removed = build_panel(terms, selected, scan_start, target_month)
    if panel.empty:
        raise RuntimeError(f"No usable EPU-selected article files found for {scan_start} through {target_month}")

    if mode == "baseline":
        observed_countries = set(panel["country"].unique())
        missing_countries = sorted(set(selected) - observed_countries)
        if missing_countries:
            raise RuntimeError(
                "Baseline run stopped before changing outputs because these countries had no usable "
                f"article files in the requested window: {missing_countries}"
            )

    existing_panel = load_existing(panel_path, PANEL_COLUMNS, {"year_month": str})
    if mode == "baseline":
        combined_panel = panel
    else:
        combined_panel = merge_keyed(existing_panel, panel, ["country", "site", "year_month"])
    if mode == "baseline":
        atomic_csv(combined_panel, panel_path)
        atomic_json({
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "methodology_fingerprint": current_methodology,
            "scan_start": scan_start,
            "scan_end": target_month,
            "countries": selected,
            "panel_rows": int(len(combined_panel)),
        }, panel_manifest_path)
        log(f"Saved completed EgPU scan panel before calibration: {panel_path}")
    else:
        atomic_csv(combined_panel, panel_path)

    if not skipped.empty:
        skipped_path = OUTPUT_ROOT / "skipped_article_files.csv"
        old_skipped = pd.read_csv(skipped_path) if skipped_path.exists() else pd.DataFrame(columns=skipped.columns)
        atomic_csv(merge_keyed(old_skipped, skipped, ["file", "reason"]), skipped_path)

    if mode == "baseline":
        baseline_panel = panel[panel["year_month"].map(lambda ym: in_window(ym, BASELINE_START, BASELINE_END))]
        params, baseline_long, baseline_drops = build_baseline(baseline_panel)
        missing_params = sorted(set(selected) - set(params))
        status_rows = []
        for country in selected:
            country_panel = baseline_panel[baseline_panel["country"] == country]
            reasons = baseline_drops.loc[
                (baseline_drops["country"] == country) & (baseline_drops["site"] == "*ALL*"), "reason"
            ].astype(str).tolist()
            status_rows.append({
                "country": country,
                "status": "calibrated" if country in params else "uncalibrated",
                "reason": "; ".join(reasons),
                "panel_rows": int(len(country_panel)),
                "sites": int(country_panel["site"].nunique()),
                "total_articles": int(country_panel["total_articles"].sum()),
                "egpu_articles": int(country_panel["egpu_count"].sum()),
            })
        atomic_csv(pd.DataFrame(status_rows), OUTPUT_ROOT / "baseline_country_status.csv")
        if missing_params:
            raise RuntimeError(
                "Baseline run stopped before replacing the frozen EgPU history because calibration "
                f"failed for these countries: {missing_params}. "
                f"Review {OUTPUT_ROOT / 'baseline_country_status.csv'}."
            )
        atomic_json(params, params_path)
        atomic_csv(baseline_long, baseline_long_path)
        atomic_csv(baseline_drops, OUTPUT_ROOT / "dropped_sites.csv")
        update_drops = pd.DataFrame(columns=["country", "site", "reason"])
        country_long = baseline_long.copy()
    else:
        if not params_path.exists() or not baseline_long_path.exists() or not AGGREGATE_PARAMS_JSON.exists():
            raise FileNotFoundError(
                f"Complete EgPU baseline cache is missing; run baseline mode through {BASELINE_END} first"
            )
        params = json.loads(params_path.read_text(encoding="utf-8"))
        baseline_long = pd.read_csv(baseline_long_path, dtype={"year_month": str})
        baseline_countries = set(baseline_long["country"].dropna().astype(str))
        if not params or set(params) != baseline_countries or not baseline_countries <= set(terms):
            raise RuntimeError("Frozen country baseline cache is inconsistent; rebuild the baseline")
        update_long, update_drops = compute_updates(panel, params)
        country_output_path = OUTPUT_ROOT / "egpu_index_long.csv"
        prior = load_existing(country_output_path, COUNTRY_COLUMNS, {"year_month": str})
        prior_updates = prior[prior["year_month"] > BASELINE_END].copy() if not prior.empty else prior
        base = merge_keyed(baseline_long, prior_updates, ["country", "year_month"])
        country_long = merge_keyed(base, update_long, ["country", "year_month"])

    if not update_drops.empty:
        atomic_csv(update_drops, OUTPUT_ROOT / "update_dropped_sites.csv")
    if country_long.empty:
        raise RuntimeError("EgPU computation produced no country index rows")

    country_long = country_long.sort_values(["country", "year_month"]).reset_index(drop=True)
    atomic_csv(country_long, OUTPUT_ROOT / "egpu_index_long.csv")
    wide = country_long.pivot_table(index="year_month", columns="country", values="EgPU", aggfunc="mean")
    wide = wide.sort_index().reindex(columns=sorted(terms)).reset_index()
    atomic_csv(wide, OUTPUT_ROOT / "egpu_index_wide.csv")

    aggregate_paths = build_aggregates(country_long, args.gdp_policy, mode)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    manifest = {
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "requested_mode": args.mode,
        "effective_mode": mode,
        "baseline": {"start": BASELINE_START, "end": BASELINE_END},
        "scan_window": {"start": scan_start, "end": target_month},
        "article_source": str(ARTICLE_ROOT),
        "terms_source": str(TERMS_JSON),
        "methodology_fingerprint": current_methodology,
        "gdp_source": str(GDP_WIDE_CSV),
        "output_root": str(OUTPUT_ROOT),
        "gdp_policy": args.gdp_policy,
        "countries_requested": selected,
        "panel_rows_scanned": int(len(panel)),
        "country_index_rows_total": int(len(country_long)),
        "duplicates_removed": int(duplicates_removed),
        "reused_existing_panel": bool(args.reuse_panel),
        "diagnostics": diagnostics,
        "aggregate_outputs": aggregate_paths,
    }
    atomic_json(manifest, REPORTS_DIR / f"egpu_run_{timestamp}.json")
    atomic_json(manifest, OUTPUT_ROOT / "latest_run_manifest.json")
    log(f"EgPU pipeline complete. Outputs: {OUTPUT_ROOT}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log(f"EgPU pipeline failed: {type(exc).__name__}: {exc!r}")
        traceback.print_exc()
        sys.exit(1)
