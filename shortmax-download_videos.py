#!/usr/bin/env python3
"""
Download ShortMax episode videos by shortPlayId.

Folder output format:
    <output>/<shortPlayId>/
    video_info.json         # raw API response
    episodes.json           # normalized episode list (best effort)
    episodes/
            episode_001/
                1080p/episode_001_1080p.mp4
                720p/episode_001_720p.mp4
                subtitles/episode_001.vtt
            episode_002/
                ...

Examples:
    python shortmax-download_videos.py --short-play-id 17329
    python shortmax-download_videos.py --csv shortplay_ids.csv --output ./downloads
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

DETAIL_API_URL = "https://api.sansekai.my.id/api/shortmax/detail?shortPlayId={short_play_id}"
ALLEPISODE_API_URL = "https://api.sansekai.my.id/api/shortmax/allepisode?shortPlayId={short_play_id}"
SHORTMAX_REFERER = "https://www.shorttv.live/"
SHORTMAX_ORIGIN = "https://www.shorttv.live"
CHUNK_SIZE = 1024 * 1024

SUCCESS_VALUES = {"1", "true", "success", "ok", "done", "yes"}
FAIL_VALUES = {"0", "false", "failed", "error", "no"}


def fetch_json_from_url(url: str, timeout: int = 30, headers: Optional[Dict[str, str]] = None) -> Any:
    default_headers = {
        "accept": "*/*",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "accept-language": "en-US,en;q=0.9",
        "cache-control": "no-cache",
    }
    if headers:
        default_headers.update(headers)

    req = Request(url, method="GET", headers=default_headers)
    print(f"[INFO] Fetching JSON from URL: {url} with timeout {timeout}s")
    with urlopen(req, timeout=timeout) as resp:
        data = resp.read().decode("utf-8")
    return json.loads(data)


def fetch_detail_json(short_play_id: str, timeout: int = 30) -> Any:
    url = DETAIL_API_URL.format(short_play_id=short_play_id)
    return fetch_json_from_url(
        url,
        timeout=timeout,
        headers={
            "referer": SHORTMAX_REFERER,
            "origin": SHORTMAX_ORIGIN,
        },
    )


def fetch_allepisode_json(short_play_id: str, timeout: int = 30) -> Any:
    url = ALLEPISODE_API_URL.format(short_play_id=short_play_id)
    return fetch_json_from_url(
        url,
        timeout=timeout,
        headers={
            "referer": SHORTMAX_REFERER,
            "origin": SHORTMAX_ORIGIN,
        },
    )


def _find_column(fieldnames: List[str], candidates: Tuple[str, ...]) -> Optional[str]:
    field_map = {name.lower(): name for name in fieldnames}
    for candidate in candidates:
        if candidate in field_map:
            return field_map[candidate]
    return None


def unwrap_shortmax_data(payload: Any) -> Any:
    if isinstance(payload, dict) and payload.get("status") == "ok" and "data" in payload:
        return payload.get("data")
    return payload


def read_rows_from_csv(csv_path: Path) -> Tuple[List[Dict[str, str]], List[str]]:
    rows: List[Dict[str, str]] = []
    fieldnames: List[str] = []
    with csv_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames:
            fieldnames = list(reader.fieldnames)
            for row in reader:
                normalized: Dict[str, str] = {}
                for name in fieldnames:
                    normalized[name] = (row.get(name) or "").strip()
                rows.append(normalized)
            return rows, fieldnames

    # Fallback: one ID per line if CSV has no headers. Build expected columns.
    fieldnames = ["shortPlayId", "check", "success_failed"]
    with csv_path.open("r", encoding="utf-8") as f:
        for line in f:
            value = line.strip().strip(",")
            if value and not value.lower().startswith("book"):
                rows.append({"shortPlayId": value, "check": "", "success_failed": ""})

    return rows, fieldnames


def write_rows_to_csv(csv_path: Path, rows: List[Dict[str, str]], fieldnames: List[str]) -> None:
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: (row.get(name) or "") for name in fieldnames})


def _status_to_text(value: str) -> str:
    text = (value or "").strip().lower()
    if text in SUCCESS_VALUES:
        return "success"
    if text in FAIL_VALUES:
        return "failed"
    return ""


def should_process_csv_row(row: Dict[str, str], success_column: str) -> bool:
    # Process rows not yet successful so failed rows are retried on next run.
    status = _status_to_text(row.get(success_column, ""))
    return status != "success"


def find_candidate_episode_lists(payload: Any) -> List[List[Dict[str, Any]]]:
    payload = unwrap_shortmax_data(payload)
    candidates: List[List[Dict[str, Any]]] = []

    def has_episode_shape(item: Dict[str, Any]) -> bool:
        # Episode items usually have episode-ish keys or contain playable URLs.
        key_hints = {
            "episode",
            "episodeno",
            "episode_no",
            "episodeid",
            "ep",
            "sort",
            "index",
            "title",
            "name",
            "video",
            "videourl",
            "playurl",
            "url",
        }
        lowered = {str(k).lower() for k in item.keys()}
        if lowered & key_hints:
            return True
        return extract_video_url(item) is not None

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            if node and all(isinstance(x, dict) for x in node):
                matches = sum(1 for x in node if has_episode_shape(x))
                # Keep only lists where most entries look like episodes.
                if matches > 0 and matches >= max(1, len(node) // 2):
                    candidates.append(node)
            for value in node:
                walk(value)

    walk(payload)
    return candidates


def _looks_like_video_url(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    text = value.strip().lower()
    if not (text.startswith("http://") or text.startswith("https://")):
        return False
    video_hints = (".mp4", ".m3u8", ".mov", ".webm", "video", "play")
    return any(hint in text for hint in video_hints)


def extract_video_url(item: Dict[str, Any]) -> Optional[str]:
    shortmax_video_urls = collect_quality_video_urls_from_chapter(item)
    if shortmax_video_urls:
        best_quality = max(shortmax_video_urls)
        return shortmax_video_urls[best_quality]

    preferred_keys = [
        "videoUrl",
        "video_url",
        "video",
        "playUrl",
        "play_url",
        "url",
        "episodeUrl",
        "episode_url",
        "src",
        "source",
    ]

    for key in preferred_keys:
        value = item.get(key)
        if _looks_like_video_url(value):
            return str(value)

    # Fallback recursive scan for any URL-like field.
    stack: List[Any] = [item]
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            for value in node.values():
                if _looks_like_video_url(value):
                    return str(value)
                if isinstance(value, (dict, list)):
                    stack.append(value)
        elif isinstance(node, list):
            stack.extend(node)

    return None


def collect_quality_video_urls_from_chapter(chapter: Dict[str, Any]) -> Dict[int, str]:
    video_url_map = chapter.get("videoUrl")
    if isinstance(video_url_map, dict):
        quality_urls: Dict[int, str] = {}
        for key, value in video_url_map.items():
            if not isinstance(value, str) or not value.startswith(("http://", "https://")):
                continue

            match = re.search(r"(\d+)", str(key))
            if not match:
                continue
            quality_urls[int(match.group(1))] = value

        if quality_urls:
            return quality_urls

    cdn_list = chapter.get("cdnList")
    if not isinstance(cdn_list, list):
        return {}

    candidates_by_quality: Dict[int, Tuple[int, int, str]] = {}
    for cdn in cdn_list:
        if not isinstance(cdn, dict):
            continue
        cdn_default = 1 if int(cdn.get("isDefault", 0) or 0) == 1 else 0
        path_list = cdn.get("videoPathList")
        if not isinstance(path_list, list):
            continue

        for item in path_list:
            if not isinstance(item, dict):
                continue
            video_path = item.get("videoPath")
            if not isinstance(video_path, str) or not video_path.startswith(("http://", "https://")):
                continue

            quality = int(item.get("quality", 0) or 0)
            if quality <= 0:
                continue
            item_default = 1 if int(item.get("isDefault", 0) or 0) == 1 else 0

            # For each quality, prefer default CDN + default stream.
            score = (cdn_default, item_default)
            current = candidates_by_quality.get(quality)
            if current is None or score > (current[0], current[1]):
                candidates_by_quality[quality] = (score[0], score[1], video_path)

    return {quality: value[2] for quality, value in candidates_by_quality.items()}


def pick_video_url_from_chapter(chapter: Dict[str, Any], preferred_quality: int = 720) -> Optional[str]:
    quality_urls = collect_quality_video_urls_from_chapter(chapter)
    if not quality_urls:
        return None

    best_quality = min(quality_urls.keys(), key=lambda q: abs(q - preferred_quality))
    return quality_urls[best_quality]


def extract_subtitle_url(item: Dict[str, Any]) -> Optional[str]:
    subtitle_keys = [
        "spriteSnapshotUrl",
        "subtitleUrl",
        "subtitle_url",
        "vttUrl",
        "vtt_url",
        "subtitle",
    ]
    for key in subtitle_keys:
        value = item.get(key)
        if isinstance(value, str) and value.startswith(("http://", "https://")):
            lower = value.lower()
            if lower.endswith(".vtt") or "vtt" in lower or "subtitle" in lower or "caption" in lower:
                return value
    return None


def extract_episode_number(item: Dict[str, Any], default_index: int) -> int:
    number_keys = ["episode", "episodeNo", "episodeNumber", "episode_num", "ep", "sort", "index", "num"]
    for key in number_keys:
        value = item.get(key)
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.strip().isdigit():
            return int(value.strip())

    text_keys = ["title", "name"]
    for key in text_keys:
        value = item.get(key)
        if isinstance(value, str):
            match = re.search(r"(\d+)", value)
            if match:
                return int(match.group(1))

    return default_index


def normalize_episodes(payload: Any, preferred_quality: int = 720) -> List[Dict[str, Any]]:
    payload = unwrap_shortmax_data(payload)

    if isinstance(payload, dict):
        episodes_payload = payload.get("episodes")
        if isinstance(episodes_payload, list) and all(isinstance(x, dict) for x in episodes_payload):
            normalized: List[Dict[str, Any]] = []
            for i, item in enumerate(episodes_payload, start=1):
                ep_no = extract_episode_number(item, default_index=i)
                quality_urls = collect_quality_video_urls_from_chapter(item)
                normalized.append(
                    {
                        "episode": ep_no,
                        "quality_urls": quality_urls,
                        "video_url": pick_video_url_from_chapter(item, preferred_quality=preferred_quality),
                        "subtitle_url": extract_subtitle_url(item),
                        "raw": item,
                    }
                )

            normalized.sort(key=lambda x: x["episode"])
            return normalized

    # Preferred path: explicit episode list payload shape from API.
    if isinstance(payload, list) and payload and all(isinstance(x, dict) for x in payload):
        if any("chapterId" in x or "cdnList" in x for x in payload):
            normalized: List[Dict[str, Any]] = []
            for i, item in enumerate(payload, start=1):
                chapter_index = item.get("chapterIndex")
                if isinstance(chapter_index, int):
                    ep_no = chapter_index + 1
                else:
                    ep_no = extract_episode_number(item, default_index=i)

                normalized.append(
                    {
                        "episode": ep_no,
                        "quality_urls": collect_quality_video_urls_from_chapter(item),
                        "video_url": pick_video_url_from_chapter(item, preferred_quality=preferred_quality),
                        "subtitle_url": extract_subtitle_url(item),
                        "raw": item,
                    }
                )

            normalized.sort(key=lambda x: x["episode"])
            return normalized

    episode_lists = find_candidate_episode_lists(payload)

    if not episode_lists:
        return []

    # Prefer the largest list, usually the full episode collection.
    episode_items = max(episode_lists, key=len)

    normalized: List[Dict[str, Any]] = []
    for i, item in enumerate(episode_items, start=1):
        url = extract_video_url(item)
        ep_no = extract_episode_number(item, default_index=i)
        normalized.append(
            {
                "episode": ep_no,
                "video_url": url,
                "quality_urls": {},
                "subtitle_url": extract_subtitle_url(item),
                "raw": item,
            }
        )

    # Keep stable order by episode number, then original position.
    normalized.sort(key=lambda x: (x["episode"], episode_items.index(x["raw"])))
    return normalized


def file_extension_from_url(url: str) -> str:
    parsed = urlparse(url)
    path = parsed.path.lower()
    for ext in (".mp4", ".m3u8", ".mov", ".webm", ".mkv"):
        if path.endswith(ext):
            return ext
    return ".mp4"


def subtitle_extension_from_url(url: str) -> str:
    parsed = urlparse(url)
    path = parsed.path.lower()
    if path.endswith(".vtt"):
        return ".vtt"
    if path.endswith(".srt"):
        return ".srt"
    return ".vtt"


def download_file(url: str, dest: Path, timeout: int = 60) -> None:
    req = Request(url, method="GET", headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=timeout) as resp, dest.open("wb") as out:
        while True:
            chunk = resp.read(CHUNK_SIZE)
            if not chunk:
                break
            out.write(chunk)


def process_short_play_id(
    short_play_id: str,
    output_dir: Path,
    skip_existing: bool = True,
    retry_count: int = 3,
    retry_sleep_sec: float = 1.0,
    preferred_quality: int = 720,
) -> Tuple[int, int, int]:
    print(f"\n[INFO] Processing shortPlayId={short_play_id}")
    book_dir = output_dir / str(short_play_id)
    episodes_dir = book_dir / "episodes"
    episodes_dir.mkdir(parents=True, exist_ok=True)

    detail_payload: Optional[Any] = None
    detail_error: Optional[str] = None
    try:
        detail_payload = fetch_detail_json(short_play_id)
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as err:
        detail_error = str(err)
        print(f"[WARN] Failed to fetch detail endpoint for shortPlayId {short_play_id}: {err}")

    payload = fetch_allepisode_json(short_play_id)

    if detail_payload is not None:
        detail_json_path = book_dir / "book_detail.json"
        detail_json_path.write_text(json.dumps(detail_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[INFO] Saved detail API response: {detail_json_path}")
    elif detail_error is not None:
        detail_error_path = book_dir / "book_detail_error.json"
        detail_error_path.write_text(
            json.dumps({"short_play_id": short_play_id, "error": detail_error}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"[INFO] Saved detail API error info: {detail_error_path}")

    raw_json_path = book_dir / "video_info.json"
    raw_json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[INFO] Saved raw API response: {raw_json_path}")

    episodes = normalize_episodes(payload, preferred_quality=preferred_quality)
    episodes_json_path = book_dir / "episodes.json"
    episodes_json_path.write_text(json.dumps(episodes, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[INFO] Saved normalized episode info: {episodes_json_path}")

    if not episodes:
        failed_json_path = book_dir / "failed_episodes.json"
        failed_json_path.write_text(
            json.dumps(
                [
                    {
                        "short_play_id": short_play_id,
                        "error": "No episode list found in API response",
                    }
                ],
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        print("[ERROR] No valid episodes found in response")
        print(f"[INFO] Saved failed episode list: {failed_json_path}")
        return 0, 0, 1

    downloaded = 0
    skipped = 0
    failed = 0
    failed_items: List[Dict[str, Any]] = []

    for idx, ep in enumerate(episodes, start=1):
        quality_urls = ep.get("quality_urls")
        if not isinstance(quality_urls, dict):
            quality_urls = {}

        usable_quality_urls: Dict[int, str] = {}
        for quality, url in quality_urls.items():
            try:
                q = int(quality)
            except (TypeError, ValueError):
                continue
            if isinstance(url, str) and url.startswith(("http://", "https://")):
                usable_quality_urls[q] = url

        # Fallback for non-cdn payloads where only a single video URL exists.
        if not usable_quality_urls:
            video_url = ep.get("video_url")
            if isinstance(video_url, str) and video_url.startswith(("http://", "https://")):
                usable_quality_urls[preferred_quality] = video_url

        if not usable_quality_urls:
            print(f"[WARN] Episode {ep.get('episode', idx)} has no video URL, skipping")
            failed += 1
            failed_items.append(
                {
                    "episode": ep.get("episode", idx),
                    "video_url": None,
                    "quality": None,
                    "filename": None,
                    "error": "No video URL found for this episode",
                }
            )
            continue

        ep_no = ep.get("episode", idx)
        episode_dir = episodes_dir / f"episode_{int(ep_no):03d}"
        episode_dir.mkdir(parents=True, exist_ok=True)

        for quality in sorted(usable_quality_urls.keys(), reverse=True):
            video_url = usable_quality_urls[quality]
            ext = file_extension_from_url(video_url)
            quality_dir = episode_dir / f"{quality}p"
            quality_dir.mkdir(parents=True, exist_ok=True)
            filename = f"episode_{int(ep_no):03d}_{quality}p{ext}"
            target = quality_dir / filename

            if skip_existing and target.exists() and target.stat().st_size > 0:
                print(f"[SKIP] Exists: {target}")
                skipped += 1
                continue

            success = False
            last_err: Optional[Exception] = None
            for attempt in range(1, max(1, retry_count) + 1):
                try:
                    print(
                        f"[DOWNLOADING] Episode {ep_no} {quality}p "
                        f"(attempt {attempt}/{retry_count}): {video_url}"
                    )
                    download_file(video_url, target)
                    print(f"[DONE] {target}")
                    downloaded += 1
                    success = True
                    break
                except (HTTPError, URLError, TimeoutError, OSError) as err:
                    last_err = err
                    print(
                        f"[WARN] Download failed for episode {ep_no} {quality}p "
                        f"on attempt {attempt}: {err}"
                    )
                    # Remove partial file so retries start cleanly.
                    if target.exists() and target.stat().st_size == 0:
                        target.unlink(missing_ok=True)
                    if attempt < retry_count:
                        time.sleep(max(0.0, retry_sleep_sec))

            if not success:
                failed += 1
                failed_items.append(
                    {
                        "episode": ep_no,
                        "video_url": video_url,
                        "quality": quality,
                        "filename": str(target.relative_to(book_dir)),
                        "error": str(last_err) if last_err else "unknown error",
                    }
                )
                print(f"[ERROR] Failed download for episode {ep_no} {quality}p after {retry_count} attempts")

        subtitle_url = ep.get("subtitle_url")
        if isinstance(subtitle_url, str) and subtitle_url.startswith(("http://", "https://")):
            subtitle_dir = episode_dir / "subtitles"
            subtitle_dir.mkdir(parents=True, exist_ok=True)
            subtitle_ext = subtitle_extension_from_url(subtitle_url)
            subtitle_name = f"episode_{int(ep_no):03d}{subtitle_ext}"
            subtitle_target = subtitle_dir / subtitle_name

            if skip_existing and subtitle_target.exists() and subtitle_target.stat().st_size > 0:
                print(f"[SKIP] Subtitle exists: {subtitle_target}")
                skipped += 1
            else:
                subtitle_success = False
                subtitle_last_err: Optional[Exception] = None
                for attempt in range(1, max(1, retry_count) + 1):
                    try:
                        print(
                            f"[DOWNLOADING] Episode {ep_no} subtitle "
                            f"(attempt {attempt}/{retry_count}): {subtitle_url}"
                        )
                        download_file(subtitle_url, subtitle_target)
                        print(f"[DONE] {subtitle_target}")
                        downloaded += 1
                        subtitle_success = True
                        break
                    except (HTTPError, URLError, TimeoutError, OSError) as err:
                        subtitle_last_err = err
                        print(f"[WARN] Subtitle download failed for episode {ep_no} on attempt {attempt}: {err}")
                        if subtitle_target.exists() and subtitle_target.stat().st_size == 0:
                            subtitle_target.unlink(missing_ok=True)
                        if attempt < retry_count:
                            time.sleep(max(0.0, retry_sleep_sec))

                if not subtitle_success:
                    failed += 1
                    failed_items.append(
                        {
                            "episode": ep_no,
                            "video_url": subtitle_url,
                            "quality": "subtitle",
                            "filename": str(subtitle_target.relative_to(book_dir)),
                            "error": str(subtitle_last_err) if subtitle_last_err else "unknown error",
                        }
                    )
                    print(f"[ERROR] Failed subtitle download for episode {ep_no} after {retry_count} attempts")

    failed_json_path = book_dir / "failed_episodes.json"
    failed_json_path.write_text(json.dumps(failed_items, ensure_ascii=False, indent=2), encoding="utf-8")
    if failed_items:
        print(f"[INFO] Saved failed episode list: {failed_json_path}")

    return downloaded, skipped, failed


def is_short_play_folder_already_downloaded(short_play_id: str, output_dir: Path) -> bool:
    book_dir = output_dir / str(short_play_id)
    if not book_dir.exists() or not book_dir.is_dir():
        return False

    # Treat as already processed when key output artifacts exist.
    if (book_dir / "episodes").exists():
        return True
    if (book_dir / "video_info.json").exists():
        return True
    if (book_dir / "episodes.json").exists():
        return True
    return False


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download ShortMax episode videos by shortPlayId")
    parser.add_argument(
        "--short-play-id",
        "--book-id",
        dest="short_play_id",
        action="append",
        help="shortPlayId to process. Use multiple times for multiple ids. --book-id is kept as an alias.",
    )
    parser.add_argument(
        "--csv",
        type=Path,
        help="CSV file with IDs. Header can be: shortPlayId, short_play_id, bookId, book_id, or id.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("downloads"),
        help="Output directory (default: ./downloads)",
    )
    parser.add_argument(
        "--no-skip-existing",
        action="store_true",
        help="Re-download files even if they already exist",
    )
    parser.add_argument(
        "--retry-count",
        type=int,
        default=3,
        help="Retry count for each failed episode download (default: 3)",
    )
    parser.add_argument(
        "--retry-sleep",
        type=float,
        default=1.0,
        help="Seconds to wait between retries (default: 1.0)",
    )
    parser.add_argument(
        "--preferred-quality",
        type=int,
        default=720,
        help="Preferred video quality when multiple sources exist (default: 720)",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)

    short_play_ids: Set[str] = set()
    csv_rows: List[Dict[str, str]] = []
    csv_fieldnames: List[str] = []
    short_play_id_col: Optional[str] = None
    check_col: Optional[str] = None
    success_col: Optional[str] = None

    if args.short_play_id:
        for raw in args.short_play_id:
            for item in str(raw).split(","):
                value = item.strip()
                if value:
                    short_play_ids.add(value)

    if args.csv:
        if not args.csv.exists():
            print(f"[ERROR] CSV not found: {args.csv}")
            return 1
        csv_rows, csv_fieldnames = read_rows_from_csv(args.csv)
        if not csv_rows:
            print(f"[ERROR] No rows found in CSV: {args.csv}")
            return 1

        # Ensure expected status columns exist.
        if "check" not in [name.lower() for name in csv_fieldnames]:
            csv_fieldnames.append("check")
        if "success_failed" not in [name.lower() for name in csv_fieldnames] and "success" not in [name.lower() for name in csv_fieldnames]:
            csv_fieldnames.append("success_failed")

        short_play_id_col = _find_column(csv_fieldnames, ("shortplayid", "short_play_id", "bookid", "book_id", "id"))
        check_col = _find_column(csv_fieldnames, ("check",)) or "check"
        success_col = _find_column(csv_fieldnames, ("success_failed", "success")) or "success_failed"

        if short_play_id_col is None:
            print(f"[ERROR] CSV must contain shortPlayId/short_play_id/bookId/book_id/id column: {args.csv}")
            return 1

        for row in csv_rows:
            value = (row.get(short_play_id_col) or "").strip()
            if not value:
                continue
            if should_process_csv_row(row, success_col):
                short_play_ids.add(value)

        if not short_play_ids:
            print(f"[INFO] No pending/failed rows to process in CSV: {args.csv}")
            return 0

    if not short_play_ids:
        print("[ERROR] Provide at least one --short-play-id or --csv")
        return 1

    args.output.mkdir(parents=True, exist_ok=True)
    skip_existing = not args.no_skip_existing
    retry_count = max(1, args.retry_count)
    retry_sleep_sec = max(0.0, args.retry_sleep)
    preferred_quality = max(1, args.preferred_quality)

    total_downloaded = 0
    total_skipped = 0
    total_failed = 0

    for short_play_id in sorted(short_play_ids):
        try:
            if skip_existing and is_short_play_folder_already_downloaded(short_play_id, args.output):
                print(f"\n[SKIP] shortPlayId={short_play_id} already has output folder, skipping")
                if args.csv and csv_rows and short_play_id_col and check_col and success_col:
                    for row in csv_rows:
                        if (row.get(short_play_id_col) or "").strip() == short_play_id:
                            row[check_col] = "success"
                            row[success_col] = "success"
                    write_rows_to_csv(args.csv, csv_rows, csv_fieldnames)
                continue

            print(f"\n[PROCESSING] shortPlayId={short_play_id}")
            downloaded, skipped, failed = process_short_play_id(
                short_play_id,
                args.output,
                skip_existing=skip_existing,
                retry_count=retry_count,
                retry_sleep_sec=retry_sleep_sec,
                preferred_quality=preferred_quality,
            )
            total_downloaded += downloaded
            total_skipped += skipped
            total_failed += failed

            if args.csv and csv_rows and short_play_id_col and check_col and success_col:
                for row in csv_rows:
                    if (row.get(short_play_id_col) or "").strip() == short_play_id:
                        row[check_col] = "success"
                        row[success_col] = "success" if failed == 0 else "failed"
                write_rows_to_csv(args.csv, csv_rows, csv_fieldnames)
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as err:
            print(f"[ERROR] Failed processing shortPlayId {short_play_id}: {err}")
            total_failed += 1
            if args.csv and csv_rows and short_play_id_col and check_col and success_col:
                for row in csv_rows:
                    if (row.get(short_play_id_col) or "").strip() == short_play_id:
                        row[check_col] = "failed"
                        row[success_col] = "failed"
                write_rows_to_csv(args.csv, csv_rows, csv_fieldnames)

    print("\n[SUMMARY]")
    print(f"Short play IDs: {len(short_play_ids)}")
    print(f"Downloaded files: {total_downloaded}")
    print(f"Skipped/missing: {total_skipped}")
    print(f"Failed downloads: {total_failed}")
    print(f"Output directory: {args.output.resolve()}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
