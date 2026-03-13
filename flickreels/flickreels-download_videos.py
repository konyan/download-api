#!/usr/bin/env python3
"""
Download FlickReels episode videos by ID.

Folder output format:
  <output>/<id>/
    video_info.json         # raw API response
    drama_info.json         # drama metadata extracted from API response
    cover/drama_cover.jpg   # drama-level cover image
    episodes.json           # normalized episode list (best effort)
    episodes/
            episode_001/
                1080p/episode_001_1080p.mp4
                720p/episode_001_720p.mp4
                chapter_cover.jpg
                subtitles/episode_001.vtt
            episode_002/
                ...

Examples:
  python download_videos.py --book-id 42000007194
  python download_videos.py --csv book_ids.csv --output ./downloads
"""

from __future__ import annotations

import argparse
import csv
from http.client import HTTPException
import json
import re
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

DETAIL_AND_EPISODES_API_URL = "https://api.sansekai.my.id/api/flickreels/detailAndAllEpisode?id={book_id}"
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


def fetch_detail_and_episodes_json(book_id: str, timeout: int = 30) -> Any:
    url = DETAIL_AND_EPISODES_API_URL.format(book_id=book_id)
    return fetch_json_from_url(
        url,
        timeout=timeout,
        headers={
            "referer": "https://www.flickreels.net/",
            "origin": "https://www.flickreels.net",
        },
    )


def _find_column(fieldnames: List[str], candidates: Tuple[str, ...]) -> Optional[str]:
    field_map = {name.lower(): name for name in fieldnames}
    for candidate in candidates:
        if candidate in field_map:
            return field_map[candidate]
    return None


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
    fieldnames = ["bookId", "check", "success_failed"]
    with csv_path.open("r", encoding="utf-8") as f:
        for line in f:
            value = line.strip().strip(",")
            if value and not value.lower().startswith("book"):
                rows.append({"bookId": value, "check": "", "success_failed": ""})

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
    raw = item.get("raw")
    if isinstance(raw, dict):
        raw_chapter_num = raw.get("chapter_num")
        if isinstance(raw_chapter_num, int):
            return raw_chapter_num
        if isinstance(raw_chapter_num, str) and raw_chapter_num.strip().isdigit():
            return int(raw_chapter_num.strip())

    number_keys = ["episode", "episodeNo", "episode_num", "ep", "sort", "num"]
    for key in number_keys:
        value = item.get(key)
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.strip().isdigit():
            return int(value.strip())

    index_value = item.get("index")
    if isinstance(index_value, int):
        return index_value + 1
    if isinstance(index_value, str) and index_value.strip().isdigit():
        return int(index_value.strip()) + 1

    text_keys = ["title", "name"]
    for key in text_keys:
        value = item.get(key)
        if isinstance(value, str):
            match = re.search(r"(\d+)", value)
            if match:
                return int(match.group(1))

    return default_index


def normalize_episodes(payload: Any, preferred_quality: int = 720) -> List[Dict[str, Any]]:
    # FlickReels shape: {"drama": {...}, "episodes": [...]}
    if isinstance(payload, dict):
        payload_episodes = payload.get("episodes")
        if isinstance(payload_episodes, list) and payload_episodes and all(isinstance(x, dict) for x in payload_episodes):
            normalized: List[Dict[str, Any]] = []
            for i, item in enumerate(payload_episodes, start=1):
                raw = item.get("raw") if isinstance(item.get("raw"), dict) else {}
                chapter_cover_url: Optional[str] = None
                raw_cover = raw.get("chapter_cover") if isinstance(raw, dict) else None
                if isinstance(raw_cover, str) and raw_cover.startswith(("http://", "https://")):
                    chapter_cover_url = raw_cover

                normalized.append(
                    {
                        "episode": extract_episode_number(item, default_index=i),
                        "quality_urls": collect_quality_video_urls_from_chapter(item),
                        "video_url": extract_video_url(item),
                        "chapter_cover_url": chapter_cover_url,
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
                        "chapter_cover_url": item.get("chapterCover") if isinstance(item.get("chapterCover"), str) else None,
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
                "chapter_cover_url": None,
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


def image_extension_from_url(url: str) -> str:
    parsed = urlparse(url)
    path = parsed.path.lower()
    for ext in (".jpg", ".jpeg", ".png", ".webp", ".gif"):
        if path.endswith(ext):
            return ext
    return ".jpg"


def download_file(url: str, dest: Path, timeout: int = 60) -> None:
    req = Request(url, method="GET", headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=timeout) as resp, dest.open("wb") as out:
        while True:
            chunk = resp.read(CHUNK_SIZE)
            if not chunk:
                break
            out.write(chunk)


def download_with_retry(
    url: str,
    target: Path,
    retry_count: int,
    retry_sleep_sec: float,
    label: str,
) -> Tuple[bool, Optional[Exception]]:
    last_err: Optional[Exception] = None
    for attempt in range(1, max(1, retry_count) + 1):
        try:
            print(f"[DOWNLOADING] {label} (attempt {attempt}/{retry_count}): {url}")
            download_file(url, target)
            print(f"[DONE] {target}")
            return True, None
        except (HTTPError, URLError, TimeoutError, HTTPException, OSError) as err:
            last_err = err
            print(f"[WARN] {label} download failed on attempt {attempt}: {err}")
            if target.exists():
                target.unlink(missing_ok=True)
            if attempt < retry_count:
                time.sleep(max(0.0, retry_sleep_sec))
    return False, last_err


def process_book_id(
    book_id: str,
    output_dir: Path,
    skip_existing: bool = True,
    retry_count: int = 3,
    retry_sleep_sec: float = 1.0,
    preferred_quality: int = 720,
) -> Tuple[int, int, int]:
    print(f"\n[INFO] Processing bookId={book_id}")
    book_dir = output_dir / str(book_id)
    cover_dir = book_dir / "cover"
    episodes_dir = book_dir / "episodes"
    cover_dir.mkdir(parents=True, exist_ok=True)
    episodes_dir.mkdir(parents=True, exist_ok=True)

    payload = fetch_detail_and_episodes_json(book_id)

    raw_json_path = book_dir / "video_info.json"
    raw_json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[INFO] Saved raw API response: {raw_json_path}")

    drama_payload = payload.get("drama") if isinstance(payload, dict) else None
    if isinstance(drama_payload, dict):
        drama_json_path = book_dir / "drama_info.json"
        drama_json_path.write_text(json.dumps(drama_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[INFO] Saved drama metadata: {drama_json_path}")

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
                        "book_id": book_id,
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

    drama_cover_url: Optional[str] = None
    if isinstance(drama_payload, dict):
        cover_value = drama_payload.get("cover")
        if isinstance(cover_value, str) and cover_value.startswith(("http://", "https://")):
            drama_cover_url = cover_value

    if drama_cover_url:
        drama_cover_ext = image_extension_from_url(drama_cover_url)
        drama_cover_target = cover_dir / f"drama_cover{drama_cover_ext}"
        if skip_existing and drama_cover_target.exists() and drama_cover_target.stat().st_size > 0:
            print(f"[SKIP] Cover exists: {drama_cover_target}")
            skipped += 1
        else:
            ok, err = download_with_retry(
                drama_cover_url,
                drama_cover_target,
                retry_count=retry_count,
                retry_sleep_sec=retry_sleep_sec,
                label="Drama cover",
            )
            if ok:
                downloaded += 1
            else:
                failed += 1
                failed_items.append(
                    {
                        "episode": None,
                        "video_url": drama_cover_url,
                        "quality": "cover",
                        "filename": str(drama_cover_target.relative_to(book_dir)),
                        "error": str(err) if err else "unknown error",
                    }
                )
                print(f"[ERROR] Failed drama cover download after {retry_count} attempts")

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

        chapter_cover_url = ep.get("chapter_cover_url")
        if isinstance(chapter_cover_url, str) and chapter_cover_url.startswith(("http://", "https://")):
            chapter_cover_ext = image_extension_from_url(chapter_cover_url)
            chapter_cover_target = episode_dir / f"chapter_cover{chapter_cover_ext}"
            if skip_existing and chapter_cover_target.exists() and chapter_cover_target.stat().st_size > 0:
                print(f"[SKIP] Chapter cover exists: {chapter_cover_target}")
                skipped += 1
            else:
                ok, err = download_with_retry(
                    chapter_cover_url,
                    chapter_cover_target,
                    retry_count=retry_count,
                    retry_sleep_sec=retry_sleep_sec,
                    label=f"Episode {ep_no} chapter cover",
                )
                if ok:
                    downloaded += 1
                else:
                    failed += 1
                    failed_items.append(
                        {
                            "episode": ep_no,
                            "video_url": chapter_cover_url,
                            "quality": "chapter_cover",
                            "filename": str(chapter_cover_target.relative_to(book_dir)),
                            "error": str(err) if err else "unknown error",
                        }
                    )
                    print(f"[ERROR] Failed chapter cover download for episode {ep_no} after {retry_count} attempts")

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

            success, last_err = download_with_retry(
                video_url,
                target,
                retry_count=retry_count,
                retry_sleep_sec=retry_sleep_sec,
                label=f"Episode {ep_no} {quality}p",
            )
            if success:
                downloaded += 1
            else:
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
                subtitle_success, subtitle_last_err = download_with_retry(
                    subtitle_url,
                    subtitle_target,
                    retry_count=retry_count,
                    retry_sleep_sec=retry_sleep_sec,
                    label=f"Episode {ep_no} subtitle",
                )
                if subtitle_success:
                    downloaded += 1
                else:
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


def is_book_folder_already_downloaded(book_id: str, output_dir: Path) -> bool:
    book_dir = output_dir / str(book_id)
    if not book_dir.exists() or not book_dir.is_dir():
        return False

    # Treat as already processed when key output artifacts exist.
    if (book_dir / "episodes").exists():
        return True
    if (book_dir / "video_info.json").exists():
        return True
    if (book_dir / "drama_info.json").exists():
        return True
    if (book_dir / "episodes.json").exists():
        return True
    return False


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download FlickReels episode videos by id")
    parser.add_argument(
        "--book-id",
        action="append",
        help="bookId to process. Use multiple times for multiple ids.",
    )
    parser.add_argument(
        "--csv",
        type=Path,
        help="CSV file with book IDs. Header can be: bookId, book_id, or id.",
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

    book_ids: Set[str] = set()
    csv_rows: List[Dict[str, str]] = []
    csv_fieldnames: List[str] = []
    book_id_col: Optional[str] = None
    check_col: Optional[str] = None
    success_col: Optional[str] = None

    if args.book_id:
        for raw in args.book_id:
            for item in str(raw).split(","):
                value = item.strip()
                if value:
                    book_ids.add(value)

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

        book_id_col = _find_column(csv_fieldnames, ("bookid", "book_id", "id"))
        check_col = _find_column(csv_fieldnames, ("check",)) or "check"
        success_col = _find_column(csv_fieldnames, ("success_failed", "success")) or "success_failed"

        if book_id_col is None:
            print(f"[ERROR] CSV must contain bookId/book_id/id column: {args.csv}")
            return 1

        for row in csv_rows:
            value = (row.get(book_id_col) or "").strip()
            if not value:
                continue
            if should_process_csv_row(row, success_col):
                book_ids.add(value)

        if not book_ids:
            print(f"[INFO] No pending/failed rows to process in CSV: {args.csv}")
            return 0

    if not book_ids:
        print("[ERROR] Provide at least one --book-id or --csv")
        return 1

    args.output.mkdir(parents=True, exist_ok=True)
    skip_existing = not args.no_skip_existing
    retry_count = max(1, args.retry_count)
    retry_sleep_sec = max(0.0, args.retry_sleep)
    preferred_quality = max(1, args.preferred_quality)

    total_downloaded = 0
    total_skipped = 0
    total_failed = 0

    for book_id in sorted(book_ids):
        try:
            if skip_existing and is_book_folder_already_downloaded(book_id, args.output):
                print(f"\n[SKIP] bookId={book_id} already has output folder, skipping")
                if args.csv and csv_rows and book_id_col and check_col and success_col:
                    for row in csv_rows:
                        if (row.get(book_id_col) or "").strip() == book_id:
                            row[check_col] = "success"
                            row[success_col] = "success"
                    write_rows_to_csv(args.csv, csv_rows, csv_fieldnames)
                continue

            print(f"\n[PROCESSING] bookId={book_id}")
            downloaded, skipped, failed = process_book_id(
                book_id,
                args.output,
                skip_existing=skip_existing,
                retry_count=retry_count,
                retry_sleep_sec=retry_sleep_sec,
                preferred_quality=preferred_quality,
            )
            total_downloaded += downloaded
            total_skipped += skipped
            total_failed += failed

            if args.csv and csv_rows and book_id_col and check_col and success_col:
                for row in csv_rows:
                    if (row.get(book_id_col) or "").strip() == book_id:
                        row[check_col] = "success"
                        row[success_col] = "success" if failed == 0 else "failed"
                write_rows_to_csv(args.csv, csv_rows, csv_fieldnames)
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as err:
            print(f"[ERROR] Failed processing bookId {book_id}: {err}")
            total_failed += 1
            if args.csv and csv_rows and book_id_col and check_col and success_col:
                for row in csv_rows:
                    if (row.get(book_id_col) or "").strip() == book_id:
                        row[check_col] = "failed"
                        row[success_col] = "failed"
                write_rows_to_csv(args.csv, csv_rows, csv_fieldnames)

    print("\n[SUMMARY]")
    print(f"Book IDs: {len(book_ids)}")
    print(f"Downloaded files: {total_downloaded}")
    print(f"Skipped/missing: {total_skipped}")
    print(f"Failed downloads: {total_failed}")
    print(f"Output directory: {args.output.resolve()}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
