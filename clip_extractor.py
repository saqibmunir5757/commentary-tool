"""
clip_extractor.py

Downloads a YouTube video once and extracts multiple clips from it.
Adapted from sports-clip-tool for longer commentary clips (up to 2.5 min).

Key difference: commentary extracts multiple clips from ONE video,
so we download the full video once and cut locally.
"""

import json
import os
import shutil
import subprocess
from typing import Optional

from config import MAX_CLIP_DURATION, CLIPS_DIR, OUTPUT_DIR

FFMPEG_BIN = shutil.which("ffmpeg") or "ffmpeg"
FFPROBE_BIN = shutil.which("ffprobe") or "ffprobe"
YTDLP_BIN = shutil.which("yt-dlp") or "yt-dlp"


def _ytdlp_base_args() -> list:
    """Build yt-dlp base args with remote JS challenge solver."""
    args = ["--no-playlist", "--no-warnings", "--remote-components", "ejs:github"]
    return args

os.makedirs(CLIPS_DIR, exist_ok=True)


def format_timestamp(seconds: float) -> str:
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = seconds % 60
    return f"{h:02d}:{m:02d}:{s:06.3f}"


def download_full_video(youtube_url: str, output_path: str = None) -> Optional[str]:
    """
    Download the full source video once. All clip extractions cut from this local file.
    Uses video ID in filename to avoid serving wrong cached video.

    Returns path to downloaded video, or None on failure.
    """
    # Extract video ID for unique filename
    video_id = None
    import re
    m = re.search(r'(?:v=|youtu\.be/|/shorts/|/live/)([a-zA-Z0-9_-]{11})', youtube_url)
    if m:
        video_id = m.group(1)

    if output_path is None:
        fname = f"source_{video_id}.mp4" if video_id else "source_video.mp4"
        output_path = os.path.join(OUTPUT_DIR, fname)

        # Clean up old source videos (different video IDs)
        for f in os.listdir(OUTPUT_DIR):
            if f.startswith("source_") and f.endswith(".mp4") and f != fname:
                old = os.path.join(OUTPUT_DIR, f)
                print(f"  [Download] Removing old cached video: {f}")
                os.remove(old)

    if os.path.exists(output_path) and os.path.getsize(output_path) > 100000:
        print(f"  [Download] Using cached source video: {output_path}")
        return output_path

    print(f"  [Download] Downloading full video...")
    cmd = [
        YTDLP_BIN,
        "--format", "bestvideo[height<=1080][ext=mp4]+bestaudio[ext=m4a]/best[height<=1080][ext=mp4]/best[ext=mp4]/best",
        "--merge-output-format", "mp4",
        "-o", output_path,
        "--progress",
    ] + _ytdlp_base_args() + [youtube_url]

    print(f"  [Download] Command: {' '.join(cmd[:6])}...")
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)
        if r.returncode == 0 and os.path.exists(output_path):
            size_mb = os.path.getsize(output_path) / (1024 * 1024)
            # Verify the duration
            dur = get_clip_duration(output_path)
            print(f"  [Download] Saved: {size_mb:.1f} MB, Duration: {dur:.0f}s ({dur/60:.1f} min)")
            return output_path
        else:
            stderr = r.stderr[-500:] if r.stderr else 'no stderr'
            stdout = r.stdout[-500:] if r.stdout else 'no stdout'
            print(f"  [Download] yt-dlp failed (code {r.returncode})")
            print(f"  [Download] stderr: {stderr}")
            print(f"  [Download] stdout: {stdout}")
    except subprocess.TimeoutExpired:
        print(f"  [Download] Timed out after 1800s")
    except Exception as e:
        print(f"  [Download] Error: {e}")

    return None


def extract_clip_from_local(
    local_video_path: str,
    start_sec: float,
    duration_sec: float,
    output_name: str,
    keep_audio: bool = True,
    output_dir: str = None,
) -> Optional[str]:
    """
    Extract a clip from a local video file using ffmpeg.

    Args:
        local_video_path: Path to the full source video
        start_sec: Start time in seconds
        duration_sec: Duration in seconds
        output_name: Output filename (without extension)
        keep_audio: Whether to keep original audio
        output_dir: Override output directory (default: global CLIPS_DIR)

    Returns:
        Path to extracted clip, or None on failure.
    """
    duration_sec = min(float(duration_sec), float(MAX_CLIP_DURATION))
    start_sec = max(0.0, float(start_sec))

    target_dir = output_dir or CLIPS_DIR
    os.makedirs(target_dir, exist_ok=True)
    output_path = os.path.join(target_dir, f"{output_name}.mp4")
    print(f"  Extracting: {format_timestamp(start_sec)} -> {format_timestamp(start_sec + duration_sec)} ({duration_sec:.1f}s)")

    audio_args = ["-c:a", "aac", "-b:a", "128k", "-ar", "44100", "-ac", "2"] if keep_audio else ["-an"]

    cmd = [
        FFMPEG_BIN, "-y",
        "-ss", str(start_sec),
        "-t", str(duration_sec),
        "-i", local_video_path,
        "-vf", "scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2:color=black,fps=30",
        "-vsync", "cfr",
        "-c:v", "libx264", "-preset", "ultrafast", "-crf", "23",
        *audio_args,
        "-avoid_negative_ts", "make_zero",
        output_path,
    ]

    try:
        r = subprocess.run(cmd, capture_output=True, timeout=120)
        if r.returncode == 0 and os.path.exists(output_path) and os.path.getsize(output_path) > 50000:
            size_kb = os.path.getsize(output_path) / 1024
            print(f"  Saved ({'audio' if keep_audio else 'muted'}): {output_path} ({size_kb:.0f} KB)")
            return output_path
        else:
            err = r.stderr[-200:].decode(errors="replace") if r.stderr else "no output"
            print(f"  Extraction failed: {err}")
    except subprocess.TimeoutExpired:
        print(f"  Extraction timed out")
    except Exception as e:
        print(f"  Extraction error: {e}")

    return None


def extract_clip_from_url(
    youtube_url: str,
    start_sec: float,
    duration_sec: float,
    output_name: str,
    keep_audio: bool = True,
    output_dir: str = None,
) -> Optional[str]:
    """
    Fallback: Extract clip directly from YouTube URL using yt-dlp.
    Uses the 3-strategy waterfall from sports-clip-tool.
    """
    duration_sec = min(float(duration_sec), float(MAX_CLIP_DURATION))
    start_sec = max(0.0, float(start_sec))
    target_dir = output_dir or CLIPS_DIR
    os.makedirs(target_dir, exist_ok=True)
    output_path = os.path.join(target_dir, f"{output_name}.mp4")

    # Strategy 1: --download-sections
    end = start_sec + duration_sec
    start_fmt = format_timestamp(start_sec)
    end_fmt = format_timestamp(end)
    out_tmpl = os.path.join(target_dir, f"{output_name}_s1.%(ext)s")

    cmd = [
        YTDLP_BIN,
        "--download-sections", f"*{start_fmt}-{end_fmt}",
        "--format", "bestvideo[height<=1080][ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
        "--merge-output-format", "mp4",
        "-o", out_tmpl,
    ] + _ytdlp_base_args() + [youtube_url]

    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if r.returncode == 0:
            import glob
            matches = glob.glob(os.path.join(target_dir, f"{output_name}_s1.*"))
            if matches:
                raw_path = matches[0]
                if keep_audio:
                    os.rename(raw_path, output_path)
                else:
                    subprocess.run([
                        FFMPEG_BIN, "-y", "-i", raw_path, "-an", "-c:v", "copy", output_path
                    ], capture_output=True, timeout=30)
                    os.remove(raw_path)
                if os.path.exists(output_path) and os.path.getsize(output_path) > 50000:
                    return output_path
    except Exception as e:
        print(f"  URL extraction error: {e}")

    return None


def get_clip_duration(clip_path: str) -> float:
    """Get duration of a video/audio file using ffprobe."""
    cmd = [
        FFPROBE_BIN, "-v", "error",
        "-show_entries", "format=duration",
        "-of", "json", clip_path,
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        data = json.loads(result.stdout)
        return float(data["format"]["duration"])
    except Exception:
        return 0.0
