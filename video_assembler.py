"""
video_assembler.py

Assembles commentary video from clips and voiceovers.

Assembly model:
- Video is a continuous stream from the source video
- Real clip segments: original audio at full volume
- Commentary segments: video continues (muted) + VO audio on top
- Hook: opening video (muted) + hook VO on top

Per-segment approach: each segment becomes a self-contained MP4,
then all are concatenated with cross-dissolve transitions.
"""

import json
import os
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

from config import OUTPUT_DIR, CLIPS_DIR, NORMALIZED_DIR

FFMPEG_BIN = shutil.which("ffmpeg") or "ffmpeg"
FFPROBE_BIN = shutil.which("ffprobe") or "ffprobe"

TRANSITION_DUR = 0.5  # seconds for slide transition


def _get_encoder():
    """Detect if VideoToolbox hardware encoder is available."""
    try:
        r = subprocess.run(
            [FFMPEG_BIN, "-encoders"],
            capture_output=True, text=True, timeout=10
        )
        if "h264_videotoolbox" in r.stdout:
            return "h264_videotoolbox"
    except Exception:
        pass
    return "libx264"


HW_ENCODER = _get_encoder()


def _encoder_args():
    """Return encoder flags: VideoToolbox uses -q:v, libx264 uses -preset/-crf."""
    if HW_ENCODER == "h264_videotoolbox":
        return ["-c:v", "h264_videotoolbox", "-q:v", "65"]
    return ["-c:v", "libx264", "-preset", "ultrafast", "-crf", "23"]


print(f"[video_assembler] Using encoder: {HW_ENCODER}")

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(NORMALIZED_DIR, exist_ok=True)


def get_clip_duration(clip_path: str) -> float:
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


def _probe_format(clip_path: str) -> dict:
    """Probe video/audio properties using ffprobe."""
    info = {"width": 0, "height": 0, "fps": 0.0, "audio_rate": 0, "audio_channels": 0}
    try:
        # Video stream
        cmd_v = [
            FFPROBE_BIN, "-v", "error",
            "-select_streams", "v:0",
            "-show_entries", "stream=width,height,r_frame_rate",
            "-of", "json", clip_path,
        ]
        r = subprocess.run(cmd_v, capture_output=True, text=True, timeout=10)
        data = json.loads(r.stdout)
        streams = data.get("streams", [])
        if streams:
            info["width"] = int(streams[0].get("width", 0))
            info["height"] = int(streams[0].get("height", 0))
            fps_str = streams[0].get("r_frame_rate", "0/1")
            num, den = fps_str.split("/")
            info["fps"] = float(num) / float(den) if float(den) > 0 else 0.0

        # Audio stream
        cmd_a = [
            FFPROBE_BIN, "-v", "error",
            "-select_streams", "a:0",
            "-show_entries", "stream=sample_rate,channels",
            "-of", "json", clip_path,
        ]
        r2 = subprocess.run(cmd_a, capture_output=True, text=True, timeout=10)
        data2 = json.loads(r2.stdout)
        streams2 = data2.get("streams", [])
        if streams2:
            info["audio_rate"] = int(streams2[0].get("sample_rate", 0))
            info["audio_channels"] = int(streams2[0].get("channels", 0))
    except Exception as e:
        print(f"  probe error: {e}")
    return info


def normalize_clip(clip_path: str, output_path: str, target_fps: int = 30) -> Optional[str]:
    """Normalize clip to 1920x1080, 30fps, AAC 44100Hz stereo. Uses fast copy if already matching."""
    info = _probe_format(clip_path)

    already_normal = (
        info["width"] == 1920
        and info["height"] == 1080
        and abs(info["fps"] - target_fps) < 1
        and info["audio_rate"] == 44100
        and info["audio_channels"] == 2
    )

    if already_normal:
        # Fast path: stream copy + timestamp fix
        cmd = [
            FFMPEG_BIN, "-y",
            "-i", clip_path,
            "-c", "copy",
            "-avoid_negative_ts", "make_zero",
            output_path,
        ]
        print(f"  → fast copy (already 1080p/30fps/44100Hz)")
    else:
        # Slow fallback: full re-encode
        print(f"  → re-encoding ({info['width']}x{info['height']}, {info['fps']:.1f}fps, {info['audio_rate']}Hz)")
        cmd = [
            FFMPEG_BIN, "-y",
            "-i", clip_path,
            "-vf", (
                f"scale=1920:1080:force_original_aspect_ratio=decrease,"
                f"pad=1920:1080:(ow-iw)/2:(oh-ih)/2:color=black,"
                f"fps={target_fps}"
            ),
            "-vsync", "cfr",
            "-af", "aresample=async=1",
            "-c:v", "libx264", "-preset", "ultrafast", "-crf", "23",
            "-c:a", "aac", "-b:a", "128k",
            "-ar", "44100", "-ac", "2",
            "-avoid_negative_ts", "make_zero",
            output_path,
        ]

    result = subprocess.run(cmd, capture_output=True, timeout=300)
    if result.returncode != 0:
        err = result.stderr[-200:].decode(errors="replace") if result.stderr else ""
        print(f"  normalize error: {err}")
        return None
    return output_path


def create_commentary_segment(
    video_clip_path: str,
    vo_audio_path: str,
    output_path: str,
    enable_zoom: bool = True,
) -> Optional[str]:
    """
    Create a commentary segment: source video (muted, with slow zoom) + VO audio.
    """
    vo_duration = get_clip_duration(vo_audio_path)
    video_duration = get_clip_duration(video_clip_path)

    if vo_duration <= 0:
        print(f"  Invalid VO duration for {vo_audio_path}")
        return None

    # Build ffmpeg command: with or without Ken Burns zoom effect
    if enable_zoom:
        fps = 30
        total_frames = max(1, int(vo_duration * fps))
        cmd = [
            FFMPEG_BIN, "-y",
            "-i", video_clip_path,
            "-i", vo_audio_path,
            "-filter_complex",
            f"[0:v]zoompan=z='min(1.1,1+0.0003*on)':x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)':d=1:s=1920x1080:fps=30[zv]",
            "-map", "[zv]",
            "-map", "1:a",
            *_encoder_args(),
            "-c:a", "aac", "-b:a", "128k",
            "-ar", "44100", "-ac", "2",
            "-shortest",
            output_path,
        ]
    else:
        cmd = [
            FFMPEG_BIN, "-y",
            "-i", video_clip_path,
            "-i", vo_audio_path,
            "-map", "0:v",
            "-map", "1:a",
            *_encoder_args(),
            "-c:a", "aac", "-b:a", "128k",
            "-ar", "44100", "-ac", "2",
            "-shortest",
            output_path,
        ]

    try:
        r = subprocess.run(cmd, capture_output=True, timeout=180)
        if r.returncode == 0 and os.path.exists(output_path):
            return output_path
        err = r.stderr[-300:].decode(errors="replace") if r.stderr else ""
        print(f"  Commentary segment creation failed: {err}")
        # Fallback without zoom
        if enable_zoom:
            print(f"  Retrying without zoom effect...")
            return create_commentary_segment(video_clip_path, vo_audio_path, output_path, enable_zoom=False)
    except Exception as e:
        print(f"  Commentary segment error: {e}")

    return None


def create_hook_segment(
    video_clip_path: str,
    hook_vo_path: str,
    output_path: str,
) -> Optional[str]:
    """
    Create hook segment: opening video (muted) + hook voiceover.
    Same as commentary segment but uses the opening of the source video.
    """
    return create_commentary_segment(video_clip_path, hook_vo_path, output_path)


def _concat_with_transitions(segment_paths: list, output_path: str) -> bool:
    """Concatenate segments with cross-dissolve transitions."""
    n = len(segment_paths)

    if n == 1:
        shutil.copy2(segment_paths[0], output_path)
        return os.path.exists(output_path)

    durations = [get_clip_duration(p) for p in segment_paths]

    inputs = []
    for p in segment_paths:
        inputs += ["-i", p]

    vf_parts = []
    af_parts = []
    cumulative = 0.0

    for i in range(n - 1):
        in_v = f"[v{i}]" if i > 0 else f"[{i}:v]"
        in_a = f"[a{i}]" if i > 0 else f"[{i}:a]"
        out_v = f"[v{i + 1}]" if i < n - 2 else "[vout]"
        out_a = f"[a{i + 1}]" if i < n - 2 else "[aout]"

        cumulative += durations[i]
        v_offset = max(0.01, cumulative - (i + 1) * TRANSITION_DUR)

        vf_parts.append(
            f"{in_v}[{i + 1}:v]xfade=transition=slideleft:duration={TRANSITION_DUR}:offset={v_offset:.3f}{out_v}"
        )
        af_parts.append(
            f"{in_a}[{i + 1}:a]acrossfade=d={TRANSITION_DUR}{out_a}"
        )

    filter_complex = ";".join(vf_parts + af_parts)

    cmd = [
        FFMPEG_BIN, "-y",
        *inputs,
        "-filter_complex", filter_complex,
        "-map", "[vout]",
        "-map", "[aout]",
        *_encoder_args(),
        "-c:a", "aac", "-b:a", "128k",
        output_path,
    ]

    result = subprocess.run(cmd, capture_output=True, timeout=600)
    if result.returncode == 0 and os.path.exists(output_path):
        print(f"  Transitions applied ({n - 1} cross-dissolves)")
        return True

    print(f"  xfade failed — falling back to simple concat")
    return _simple_concat(segment_paths, output_path)


def _simple_concat(segment_paths: list, output_path: str) -> bool:
    """Fallback: simple concat without transitions."""
    concat_list = os.path.join(os.path.dirname(output_path), "concat.txt")
    with open(concat_list, "w") as f:
        for p in segment_paths:
            f.write(f"file '{os.path.abspath(p)}'\n")

    result = subprocess.run([
        FFMPEG_BIN, "-y",
        "-f", "concat", "-safe", "0",
        "-i", concat_list,
        "-c", "copy",
        "-avoid_negative_ts", "make_zero",
        output_path,
    ], capture_output=True, timeout=300)

    if result.returncode == 0 and os.path.exists(output_path):
        return True

    # Re-encode fallback
    result2 = subprocess.run([
        FFMPEG_BIN, "-y",
        "-f", "concat", "-safe", "0",
        "-i", concat_list,
        *_encoder_args(),
        "-c:a", "aac", "-b:a", "128k",
        "-avoid_negative_ts", "make_zero",
        output_path,
    ], capture_output=True, timeout=300)
    return result2.returncode == 0 and os.path.exists(output_path)


def assemble_video(
    assembled_segments: list,
    output_filename: str = "final_commentary.mp4",
    music_path: str = None,
    progress_callback=None,
    output_dir: str = None,
    normalized_dir: str = None,
    transitions: bool = False,
) -> Optional[str]:
    """
    Assemble all prepared segments into a final video.

    Args:
        assembled_segments: List of dicts with "segment_path" key (each is a self-contained MP4)
        output_filename: Output filename
        music_path: Optional background music
        progress_callback: Optional callback for progress updates
        output_dir: Override output directory (default: global OUTPUT_DIR)
        normalized_dir: Override normalized clips directory (default: global NORMALIZED_DIR)

    Returns:
        Path to final video, or None on failure.
    """
    out_dir = output_dir or OUTPUT_DIR
    norm_dir = normalized_dir or NORMALIZED_DIR
    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(norm_dir, exist_ok=True)
    output_path = os.path.join(out_dir, output_filename)

    valid_segments = [
        s for s in assembled_segments
        if s.get("segment_path") and os.path.exists(s["segment_path"])
    ]

    if not valid_segments:
        print("No valid segments to assemble.")
        return None

    # Step 1: Normalize all segments (parallel)
    print("\nNormalizing segments...")
    n_segs = len(valid_segments)

    def _normalize_one(i, seg):
        norm_path = os.path.join(norm_dir, f"norm_{i:03d}.mp4")
        result = normalize_clip(seg["segment_path"], norm_path)
        if result:
            dur = get_clip_duration(norm_path)
            print(f"  Segment {i} ({seg.get('type', '?')}): {dur:.1f}s")
            return (i, norm_path)
        else:
            print(f"  Segment {i}: normalization failed — skipping")
            return (i, None)

    results = {}
    completed = 0
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = {
            pool.submit(_normalize_one, i, seg): i
            for i, seg in enumerate(valid_segments)
        }
        for future in as_completed(futures):
            idx, norm_path = future.result()
            completed += 1
            if norm_path:
                results[idx] = norm_path
            pct = 40 + int((completed / n_segs) * 30)
            if progress_callback:
                progress_callback(f"Normalized {completed}/{n_segs} segments...", pct=pct)

    # Maintain original order
    norm_paths = [results[i] for i in sorted(results.keys())]

    if not norm_paths:
        print("No segments survived normalization.")
        return None

    # Step 2: Concatenate segments
    concat_path = os.path.join(out_dir, "concat_video.mp4")
    if transitions:
        if progress_callback:
            progress_callback("Concatenating with transitions...", pct=70)
        print("\nConcatenating with transitions...")
        ok = _concat_with_transitions(norm_paths, concat_path)
    else:
        if progress_callback:
            progress_callback("Concatenating segments...", pct=70)
        print("\nConcatenating segments...")
        ok = _simple_concat(norm_paths, concat_path)
    if not ok:
        print("Concatenation failed.")
        return None

    # Step 3: Optional music mix
    if music_path and os.path.exists(music_path):
        if progress_callback:
            progress_callback("Mixing background music...", pct=85)
        print("\nMixing background music...")

        total_dur = get_clip_duration(concat_path)
        fade_out_start = max(0, total_dur - 3.0)

        cmd = [
            FFMPEG_BIN, "-y",
            "-i", concat_path,
            "-i", music_path,
            "-filter_complex",
            f"[1:a]volume=0.10,afade=t=in:ss=0:d=2,afade=t=out:st={fade_out_start:.1f}:d=3,"
            f"aloop=loop=-1:size=2147483647[music];"
            f"[0:a][music]amix=inputs=2:duration=first:dropout_transition=0[final_audio]",
            "-map", "0:v",
            "-map", "[final_audio]",
            "-c:v", "copy",
            "-c:a", "aac", "-b:a", "192k",
            "-shortest",
            output_path,
        ]

        r = subprocess.run(cmd, capture_output=True, timeout=600)
        if r.returncode == 0 and os.path.exists(output_path):
            try:
                os.remove(concat_path)
            except OSError:
                pass
        else:
            print("  Music mix failed — using video without music")
            os.rename(concat_path, output_path)
    else:
        os.rename(concat_path, output_path)

    # Report
    if os.path.exists(output_path):
        size_mb = os.path.getsize(output_path) / (1024 * 1024)
        dur = get_clip_duration(output_path)
        print(f"\nFinal video: {output_path}")
        print(f"  Size: {size_mb:.1f} MB | Duration: {dur:.1f}s ({dur / 60:.1f} min)")
        return output_path

    return None


def _escape_subtitle_path(path: str) -> str:
    """Escape path for ffmpeg subtitle filter (colons, backslashes, quotes)."""
    path = path.replace("\\", "/")
    path = path.replace(":", "\\:")
    path = path.replace("'", "\\'")
    return path


def _format_srt_time(seconds: float) -> str:
    """Format seconds as SRT timestamp: HH:MM:SS,mmm"""
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    ms = int((seconds % 1) * 1000)
    return f"{h:02d}:{m:02d}:{s:02d},{ms:03d}"


def generate_srt_file(script: dict, assembled_segments: list, output_dir: str = None) -> Optional[str]:
    """Generate an SRT subtitle file from the script's VO segments."""
    out_dir = output_dir or OUTPUT_DIR
    srt_path = os.path.join(out_dir, "subtitles.srt")

    # Build timeline: calculate actual timestamps based on assembled segment durations
    entries = []
    current_time = 0.0

    for asm_seg in assembled_segments:
        seg_id = asm_seg["segment_id"]
        seg_path = asm_seg.get("segment_path")
        seg_type = asm_seg.get("type", "")

        if not seg_path or not os.path.exists(seg_path):
            continue

        seg_duration = get_clip_duration(seg_path)

        # Find matching script segment
        script_seg = None
        for s in script.get("segments", []):
            if s["segment_id"] == seg_id:
                script_seg = s
                break

        if script_seg and script_seg["type"].endswith("_voiceover") and script_seg.get("vo_text"):
            vo_text = script_seg["vo_text"]
            words = vo_text.split()

            # Split into subtitle chunks of ~10-12 words
            chunk_size = 10
            chunk_duration = seg_duration / max(1, len(words) / chunk_size)

            for i in range(0, len(words), chunk_size):
                chunk = " ".join(words[i:i + chunk_size])
                start = current_time + (i / max(1, len(words))) * seg_duration
                end = min(current_time + seg_duration, start + chunk_duration)
                entries.append((start, end, chunk))

        current_time += seg_duration

    if not entries:
        return None

    with open(srt_path, "w") as f:
        for idx, (start, end, text) in enumerate(entries, 1):
            f.write(f"{idx}\n")
            f.write(f"{_format_srt_time(start)} --> {_format_srt_time(end)}\n")
            f.write(f"{text}\n\n")

    return srt_path


def burn_subtitles(video_path: str, subtitle_path: str) -> bool:
    """Burn SRT subtitles into the video file in-place."""
    temp_path = video_path + ".sub_temp.mp4"
    escaped_path = _escape_subtitle_path(os.path.abspath(subtitle_path))

    style = "FontName=Arial,FontSize=22,PrimaryColour=&H00FFFFFF,OutlineColour=&H00000000,BorderStyle=3,Outline=2,Shadow=1,MarginV=40"

    cmd = [
        FFMPEG_BIN, "-y",
        "-i", video_path,
        "-vf", f"subtitles='{escaped_path}':force_style='{style}'",
        *_encoder_args(),
        "-c:a", "copy",
        temp_path,
    ]

    try:
        r = subprocess.run(cmd, capture_output=True, timeout=600)
        if r.returncode == 0 and os.path.exists(temp_path):
            os.replace(temp_path, video_path)
            return True
        err = r.stderr[-300:].decode(errors="replace") if r.stderr else ""
        print(f"  Subtitle burn failed: {err}")
    except Exception as e:
        print(f"  Subtitle burn error: {e}")

    # Clean up temp file on failure
    if os.path.exists(temp_path):
        os.remove(temp_path)
    return False
