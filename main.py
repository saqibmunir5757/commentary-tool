"""
main.py

Commentary Video Pipeline Orchestrator.

Pipeline:
1. Download transcript from YouTube URL
2. Analyze transcript (speakers, topics, stances)
3. Search for supporting facts
4. Generate commentary script
5. Generate/collect voiceovers
6. Download source video + extract clips
7. Assemble final video
"""

import json
import os
from typing import Optional

from config import OUTPUT_DIR, CLIPS_DIR, VOICEOVER_DIR, NORMALIZED_DIR, ensure_session_dirs
from transcript_downloader import download_transcript
from transcript_analyzer import analyze_transcript
from fact_searcher import search_facts_for_topics
from script_generator import generate_script
from clip_extractor import download_full_video, extract_clip_from_local, get_clip_duration as get_video_duration
from voiceover_handler import generate_tts_voiceovers
from video_assembler import (
    assemble_video,
    create_commentary_segment,
    create_hook_segment,
    get_clip_duration,
    generate_srt_file,
    burn_subtitles,
)
from sponsorblock import fetch_sponsor_segments, find_clean_video_range, overlaps_ad

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(CLIPS_DIR, exist_ok=True)
os.makedirs(VOICEOVER_DIR, exist_ok=True)


def run_pipeline(
    youtube_url: str,
    stance_id: str,
    target_duration_minutes: int = None,
    tts_engine: str = "edge_tts",
    tts_voice: str = "en-US-GuyNeural",
    music_path: str = None,
    progress_callback=None,
    # Pre-computed data (for web UI flow where steps are done separately)
    transcript_data: dict = None,
    analysis: dict = None,
    script: dict = None,
    vo_data: dict = None,
    heygen_data: dict = None,
    session_id: str = None,
) -> Optional[str]:
    """
    Run the full commentary video pipeline.

    Can be called with just youtube_url + stance_id (runs everything),
    or with pre-computed data from the web UI flow.

    Returns path to final video, or None on failure.
    """
    # Session-scoped directories (isolates clips/voiceovers/manifest per project)
    if session_id:
        dirs = ensure_session_dirs(session_id)
        clips_dir = dirs["clips_dir"]
        voiceover_dir = dirs["voiceover_dir"]
        normalized_dir = dirs["normalized_dir"]
        session_dir = dirs["session_dir"]
    else:
        clips_dir = CLIPS_DIR
        voiceover_dir = VOICEOVER_DIR
        normalized_dir = NORMALIZED_DIR
        session_dir = OUTPUT_DIR

    def _save(filename, data):
        path = os.path.join(session_dir, filename)
        with open(path, "w") as f:
            json.dump(data, f, indent=2, default=str)
        print(f"  Saved: {path}")

    def progress(msg, pct=None):
        print(f"[Pipeline] {msg}")
        if progress_callback:
            progress_callback(msg, pct=pct)

    # Step 1: Download transcript
    if not transcript_data:
        progress("Step 1/7: Downloading transcript...")
        transcript_data = download_transcript(youtube_url)
        if transcript_data.get("error"):
            progress(f"Error: {transcript_data['error']}")
            return None
        _save("transcript.json", transcript_data)

    # Step 2: Analyze transcript
    if not analysis:
        progress("Step 2/7: Analyzing transcript...")
        analysis = analyze_transcript(transcript_data)
        _save("analysis.json", analysis)

    # Step 3: Search for facts
    progress("Step 3/7: Searching for supporting facts...")
    stance_label = "Balanced"
    for opt in analysis.get("stance_options", []):
        if opt["id"] == stance_id:
            stance_label = opt["label"]
            break

    facts = search_facts_for_topics(
        speakers=analysis.get("speakers", []),
        topics=analysis.get("topics", []),
        stance_label=stance_label,
    )
    _save("facts.json", facts)

    # Step 4: Generate script
    if not script:
        progress("Step 4/7: Generating commentary script...")
        script = generate_script(
            transcript_data=transcript_data,
            analysis=analysis,
            stance_id=stance_id,
            facts=facts,
            target_duration_minutes=target_duration_minutes,
        )
        if script.get("error"):
            progress(f"Script generation error: {script['error']}")
            return None
        _save("script.json", script)

    segments = script.get("segments", [])
    if not segments:
        progress("No segments in script.")
        return None

    # Step 5: Generate voiceovers (TTS) — skip if HeyGen covers all VO segments
    heygen_covers_all = False
    if heygen_data and heygen_data.get("successful", 0) > 0:
        heygen_seg_ids = {hs["segment_id"] for hs in heygen_data.get("heygen_segments", []) if hs.get("success")}
        vo_seg_ids = {s["segment_id"] for s in segments if s["type"].endswith("_voiceover")}
        heygen_covers_all = vo_seg_ids.issubset(heygen_seg_ids)
        if heygen_covers_all:
            progress("Step 5/7: Skipping TTS — HeyGen avatar videos cover all voiceover segments")
        else:
            missing = vo_seg_ids - heygen_seg_ids
            progress(f"Step 5/7: HeyGen missing segments {missing} — generating TTS fallback")

    if not heygen_covers_all and not vo_data:
        progress("Step 5/7: Generating voiceovers...")
        vo_data = generate_tts_voiceovers(
            script=script,
            voice=tts_voice,
            progress_callback=progress_callback,
            output_dir=voiceover_dir,
        )
        _save("voiceovers.json", vo_data)

    # Build VO lookup from vo_data (maps segment_id -> audio_path)
    vo_lookup = {}
    if vo_data:
        for vo_seg in vo_data.get("voiceover_segments", []):
            path = vo_seg["audio_path"]
            exists = os.path.exists(path) if path else False
            progress(f"  VO segment {vo_seg['segment_id']}: {path} (exists={exists})")
            vo_lookup[vo_seg["segment_id"]] = path

    if vo_lookup:
        progress(f"  VO lookup has {len(vo_lookup)} entries")
    elif not heygen_covers_all:
        progress("  WARNING: vo_data is empty/None!")

    # Step 6: Download source video + extract clips
    progress("Step 6/7: Downloading source video and extracting clips...")

    # Check for existing assembled manifest to resume from
    import glob as _glob
    manifest_path = os.path.join(session_dir, "assembled_manifest.json")
    existing_assembled = {}
    if os.path.exists(manifest_path):
        try:
            with open(manifest_path) as f:
                prev_manifest = json.load(f)
            for entry in prev_manifest:
                if entry.get("segment_path") and os.path.exists(entry["segment_path"]):
                    existing_assembled[entry["segment_id"]] = entry
            if existing_assembled:
                progress(f"  Found {len(existing_assembled)}/{len(segments)} segments from previous run")
        except Exception:
            existing_assembled = {}

    # Only clean clips that don't belong to resumed segments
    resumed_paths = {e["segment_path"] for e in existing_assembled.values()}
    for old_clip in _glob.glob(os.path.join(clips_dir, "*.mp4")):
        if old_clip not in resumed_paths:
            os.remove(old_clip)

    video_id = transcript_data.get("video_id", "video")
    source_video = download_full_video(youtube_url)
    if not source_video:
        progress("Failed to download source video.")
        return None

    source_duration = get_video_duration(source_video)

    # Fetch sponsor/ad segments to skip
    ad_segments = fetch_sponsor_segments(video_id)
    if ad_segments:
        total_ad = sum(s["duration"] for s in ad_segments)
        progress(f"  Found {len(ad_segments)} ad/sponsor segments ({total_ad:.0f}s) — will skip these")
    else:
        progress("  No ad segments detected")

    # Build HeyGen lookup (segment_id -> local MP4 path) if available
    heygen_lookup = {}
    if heygen_data:
        for hs in heygen_data.get("heygen_segments", []):
            if hs.get("success") and hs.get("heygen_video_path"):
                heygen_lookup[hs["segment_id"]] = hs["heygen_video_path"]
        progress(f"  HeyGen mode: {len(heygen_lookup)} avatar segments available")

    # Build assembled segments — skip segments that already exist from a previous run
    assembled_segments = []
    total_segs = len(segments)

    for seg_idx, seg in enumerate(segments):
        seg_id = seg["segment_id"]
        seg_type = seg["type"]

        # Resume: reuse segment from previous run if it exists
        if seg_id in existing_assembled:
            entry = existing_assembled[seg_id]
            assembled_segments.append(entry)
            dur = get_clip_duration(entry["segment_path"])
            progress(f"  Segment {seg_id} ({seg_type}): resumed from previous run ({dur:.1f}s)", pct=int((seg_idx / total_segs) * 40))
            continue

        if seg_type in ("hook_voiceover", "commentary_voiceover"):
            # Check if HeyGen avatar video exists for this segment
            heygen_path = heygen_lookup.get(seg_id)
            if heygen_path and os.path.exists(heygen_path):
                # Use HeyGen MP4 directly — it already has avatar + voice
                dur = get_clip_duration(heygen_path)
                assembled_segments.append({
                    "segment_id": seg_id,
                    "type": seg_type,
                    "segment_path": heygen_path,
                })
                label = "Hook" if seg_type == "hook_voiceover" else "Commentary"
                progress(f"  {label} segment {seg_id} ready ({dur:.1f}s) — HeyGen avatar", pct=int((seg_idx / total_segs) * 40))
                _save("assembled_manifest.json", assembled_segments)
                continue

            # Fallback to TTS + source video approach
            if seg_type == "hook_voiceover":
                vo_path = vo_lookup.get(seg_id)
                if not vo_path or not os.path.exists(vo_path):
                    progress(f"  No VO for hook segment {seg_id} — skipping")
                    continue

                vo_dur = get_clip_duration(vo_path)
                hook_start, hook_end = find_clean_video_range(
                    0.0, vo_dur + 2, source_duration, ad_segments
                )
                hook_clip = extract_clip_from_local(
                    source_video, hook_start, hook_end - hook_start, f"hook_{seg_id}",
                    keep_audio=False, output_dir=clips_dir,
                )
                if not hook_clip:
                    continue

                hook_out = os.path.join(clips_dir, f"assembled_hook_{seg_id}.mp4")
                result = create_hook_segment(hook_clip, vo_path, hook_out)
                if result:
                    assembled_segments.append({
                        "segment_id": seg_id,
                        "type": seg_type,
                        "segment_path": result,
                    })
                    _save("assembled_manifest.json", assembled_segments)
                    seg_pct = int((seg_idx / total_segs) * 40)
                    if hook_start > 0.5:
                        progress(f"  Hook segment ready ({vo_dur:.1f}s) — skipped ad at start", pct=seg_pct)
                    else:
                        progress(f"  Hook segment ready ({vo_dur:.1f}s)", pct=seg_pct)

            else:  # commentary_voiceover
                vo_path = vo_lookup.get(seg_id)
                if not vo_path or not os.path.exists(vo_path):
                    progress(f"  No VO for commentary segment {seg_id} — skipping")
                    continue

                vo_dur = get_clip_duration(vo_path)
                prev_clip_end = _get_prev_clip_end(segments, seg_id)
                bg_start, bg_end = find_clean_video_range(
                    prev_clip_end, vo_dur + 2, source_duration, ad_segments
                )

                commentary_clip = extract_clip_from_local(
                    source_video, bg_start, bg_end - bg_start, f"commentary_video_{seg_id}",
                    keep_audio=False, output_dir=clips_dir,
                )
                if not commentary_clip:
                    continue

                commentary_out = os.path.join(clips_dir, f"assembled_commentary_{seg_id}.mp4")
                result = create_commentary_segment(commentary_clip, vo_path, commentary_out)
                if result:
                    assembled_segments.append({
                        "segment_id": seg_id,
                        "type": seg_type,
                        "segment_path": result,
                    })
                    _save("assembled_manifest.json", assembled_segments)
                    seg_pct = int((seg_idx / total_segs) * 40)
                    if bg_start != prev_clip_end:
                        progress(f"  Commentary segment {seg_id} ready ({vo_dur:.1f}s) — skipped ad", pct=seg_pct)
                    else:
                        progress(f"  Commentary segment {seg_id} ready ({vo_dur:.1f}s)", pct=seg_pct)

        elif seg_type == "real_clip":
            # Real clip: extract from source with original audio, skip ad sections
            start = seg.get("clip_start_sec", 0)
            end = seg.get("clip_end_sec", start + 45)
            duration = end - start

            ad_hit = overlaps_ad(start, end, ad_segments)
            if ad_hit:
                clean_start, clean_end = find_clean_video_range(
                    start, duration, source_duration, ad_segments
                )
                ad_cat = ad_hit["category"]
                progress(f"  Clip {seg_id}: skipping {ad_cat} segment ({ad_hit['start']:.0f}s-{ad_hit['end']:.0f}s), adjusted to {clean_start:.0f}s-{clean_end:.0f}s")
                start = clean_start
                duration = clean_end - clean_start

            clip_path = extract_clip_from_local(
                source_video, start, duration, f"clip_{seg_id}",
                keep_audio=True, output_dir=clips_dir,
            )
            if clip_path:
                assembled_segments.append({
                    "segment_id": seg_id,
                    "type": seg_type,
                    "segment_path": clip_path,
                })
                clip_dur = get_clip_duration(clip_path)
                progress(f"  Real clip {seg_id} ready ({clip_dur:.1f}s)", pct=int((seg_idx / total_segs) * 40))
                _save("assembled_manifest.json", assembled_segments)

    if not assembled_segments:
        progress("No segments were successfully assembled.")
        return None

    _save("assembled_manifest.json", assembled_segments)

    # Step 7: Final assembly
    progress("Step 7/7: Assembling final video...")
    final_path = assemble_video(
        assembled_segments=assembled_segments,
        music_path=music_path,
        progress_callback=progress_callback,
        output_dir=session_dir,
        normalized_dir=normalized_dir,
    )

    # Step 8: Generate and burn subtitles
    if final_path:
        progress("Generating subtitles...", pct=90)
        srt_path = generate_srt_file(script, assembled_segments, output_dir=session_dir)
        if srt_path:
            progress("Burning subtitles into video...", pct=95)
            ok = burn_subtitles(final_path, srt_path)
            if ok:
                progress("Subtitles burned successfully", pct=98)
            else:
                progress("Subtitle burning skipped (video still ok without subs)", pct=98)

    if final_path:
        progress(f"Done! Final video: {final_path}")
    else:
        progress("Final assembly failed.")

    return final_path


def _get_prev_clip_end(segments: list, current_seg_id: int) -> float:
    """Find the end timestamp of the previous real_clip segment."""
    prev_end = 0.0
    for seg in segments:
        if seg["segment_id"] >= current_seg_id:
            break
        if seg["type"] == "real_clip":
            prev_end = seg.get("clip_end_sec", prev_end)
    return prev_end


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3:
        print("Usage: python main.py <youtube_url> <stance_id>")
        print("  stance_id: speaker_0, speaker_1, balanced")
        sys.exit(1)

    url = sys.argv[1]
    stance = sys.argv[2]
    result = run_pipeline(url, stance)
    if result:
        print(f"\nSuccess: {result}")
    else:
        print("\nPipeline failed.")
