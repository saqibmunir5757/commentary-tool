"""
server.py

FastAPI web server for the Commentary Video Tool.
Provides multi-step wizard endpoints with SSE progress streaming.
"""

import asyncio
import json
import os
import shutil
import tempfile
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from queue import Queue, Empty
from typing import Optional

from fastapi import FastAPI, File, Form, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from config import OUTPUT_DIR, VOICEOVER_DIR, HEYGEN_API_KEY, get_session_dirs, ensure_session_dirs
from transcript_downloader import download_transcript
from transcript_analyzer import analyze_transcript
from fact_searcher import search_facts_for_topics
from script_generator import generate_script
from voiceover_handler import generate_tts_voiceovers, register_uploaded_voiceover
from main import run_pipeline
from sponsorblock import fetch_sponsor_segments
from heygen_handler import list_avatars, list_voices, generate_all_commentary_segments
from heygen_browser import generate_all_segments_browser_sync, generate_single_video_browser_sync
from ai33_tts import list_voices as ai33_list_voices, search_voices as ai33_search_voices

app = FastAPI(title="Commentary Video Tool")
executor = ThreadPoolExecutor(max_workers=2)

# In-memory job store
jobs: dict = {}

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(VOICEOVER_DIR, exist_ok=True)

# Session store for multi-step wizard (in-memory cache, backed by disk)
sessions: dict = {}

SESSIONS_DIR = os.path.join(OUTPUT_DIR, "sessions")
os.makedirs(SESSIONS_DIR, exist_ok=True)


def _compute_step(session: dict) -> int:
    """Determine current wizard step from session data."""
    if session.get("final_video"):
        return 6
    if session.get("vo_data") or session.get("heygen_data"):
        return 5
    if session.get("script"):
        return 4
    if session.get("analysis"):
        return 3
    if session.get("transcript_data"):
        return 2
    return 1


def _save_session(session_id: str):
    """Persist session dict to disk as JSON."""
    session = _load_session(session_id)
    if not session:
        return
    session["current_step"] = _compute_step(session)
    session["updated_at"] = time.time()
    path = os.path.join(SESSIONS_DIR, f"{session_id}.json")
    with open(path, "w") as f:
        json.dump(session, f, default=str)


def _load_session(session_id: str) -> dict | None:
    """Load session from disk if not in memory."""
    if session_id in sessions:
        return sessions[session_id]
    path = os.path.join(SESSIONS_DIR, f"{session_id}.json")
    if os.path.exists(path):
        with open(path) as f:
            sessions[session_id] = json.load(f)
        return sessions[session_id]
    return None


# ── Static files ─────────────────────────────────────────────────────────────

_static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
app.mount("/static", StaticFiles(directory=_static_dir), name="static")

@app.get("/", response_class=HTMLResponse)
async def home():
    html_path = os.path.join(_static_dir, "home.html")
    with open(html_path) as f:
        return HTMLResponse(content=f.read())

@app.get("/commentary", response_class=HTMLResponse)
async def commentary_page():
    html_path = os.path.join(_static_dir, "index.html")
    with open(html_path) as f:
        return HTMLResponse(content=f.read())

@app.get("/logs", response_class=HTMLResponse)
async def logs_page():
    html_path = os.path.join(_static_dir, "logs.html")
    with open(html_path) as f:
        return HTMLResponse(content=f.read())

@app.get("/active-jobs")
async def active_jobs():
    active = [jid for jid, j in jobs.items() if j.get("status") == "running"]
    return {"job_ids": active}

@app.get("/api/voices")
async def get_voices():
    """Fetch available TTS voices from ai33.pro API."""
    voices = ai33_list_voices()
    return {"voices": voices}

@app.get("/api/voices/search")
async def search_voices(q: str = "", page_size: int = 25):
    """Search the full ai33.pro voice library by keyword."""
    if not q or len(q) < 2:
        return {"voices": []}
    results = ai33_search_voices(q, page_size=page_size)
    return {"voices": results}

# ── Voice Favourites ─────────────────────────────────────────────────────────
_FAV_FILE = os.path.join(os.path.dirname(__file__), "voice_favourites.json")

def _load_favs() -> list:
    if os.path.exists(_FAV_FILE):
        try:
            with open(_FAV_FILE) as f:
                return json.load(f)
        except Exception:
            return []
    return []

def _save_favs(favs: list):
    with open(_FAV_FILE, "w") as f:
        json.dump(favs, f, indent=2)

@app.get("/api/voices/favourites")
async def get_favourites():
    return {"voices": _load_favs()}

@app.post("/api/voices/favourites")
async def add_favourite(
    voice_id: str = Form(...),
    name: str = Form(""),
    preview_url: str = Form(""),
    accent: str = Form(""),
    gender: str = Form(""),
    category: str = Form(""),
    description: str = Form(""),
):
    favs = _load_favs()
    if any(f["voice_id"] == voice_id for f in favs):
        return {"ok": True, "message": "Already favourited"}
    favs.append({
        "voice_id": voice_id,
        "name": name,
        "preview_url": preview_url,
        "labels": {"accent": accent, "gender": gender},
        "category": category,
        "description": description,
    })
    _save_favs(favs)
    return {"ok": True}

@app.delete("/api/voices/favourites/{voice_id}")
async def remove_favourite(voice_id: str):
    favs = _load_favs()
    favs = [f for f in favs if f["voice_id"] != voice_id]
    _save_favs(favs)
    return {"ok": True}

@app.get("/script", response_class=HTMLResponse)
async def script_page():
    html_path = os.path.join(_static_dir, "script.html")
    with open(html_path) as f:
        return HTMLResponse(content=f.read())


# ── Script-Only Avatar Video ─────────────────────────────────────────────────

SCRIPT_VIDEO_DIR = os.path.join(OUTPUT_DIR, "script_videos")
os.makedirs(SCRIPT_VIDEO_DIR, exist_ok=True)

@app.post("/generate-script-video")
async def gen_script_video(script_text: str = Form(...)):
    """Generate a single HeyGen avatar video from raw script text."""
    if not script_text.strip():
        return {"error": "Script text is empty."}

    job_id = str(uuid.uuid4())[:8]
    jobs[job_id] = {"status": "running", "queue": Queue(), "result": None, "error": None}

    def _run():
        q = jobs[job_id]["queue"]
        try:
            def progress(msg, pct=None):
                event = {"type": "progress", "message": msg}
                if pct is not None:
                    event["pct"] = pct
                q.put(event)

            result = generate_single_video_browser_sync(
                script_text=script_text,
                progress_callback=progress,
            )

            if result["success"]:
                # Copy to script_videos dir for serving
                import shutil as _shutil
                filename = os.path.basename(result["video_path"])
                serve_path = os.path.join(SCRIPT_VIDEO_DIR, filename)
                if result["video_path"] != serve_path:
                    _shutil.copy2(result["video_path"], serve_path)

                jobs[job_id]["result"] = result
                jobs[job_id]["status"] = "completed"
                q.put({"type": "done", "video_path": serve_path})
            else:
                jobs[job_id]["status"] = "failed"
                jobs[job_id]["error"] = result.get("error", "Unknown error")
                q.put({"type": "error", "message": result.get("error", "Unknown error")})
        except Exception as e:
            jobs[job_id]["status"] = "failed"
            jobs[job_id]["error"] = str(e)
            q.put({"type": "error", "message": str(e)})

    executor.submit(_run)
    return {"job_id": job_id}

@app.get("/download-script-video/{filename}")
async def download_script_video(filename: str):
    """Serve a generated script video for download."""
    path = os.path.join(SCRIPT_VIDEO_DIR, filename)
    if not os.path.exists(path):
        return {"error": "File not found"}
    return FileResponse(path, media_type="video/mp4", filename=filename)

@app.get("/script-history", response_class=HTMLResponse)
async def script_history_page():
    html_path = os.path.join(_static_dir, "script-history.html")
    with open(html_path) as f:
        return HTMLResponse(content=f.read())

@app.get("/script-video-list")
async def script_video_list():
    """List all generated script videos with metadata."""
    videos = []
    if os.path.exists(SCRIPT_VIDEO_DIR):
        for f in sorted(os.listdir(SCRIPT_VIDEO_DIR)):
            if f.endswith(".mp4"):
                path = os.path.join(SCRIPT_VIDEO_DIR, f)
                stat = os.stat(path)
                videos.append({
                    "filename": f,
                    "size_mb": round(stat.st_size / (1024 * 1024), 1),
                    "created": time.strftime("%Y-%m-%d %H:%M", time.localtime(stat.st_mtime)),
                })
    return {"videos": videos}


# ── Session Management ───────────────────────────────────────────────────────

@app.get("/sessions")
async def list_sessions():
    """List all saved sessions (drafts + completed) for the dashboard."""
    items = []
    for fname in os.listdir(SESSIONS_DIR):
        if not fname.endswith(".json"):
            continue
        sid = fname[:-5]
        try:
            with open(os.path.join(SESSIONS_DIR, fname)) as f:
                data = json.load(f)
            td = data.get("transcript_data", {})
            items.append({
                "session_id": sid,
                "title": td.get("title", "Untitled"),
                "channel": td.get("channel", ""),
                "thumbnail": td.get("thumbnail", ""),
                "duration_seconds": td.get("duration_seconds", 0),
                "status": data.get("status", "draft"),
                "current_step": _compute_step(data),
                "created_at": data.get("created_at", 0),
                "updated_at": data.get("updated_at", 0),
                "final_video": data.get("final_video"),
            })
        except Exception:
            continue
    items.sort(key=lambda x: x.get("updated_at", 0), reverse=True)
    return {"sessions": items}


@app.get("/session/{session_id}")
async def get_session(session_id: str):
    """Get full session state for restoring UI after refresh."""
    session = _load_session(session_id)
    if not session:
        return {"error": "Session not found."}

    td = session.get("transcript_data", {})
    result = {
        "session_id": session_id,
        "status": session.get("status", "draft"),
        "current_step": _compute_step(session),
        "youtube_url": session.get("youtube_url"),
        "transcript_info": {
            "video_id": td.get("video_id"),
            "title": td.get("title"),
            "duration_seconds": td.get("duration_seconds"),
            "channel": td.get("channel"),
            "thumbnail": td.get("thumbnail"),
            "transcript_length": len(td.get("transcript", [])),
        },
        "ad_segments": session.get("ad_segments", []),
        "analysis": session.get("analysis"),
        "stance_id": session.get("stance_id"),
        "script": session.get("script"),
        "vo_data": session.get("vo_data"),
        "heygen_data": session.get("heygen_data"),
        "music_path": session.get("music_path"),
        "final_video": session.get("final_video"),
    }
    return result


@app.delete("/session/{session_id}")
async def delete_session(session_id: str):
    """Delete a session (draft or completed)."""
    path = os.path.join(SESSIONS_DIR, f"{session_id}.json")
    if os.path.exists(path):
        os.remove(path)
    sessions.pop(session_id, None)
    return {"status": "ok"}


# ── Step 1: Download Transcript ──────────────────────────────────────────────

@app.post("/transcript")
async def get_transcript(youtube_url: str = Form(...)):
    """Download transcript and metadata from YouTube URL."""
    session_id = str(uuid.uuid4())[:8]
    result = await asyncio.get_event_loop().run_in_executor(
        executor, download_transcript, youtube_url
    )
    if result.get("error"):
        return {"error": result["error"]}

    # Fetch SponsorBlock ad segments
    video_id = result.get("video_id", "")
    ad_segments = await asyncio.get_event_loop().run_in_executor(
        executor, fetch_sponsor_segments, video_id
    ) if video_id else []

    sessions[session_id] = {
        "youtube_url": youtube_url,
        "transcript_data": result,
        "ad_segments": ad_segments,
        "status": "draft",
        "created_at": time.time(),
        "updated_at": time.time(),
    }
    _save_session(session_id)
    return {
        "session_id": session_id,
        "video_id": video_id,
        "title": result.get("title"),
        "duration_seconds": result.get("duration_seconds"),
        "channel": result.get("channel"),
        "thumbnail": result.get("thumbnail"),
        "transcript_length": len(result.get("transcript", [])),
        "ad_segments": ad_segments,
    }


# ── Step 2: Analyze Transcript ───────────────────────────────────────────────

@app.post("/analyze")
async def analyze(session_id: str = Form(...)):
    """Analyze transcript to identify speakers, topics, stances."""
    session = _load_session(session_id)
    if not session:
        return {"error": "Session not found. Start with /transcript first."}

    transcript_data = session["transcript_data"]
    analysis = await asyncio.get_event_loop().run_in_executor(
        executor, analyze_transcript, transcript_data
    )
    session["analysis"] = analysis
    _save_session(session_id)
    return analysis


# ── Step 3: Generate Script ──────────────────────────────────────────────────

@app.post("/generate-script")
async def gen_script(
    session_id: str = Form(...),
    stance_id: str = Form(...),
    target_duration: int = Form(None),
    selected_topics: str = Form(None),
    custom_stances: str = Form(None),
    tone_preset: str = Form(None),
):
    """Generate commentary script with chosen stance and selected topics."""
    session = _load_session(session_id)
    if not session:
        return {"error": "Session not found."}

    transcript_data = session["transcript_data"]
    analysis = session.get("analysis")
    if not analysis:
        return {"error": "Run /analyze first."}

    # Clear assembled manifest + old clips so new script starts fresh
    import glob as _glob
    dirs = get_session_dirs(session_id)
    manifest_path = os.path.join(dirs["session_dir"], "assembled_manifest.json")
    if os.path.exists(manifest_path):
        os.remove(manifest_path)
    s_clips_dir = dirs["clips_dir"]
    if os.path.isdir(s_clips_dir):
        for f in _glob.glob(os.path.join(s_clips_dir, "*.mp4")):
            os.remove(f)

    # Parse selected topic IDs
    selected_topic_ids = None
    if selected_topics:
        try:
            selected_topic_ids = json.loads(selected_topics)
        except json.JSONDecodeError:
            pass

    # Parse custom stances
    custom_stance_list = None
    if custom_stances:
        try:
            custom_stance_list = json.loads(custom_stances)
        except json.JSONDecodeError:
            pass

    # Search for facts — support multiple comma-separated stance IDs
    stance_ids = [s.strip() for s in stance_id.split(",") if s.strip() and s.strip() != "custom"]
    stance_labels = []
    for sid in stance_ids:
        for opt in analysis.get("stance_options", []):
            if opt["id"] == sid:
                stance_labels.append(opt["label"])
                break
    if custom_stance_list:
        stance_labels.extend(custom_stance_list)
    stance_label = " + ".join(stance_labels) if stance_labels else stance_id

    facts = await asyncio.get_event_loop().run_in_executor(
        executor,
        search_facts_for_topics,
        analysis.get("speakers", []),
        analysis.get("topics", []),
        stance_label,
    )

    # Generate script
    def _gen():
        return generate_script(
            transcript_data=transcript_data,
            analysis=analysis,
            stance_id=stance_id,
            facts=facts,
            target_duration_minutes=target_duration,
            selected_topic_ids=selected_topic_ids,
            custom_stances=custom_stance_list,
            tone_preset=tone_preset,
        )

    script = await asyncio.get_event_loop().run_in_executor(executor, _gen)

    session["script"] = script
    session["stance_id"] = stance_id
    session["facts"] = facts
    _save_session(session_id)
    return script


# ── Step 4: Update Script (edit VO text) ─────────────────────────────────────

@app.post("/update-script")
async def update_script(session_id: str = Form(...), script_json: str = Form(...)):
    """Update script with user edits to VO text."""
    session = _load_session(session_id)
    if not session:
        return {"error": "Session not found."}
    try:
        script = json.loads(script_json)
        session["script"] = script
        _save_session(session_id)
        return {"status": "ok"}
    except json.JSONDecodeError as e:
        return {"error": f"Invalid JSON: {e}"}


# ── Step 4b: Regenerate Single Segment Script ────────────────────────────────

@app.post("/regenerate-segment")
async def regen_segment(
    session_id: str = Form(...),
    segment_id: int = Form(...),
    instructions: str = Form(None),
):
    """Regenerate script text for a single commentary/hook segment."""
    session = _load_session(session_id)
    if not session:
        return {"error": "Session not found."}

    script = session.get("script")
    if not script:
        return {"error": "No script found."}

    # Find the segment
    segment = None
    for seg in script.get("segments", []):
        if seg["segment_id"] == segment_id:
            segment = seg
            break
    if not segment:
        return {"error": f"Segment {segment_id} not found."}
    if segment["type"] == "real_clip":
        return {"error": "Cannot regenerate real clip segments."}

    from script_generator import regenerate_single_segment

    def _regen():
        return regenerate_single_segment(
            transcript_data=session.get("transcript_data", {}),
            analysis=session.get("analysis", {}),
            script=script,
            segment=segment,
            facts=session.get("facts", []),
            instructions=instructions,
        )

    new_segment = await asyncio.get_event_loop().run_in_executor(executor, _regen)

    if new_segment and not new_segment.get("error"):
        # Update the segment in the script
        for i, seg in enumerate(script["segments"]):
            if seg["segment_id"] == segment_id:
                script["segments"][i] = new_segment
                break
        session["script"] = script
        _save_session(session_id)
        return new_segment

    return new_segment or {"error": "Regeneration failed"}


# ── Step 4c: Generate Hook Variants ─────────────────────────────────────────

@app.post("/generate-hooks")
async def gen_hooks(
    session_id: str = Form(...),
):
    """Generate 3 hook variants for the user to choose from."""
    session = _load_session(session_id)
    if not session:
        return {"error": "Session not found."}

    from script_generator import generate_hook_variants

    def _gen():
        return generate_hook_variants(
            transcript_data=session.get("transcript_data", {}),
            analysis=session.get("analysis", {}),
            stance_id=session.get("stance_id", "balanced"),
            facts=session.get("facts", []),
        )

    hooks = await asyncio.get_event_loop().run_in_executor(executor, _gen)
    return {"hooks": hooks}


# ── Step 5: Generate TTS Voiceover ───────────────────────────────────────────

@app.post("/generate-voiceover")
async def gen_voiceover(
    session_id: str = Form(...),
    voice: str = Form("21m00Tcm4TlvDq8ikWAM"),
    model_id: str = Form("eleven_multilingual_v2"),
):
    """Generate TTS voiceover for all VO segments. Returns job_id for SSE streaming."""
    session = _load_session(session_id)
    if not session or not session.get("script"):
        return {"error": "No script found. Run /generate-script first."}

    job_id = str(uuid.uuid4())[:8]
    jobs[job_id] = {"status": "running", "queue": Queue(), "result": None, "error": None}

    def _run():
        q = jobs[job_id]["queue"]
        try:
            def progress(msg, pct=None):
                event = {"type": "progress", "message": msg}
                if pct is not None:
                    event["pct"] = pct
                q.put(event)

            dirs = ensure_session_dirs(session_id)
            vo_data = generate_tts_voiceovers(
                script=session["script"],
                voice=voice,
                model_id=model_id,
                progress_callback=progress,
                output_dir=dirs["voiceover_dir"],
            )
            session["vo_data"] = vo_data
            _save_session(session_id)
            jobs[job_id]["result"] = vo_data
            jobs[job_id]["status"] = "completed"
            q.put({"type": "done", "data": vo_data})
        except Exception as e:
            jobs[job_id]["status"] = "failed"
            jobs[job_id]["error"] = str(e)
            q.put({"type": "error", "message": str(e)})

    executor.submit(_run)
    return {"job_id": job_id}


# ── Step 5b: Upload Voiceover ────────────────────────────────────────────────

@app.post("/upload-voiceover")
async def upload_vo(
    session_id: str = Form(...),
    segment_id: int = Form(...),
    file: UploadFile = File(...),
):
    """Upload a recorded voiceover for a specific segment."""
    session = _load_session(session_id)
    if not session:
        return {"error": "Session not found."}

    # Save uploaded file to session-scoped dir
    dirs = ensure_session_dirs(session_id)
    file_path = os.path.join(dirs["voiceover_dir"], f"vo_seg_{segment_id:03d}.mp3")
    with open(file_path, "wb") as f:
        content = await file.read()
        f.write(content)

    vo_text = ""
    script = session.get("script", {})
    for seg in script.get("segments", []):
        if seg["segment_id"] == segment_id:
            vo_text = seg.get("vo_text", "")
            break

    result = register_uploaded_voiceover(segment_id, file_path, vo_text, output_dir=dirs["voiceover_dir"])
    if result:
        return {"status": "ok", "duration_sec": result["duration_sec"]}
    return {"error": "Invalid audio file"}


# ── Step 5c: Regenerate Single VO Segment ──────────────────────────────────

@app.post("/regenerate-voiceover")
async def regen_vo(
    session_id: str = Form(...),
    segment_id: int = Form(...),
    vo_text: str = Form(...),
    voice: str = Form("21m00Tcm4TlvDq8ikWAM"),
    model_id: str = Form("eleven_multilingual_v2"),
):
    """Regenerate TTS voiceover for a single segment."""
    from ai33_tts import generate_voiceover

    session = _load_session(session_id)
    if not session:
        return {"error": "Session not found."}

    dirs = ensure_session_dirs(session_id)
    output_path = os.path.join(dirs["voiceover_dir"], f"vo_seg_{segment_id:03d}.mp3")

    def _gen():
        return generate_voiceover(
            text=vo_text,
            output_path=output_path,
            voice_id=voice,
            model_id=model_id,
        )

    audio_path = await asyncio.get_event_loop().run_in_executor(executor, _gen)

    if audio_path and os.path.exists(audio_path):
        from clip_extractor import get_clip_duration
        duration = get_clip_duration(audio_path)

        # Update vo_data in session
        vo_data = session.get("vo_data", {"voiceover_segments": [], "total_vo_duration_sec": 0})
        vo_segs = vo_data.get("voiceover_segments", [])
        # Replace or add the segment
        found = False
        for vs in vo_segs:
            if vs["segment_id"] == segment_id:
                vs["audio_path"] = audio_path
                vs["duration_sec"] = duration
                vs["vo_text"] = vo_text
                found = True
                break
        if not found:
            vo_segs.append({
                "segment_id": segment_id,
                "type": "voiceover",
                "audio_path": audio_path,
                "duration_sec": duration,
                "vo_text": vo_text,
            })
        vo_data["voiceover_segments"] = vo_segs
        session["vo_data"] = vo_data
        _save_session(session_id)

        return {"status": "ok", "duration_sec": duration, "audio_path": audio_path}
    return {"error": "TTS generation failed"}


# ── Step 5d: Upload Background Music ──────────────────────────────────────

@app.post("/upload-music")
async def upload_music(
    session_id: str = Form(...),
    file: UploadFile = File(...),
):
    """Upload a background music file."""
    session = _load_session(session_id)
    if not session:
        return {"error": "Session not found."}

    dirs = ensure_session_dirs(session_id)
    music_path = os.path.join(dirs["session_dir"], "bg_music.mp3")
    with open(music_path, "wb") as f:
        content = await file.read()
        f.write(content)

    session["music_path"] = music_path
    _save_session(session_id)
    return {"status": "ok", "filename": file.filename}


@app.post("/remove-music")
async def remove_music(session_id: str = Form(...)):
    """Remove background music selection."""
    session = _load_session(session_id)
    if not session:
        return {"error": "Session not found."}
    session.pop("music_path", None)
    _save_session(session_id)
    return {"status": "ok"}


# ── Step 6: Assemble Final Video ─────────────────────────────────────────────

@app.post("/assemble")
async def assemble(session_id: str = Form(...), vo_mode: str = Form("tts"), transitions: str = Form("0"), subtitles: str = Form("0")):
    """Assemble the final commentary video. Returns job_id for SSE streaming."""
    session = _load_session(session_id)
    if not session or not session.get("script"):
        return {"error": "No script found."}

    music_path = session.get("music_path")
    use_transitions = transitions == "1"
    use_subtitles = subtitles == "1"

    job_id = str(uuid.uuid4())[:8]
    jobs[job_id] = {"status": "running", "queue": Queue(), "result": None, "error": None}

    def _run():
        q = jobs[job_id]["queue"]
        try:
            def progress(msg, pct=None):
                event = {"type": "progress", "message": msg}
                if pct is not None:
                    event["pct"] = pct
                q.put(event)

            # Diagnostic: log what data is being passed to pipeline
            _hd = session.get("heygen_data")
            _hd_success = _hd.get("successful", 0) if isinstance(_hd, dict) else 0
            _hd_total = _hd.get("total", 0) if isinstance(_hd, dict) else 0
            _vd = session.get("vo_data")
            progress(f"[Assemble] vo_mode={vo_mode}, heygen_data={_hd_success}/{_hd_total} successful, vo_data={'present' if _vd else 'None'}")

            # Pass heygen_data if mode is heygen/heygen_browser OR if we have successful segments
            pass_heygen = _hd if (vo_mode in ("heygen", "heygen_browser") or _hd_success > 0) else None
            if not pass_heygen and _hd:
                progress(f"[Assemble] WARNING: heygen_data exists but NOT passed (vo_mode={vo_mode}, successful={_hd_success})")

            result = run_pipeline(
                youtube_url=session["youtube_url"],
                stance_id=session.get("stance_id", "balanced"),
                music_path=music_path,
                progress_callback=progress,
                transcript_data=session.get("transcript_data"),
                analysis=session.get("analysis"),
                script=session.get("script"),
                vo_data=session.get("vo_data"),
                heygen_data=pass_heygen,
                session_id=session_id,
                transitions=use_transitions,
                subtitles=use_subtitles,
            )

            if result:
                session["status"] = "completed"
                video_rel = f"{session_id}/{os.path.basename(result)}"
                session["final_video"] = video_rel
                _save_session(session_id)
                jobs[job_id]["result"] = result
                jobs[job_id]["status"] = "completed"
                q.put({"type": "done", "video": video_rel})
            else:
                jobs[job_id]["status"] = "failed"
                jobs[job_id]["error"] = "Assembly failed"
                q.put({"type": "error", "message": "Assembly failed"})
        except Exception as e:
            jobs[job_id]["status"] = "failed"
            jobs[job_id]["error"] = str(e)
            q.put({"type": "error", "message": str(e)})

    executor.submit(_run)
    return {"job_id": job_id}


# ── HeyGen Avatar Integration ───────────────────────────────────────────────

@app.get("/heygen/avatars")
async def heygen_avatars():
    """List available HeyGen avatars (free/Avatar III only)."""
    if not HEYGEN_API_KEY:
        return {"error": "HEYGEN_API_KEY not configured"}
    avatars = await asyncio.get_event_loop().run_in_executor(executor, list_avatars)
    return {"avatars": avatars}


@app.get("/heygen/voices")
async def heygen_voices():
    """List available HeyGen English voices."""
    if not HEYGEN_API_KEY:
        return {"error": "HEYGEN_API_KEY not configured"}
    voices = await asyncio.get_event_loop().run_in_executor(executor, list_voices)
    return {"voices": voices}


@app.post("/generate-heygen")
async def gen_heygen(
    session_id: str = Form(...),
    avatar_id: str = Form(...),
    voice_id: str = Form(...),
):
    """Generate HeyGen avatar videos for all commentary segments. Returns job_id for SSE."""
    session = _load_session(session_id)
    if not session or not session.get("script"):
        return {"error": "No script found. Run /generate-script first."}
    if not HEYGEN_API_KEY:
        return {"error": "HEYGEN_API_KEY not configured"}

    job_id = str(uuid.uuid4())[:8]
    jobs[job_id] = {"status": "running", "queue": Queue(), "result": None, "error": None}

    def _run():
        q = jobs[job_id]["queue"]
        try:
            def progress(msg, pct=None):
                event = {"type": "progress", "message": msg}
                if pct is not None:
                    event["pct"] = pct
                q.put(event)

            dirs = ensure_session_dirs(session_id)
            heygen_data = generate_all_commentary_segments(
                script=session["script"],
                avatar_id=avatar_id,
                voice_id=voice_id,
                progress_callback=progress,
                output_dir=dirs["heygen_clips_dir"],
            )

            session["heygen_data"] = heygen_data
            session["heygen_avatar_id"] = avatar_id
            session["heygen_voice_id"] = voice_id
            _save_session(session_id)
            jobs[job_id]["result"] = heygen_data
            jobs[job_id]["status"] = "completed"
            q.put({"type": "done", "data": heygen_data})
        except Exception as e:
            jobs[job_id]["status"] = "failed"
            jobs[job_id]["error"] = str(e)
            q.put({"type": "error", "message": str(e)})

    executor.submit(_run)
    return {"job_id": job_id}


# ── HeyGen Browser Automation (uses subscription credits) ────────────────────

@app.post("/generate-heygen-browser")
async def gen_heygen_browser(
    session_id: str = Form(...),
    avatar_name: str = Form("default"),
):
    """Generate HeyGen avatar videos via browser automation (uses subscription credits).
    Opens a browser window — first time requires manual login. Returns job_id for SSE."""
    session = _load_session(session_id)
    if not session or not session.get("script"):
        return {"error": "No script found. Run /generate-script first."}

    job_id = str(uuid.uuid4())[:8]
    jobs[job_id] = {"status": "running", "queue": Queue(), "result": None, "error": None}

    def _run():
        q = jobs[job_id]["queue"]
        try:
            def progress(msg, pct=None):
                event = {"type": "progress", "message": msg}
                if pct is not None:
                    event["pct"] = pct
                q.put(event)

            def on_seg_complete(interim_data, seg_result):
                session["heygen_data"] = interim_data
                _save_session(session_id)
                status = "done" if seg_result["success"] else "failed"
                q.put({
                    "type": "segment_complete",
                    "segment_id": seg_result["segment_id"],
                    "success": seg_result["success"],
                    "total": interim_data["total"],
                    "successful": interim_data["successful"],
                    "failed": interim_data["failed"],
                })

            dirs = ensure_session_dirs(session_id)
            heygen_data = generate_all_segments_browser_sync(
                script=session["script"],
                avatar_name=avatar_name,
                progress_callback=progress,
                existing_heygen_data=session.get("heygen_data"),
                on_segment_complete=on_seg_complete,
                output_dir=dirs["heygen_clips_dir"],
            )

            session["heygen_data"] = heygen_data
            _save_session(session_id)
            jobs[job_id]["result"] = heygen_data
            jobs[job_id]["status"] = "completed"
            q.put({"type": "done", "data": heygen_data})
        except Exception as e:
            jobs[job_id]["status"] = "failed"
            jobs[job_id]["error"] = str(e)
            q.put({"type": "error", "message": str(e)})

    executor.submit(_run)
    return {"job_id": job_id}


# ── Batch Process (single URL, full auto pipeline) ──────────────────────────

@app.post("/batch-process")
async def batch_process(
    youtube_url: str = Form(...),
    tone_preset: str = Form(None),
):
    """Run the full pipeline for a single URL (used by batch queue).
    Auto-selects 'balanced' stance. Returns job_id for SSE streaming."""
    job_id = str(uuid.uuid4())[:8]
    jobs[job_id] = {"status": "running", "queue": Queue(), "result": None, "error": None}

    def _run():
        q = jobs[job_id]["queue"]
        try:
            def progress(msg, pct=None):
                event = {"type": "progress", "message": msg}
                if pct is not None:
                    event["pct"] = pct
                q.put(event)

            # Run full pipeline with balanced stance, optional tone preset
            result = run_pipeline(
                youtube_url=youtube_url,
                stance_id="balanced",
                progress_callback=progress,
            )

            if result:
                jobs[job_id]["result"] = result
                jobs[job_id]["status"] = "completed"
                q.put({"type": "done", "video": os.path.basename(result)})
            else:
                jobs[job_id]["status"] = "failed"
                jobs[job_id]["error"] = "Pipeline failed"
                q.put({"type": "error", "message": "Pipeline failed"})
        except Exception as e:
            jobs[job_id]["status"] = "failed"
            jobs[job_id]["error"] = str(e)
            q.put({"type": "error", "message": str(e)})

    executor.submit(_run)
    return {"job_id": job_id}


# ── SSE Progress Stream ─────────────────────────────────────────────────────

@app.get("/stream/{job_id}")
async def stream_progress(job_id: str):
    """Server-Sent Events stream for job progress."""
    job = jobs.get(job_id)
    if not job:
        return {"error": "Job not found"}

    async def event_generator():
        q = job["queue"]
        while True:
            try:
                event = q.get(timeout=1)
                yield f"data: {json.dumps(event)}\n\n"
                if event.get("type") in ("done", "error"):
                    break
            except Empty:
                yield f"data: {json.dumps({'type': 'heartbeat'})}\n\n"
                if job["status"] in ("completed", "failed"):
                    break
            await asyncio.sleep(0.1)

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.get("/status/{job_id}")
async def job_status(job_id: str):
    job = jobs.get(job_id)
    if not job:
        return {"error": "Job not found"}
    return {
        "status": job["status"],
        "result": job.get("result"),
        "error": job.get("error"),
    }


# ── Voiceover audio serving ──────────────────────────────────────────────────

@app.get("/voiceover-audio/{segment_id}")
async def serve_voiceover(segment_id: int, session_id: str = None):
    """Serve a generated voiceover audio file for preview."""
    if session_id:
        dirs = get_session_dirs(session_id)
        path = os.path.join(dirs["voiceover_dir"], f"vo_seg_{segment_id:03d}.mp3")
    else:
        path = os.path.join(VOICEOVER_DIR, f"vo_seg_{segment_id:03d}.mp3")
    if os.path.exists(path):
        return FileResponse(path, media_type="audio/mpeg")
    return {"error": "Voiceover not found"}


# ── Video serving ────────────────────────────────────────────────────────────

@app.get("/video/{filename:path}")
async def serve_video(filename: str):
    path = os.path.join(OUTPUT_DIR, filename)
    if os.path.exists(path):
        return FileResponse(
            path,
            media_type="video/mp4",
            headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
        )
    return {"error": "Video not found"}


@app.get("/videos")
async def list_videos():
    videos = []
    for f in os.listdir(OUTPUT_DIR):
        if f.endswith(".mp4"):
            path = os.path.join(OUTPUT_DIR, f)
            videos.append({
                "filename": f,
                "size_mb": round(os.path.getsize(path) / (1024 * 1024), 1),
            })
    return {"videos": videos}


# ── Run server ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
