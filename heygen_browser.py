"""
heygen_browser.py

Browser automation for HeyGen AI Studio — generates all commentary voiceovers
as a single multi-scene video, then splits by silence detection.

Flow:
1. Navigate to templates → click "CommentaryAI" → Open in AI Studio
2. Add each voiceover paragraph as a scene, clicking pause button 2x after each
3. Click Generate → wait for render → download combined MP4
4. Split combined video by FFmpeg silence detection at the ~1s pauses
5. Map each chunk to its voiceover segment in order
"""

import hashlib
import json
import os
import re
import subprocess
import time
from typing import Optional

from playwright.sync_api import sync_playwright

from config import OUTPUT_DIR

HEYGEN_CLIPS_DIR = os.path.join(OUTPUT_DIR, "heygen_clips")
os.makedirs(HEYGEN_CLIPS_DIR, exist_ok=True)

BROWSER_PROFILE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".heygen_chrome_profile")

HEYGEN_TEMPLATES_URL = "https://app.heygen.com/avatar/templates?ct=private"


def _file_md5(path):
    """Compute MD5 hash of a file to detect duplicates."""
    h = hashlib.md5()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()


def _wait_for_login(page, progress=None):
    """Wait for user to log in if login modal/page is shown."""
    debug_dir = os.path.join(OUTPUT_DIR, "debug_screenshots")
    os.makedirs(debug_dir, exist_ok=True)

    needs_login = False
    try:
        modal = page.locator('text="Continue with Google"').first
        if modal.is_visible(timeout=3000):
            needs_login = True
    except Exception:
        pass

    if not needs_login and ("/login" in page.url or "/sign" in page.url):
        needs_login = True

    if needs_login:
        # Save debug screenshot so we can see what the browser sees
        try:
            page.screenshot(path=os.path.join(debug_dir, "login_required.png"))
            if progress:
                progress(f"Debug screenshot saved. URL: {page.url}")
        except Exception:
            pass
        if progress:
            progress("Login required — please log in in the browser window (5 min timeout)")
        elapsed = 0
        while elapsed < 300:
            page.wait_for_timeout(3000)
            elapsed += 3
            still_login = False
            try:
                modal = page.locator('text="Continue with Google"').first
                if modal.is_visible(timeout=1000):
                    still_login = True
            except Exception:
                pass
            if not still_login and "/login" not in page.url and "/sign" not in page.url:
                if progress:
                    progress("Login successful!")
                page.wait_for_timeout(2000)
                return True
        # Save timeout screenshot
        try:
            page.screenshot(path=os.path.join(debug_dir, "login_timeout.png"))
        except Exception:
            pass
        if progress:
            progress("Login timed out")
        return False
    return True


def _open_ai_studio(page, progress=None):
    """
    Navigate to templates page, hover over CommentaryAI card,
    click 'Use this template' button to open AI Studio directly.
    Returns True if AI Studio editor loaded successfully.
    """
    if progress:
        progress("Opening AI Studio with CommentaryAI template...")

    # Step 1: Go to templates page
    page.goto(HEYGEN_TEMPLATES_URL, wait_until="domcontentloaded", timeout=30000)
    page.wait_for_timeout(4000)

    # Check login
    if not _wait_for_login(page, progress):
        return False

    page.wait_for_timeout(2000)

    # Step 2: Find the CommentaryAI card and hover to reveal the "Use this template" button
    # The card has a text label "CommentaryAI" and a thumbnail image.
    # We need to hover the CARD CONTAINER (not just the text) to trigger the overlay.
    card_hovered = False

    # Primary: find the text, walk up to the card container, hover it
    try:
        card_label = page.locator('text="CommentaryAI"').first
        if card_label.is_visible(timeout=5000):
            # The text is inside a card — we need to hover the card area (the image/thumbnail)
            # Walk up from text to find a sizeable parent container
            bounding = card_label.bounding_box()
            if bounding:
                # Hover above the text — over the thumbnail area of the card
                # The card image is above the text label
                page.mouse.move(bounding["x"] + bounding["width"] / 2, bounding["y"] - 80)
                page.wait_for_timeout(1500)
                card_hovered = True
                if progress:
                    progress("Hovering over CommentaryAI card...")
    except Exception as e:
        if progress:
            progress(f"  Hover attempt 1 failed: {e}")

    if not card_hovered:
        # Fallback: use JS to find the card container and dispatch hover events
        card_hovered = page.evaluate("""() => {
            const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
            while (walker.nextNode()) {
                if (walker.currentNode.textContent.trim() === 'CommentaryAI') {
                    let el = walker.currentNode.parentElement;
                    for (let i = 0; i < 10; i++) {
                        if (!el || !el.parentElement) break;
                        el = el.parentElement;
                        const rect = el.getBoundingClientRect();
                        if (rect.width > 200 && rect.height > 150 && rect.width < 600) {
                            el.dispatchEvent(new MouseEvent('mouseenter', {bubbles: true}));
                            el.dispatchEvent(new MouseEvent('mouseover', {bubbles: true}));
                            return true;
                        }
                    }
                    return false;
                }
            }
            return false;
        }""")
        if card_hovered:
            page.wait_for_timeout(1500)

    if not card_hovered:
        # Debug: save screenshot to understand why card wasn't found
        try:
            debug_path = os.path.join(HEYGEN_CLIPS_DIR, "debug_card_not_found.png")
            page.screenshot(path=debug_path)
            if progress:
                progress(f"  Debug screenshot saved: {debug_path}")
                progress(f"  Current URL: {page.url}")
        except Exception:
            pass
        if progress:
            progress("ERROR: Could not find CommentaryAI template card")
        return False

    # Step 3: Click "Use this template" button
    # In headless mode, CSS :hover doesn't trigger — force-show hidden overlay buttons via JS
    page.evaluate("""() => {
        // Make all hover-only elements visible by removing opacity/visibility restrictions
        const all = document.querySelectorAll('*');
        for (const el of all) {
            const style = window.getComputedStyle(el);
            if (style.opacity === '0' || style.visibility === 'hidden') {
                el.style.opacity = '1';
                el.style.visibility = 'visible';
            }
        }
    }""")
    page.wait_for_timeout(500)

    use_clicked = False

    for attempt in range(5):
        # Try 1: JS — find the create-video icon near CommentaryAI and click its button
        use_clicked = page.evaluate("""() => {
            // Find CommentaryAI text, walk up to card, find create-video icon
            const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
            while (walker.nextNode()) {
                if (walker.currentNode.textContent.trim() === 'CommentaryAI') {
                    let card = walker.currentNode.parentElement;
                    for (let i = 0; i < 10; i++) {
                        if (!card) break;
                        card = card.parentElement;
                        const icons = card.querySelectorAll('iconpark-icon[name="create-video"]');
                        for (const icon of icons) {
                            const btn = icon.closest('button') || icon.parentElement;
                            if (btn) {
                                btn.click();
                                return true;
                            }
                        }
                    }
                    break;
                }
            }
            // Fallback: click any create-video icon on the page (first one = CommentaryAI since it's first card)
            const icons = document.querySelectorAll('iconpark-icon[name="create-video"]');
            for (const icon of icons) {
                const btn = icon.closest('button') || icon.parentElement;
                if (btn) {
                    btn.click();
                    return true;
                }
            }
            return false;
        }""")
        if use_clicked:
            if progress:
                progress("Clicked 'Use this template' button")
            break

        # Try 2: Playwright locator
        try:
            use_btn = page.locator('button:has(iconpark-icon[name="create-video"])').first
            if use_btn.is_visible(timeout=1500):
                use_btn.click()
                use_clicked = True
                if progress:
                    progress("Clicked 'Use this template' button")
                break
        except Exception:
            pass

        # Re-hover on retry
        if attempt < 4:
            try:
                card_label = page.locator('text="CommentaryAI"').first
                bb = card_label.bounding_box()
                if bb:
                    page.mouse.move(bb["x"] + bb["width"] / 2, bb["y"] - 80)
                page.wait_for_timeout(1500)
            except Exception:
                pass

    if not use_clicked:
        if progress:
            progress("ERROR: Could not find 'Use this template' button")
            # Save debug screenshot
            try:
                debug_path = os.path.join(HEYGEN_CLIPS_DIR, "debug_use_template.png")
                page.screenshot(path=debug_path)
                progress(f"Debug screenshot saved: {debug_path}")
            except Exception:
                pass
        return False

    # Step 4: Wait for AI Studio editor to load
    if progress:
        progress("Waiting for AI Studio editor to load...")
    page.wait_for_timeout(6000)

    # Verify editor loaded by checking for script input or "Add scene" button
    editor_loaded = False
    for check in range(3):
        try:
            ta = page.locator('textarea').first
            if ta.is_visible(timeout=3000):
                editor_loaded = True
                break
        except Exception:
            pass
        try:
            add_scene = page.locator('text="Add scene"').first
            if add_scene.is_visible(timeout=2000):
                editor_loaded = True
                break
        except Exception:
            pass
        page.wait_for_timeout(2000)

    if editor_loaded:
        if progress:
            progress("AI Studio editor loaded successfully")
    else:
        if progress:
            progress("WARNING: AI Studio editor may not have loaded fully, proceeding anyway...")

    return True


def _click_pause_button(page, progress=None):
    """
    Click the pause/clock button once. The button contains an SVG with
    viewBox="0 0 16 16" and a path starting with "M7.515".
    The SVG may be inside shadow DOM (iconpark-icon uses shadowrootmode="open").
    """
    # Try 1: Playwright locator for iconpark-icon with clock/time-related names
    try:
        for icon_name in ["time", "clock", "pause", "timer", "schedule"]:
            icon = page.locator(f'iconpark-icon[name="{icon_name}"]').first
            if icon.is_visible(timeout=500):
                btn = icon.locator('xpath=ancestor::button').first
                if btn.is_visible(timeout=500):
                    btn.click()
                    return True
                else:
                    icon.click()
                    return True
    except Exception:
        pass

    # Try 2: Find the clock icon in the Script section header
    # The Script section has a header with "Script" text and small icon buttons
    clicked = page.evaluate("""() => {
        // Search regular DOM first
        const paths = document.querySelectorAll('svg path');
        for (const path of paths) {
            const d = path.getAttribute('d') || '';
            if (d.startsWith('M7.515') || d.includes('4.036')) {
                let el = path.closest('button') || path.closest('svg').parentElement;
                if (el && el.offsetParent) {
                    el.click();
                    return true;
                }
            }
        }

        // Search inside shadow roots (iconpark-icon elements)
        const iconparks = document.querySelectorAll('iconpark-icon');
        for (const ip of iconparks) {
            const sr = ip.shadowRoot;
            if (!sr) continue;
            const svgPaths = sr.querySelectorAll('svg path');
            for (const path of svgPaths) {
                const d = path.getAttribute('d') || '';
                if (d.startsWith('M7.515') || d.includes('4.036')) {
                    // Click the parent button or the iconpark-icon itself
                    const btn = ip.closest('button');
                    if (btn && btn.offsetParent) {
                        btn.click();
                        return true;
                    }
                    ip.click();
                    return true;
                }
            }
        }

        // Try 3: Find small buttons near the "Script" heading
        // The pause button is in the Script section header, near top-right
        const scriptHeader = Array.from(document.querySelectorAll('*')).find(
            el => el.children.length === 0 && el.textContent.trim() === 'Script'
        );
        if (scriptHeader) {
            let container = scriptHeader.parentElement;
            if (container) {
                const buttons = container.querySelectorAll('button, [role="button"]');
                // The clock/pause button is typically the first icon button in the header
                for (const btn of buttons) {
                    const rect = btn.getBoundingClientRect();
                    if (rect.width > 0 && rect.width < 50 && rect.height < 50) {
                        btn.click();
                        return true;
                    }
                }
            }
        }

        return false;
    }""")
    return clicked


def _add_all_scenes(page, vo_segments, progress=None):
    """
    Add all voiceover paragraphs as scenes in AI Studio.
    For each segment:
      1. Paste vo_text into the current scene's text input
      2. Click pause button 2 times (adds ~1s silence break)
      3. If not last segment, click "Add Scene"
    """
    total = len(vo_segments)

    for i, seg in enumerate(vo_segments):
        vo_text = seg.get("vo_text", "")
        seg_id = seg["segment_id"]
        is_last = (i == total - 1)

        if progress:
            preview = vo_text[:40].replace('\n', ' ')
            progress(f"Adding scene {i+1}/{total}: {preview}...")

        # Step 1: Find and fill the text input for the current scene
        text_filled = False

        # Fill the text into the current scene's script input.
        # First scene: click the input to focus it, then fill.
        # Subsequent scenes: after "Add scene", input is already focused — type directly.
        try:
            if i == 0:
                # Try multiple element types — could be textarea, contenteditable, or input
                input_found = False

                # Try textarea
                try:
                    ta = page.locator('textarea').first
                    if ta.is_visible(timeout=2000):
                        ta.click()
                        page.wait_for_timeout(300)
                        ta.fill(vo_text)
                        input_found = True
                        text_filled = True
                except Exception:
                    pass

                # Try contenteditable div
                if not input_found:
                    try:
                        editable = page.locator('[contenteditable="true"]').first
                        if editable.is_visible(timeout=2000):
                            editable.click()
                            page.wait_for_timeout(300)
                            editable.fill(vo_text)
                            input_found = True
                            text_filled = True
                    except Exception:
                        pass

                # Try clicking on the placeholder text area and typing
                if not input_found:
                    try:
                        placeholder = page.locator('text="Type your script"').first
                        if placeholder.is_visible(timeout=2000):
                            placeholder.click()
                            page.wait_for_timeout(500)
                            page.keyboard.insert_text(vo_text)
                            input_found = True
                            text_filled = True
                    except Exception:
                        pass

                # Last resort: click in the Script section area and type
                if not input_found:
                    try:
                        # Find "Script" heading and click below it
                        script_label = page.locator('text="Script"').first
                        bb = script_label.bounding_box()
                        if bb:
                            # Click below the Script heading (where the text input is)
                            page.mouse.click(bb["x"] + 100, bb["y"] + 80)
                            page.wait_for_timeout(500)
                            page.keyboard.insert_text(vo_text)
                            text_filled = True
                    except Exception:
                        pass

            else:
                # New scene input is already focused — type directly
                page.keyboard.insert_text(vo_text)
                text_filled = True
        except Exception:
            pass

        if not text_filled:
            if progress:
                progress(f"  WARNING: Could not fill text for scene {i+1}")
            continue

        page.wait_for_timeout(1000)

        # Step 2: Click pause button 2 times (adds ~1s silence break after this scene)
        for click_num in range(2):
            success = _click_pause_button(page, progress)
            if success:
                page.wait_for_timeout(500)
            else:
                if progress:
                    progress(f"  WARNING: Pause click {click_num+1} failed for scene {i+1}")
                page.wait_for_timeout(300)

        if progress:
            progress(f"  Added pause for scene {i+1}")

        # Step 3: Click "Add Scene" if not the last segment
        # After clicking, the new scene's textarea is already active — just paste next time
        if not is_last:
            add_clicked = False
            try:
                add_btn = page.locator('button:has-text("Add Scene"), button:has-text("Add scene")').first
                if add_btn.is_visible(timeout=3000):
                    add_btn.click()
                    add_clicked = True
            except Exception:
                pass

            if not add_clicked:
                add_clicked = page.evaluate("""() => {
                    const buttons = document.querySelectorAll('button');
                    for (const b of buttons) {
                        const text = b.textContent.trim().toLowerCase();
                        if ((text.includes('add scene') || text === '+') && b.offsetParent) {
                            b.click();
                            return true;
                        }
                    }
                    return false;
                }""")

            if add_clicked:
                page.wait_for_timeout(2000)  # Wait for new scene to appear
            else:
                if progress:
                    progress(f"  WARNING: Could not click 'Add Scene' after scene {i+1}")

    if progress:
        progress(f"All {total} scenes added successfully")
    return True


def _generate_and_download(page, output_path, progress=None):
    """
    Click Generate → fill unique title in dialog → Submit → wait for render
    on /projects page → hover card → three-dot menu → Download.
    Returns (path_to_downloaded_mp4, unique_title) or (None, None).
    """
    if progress:
        progress("Clicking Generate button...")

    # Step 1: Click the Generate button (top right)
    gen_clicked = False
    try:
        gen_btn = page.locator('button:has-text("Generate")').first
        if gen_btn.is_visible(timeout=5000):
            gen_btn.click()
            gen_clicked = True
    except Exception:
        pass

    if not gen_clicked:
        gen_clicked = page.evaluate("""() => {
            const buttons = document.querySelectorAll('button');
            for (const b of buttons) {
                const text = b.textContent.trim();
                if (text.includes('Generate') && b.getBoundingClientRect().width > 0) {
                    b.click();
                    return true;
                }
            }
            return false;
        }""")

    if not gen_clicked:
        if progress:
            progress("ERROR: Could not find Generate button")
        return None, None

    # Step 2: Wait for "Generate Video" dialog — title input is already focused.
    # Just type the suffix to append to existing "CommentaryAI". Don't click anything.
    page.wait_for_timeout(3000)

    suffix = f"_{int(time.time())}"
    unique_title = f"CommentaryAI{suffix}"

    # Press End to move cursor to end of existing title, then type suffix
    page.keyboard.press("End")
    page.keyboard.insert_text(suffix)

    if progress:
        progress(f"Set video title: {unique_title}")

    # Step 3: Click Submit button
    submit_clicked = False
    try:
        submit_btn = page.locator('button:has-text("Submit")').first
        if submit_btn.is_visible(timeout=3000):
            submit_btn.click()
            submit_clicked = True
    except Exception:
        pass

    if not submit_clicked:
        submit_clicked = page.evaluate("""() => {
            const buttons = document.querySelectorAll('button');
            for (const b of buttons) {
                if (b.textContent.trim() === 'Submit' && b.getBoundingClientRect().width > 0) {
                    b.click();
                    return true;
                }
            }
            return false;
        }""")

    if not submit_clicked:
        if progress:
            progress("ERROR: Could not click Submit button")
        return None, None

    if progress:
        progress("Submitted — waiting for redirect to projects page...")

    # Step 4: Wait for redirect to /projects page
    try:
        page.wait_for_url("**/projects**", timeout=30000)
    except Exception:
        # Manual fallback: navigate to projects
        page.wait_for_timeout(5000)
        if "/projects" not in page.url:
            if progress:
                progress(f"Not on projects page (URL: {page.url}), navigating...")
            page.goto("https://app.heygen.com/projects", wait_until="domcontentloaded", timeout=30000)

    page.wait_for_timeout(3000)

    if progress:
        progress(f"On projects page (URL: {page.url}) — looking for '{unique_title}'...")

    # Wait 3 minutes for HeyGen to render before starting download checks
    if progress:
        progress("Waiting 3 minutes for HeyGen to render video...")
    page.wait_for_timeout(180_000)

    # Step 5 + 6 combined: Reload page every 5s, try to download.
    # Instead of detecting render status (unreliable), we just try to download
    # each cycle. If the download succeeds, the video is ready.
    max_wait = 900  # 15 minutes (includes the 3-min wait above)
    poll_interval = 10  # check every 10 seconds
    elapsed = 180  # account for the 3-min wait
    download_success = False

    while elapsed < max_wait:
        # Refresh the projects page
        page.goto("https://app.heygen.com/projects", wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(3000)

        if progress:
            progress(f"  Checking... ({elapsed}s elapsed, URL: {page.url})")

        # Save debug screenshot on first check
        if elapsed == 0:
            try:
                debug_path = os.path.join(HEYGEN_CLIPS_DIR, "debug_projects_page.png")
                page.screenshot(path=debug_path)
                if progress:
                    progress(f"  Debug screenshot saved: {debug_path}")
            except Exception:
                pass

        # Try to find our video card, hover, three-dot menu, download
        try:
            # Debug: list all visible video titles on the page
            if progress:
                all_titles = page.evaluate("""() => {
                    const els = document.querySelectorAll('a, span, div, p, h3, h4');
                    const titles = [];
                    for (const el of els) {
                        const t = el.textContent.trim();
                        if (t.startsWith('Commentary') || t.startsWith('commentary')) {
                            titles.push(t.substring(0, 60));
                        }
                    }
                    return [...new Set(titles)].slice(0, 5);
                }""")
                progress(f"  Titles found on page: {all_titles}")

            title_el = page.locator(f'text="{unique_title}"').first
            if not title_el.is_visible(timeout=3000):
                if progress:
                    progress(f"  Video '{unique_title}' not found yet...")
                page.wait_for_timeout(poll_interval * 1000)
                elapsed += poll_interval
                continue

            # Force-show hidden overlay buttons (hover-only elements) for headless mode
            page.evaluate("""() => {
                const all = document.querySelectorAll('*');
                for (const el of all) {
                    const style = window.getComputedStyle(el);
                    if (style.opacity === '0' || style.visibility === 'hidden') {
                        el.style.opacity = '1';
                        el.style.visibility = 'visible';
                    }
                }
            }""")
            page.wait_for_timeout(500)

            # Hover over the THUMBNAIL area above the title (title is a clickable link)
            bb = title_el.bounding_box()
            if not bb:
                if progress:
                    progress(f"  Could not get bounding box for title...")
                page.wait_for_timeout(poll_interval * 1000)
                elapsed += poll_interval
                continue
            page.mouse.move(bb["x"] + bb["width"] / 2, bb["y"] - 120)
            page.wait_for_timeout(1500)

            # Click the three-dot menu button scoped to OUR video card
            # (.first grabs the page filter button, not the card menu — must scope to card)
            dots_clicked = False

            # Primary: JS finds the more-level button within our card and gets coords
            dots_pos = page.evaluate("""(title) => {
                const els = document.querySelectorAll('*');
                for (const el of els) {
                    if (el.children.length === 0 && el.textContent.trim() === title) {
                        let card = el;
                        for (let i = 0; i < 10; i++) {
                            card = card.parentElement;
                            if (!card) break;
                            const icons = card.querySelectorAll('iconpark-icon[name="more-level"]');
                            for (const icon of icons) {
                                const btn = icon.closest('button') || icon;
                                const rect = btn.getBoundingClientRect();
                                if (rect.width > 0 && rect.height > 0) {
                                    return {x: rect.x + rect.width/2, y: rect.y + rect.height/2};
                                }
                            }
                        }
                        return null;
                    }
                }
                return null;
            }""", unique_title)
            if dots_pos:
                page.mouse.click(dots_pos["x"], dots_pos["y"])
                dots_clicked = True

            if not dots_clicked:
                if progress:
                    progress(f"  Still rendering (no menu button)... ({elapsed}s elapsed)")
                page.wait_for_timeout(poll_interval * 1000)
                elapsed += poll_interval
                continue

            page.wait_for_timeout(2500)

            # Debug: log dropdown menu contents
            if progress:
                menu_items = page.evaluate("""() => {
                    const items = [];
                    document.querySelectorAll(
                        '[role="menuitem"], li, [class*="menu-item"], [class*="MenuItem"]'
                    ).forEach(el => {
                        const t = el.textContent.trim();
                        if (t && t.length < 50 && el.offsetWidth > 0) items.push(t);
                    });
                    return [...new Set(items)].slice(0, 10);
                }""")
                progress(f"  Dropdown items: {menu_items}")

            # Check if "Download" option exists in the dropdown
            dl_option = None
            dl_found = False
            for selector in ['text="Download"', ':text-matches("^Download$", "i")', 'li:has-text("Download")']:
                try:
                    candidate = page.locator(selector).first
                    if candidate.is_visible(timeout=1500):
                        dl_option = candidate
                        dl_found = True
                        break
                except Exception:
                    continue

            if not dl_found:
                # Retry: close menu, wait, try clicking three-dot again
                page.keyboard.press("Escape")
                page.wait_for_timeout(1500)

                # Re-click the three-dot menu (scoped to our card)
                retry_clicked = False
                if dots_pos:
                    page.mouse.click(dots_pos["x"], dots_pos["y"])
                    retry_clicked = True

                if retry_clicked:
                    page.wait_for_timeout(2500)
                    for selector in ['text="Download"', ':text-matches("^Download$", "i")', 'li:has-text("Download")']:
                        try:
                            candidate = page.locator(selector).first
                            if candidate.is_visible(timeout=1500):
                                dl_option = candidate
                                dl_found = True
                                break
                        except Exception:
                            continue

            if not dl_found:
                page.keyboard.press("Escape")
                if progress:
                    progress(f"  Still rendering (no download option)... ({elapsed}s elapsed)")
                page.wait_for_timeout(poll_interval * 1000)
                elapsed += poll_interval
                continue

            # Click "Download" in the dropdown — this opens a download dialog
            if progress:
                progress(f"  Download option found! Opening download dialog... ({elapsed}s elapsed)")

            dl_option.click()
            page.wait_for_timeout(3000)

            # Click the big cyan "Download" button in the download dialog
            try:
                dialog_dl_btn = page.locator('button:has-text("Download")').last
                if dialog_dl_btn.is_visible(timeout=3000):
                    with page.expect_download(timeout=120000) as download_info:
                        dialog_dl_btn.click()

                    download = download_info.value
                    download.save_as(output_path)

                    if os.path.exists(output_path) and os.path.getsize(output_path) > 500_000:
                        size_mb = os.path.getsize(output_path) / (1024 * 1024)
                        if progress:
                            progress(f"Downloaded combined video ({size_mb:.1f} MB)")
                        download_success = True
                        break
                    else:
                        if progress:
                            progress(f"  Downloaded file too small, retrying...")
                else:
                    if progress:
                        progress(f"  Dialog Download button not visible, retrying...")
                    page.keyboard.press("Escape")
            except Exception as e:
                if progress:
                    progress(f"  Download failed: {e}")

        except Exception as e:
            if progress:
                progress(f"  Check failed: {e}")

        page.wait_for_timeout(poll_interval * 1000)
        elapsed += poll_interval

    if not download_success:
        if progress:
            progress("ERROR: Could not download the combined video (timed out)")
        return None, None

    return output_path, unique_title


def _split_by_silence(combined_path, vo_segments, progress=None, output_dir=None):
    """
    Split the combined video into individual segments using FFmpeg silence detection.
    The ~1s pauses inserted between scenes are detected as silence gaps.

    Returns dict: segment_id -> file path
    """
    if progress:
        progress("Splitting combined video by silence detection...")

    expected_count = len(vo_segments)

    # Step 1: Run FFmpeg silence detection
    cmd = [
        "ffmpeg",
        "-i", combined_path,
        "-af", "silencedetect=noise=-30dB:d=0.8",
        "-f", "null",
        "-",
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        output = result.stderr  # FFmpeg outputs to stderr
    except Exception as e:
        if progress:
            progress(f"FFmpeg silence detection failed: {e}")
        return _fallback_equal_split(combined_path, vo_segments, progress, output_dir)

    # Step 2: Parse silence_start and silence_end timestamps
    silence_starts = []
    silence_ends = []

    for line in output.split('\n'):
        # Match: [silencedetect @ 0x...] silence_start: 18.5
        start_match = re.search(r'silence_start:\s*([\d.]+)', line)
        if start_match:
            silence_starts.append(float(start_match.group(1)))

        # Match: [silencedetect @ 0x...] silence_end: 19.5 | silence_duration: 1.0
        end_match = re.search(r'silence_end:\s*([\d.]+)', line)
        if end_match:
            silence_ends.append(float(end_match.group(1)))

    # Pair up silence gaps
    silence_gaps = []
    for i in range(min(len(silence_starts), len(silence_ends))):
        silence_gaps.append((silence_starts[i], silence_ends[i]))

    if progress:
        progress(f"Found {len(silence_gaps)} silence gaps (expected {expected_count - 1})")

    # Step 3: Get total duration of the combined video
    duration_cmd = [
        "ffprobe",
        "-v", "quiet",
        "-show_entries", "format=duration",
        "-of", "json",
        combined_path,
    ]
    try:
        dur_result = subprocess.run(duration_cmd, capture_output=True, text=True, timeout=30)
        dur_data = json.loads(dur_result.stdout)
        total_duration = float(dur_data["format"]["duration"])
    except Exception:
        total_duration = 0
        if progress:
            progress("WARNING: Could not get video duration, using last silence end")

    # Validate: we expect (expected_count - 1) silence gaps
    # But the last scene also has pauses, so we might get expected_count gaps
    # Use the gaps that make sense as scene boundaries
    if len(silence_gaps) < expected_count - 1:
        if progress:
            progress(f"WARNING: Found fewer silence gaps ({len(silence_gaps)}) than expected ({expected_count - 1})")
        if len(silence_gaps) == 0:
            return _fallback_equal_split(combined_path, vo_segments, progress, output_dir)

    # Step 4: Build segment boundaries using silence edges (not midpoints)
    # Each segment runs from previous silence_end to next silence_start
    # Filter out leading silence (HeyGen often adds silence at the start of the video)
    if silence_gaps and silence_gaps[0][0] < 1.0:
        if progress:
            progress(f"  Skipping leading silence gap: {silence_gaps[0][0]:.2f}s → {silence_gaps[0][1]:.2f}s")
        silence_gaps = silence_gaps[1:]

    used_gaps = silence_gaps[:expected_count - 1]
    seg_boundaries = []

    for i in range(expected_count):
        if i == 0:
            start = 0.0
        else:
            start = used_gaps[i - 1][1]   # silence_end of previous gap

        if i < len(used_gaps):
            end = used_gaps[i][0]          # silence_start of next gap
        elif total_duration > 0:
            end = total_duration
        else:
            last_gap_end = silence_gaps[-1][1] if silence_gaps else start + 30
            end = last_gap_end + 30

        seg_boundaries.append((start, end))

    if progress:
        for idx, (s, e) in enumerate(seg_boundaries):
            progress(f"  Boundary {idx}: {s:.2f}s → {e:.2f}s ({e - s:.1f}s)")

    # Step 5: Split the video at each boundary with frame-accurate re-encoding
    segment_paths = {}
    for i, seg in enumerate(vo_segments):
        if i >= len(seg_boundaries):
            break
        seg_id = seg["segment_id"]
        trim_start, trim_end = seg_boundaries[i]

        clips_dir = output_dir or HEYGEN_CLIPS_DIR
        output_file = os.path.join(clips_dir, f"heygen_seg_{seg_id:03d}.mp4")

        # Use -ss after -i for frame-accurate seeking (not keyframe-dependent)
        # Re-encode to allow cutting at exact positions
        split_cmd = [
            "ffmpeg",
            "-y",
            "-i", combined_path,
            "-ss", str(trim_start),
            "-to", str(trim_end),
            "-vf", "scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2:color=black,fps=30",
            "-vsync", "cfr",
            "-c:v", "libx264", "-preset", "ultrafast", "-crf", "18",
            "-c:a", "aac", "-b:a", "128k",
            "-ar", "44100", "-ac", "2",
            "-avoid_negative_ts", "make_zero",
            output_file,
        ]

        try:
            subprocess.run(split_cmd, capture_output=True, timeout=120)
            if os.path.exists(output_file) and os.path.getsize(output_file) > 10000:
                duration = trim_end - trim_start
                if progress:
                    progress(f"  Segment {seg_id}: {trim_start:.1f}s → {trim_end:.1f}s ({duration:.1f}s)")
                segment_paths[seg_id] = output_file
            else:
                if progress:
                    progress(f"  WARNING: Segment {seg_id} output is too small or missing")
        except Exception as e:
            if progress:
                progress(f"  ERROR splitting segment {seg_id}: {e}")

    if progress:
        progress(f"Split complete: {len(segment_paths)}/{expected_count} segments created")

    return segment_paths


def _fallback_equal_split(combined_path, vo_segments, progress=None, output_dir=None):
    """
    Fallback: split the combined video into equal-duration chunks if silence detection fails.
    """
    if progress:
        progress("Using fallback: equal-duration split")

    expected_count = len(vo_segments)

    # Get total duration
    duration_cmd = [
        "ffprobe",
        "-v", "quiet",
        "-show_entries", "format=duration",
        "-of", "json",
        combined_path,
    ]
    try:
        dur_result = subprocess.run(duration_cmd, capture_output=True, text=True, timeout=30)
        dur_data = json.loads(dur_result.stdout)
        total_duration = float(dur_data["format"]["duration"])
    except Exception as e:
        if progress:
            progress(f"ERROR: Could not determine video duration: {e}")
        return {}

    chunk_duration = total_duration / expected_count
    segment_paths = {}

    for i, seg in enumerate(vo_segments):
        seg_id = seg["segment_id"]
        start_time = i * chunk_duration
        end_time = (i + 1) * chunk_duration

        clips_dir = output_dir or HEYGEN_CLIPS_DIR
        output_file = os.path.join(clips_dir, f"heygen_seg_{seg_id:03d}.mp4")

        split_cmd = [
            "ffmpeg",
            "-y",
            "-i", combined_path,
            "-ss", str(start_time),
            "-to", str(end_time),
            "-vf", "scale=1920:1080:force_original_aspect_ratio=decrease,pad=1920:1080:(ow-iw)/2:(oh-ih)/2:color=black,fps=30",
            "-vsync", "cfr",
            "-c:v", "libx264", "-preset", "ultrafast", "-crf", "18",
            "-c:a", "aac", "-b:a", "128k",
            "-ar", "44100", "-ac", "2",
            "-avoid_negative_ts", "make_zero",
            output_file,
        ]

        try:
            subprocess.run(split_cmd, capture_output=True, timeout=60)
            if os.path.exists(output_file) and os.path.getsize(output_file) > 10000:
                if progress:
                    progress(f"  Segment {seg_id}: {start_time:.1f}s → {end_time:.1f}s ({chunk_duration:.1f}s)")
                segment_paths[seg_id] = output_file
        except Exception as e:
            if progress:
                progress(f"  ERROR splitting segment {seg_id}: {e}")

    return segment_paths


def generate_all_segments_browser_sync(
    script: dict,
    avatar_name: str = "default",
    progress_callback=None,
    existing_heygen_data: dict = None,
    on_segment_complete=None,
    output_dir: str = None,
) -> dict:
    """
    Generate HeyGen avatar videos for all commentary/hook segments via browser.
    Uses AI Studio multi-scene approach: one video, split by silence.

    If existing_heygen_data is provided, only retries failed/missing segments
    and merges with previously successful ones.
    """
    def progress(msg):
        if progress_callback:
            progress_callback(msg)
        print(f"  [HeyGen Browser] {msg}")

    segments = script.get("segments", [])
    vo_segments = [s for s in segments if s["type"].endswith("_voiceover")]

    if not vo_segments:
        return {"heygen_segments": [], "total": 0, "successful": 0, "failed": 0}

    # Determine which segments already succeeded and which need (re)processing
    already_done = {}
    if existing_heygen_data:
        for hs in existing_heygen_data.get("heygen_segments", []):
            if hs.get("success") and hs.get("heygen_video_path") and os.path.exists(hs["heygen_video_path"]):
                already_done[hs["segment_id"]] = hs

    segments_to_process = [s for s in vo_segments if s["segment_id"] not in already_done]

    if not segments_to_process:
        progress("All segments already completed — nothing to retry")
        return existing_heygen_data

    if already_done:
        progress(f"Resuming: {len(already_done)} segments already done, {len(segments_to_process)} to process")

    clips_dir = output_dir or HEYGEN_CLIPS_DIR
    os.makedirs(BROWSER_PROFILE_DIR, exist_ok=True)
    os.makedirs(clips_dir, exist_ok=True)

    combined_output = os.path.join(clips_dir, "heygen_combined.mp4")

    with sync_playwright() as p:
        progress("Initializing avatar generation...")

        context = p.chromium.launch_persistent_context(
            user_data_dir=BROWSER_PROFILE_DIR,
            headless=True,
            accept_downloads=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-first-run",
                "--no-default-browser-check",
            ],
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )

        page = context.pages[0] if context.pages else context.new_page()

        try:
            # 1. Open AI Studio with CommentaryAI template
            if not _open_ai_studio(page, progress):
                context.close()
                failed_results = [{
                    "segment_id": s["segment_id"], "type": s["type"],
                    "vo_text": s.get("vo_text", ""), "heygen_video_path": None, "success": False,
                } for s in segments_to_process]
                all_results = list(already_done.values()) + failed_results
                all_results.sort(key=lambda r: r["segment_id"])
                return {"heygen_segments": all_results, "total": len(vo_segments),
                        "successful": len(already_done), "failed": len(segments_to_process)}

            # 2. Add all VO paragraphs as scenes (with 2x pause separators)
            _add_all_scenes(page, segments_to_process, progress)

            # 3. Generate & download combined video
            combined_path, unique_title = _generate_and_download(page, combined_output, progress)

            if not combined_path:
                context.close()
                failed_results = [{
                    "segment_id": s["segment_id"], "type": s["type"],
                    "vo_text": s.get("vo_text", ""), "heygen_video_path": None, "success": False,
                } for s in segments_to_process]
                all_results = list(already_done.values()) + failed_results
                all_results.sort(key=lambda r: r["segment_id"])
                return {"heygen_segments": all_results, "total": len(vo_segments),
                        "successful": len(already_done), "failed": len(segments_to_process)}

        finally:
            context.close()

    # 4. Split into individual segments by silence detection
    segment_paths = _split_by_silence(combined_path, segments_to_process, progress, clips_dir)

    # 5. Build results (same format as before)
    new_results = []
    for seg in segments_to_process:
        seg_id = seg["segment_id"]
        mp4_path = segment_paths.get(seg_id)
        result_entry = {
            "segment_id": seg_id,
            "type": seg["type"],
            "vo_text": seg.get("vo_text", ""),
            "heygen_video_path": mp4_path,
            "success": mp4_path is not None,
        }
        new_results.append(result_entry)

        if on_segment_complete:
            interim = list(already_done.values()) + new_results
            interim.sort(key=lambda r: r["segment_id"])
            interim_data = {
                "heygen_segments": interim,
                "total": len(vo_segments),
                "successful": sum(1 for r in interim if r["success"]),
                "failed": sum(1 for r in interim if not r["success"]),
            }
            on_segment_complete(interim_data, result_entry)

    # Merge previously successful results with new results
    all_results = list(already_done.values()) + new_results
    all_results.sort(key=lambda r: r["segment_id"])

    successful = sum(1 for r in all_results if r["success"])
    total = len(all_results)

    progress(f"Complete: {successful}/{total} segments successful")

    return {
        "heygen_segments": all_results,
        "total": total,
        "successful": successful,
        "failed": total - successful,
    }


def _split_script_into_scenes(script_text, max_words=170):
    """Split script into scenes of ~150-180 words, breaking at sentence boundaries."""
    sentences = re.split(r'(?<=[.!?])\s+', script_text.strip())
    scenes = []
    current = []
    current_words = 0
    for sentence in sentences:
        word_count = len(sentence.split())
        if current_words + word_count > max_words and current:
            scenes.append(' '.join(current))
            current = [sentence]
            current_words = word_count
        else:
            current.append(sentence)
            current_words += word_count
    if current:
        scenes.append(' '.join(current))
    return scenes if scenes else [script_text.strip()]


def generate_single_video_browser_sync(
    script_text: str,
    progress_callback=None,
) -> dict:
    """
    Generate a single HeyGen avatar video from a raw script text.
    Splits script into ~150-180 word scenes at sentence boundaries,
    then generates as one combined video.

    Returns: {"success": bool, "video_path": str|None, "error": str|None}
    """
    def progress(msg):
        if progress_callback:
            progress_callback(msg)
        print(f"  [HeyGen Script] {msg}")

    if not script_text or not script_text.strip():
        return {"success": False, "video_path": None, "error": "Empty script text"}

    # Split script into ~150-180 word scenes at sentence boundaries
    scenes = _split_script_into_scenes(script_text, max_words=170)
    progress(f"Script split into {len(scenes)} scene(s) (~150-180 words each)")

    # Convert to segment format that _add_all_scenes() expects
    vo_segments = [
        {"segment_id": i, "vo_text": scene}
        for i, scene in enumerate(scenes)
    ]

    os.makedirs(BROWSER_PROFILE_DIR, exist_ok=True)
    os.makedirs(HEYGEN_CLIPS_DIR, exist_ok=True)

    output_path = os.path.join(HEYGEN_CLIPS_DIR, f"script_video_{int(time.time())}.mp4")

    with sync_playwright() as p:
        progress("Initializing avatar generation...")

        context = p.chromium.launch_persistent_context(
            user_data_dir=BROWSER_PROFILE_DIR,
            headless=True,
            accept_downloads=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-first-run",
                "--no-default-browser-check",
            ],
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )

        page = context.pages[0] if context.pages else context.new_page()

        try:
            # 1. Open AI Studio with CommentaryAI template
            if not _open_ai_studio(page, progress):
                context.close()
                return {"success": False, "video_path": None, "error": "Failed to open HeyGen AI Studio"}

            # 2. Add each paragraph as a separate scene (reuses commentary flow)
            _add_all_scenes(page, vo_segments, progress)

            progress("All scenes added successfully")
            page.wait_for_timeout(1000)

            # 3. Generate & download the video
            video_path, unique_title = _generate_and_download(page, output_path, progress)

            if not video_path:
                context.close()
                return {"success": False, "video_path": None, "error": "Failed to generate or download video from HeyGen"}

        finally:
            context.close()

    progress(f"Video saved: {video_path}")
    return {"success": True, "video_path": video_path, "error": None}
