"""
test_script_headed.py — Interactive headed HeyGen debugger.
Opens the CommentaryAI template, then lets YOU drive.
Press Enter at each step to take a screenshot.

Usage: python3 test_script_headed.py
"""
import json
import os
import time
from playwright.sync_api import sync_playwright

BROWSER_PROFILE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".heygen_chrome_profile")
HEYGEN_AUTH_JSON = os.path.join(os.path.dirname(os.path.abspath(__file__)), "heygen_auth.json")
HEYGEN_TEMPLATES_URL = "https://app.heygen.com/avatar/templates?ct=private"
SCREENSHOTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "debug_screenshots")


def log(msg):
    print(f"  [{time.strftime('%H:%M:%S')}] {msg}")


def take_screenshot(page, name):
    os.makedirs(SCREENSHOTS_DIR, exist_ok=True)
    path = os.path.join(SCREENSHOTS_DIR, f"{name}.png")
    page.screenshot(path=path)
    log(f"Screenshot saved: debug_screenshots/{name}.png")
    return path


def wait_for_user(page, step_name):
    """Pause, take screenshot, wait for Enter."""
    take_screenshot(page, step_name)
    print(f"\n  >>> Step: {step_name}")
    print(f"  >>> Do what you need in the browser, then press Enter here to take next screenshot...")
    print(f"  >>> (type 'q' to quit)\n")
    resp = input("  > ")
    if resp.strip().lower() == 'q':
        return False
    take_screenshot(page, f"{step_name}_after")
    return True


def main():
    os.makedirs(BROWSER_PROFILE_DIR, exist_ok=True)

    with sync_playwright() as p:
        log("Launching HEADED browser...")
        context = p.chromium.launch_persistent_context(
            user_data_dir=BROWSER_PROFILE_DIR,
            headless=False,
            accept_downloads=True,
            args=["--disable-blink-features=AutomationControlled", "--no-first-run", "--no-default-browser-check"],
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )

        # Load auth cookies
        if os.path.exists(HEYGEN_AUTH_JSON):
            try:
                with open(HEYGEN_AUTH_JSON) as f:
                    data = json.load(f)
                cookies = data.get("cookies", [])
                if cookies:
                    context.add_cookies(cookies)
                    log(f"Loaded {len(cookies)} cookies")
            except Exception:
                pass

        page = context.pages[0] if context.pages else context.new_page()

        # Navigate to templates
        log(f"Going to {HEYGEN_TEMPLATES_URL}")
        page.goto(HEYGEN_TEMPLATES_URL, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(5000)
        log(f"URL: {page.url}")

        if "auth.heygen.com" in page.url or "/login" in page.url:
            log("LOGIN REQUIRED — log in in the browser")
            for _ in range(100):
                page.wait_for_timeout(3000)
                if "auth.heygen.com" not in page.url and "/login" not in page.url:
                    log("Login OK!")
                    break
            else:
                log("Login timed out")
                context.close()
                return

        page.wait_for_timeout(2000)

        # Find and click CommentaryAI template
        log("Looking for CommentaryAI card...")
        try:
            card = page.locator('text="CommentaryAI"').first
            if card.is_visible(timeout=5000):
                bb = card.bounding_box()
                if bb:
                    page.mouse.move(bb["x"] + bb["width"] / 2, bb["y"] - 80)
                    page.wait_for_timeout(1500)
        except Exception:
            pass

        page.evaluate("""() => {
            for (const el of document.querySelectorAll('*')) {
                const s = window.getComputedStyle(el);
                if (s.opacity === '0' || s.visibility === 'hidden') {
                    el.style.opacity = '1'; el.style.visibility = 'visible';
                }
            }
        }""")

        use_clicked = page.evaluate("""() => {
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
                            if (btn) { btn.click(); return true; }
                        }
                    }
                    break;
                }
            }
            return false;
        }""")
        log(f"Template opened: {use_clicked}")

        if not use_clicked:
            log("ERROR: Could not open template")
            context.close()
            return

        log("Waiting for editor...")
        page.wait_for_timeout(8000)

        # ── Automated 3-scene test ──
        test_scenes = [
            "This is scene one. Testing the first scene text fill into the contenteditable area of HeyGen AI Studio editor.",
            "This is scene two. A completely different paragraph to verify the second scene gets its own unique text.",
            "This is scene three. Final scene to confirm all three scenes are added correctly with separate text.",
        ]

        total = len(test_scenes)
        step = 0

        for i, vo_text in enumerate(test_scenes):
            is_last = (i == total - 1)
            log(f"Scene {i+1}/{total}: {vo_text[:50]}...")

            # Fill text into the scene's text area
            filled = False

            if i == 0:
                # Scene 1: text box is already active, just type directly
                try:
                    page.keyboard.insert_text(vo_text)
                    filled = True
                    log("  Scene 1: typed directly")
                except Exception as e:
                    log(f"  Scene 1 FAILED: {e}")
            else:
                # Scene 2+: click by nth-child pattern (N = 5, 8, 11... = 2 + i*3)
                nth = 2 + (i * 3)
                selector = f'div.te-scriptpanel-redesign > div:nth-child(2) > div > div > div:nth-child({nth}) > div > div > div:nth-child(2) > div:nth-child(1) > div > span'
                try:
                    target = page.locator(selector).first
                    if target.is_visible(timeout=3000):
                        target.click()
                        page.wait_for_timeout(500)
                        page.keyboard.insert_text(vo_text)
                        filled = True
                        log(f"  Clicked nth-child({nth}) + typed")
                except Exception as e:
                    log(f"  FAILED: {e}")

            page.wait_for_timeout(500)
            step += 1
            take_screenshot(page, f"step_{step:02d}_scene_{i+1}_filled")

            # Add Scene (if not last)
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
                        for (const b of document.querySelectorAll('button')) {
                            if (b.textContent.trim().toLowerCase().includes('add scene') && b.offsetParent) {
                                b.click(); return true;
                            }
                        }
                        return false;
                    }""")
                if add_clicked:
                    log("  Add Scene clicked")
                    page.wait_for_timeout(2000)
                    step += 1
                    take_screenshot(page, f"step_{step:02d}_after_add_scene")
                else:
                    log("  WARNING: Add Scene failed")

        step += 1
        take_screenshot(page, f"step_{step:02d}_all_done")
        log(f"\nAll {total} scenes added! Keeping browser open 30s to inspect...")
        page.wait_for_timeout(30000)
        context.close()
        log("Done.")


if __name__ == "__main__":
    main()
