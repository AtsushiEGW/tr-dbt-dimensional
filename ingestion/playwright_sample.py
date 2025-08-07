import os
os.environ["DISPLAY"] = ":99"

from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)  # headless=FalseでGUI表示
    page = browser.new_page()
    page.goto("https://example.com/login")

    page.pause()  # 👈 ここでNoVNC画面を使って「手動でログイン」！

    # 以降は自動処理でOK
    page.goto("https://example.com/dashboard")
    print(page.title())
    input('Press Enter to close the browser...')
    browser.close()