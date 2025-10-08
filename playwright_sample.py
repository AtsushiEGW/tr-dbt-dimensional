import os
os.environ["DISPLAY"] = ":99"

from playwright.sync_api import sync_playwright

# Chromium画面は以下で確認可能
# http://localhost:6080/vnc.html?autoconnect=1&host=localhost&port=6080&resize=remote



print("Starting Playwright with GUI...")
with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)  # headless=FalseでGUI表示
    page = browser.new_page()
    print("Opening browser...")
    page.goto("https://qiita.com/")
    print("Waiting for manual login...")

    # page.pause()  # 👈 ここでNoVNC画面を使って「手動でログイン」！
    input('Press Enter after manual login...')  # 手動ログイン後にEnterを押す

    # 以降は自動処理でOK
    page.goto("https://qiita.com/s_rokuemon/items/3e66cde2a1435825f9be")
    print(page.title())
    input('Press Enter to close the browser...')
    browser.close()