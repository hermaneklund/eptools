from pathlib import Path
import os
import threading
import time
import webbrowser
import urllib.request

import uvicorn
import app as portfolio_app

BASE_DIR = Path(__file__).resolve().parent

# Keep DB/Excel in their existing locations
os.environ.setdefault("PORTFOLIO_DB_PATH", r"C:\Users\HermanEklund\portfolio-app\data\portfolio.db")
os.environ.setdefault("PORTFOLIO_EXCEL_PATH", r"C:\Users\HermanEklund\Desktop\E&P\Portfolio Data.xlsm")


def _open_browser():
    url = "http://127.0.0.1:8000/"
    health = "http://127.0.0.1:8000/health"
    for _ in range(20):
        try:
            with urllib.request.urlopen(health, timeout=1) as resp:
                if resp.status == 200:
                    try:
                        os.startfile(url)
                    except Exception:
                        webbrowser.open(url)
                    return
        except Exception:
            time.sleep(0.5)
    try:
        os.startfile(url)
    except Exception:
        webbrowser.open(url)


if __name__ == "__main__":
    threading.Thread(target=_open_browser, daemon=True).start()
    uvicorn.run(portfolio_app.app, host="127.0.0.1", port=8000, reload=False)
