# USD Pulse Wire

Webapp theo doi tin tuc tai chinh realtime, uu tien cac tin co kha nang tac dong den dong USD.

## Tinh nang

- Tu dong nap tin moi va day truc tiep ve trinh duyet bang Server-Sent Events (SSE).
- Uu tien nguon chinh thong va bao tai chinh lon:
  - Federal Reserve press releases va speeches
  - U.S. Treasury press releases
  - BEA current releases
  - CNBC RSS
  - MarketWatch RSS
- Cham diem muc do anh huong den USD theo cac nhom tin: Fed policy, inflation, growth, labor, treasury/fiscal, FX/risk sentiment.
- Dashboard co bo loc theo muc do anh huong, tim kiem headline, theo doi suc khoe tung nguon tin.

## Chay local

1. Tao virtual environment:

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
```

2. Cai dependency:

```powershell
pip install -r requirements.txt
```

3. Chay app:

```powershell
uvicorn app.main:app --reload
```

4. Mo trinh duyet tai [http://127.0.0.1:8000](http://127.0.0.1:8000)

## Public URL tam thoi

Ban hien tai co the expose app ra internet bang SSH tunnel ho tro reverse tunnel. Trong workspace nay toi da xac nhan app co the public qua `localhost.run`.

Luu y: URL kieu nay chi song trong luc may local va process tunnel con chay.

## Deploy ben vung voi Render

Project da co san:

- `.python-version`
- `render.yaml`
- `Dockerfile`

Theo tai lieu Render, FastAPI co the deploy bang:

- Build Command: `pip install -r requirements.txt`
- Start Command: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`
- Health Check: `/health`

Neu ban dua code len mot repo public/private, Render co the deploy thanh web service cong khai.

## Deploy bang Docker image

Project da san sang build image:

```powershell
docker build -t usd-pulse-wire .
docker run -p 10000:10000 usd-pulse-wire
```

Lenh start trong image:

```text
uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-10000}
```

## Cau hinh

Co the thay doi qua environment variables:

- `POLL_INTERVAL_SECONDS` mac dinh `60`
- `HISTORY_LIMIT` mac dinh `220`
- `HTTP_TIMEOUT_SECONDS` mac dinh `15`
- `DEFAULT_FEED_LIMIT` mac dinh `90`
- `REQUEST_USER_AGENT` de doi user-agent khi fetch nguon tin

## Luu y ve nguon tin

- Ban mac dinh uu tien nguon official va cac feed public uy tin de app chay ngay khong can API key.
- BLS thuc hien bot protection manh voi RSS/public scraping, vi vay ban mac dinh khong fetch truc tiep CPI/payroll feed tu BLS. Neu can do phu day du hon, co the bo sung nguon licensed/premium o lop `app/services/sources.py`.

## Cau truc

- `app/main.py`: FastAPI app, API va SSE stream
- `app/services/sources.py`: adapter cho tung nguon tin
- `app/services/impact.py`: scoring engine cho USD impact
- `app/services/hub.py`: polling, dedupe, broadcast realtime
- `app/static/`: UI dashboard
