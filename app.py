import asyncio
import json
import time
from collections import OrderedDict

from aiohttp import web, ClientSession, ClientTimeout

# ---------------- конфигурация ----------------
PORT = 8000
MAX_LOCAL = 400_000                      # максимум записей в локальном кэше
MAX_BYTES = 220 * 1024 * 1024            # 220 МБ — с запасом под 256 МБ лимита
CATALOG_TIMEOUT = 2.0                    # < 3 с ожидания читателя
MAX_CONCURRENT_CATALOG = 4               # throttle к каталогу
BACKOFF_MAX = 30.0
BACKOFF_BASE = 1.0
MAX_BODY = 200 * 1024 * 1024             # aiohttp не должен рубить снимок 110 МБ
MAX_LINE = 2 * 1024 * 1024               # предохранитель на строку снимка


# ================= ХРАНИЛИЩЕ =================
class Store:
    def __init__(self):
        # id -> готовые JSON-байты записи
        self.data: OrderedDict[str, bytes] = OrderedDict()
        # снятые через DELETE — не отдаём до следующего снимка
        self.removed: set[str] = set()
        # текущий объём data в байтах
        self.bytes = 0

        # счётчики (монотонно растут)
        self.served_local = 0
        self.served_from_catalog = 0
        self.catalog_reads = 0
        self.evictions = 0

    def get_local(self, qid: str):
        return self.data.get(qid)

    def put_local(self, qid: str, body: bytes):
        old = self.data.get(qid)
        if old is not None:
            self.bytes -= len(old)
            del self.data[qid]
        self.data[qid] = body
        self.bytes += len(body)
        self._evict()

    def _evict(self):
        while len(self.data) > MAX_LOCAL or self.bytes > MAX_BYTES:
            _, body = self.data.popitem(last=False)
            self.bytes -= len(body)
            self.evictions += 1

    def remove(self, qid: str) -> bool:
        existed = qid in self.data or qid in self.removed
        old = self.data.pop(qid, None)
        if old is not None:
            self.bytes -= len(old)
        self.removed.add(qid)
        return existed

    def replace_snapshot(self, new_data: OrderedDict, new_bytes: int) -> int:
        old_ids = set(self.data.keys())
        new_ids = set(new_data.keys())
        dropped = len(old_ids - new_ids)

        self.data = new_data
        self.bytes = new_bytes
        self.removed.clear()
        self._evict()
        return dropped


# ================= КЛИЕНТ КАТАЛОГА =================
class CatalogClient:
    def __init__(self, store: Store):
        self.store = store
        self.url: str | None = None
        self.session: ClientSession | None = None
        self.sem = asyncio.Semaphore(MAX_CONCURRENT_CATALOG)
        self.backoff_until = 0.0
        self.backoff = 0.0
        self.inflight: dict[str, asyncio.Future] = {}

    def set_url(self, url: str):
        self.url = url.rstrip('/')
        self.backoff_until = 0.0
        self.backoff = 0.0

    async def fetch(self, qid: str):
        if self.url is None:
            return None
        if time.monotonic() < self.backoff_until:
            return None

        # single-flight: одинаковые id не порождают дублей запросов
        fut = self.inflight.get(qid)
        if fut is not None:
            return await fut

        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self.inflight[qid] = fut
        try:
            result = await self._do_fetch(qid)
            fut.set_result(result)
            return result
        except Exception as e:
            fut.set_exception(e)
            raise
        finally:
            self.inflight.pop(qid, None)

    async def _do_fetch(self, qid: str):
        async with self.sem:
            # повторная проверка после ожидания семафора
            if time.monotonic() < self.backoff_until:
                return None

            self.store.catalog_reads += 1
            try:
                async with self.session.get(
                    f"{self.url}/quote/{qid}",
                    timeout=ClientTimeout(total=CATALOG_TIMEOUT),
                ) as resp:
                    if resp.status == 200:
                        body = await resp.read()
                        self._reset_backoff()
                        return body
                    if resp.status == 404:
                        self._reset_backoff()
                        return None
                    if resp.status == 503:
                        ra = resp.headers.get('Retry-After')
                        try:
                            delay = float(ra) if ra else BACKOFF_BASE
                        except (TypeError, ValueError):
                            delay = BACKOFF_BASE
                        self._apply_backoff(delay)
                        return None
                    # неожиданный код
                    self._apply_backoff(None)
                    return None
            except (asyncio.TimeoutError, OSError):
                self._apply_backoff(None)
                return None

    def _apply_backoff(self, delay):
        if delay is None:
            self.backoff = min(BACKOFF_MAX, max(BACKOFF_BASE, self.backoff * 2))
        else:
            self.backoff = min(BACKOFF_MAX, max(BACKOFF_BASE, delay))
        self.backoff_until = time.monotonic() + self.backoff

    def _reset_backoff(self):
        self.backoff = 0.0
        self.backoff_until = 0.0


# ================= ПАРСЕР СНИМКА =================
async def _parse_snapshot(request) -> tuple[OrderedDict, int]:
    """
    Построчный стриминговый парсинг.
    Формат гарантирован:
        {"quotes":[
        {...},
        {...},
        ]}
    Возвращает (data: id -> bytes, total_bytes).
    """
    buf = bytearray()
    new_data: OrderedDict[str, bytes] = OrderedDict()
    total_bytes = 0
    state = 0  # 0 — ждём преамбулу, 1 — читаем записи, 2 — конец

    async for chunk in request.content.iter_chunked(64 * 1024):
        buf.extend(chunk)
        while True:
            nl = buf.find(b'\n')
            if nl < 0:
                if len(buf) > MAX_LINE:
                    raise ValueError('line too long')
                break
            line = bytes(buf[:nl]).strip()
            del buf[:nl + 1]

            if not line:
                continue

            if state == 0:
                if line != b'{"quotes":[':
                    raise ValueError('bad preamble')
                state = 1
                continue

            if state == 1:
                if line == b']}':
                    state = 2
                    break
                if line.endswith(b','):
                    line = line[:-1]
                try:
                    rec = json.loads(line)
                except Exception:
                    raise ValueError('bad record')
                if not isinstance(rec, dict):
                    raise ValueError('bad record')
                qid = rec.get('id')
                if not isinstance(qid, str) or not qid:
                    raise ValueError('bad id')
                body = json.dumps(
                    rec, ensure_ascii=False, separators=(',', ':')
                ).encode('utf-8')
                new_data[qid] = body
                total_bytes += len(body)
                continue

            if state == 2:
                # после ]} осмысленных данных нет
                continue

    if state != 2:
        raise ValueError('incomplete snapshot')
    return new_data, total_bytes


# ================= ГЛОБАЛЬНОЕ СОСТОЯНИЕ =================
store = Store()
catalog = CatalogClient(store)
import_lock = asyncio.Lock()


# ================= ХЕНДЛЕРЫ =================
async def handle_health(request):
    return web.json_response({"status": "healthy"})


async def handle_set_source(request):
    try:
        payload = await request.json()
    except Exception:
        raise web.HTTPBadRequest(text='invalid json')
    url = payload.get('url')
    if not isinstance(url, str) or not url:
        raise web.HTTPBadRequest(text='url required')
    catalog.set_url(url)
    return web.json_response({"source": catalog.url})


async def handle_import(request):
    if import_lock.locked():
        raise web.HTTPConflict(text='import in progress')
    async with import_lock:
        try:
            new_data, total_bytes = await _parse_snapshot(request)
        except ValueError as e:
            raise web.HTTPBadRequest(text=str(e))
        dropped = store.replace_snapshot(new_data, total_bytes)
    return web.json_response({
        "imported": len(new_data),
        "dropped": dropped,
    })


async def handle_put(request):
    qid = request.match_info['quote_id']
    try:
        payload = await request.json()
    except Exception:
        raise web.HTTPBadRequest(text='invalid json')

    author = payload.get('author')
    text = payload.get('text')
    if not isinstance(author, str) or not (1 <= len(author) <= 200):
        raise web.HTTPUnprocessableEntity(text='author invalid')
    if not isinstance(text, str) or not (1 <= len(text) <= 16384):
        raise web.HTTPUnprocessableEntity(text='text invalid')

    rec = dict(payload)
    rec['id'] = qid
    body = json.dumps(
        rec, ensure_ascii=False, separators=(',', ':')
    ).encode('utf-8')

    store.removed.discard(qid)
    store.put_local(qid, body)
    return web.json_response(rec)


async def handle_delete(request):
    qid = request.match_info['quote_id']
    if not store.remove(qid):
        raise web.HTTPNotFound(text='not found')
    return web.json_response({"deleted": True})


async def handle_get_quote(request):
    qid = request.match_info['quote_id']

    if qid in store.removed:
        raise web.HTTPNotFound(text='not found')

    body = store.get_local(qid)
    if body is not None:
        store.served_local += 1
        return web.Response(
            body=body,
            content_type='application/json',
            headers={'X-Source': 'LOCAL'},
        )

    store.served_from_catalog += 1
    body = await catalog.fetch(qid)
    if body is None:
        raise web.HTTPNotFound(text='not found')

    store.put_local(qid, body)
    return web.Response(
        body=body,
        content_type='application/json',
        headers={'X-Source': 'CATALOG'},
    )


async def handle_stats(request):
    return web.json_response({
        "served_local": store.served_local,
        "served_from_catalog": store.served_from_catalog,
        "catalog_reads": store.catalog_reads,
        "evictions": store.evictions,
        "local": len(store.data),
        "bytes": store.bytes,
        "catalog": len(store.data) + len(store.removed),
    })


# ================= ЖИЗНЕННЫЙ ЦИКЛ =================
async def on_startup(app):
    catalog.session = ClientSession()


async def on_cleanup(app):
    if catalog.session:
        await catalog.session.close()


def make_app():
    app = web.Application(client_max_size=MAX_BODY)
    app.router.add_get('/health', handle_health)
    app.router.add_post('/catalog/source', handle_set_source)
    app.router.add_post('/import', handle_import)
    app.router.add_put('/catalog/{quote_id}', handle_put)
    app.router.add_delete('/catalog/{quote_id}', handle_delete)
    app.router.add_get('/quotes/{quote_id}', handle_get_quote)
    app.router.add_get('/stats', handle_stats)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app


if __name__ == '__main__':
    web.run_app(make_app(), host='0.0.0.0', port=PORT, access_log=None)