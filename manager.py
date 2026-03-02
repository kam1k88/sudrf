#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import zipfile
import requests
import time
import random
import boto3
import codecs
from urllib.parse import urlparse, unquote
import mimetypes
import uuid
import re
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import botocore
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ============================================================
#  АВТО-ОПРЕДЕЛЕНИЕ ДИРЕКТОРИИ ЗАПУСКА
# ============================================================
WORKDIR = os.getcwd()

# ============================================================
#  НАСТРОЙКИ
# ============================================================
BUCKET_NAME = "base-moscow-sud"
STATE_KEY = "batches_state.json"

BATCH_SIZE = 300
RETRY_COUNT = 4
TIMEOUT = 30

MIN_WORKERS = 3
MAX_WORKERS = 6
CURRENT_WORKERS = 4
TRANSFER_MAX_WORKERS = 4

LOCAL_BATCH_DIR = os.path.join(WORKDIR, BUCKET_NAME)
os.makedirs(LOCAL_BATCH_DIR, exist_ok=True)

JSON_READ_CHUNK = 65536

# ============================================================
#  ПОИСК json.zip + data.json
# ============================================================
def find_json_zip():
    for f in os.listdir(WORKDIR):
        if f.lower() == "json.zip":
            return os.path.join(WORKDIR, f)
    print("❌ json.zip не найден:", WORKDIR)
    exit(1)

ZIP_PATH = find_json_zip()

def ensure_data_json_exists():
    with zipfile.ZipFile(ZIP_PATH, "r") as zf:
        if "data.json" not in zf.namelist():
            print("❌ В json.zip нет data.json")
            exit(1)

ensure_data_json_exists()
JSON_NAME = "data.json"

# ============================================================
#  S3 КЛИЕНТ
# ============================================================
s3 = boto3.client(
    "s3",
    endpoint_url="https://storage.yandexcloud.net",
    aws_access_key_id=os.environ.get("YC_ACCESS_KEY_ID"),
    aws_secret_access_key=os.environ.get("YC_SECRET_ACCESS_KEY")
)

# ============================================================
#  REQUESTS SESSION + RETRY
# ============================================================
session = requests.Session()
retries = Retry(
    total=5,
    backoff_factor=0.6,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset(['GET', 'POST', 'PUT', 'DELETE', 'HEAD', 'OPTIONS'])
)
adapter = HTTPAdapter(max_retries=retries, pool_connections=100, pool_maxsize=100)
session.mount("https://", adapter)
session.mount("http://", adapter)

# ============================================================
#  BLOOM FILTER (для ссылок и для контента)
# ============================================================
class BloomFilter:
    def __init__(self, size_bits=16_000_000, hash_count=7, data=None):
        self.size_bits = size_bits
        self.size_bytes = size_bits // 8
        self.hash_count = hash_count

        if data is None:
            self.bitarray = bytearray(self.size_bytes)
        else:
            ba = bytearray(self.size_bytes)
            ba[:min(len(data), self.size_bytes)] = data[:self.size_bytes]
            self.bitarray = ba

    def _hashes(self, item):
        b = item.encode("utf-8")
        h1 = int(hashlib.md5(b).hexdigest(), 16)
        h2 = int(hashlib.sha1(b).hexdigest(), 16)
        for i in range(self.hash_count):
            yield (h1 + i * h2) % self.size_bits

    def add(self, item):
        for pos in self._hashes(item):
            self.bitarray[pos // 8] |= (1 << (pos % 8))

    def contains(self, item):
        for pos in self._hashes(item):
            if not (self.bitarray[pos // 8] & (1 << (pos % 8))):
                return False
        return True

    def to_bytes(self):
        return bytes(self.bitarray)

# ============================================================
#  ЗАГРУЗКА/СОХРАНЕНИЕ BLOOM'ов
# ============================================================
def load_blooms():
    link_bloom = BloomFilter()
    content_bloom = BloomFilter()
    link_etag = None
    content_etag = None
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key="downloaded_links.bloom")
        data = obj["Body"].read()
        link_bloom = BloomFilter(data=data)
        link_etag = obj.get("ETag")
    except Exception:
        pass
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key="downloaded_content.bloom")
        data = obj["Body"].read()
        content_bloom = BloomFilter(data=data)
        content_etag = obj.get("ETag")
    except Exception:
        pass
    return link_bloom, link_etag, content_bloom, content_etag

def save_blooms(link_bloom, link_etag, content_bloom, content_etag):
    try:
        s3.put_object(Bucket=BUCKET_NAME, Key="downloaded_links.bloom", Body=link_bloom.to_bytes(), ContentType="application/octet-stream")
    except Exception as e:
        print(f"⚠ Ошибка сохранения link_bloom: {e}")
    try:
        s3.put_object(Bucket=BUCKET_NAME, Key="downloaded_content.bloom", Body=content_bloom.to_bytes(), ContentType="application/octet-stream")
    except Exception as e:
        print(f"⚠ Ошибка сохранения content_bloom: {e}")

# ============================================================
#  ПОТОКОВОЕ ЧТЕНИЕ JSON (с прогрессом по батчу)
# ============================================================
def iter_json_array_from_zip(zip_path, json_name):
    decoder = codecs.getincrementaldecoder("utf-8")()
    with zipfile.ZipFile(zip_path, "r") as zf:
        with zf.open(json_name, "r") as f:
            buf = ""
            depth = 0
            inside = False

            while True:
                chunk = f.read(JSON_READ_CHUNK)
                if not chunk:
                    break

                text = decoder.decode(chunk)

                for ch in text:
                    if ch == '{':
                        depth += 1
                        inside = True

                    if inside:
                        buf += ch

                    if ch == '}':
                        depth -= 1
                        if depth == 0 and inside:
                            try:
                                yield json.loads(buf)
                            except Exception:
                                pass
                            buf = ""
                            inside = False

            decoder.decode(b"", final=True)

# ============================================================
#  ИЗВЛЕЧЕНИЕ ССЫЛОК
# ============================================================
def extract_links(obj):
    return [att.get("link") for att in obj.get("attachments", []) if att.get("link")]

def dedup_preserve_order(seq):
    seen = set()
    out = []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out

# ============================================================
#  ЗАГРУЗКА/СОХРАНЕНИЕ batches_state.json
# ============================================================
def load_batches_state():
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=STATE_KEY)
        data = obj["Body"].read().decode("utf-8")
        etag = obj.get("ETag")
        return json.loads(data), etag
    except Exception:
        return {}, None

def save_batches_state(state, etag):
    body = json.dumps(state, indent=2).encode("utf-8")
    try:
        s3.put_object(Bucket=BUCKET_NAME, Key=STATE_KEY, Body=body, ContentType="application/json")
        return True
    except Exception:
        return False

def mark_batch_done(batch_id):
    while True:
        state, etag = load_batches_state()
        state[str(batch_id)] = "done"
        if save_batches_state(state, etag):
            return
        time.sleep(0.2)

def mark_batch_incomplete(batch_id):
    while True:
        state, etag = load_batches_state()
        state[str(batch_id)] = "incomplete"
        if save_batches_state(state, etag):
            return
        time.sleep(0.2)

# ============================================================
#  СКАЧИВАНИЕ ОДНОГО ФАЙЛА (сессия + 404 финальная)
# ============================================================
def sanitize_filename(name):
    name = name.replace("\u2028", " ")
    name = re.sub(r'[<>:"/\\|?*]', "_", name)
    name = re.sub(r'\s+', " ", name)
    return name.strip()

def resolve_filename(resp, link):
    cd = resp.headers.get("Content-Disposition", "")

    m = re.search(r'filename\*\s*=\s*utf-8\'\'([^;]+)', cd, re.IGNORECASE)
    if m:
        return sanitize_filename(unquote(m.group(1)))

    m = re.search(r'filename\s*=\s*"([^"]+)"', cd)
    if m:
        return sanitize_filename(m.group(1))

    url_name = os.path.basename(urlparse(link).path)
    if url_name:
        return sanitize_filename(url_name)

    ext = mimetypes.guess_extension(resp.headers.get("Content-Type", "")) or ""
    return f"{uuid.uuid4()}{ext}"

def download_and_upload(link, batch_id):
    headers = {"User-Agent": "Mozilla/5.0"}

    for attempt in range(1, RETRY_COUNT + 1):
        try:
            resp = session.get(link, timeout=TIMEOUT, headers=headers)

            if resp.status_code == 503:
                ra = resp.headers.get("Retry-After")
                if ra:
                    try:
                        delay = float(ra)
                    except:
                        delay = [1, 3, 7, 15][min(attempt - 1, 3)] + random.uniform(0.2, 0.8)
                else:
                    delay = [1, 3, 7, 15][min(attempt - 1, 3)] + random.uniform(0.2, 0.8)
                print(f"503, попытка {attempt}, жду {delay:.1f} сек: {link}")
                time.sleep(delay)
                continue

            if resp.status_code == 404:
                print(f"404 Not Found: {link}")
                return False, False, None  # not accepted, not 503, no content

            resp.raise_for_status()

            if len(resp.content) == 0:
                print(f"⚠ Пустой ответ: {link}")
                return False, False, None

            filename = resolve_filename(resp, link)
            key = f"batch_{batch_id:06d}/{filename}"

            s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=resp.content)
            time.sleep(0.2)
            return True, False, key

        except Exception as e:
            delay = [1, 3, 7, 15][min(attempt - 1, 3)] + random.uniform(0.2, 0.8)
            print(f"Ошибка {link}, попытка {attempt}: {e}, жду {delay:.1f} сек")
            time.sleep(delay)

    return False, True, None

# ============================================================
#  СПИСОК ОБЪЕКТОВ В БАТЧЕ
# ============================================================
def list_objects_in_batch(batch_id):
    prefix = f"batch_{batch_id:06d}/"
    paginator = s3.get_paginator("list_objects_v2")
    objs = []
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix):
        for o in page.get("Contents", []):
            objs.append({"Key": o["Key"], "LastModified": o["LastModified"], "Size": o["Size"]})
    return objs

def count_files_in_batch(batch_id):
    return len(list_objects_in_batch(batch_id))

# ============================================================
#  ХЭШИРОВАНИЕ ОБЪЕКТА (полностью)
# ============================================================
def compute_object_sha256(bucket, key):
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        data = obj["Body"].read()
        return hashlib.sha256(data).hexdigest()
    except Exception as e:
        print(f"⚠ Ошибка чтения {key} для хэширования: {e}")
        return None

# ============================================================
#  МАНИФЕСТ ДЛЯ УДАЛЕНИЯ (когда нет прав)
# ============================================================
def save_delete_manifest(batch_id, keys):
    if not keys:
        return
    manifest_key = f"to_delete_batch_{batch_id:06d}.json"
    body = json.dumps({"batch": batch_id, "keys": keys, "ts": int(time.time())}, ensure_ascii=False, indent=2).encode("utf-8")
    try:
        s3.put_object(Bucket=BUCKET_NAME, Key=manifest_key, Body=body, ContentType="application/json")
        print(f"ℹ Манифест на удаление сохранён в бакете: {manifest_key}")
    except Exception as e:
        print(f"⚠ Не удалось сохранить манифест в бакете: {e}")
    local_dir = os.path.join(LOCAL_BATCH_DIR, "to_delete")
    os.makedirs(local_dir, exist_ok=True)
    local_path = os.path.join(local_dir, manifest_key)
    try:
        with open(local_path, "wb") as f:
            f.write(body)
        print(f"ℹ Манифест на удаление сохранён локально: {local_path}")
    except Exception as e:
        print(f"⚠ Не удалось сохранить локальный манифест: {e}")

# ============================================================
#  SAFE DELETE (пытаемся, но если AccessDenied — манифест)
# ============================================================
def safe_delete_object(bucket, key, max_attempts=2):
    for attempt in range(1, max_attempts + 1):
        try:
            s3.delete_object(Bucket=bucket, Key=key)
            return True, None
        except botocore.exceptions.ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in ("AccessDenied", "AllAccessDisabled"):
                return False, "AccessDenied"
            delay = (2 ** (attempt - 1)) + random.uniform(0.1, 0.5)
            print(f"Ошибка удаления {key}, попытка {attempt}: {e}, жду {delay:.1f}s")
            time.sleep(delay)
        except Exception as e:
            delay = (2 ** (attempt - 1)) + random.uniform(0.1, 0.5)
            print(f"Ошибка удаления {key}, попытка {attempt}: {e}, жду {delay:.1f}s")
            time.sleep(delay)
    return False, "Failed"

# ============================================================
#  DEDUP ПО СОДЕРЖИМОМУ И НАБОР РОВНО 300 УНИКАЛЬНЫХ
# ============================================================
def postprocess_uploaded_object(bucket, key, content_bloom, link_bloom, link, delete_manifest):
    h = compute_object_sha256(bucket, key)
    if h is None:
        delete_manifest.append(key)
        return "read_error", None

    if content_bloom.contains(h):
        delete_manifest.append(key)
        return "duplicate", h

    content_bloom.add(h)
    if link:
        link_bloom.add(link)
    return "accepted", h

# ============================================================
#  ОБРАБОТКА БАТЧА: единый прогрессбар на весь батч (accepted)
# ============================================================
def process_batch_until_full(candidate_links, batch_id, workers, link_bloom, content_bloom):
    accepted = 0
    ok = 0
    fail = 0
    count_503 = 0
    delete_manifest = []
    new_links = []
    start_time = time.time()

    pending_links = list(candidate_links)

    # единый прогрессбар на весь батч
    with tqdm(total=BATCH_SIZE, desc=f"Batch {batch_id}", ncols=100, leave=True) as pbar:
        pbar.update(accepted)  # если батч был частично принят ранее

        # Пока не набрали BATCH_SIZE и есть ссылки
        while accepted < BATCH_SIZE and pending_links:
            window = pending_links[: max(workers * 4, 50)]
            pending_links = pending_links[len(window):]

            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {executor.submit(download_and_upload, link, batch_id): link for link in window}

                for future in as_completed(futures):
                    link = futures[future]
                    try:
                        success, is_503, key = future.result()
                    except Exception as e:
                        success, is_503, key = False, False, None
                        print(f"Ошибка выполнения задачи для {link}: {e}")

                    if success and key:
                        ok += 1
                        status, h = postprocess_uploaded_object(
                            BUCKET_NAME, key, content_bloom, link_bloom, link, delete_manifest
                        )
                        if status == "accepted":
                            accepted += 1
                            new_links.append(link)
                            pbar.update(1)
                    else:
                        fail += 1

                    if is_503:
                        count_503 += 1

                    if accepted >= BATCH_SIZE:
                        # если цель достигнута — прерываем обработку текущего окна
                        break

            # небольшая пауза, чтобы не перегружать целевой сайт
            time.sleep(0.1)

    elapsed = time.time() - start_time
    rps = ok / elapsed if elapsed > 0 else 0.0
    stats = {
        "ok": ok,
        "fail": fail,
        "count_503": count_503,
        "new_links": new_links,
        "elapsed": elapsed,
        "accepted": accepted,
        "rps": rps
    }
    return stats, delete_manifest

# ============================================================
#  АДАПТИВНАЯ НАСТРОЙКА КОЛИЧЕСТВА ПОТОКОВ
# ============================================================
def adjust_workers(current_workers, stats):
    total = stats["ok"] + stats["fail"]
    if total == 0:
        return current_workers

    ratio_503 = stats["count_503"] / total

    if ratio_503 > 0.30 and current_workers > MIN_WORKERS:
        new = current_workers - 1
        print(f"⚙ Много 503 ({ratio_503:.2%}), уменьшаю потоки: {current_workers} → {new}")
        return new

    if ratio_503 < 0.10 and current_workers < MAX_WORKERS:
        new = current_workers + 1
        print(f"⚙ Мало 503 ({ratio_503:.2%}), увеличиваю потоки: {current_workers} → {new}")
        return new

    return current_workers

# ============================================================
#  СКАНИРОВАНИЕ И СИНХРОНИЗАЦИЯ СТАТУСОВ
# ============================================================
def scan_and_sync_batches():
    print("\n🔍 Сканирую бакет и синхронизирую состояния батчей...")

    paginator = s3.get_paginator("list_objects_v2")
    batch_file_counts = {}

    for page in paginator.paginate(Bucket=BUCKET_NAME):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.startswith("batch_"):
                continue
            folder = key.split("/")[0]
            try:
                batch_num = int(folder.replace("batch_", ""))
            except:
                continue
            batch_file_counts.setdefault(batch_num, 0)
            batch_file_counts[batch_num] += 1

    state, etag = load_batches_state()
    if state is None:
        state = {}

    updated = False

    for batch_num, file_count in batch_file_counts.items():
        key = str(batch_num)
        cur = state.get(key)

        if file_count == 0:
            new_status = "free"
            if cur != new_status:
                state[key] = new_status
                updated = True
            continue

        if cur in ("done", "incomplete"):
            continue

        if cur is None:
            state[key] = "incomplete"
            updated = True

    known = set(int(b) for b in state.keys())
    missing = known - set(batch_file_counts.keys())
    for b in missing:
        key = str(b)
        if state.get(key) != "free":
            state[key] = "free"
            updated = True

    if updated:
        save_batches_state(state, etag)
        print("✔ Состояние обновлено.\n")
    else:
        print("✔ Состояние актуально.\n")

# ============================================================
#  ВЫБОР СЛЕДУЮЩЕГО БАТЧА (с проверкой incomplete через 3 секунды)
# ============================================================
def acquire_next_batch():
    while True:
        state, etag = load_batches_state()
        if state is None:
            state = {}

        incomplete = sorted(int(b) for b, st in state.items() if st == "incomplete")

        for batch_id in incomplete:
            c1 = count_files_in_batch(batch_id)
            time.sleep(3)
            c2 = count_files_in_batch(batch_id)

            if c2 > c1:
                print(f"⏭ Батч {batch_id} качается другим ({c1}→{c2}), пропускаю.")
                continue

            print(f"🔄 Зависший/недоделанный батч {batch_id} ({c2} файлов), беру на продолжение.")
            state[str(batch_id)] = "incomplete"
            if save_batches_state(state, etag):
                return batch_id

        free = [int(b) for b, st in state.items() if st == "free"]
        if free:
            batch_id = min(free)
        else:
            existing = [int(b) for b in state.keys()] if state else []
            batch_id = max([0] + existing) + 1

        state[str(batch_id)] = "incomplete"
        if save_batches_state(state, etag):
            return batch_id

        time.sleep(0.2)

# ============================================================
#  НЕПРЕРЫВНАЯ ОБРАБОТКА (главная логика)
# ============================================================
def continuous_processing():
    global CURRENT_WORKERS
    print("\n🚀 Запускаю непрерывную обработку. Остановить: Ctrl+C.\n")

    while True:
        scan_and_sync_batches()

        batch_id = acquire_next_batch()
        print(f"\n=== Взял батч {batch_id} ===")

        link_bloom, link_etag, content_bloom, content_etag = load_blooms()

        start_obj = (batch_id - 1) * BATCH_SIZE + 1
        end_obj = batch_id * BATCH_SIZE

        raw_links = []
        for idx, obj in enumerate(iter_json_array_from_zip(ZIP_PATH, JSON_NAME), start=1):
            if idx < start_obj:
                continue
            if idx > end_obj:
                break
            raw_links.extend(extract_links(obj))

        batch_links = dedup_preserve_order(raw_links)
        batch_links = [l for l in batch_links if not link_bloom.contains(l)]

        candidate_links = batch_links[: BATCH_SIZE * 3]

        if not candidate_links:
            print(f"⚠ В батче {batch_id} нет новых ссылок, помечаю как done/incomplete по наличию файлов.\n")
            cnt = count_files_in_batch(batch_id)
            if cnt >= BATCH_SIZE:
                mark_batch_done(batch_id)
                print(f"✔ Батч {batch_id} помечен как done (файлов в бакете: {cnt}).\n")
            else:
                mark_batch_incomplete(batch_id)
                print(f"⚠ Батч {batch_id} помечен как incomplete (файлов в бакете: {cnt}).\n")
            continue

        stats, delete_manifest = process_batch_until_full(candidate_links, batch_id, CURRENT_WORKERS, link_bloom, content_bloom)

        save_blooms(link_bloom, link_etag, content_bloom, content_etag)

        if delete_manifest:
            save_delete_manifest(batch_id, delete_manifest)

        CURRENT_WORKERS = adjust_workers(CURRENT_WORKERS, stats)

        final_count = count_files_in_batch(batch_id)
        if stats["accepted"] >= BATCH_SIZE:
            mark_batch_done(batch_id)
            print(f"✔ Батч {batch_id} помечен как done. Принято уникальных: {stats['accepted']}\n")
        else:
            mark_batch_incomplete(batch_id)
            print(f"⚠ Батч {batch_id} incomplete. Принято уникальных: {stats['accepted']}, файлов в бакете: {final_count}\n")

# ============================================================
#  СКАЧАТЬ/ЗАГРУЗИТЬ БАТЧИ (утилиты)
# ============================================================
def download_object(key):
    local_path = os.path.join(LOCAL_BATCH_DIR, key)
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    try:
        s3.download_file(BUCKET_NAME, key, local_path)
        return True, key
    except Exception as e:
        return False, (key, str(e))

def download_batches():
    print(f"\n📥 Скачивание батчей в {LOCAL_BATCH_DIR}")
    mode = input("Скачать все батчи? [y/N]: ").strip().lower()

    start_batch = None
    end_batch = None

    if mode != "y":
        try:
            start_batch = int(input("С какого батча: ").strip())
            end_batch = int(input("По какой батч: ").strip())
        except:
            print("Неверный ввод.\n")
            return

    paginator = s3.get_paginator("list_objects_v2")
    keys = []

    for page in paginator.paginate(Bucket=BUCKET_NAME):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.startswith("batch_"):
                continue
            try:
                batch_num = int(key.split("/")[0].replace("batch_", ""))
            except:
                continue

            if mode == "y" or (start_batch <= batch_num <= end_batch):
                keys.append(key)

    print(f"Найдено файлов: {len(keys)}")

    ok = 0
    fail = 0

    with ThreadPoolExecutor(max_workers=TRANSFER_MAX_WORKERS) as executor:
        futures = {executor.submit(download_object, key): key for key in keys}

        for future in tqdm(as_completed(futures), total=len(futures), ncols=100, desc="Скачивание"):
            success, info = future.result()
            if success:
                ok += 1
            else:
                fail += 1
                print(f"\nОшибка скачивания {info[0]}: {info[1]}")

    print(f"\nГотово. Успешно: {ok}, ошибок: {fail}\n")

def upload_object(local_path, key):
    try:
        s3.upload_file(local_path, BUCKET_NAME, key)
        return True, key
    except Exception as e:
        return False, (key, str(e))

def upload_batches():
    print(f"\n📤 Загрузка батчей из {LOCAL_BATCH_DIR} в бакет {BUCKET_NAME}")
    mode = input("Загрузить все батчи, найденные локально? [y/N]: ").strip().lower()

    start_batch = None
    end_batch = None

    if mode != "y":
        try:
            start_batch = int(input("С какого батча загружать: ").strip())
            end_batch = int(input("По какой батч загружать: ").strip())
        except ValueError:
            print("Неверный ввод диапазона.\n")
            return
        print(f"Будут загружены батчи {start_batch}–{end_batch}.")

    files_to_upload = []

    for root, dirs, files in os.walk(LOCAL_BATCH_DIR):
        for file in files:
            full_path = os.path.join(root, file)
            rel_path = os.path.relpath(full_path, LOCAL_BATCH_DIR).replace("\\", "/")

            parts = rel_path.split("/")
            if len(parts) < 2:
                continue

            batch_folder = parts[0]
            if not batch_folder.startswith("batch_"):
                continue

            try:
                batch_num = int(batch_folder.replace("batch_", ""))
            except:
                continue

            if mode == "y" or (start_batch <= batch_num <= end_batch):
                files_to_upload.append((full_path, rel_path))

    print(f"Файлов для загрузки: {len(files_to_upload)}")

    if not files_to_upload:
        print("Нечего загружать.\n")
        return

    ok = 0
    fail = 0

    with ThreadPoolExecutor(max_workers=TRANSFER_MAX_WORKERS) as executor:
        futures = {
            executor.submit(upload_object, local, key): key
            for local, key in files_to_upload
        }

        for future in tqdm(as_completed(futures), total=len(futures), ncols=100, desc="Загрузка"):
            success, info = future.result()
            if success:
                ok += 1
            else:
                fail += 1
                key, err = info
                print(f"\nОшибка загрузки {key}: {err}")

    print("\nГотово загрузка.")
    print(f"Успешно загружено: {ok}")
    print(f"Ошибок: {fail}\n")

# ============================================================
#  МЕНЮ
# ============================================================
def main_menu():
    while True:
        print("=== Менеджер батчей ===")
        print("1. Непрерывная синхронизация и обработка батчей (JSON.ZIP→YC-БАКЕТ)")
        print("2. Скачать батчи на локальный диск (YC-БАКЕТ→WORKDIR)")
        print("3. Загрузить батчи с локального диска (WORKDIR→YC-БАКЕТ)")
        print("4. Выход")

        choice = input("Выбор: ").strip()

        if choice == "1":
            try:
                continuous_processing()
            except KeyboardInterrupt:
                print("\nОстановка непрерывной обработки.\n")

        elif choice == "2":
            download_batches()

        elif choice == "3":
            upload_batches()

        elif choice == "4":
            print("Выход.")
            break

        else:
            print("Неизвестный пункт меню.\n")

# ============================================================
#  ТОЧКА ВХОДА
# ============================================================
if __name__ == "__main__":
    main_menu()
