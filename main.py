import os
import json
import time
import uuid
import threading
import requests
import base64
from io import BytesIO
from PIL import Image
from flask import Flask, request, jsonify
from flask_cors import CORS
import database as db

app = Flask(__name__)
CORS(app)

# --- Configuration & Constants ---
API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.ewogICJyb2xlIjogImFub24iLAogICJpc3MiOiAic3VwYWJhc2UiLAogICJpYXQiOiAxNzM0OTY5NjAwLAogICJleHAiOiAxODkyNzM2MDAwCn0.4NnK23LGYvKPGuKI5rwQN2KbLMzzdE4jXpHwbGCqPqY"

MAX_CONCURRENT_TASKS = 10

# 10 dakika timeout
IMAGE_POLL_INTERVAL = 2
IMAGE_POLL_ITERS    = 300  # 300 x 2s = 600s
VIDEO_POLL_INTERVAL = 5
VIDEO_POLL_ITERS    = 120  # 120 x 5s = 600s

# Deevid URLs
URL_AUTH             = "https://sp.deevid.ai/auth/v1/token?grant_type=password"
URL_UPLOAD           = "https://api.deevid.ai/file-upload/image"
URL_SUBMIT_IMG       = "https://api.deevid.ai/text-to-image/task/submit"
URL_SUBMIT_VIDEO     = "https://api.deevid.ai/image-to-video/task/submit"
URL_SUBMIT_TXT_VIDEO = "https://api.deevid.ai/text-to-video/task/submit"
URL_ASSETS           = "https://api.deevid.ai/my-assets?limit=50&assetType=All&filter=CREATION"
URL_VIDEO_TASKS      = "https://api.deevid.ai/video/tasks?page=1&size=20"
URL_QUOTA            = "https://api.deevid.ai/subscription/plan"

# ElevenLabs
ELEVENLABS_API_KEY   = "sk_d7cd9c0991b928ab3a7b9f04b0dedfcd7d56d790f2cca302"
ELEVENLABS_TTS_URL   = "https://api.elevenlabs.io/v1/text-to-speech"
ELEVENLABS_VOICES_URL = "https://api.elevenlabs.io/v1/voices"

DEVICE_HEADERS = {
    "x-device": "TABLET",
    "x-device-id": "3401879229",
    "x-os": "WINDOWS",
    "x-platform": "WEB",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# --- Helper Functions ---

def verify_api_key():
    auth_header = request.headers.get('Authorization')
    if not auth_header:
        return None
    provided_key = auth_header[7:] if auth_header.startswith('Bearer ') else auth_header
    return db.get_api_key_id(provided_key)


def refresh_quota(token):
    try:
        requests.get(URL_QUOTA, headers={"authorization": f"Bearer {token}", **DEVICE_HEADERS}, timeout=10)
    except:
        pass


def login_with_retry(api_key_id):
    tried_count = 0
    max_tries = db.get_account_count(api_key_id)

    if max_tries == 0:
        print("No accounts loaded!")
        return None, None

    while tried_count < max_tries:
        account = db.get_next_account(api_key_id)
        if not account:
            break

        tried_count += 1
        try:
            resp = requests.post(
                URL_AUTH,
                json={"email": account['email'].strip(), "password": account['password'].strip(), "gotrue_meta_security": {}},
                headers={"apikey": API_KEY},
                timeout=15
            )
            if resp.status_code == 200:
                token = resp.json().get('access_token')
                if token:
                    refresh_quota(token)
                    return token, account
            print(f"Login failed for {account['email']}: {resp.status_code} - {resp.text}")
            db.release_account(api_key_id, account['email'])
        except Exception as e:
            print(f"Login error for {account['email']}: {e}")
            db.release_account(api_key_id, account['email'])

    return None, None


def resize_image(image_bytes):
    try:
        img = Image.open(BytesIO(image_bytes))
        width, height = img.size
        if max(width, height) > 3000:
            scale = 3000 / max(width, height)
            img = img.resize((round(width * scale), round(height * scale)), Image.LANCZOS)
        out = BytesIO()
        img.save(out, format="PNG")
        out.seek(0)
        return out
    except Exception as e:
        print(f"Resize error: {e}")
        return None


def upload_image(token, image_bytes):
    resized = resize_image(image_bytes)
    if not resized:
        return None
    try:
        resp = requests.post(
            URL_UPLOAD,
            headers={"authorization": f"Bearer {token}", **DEVICE_HEADERS},
            files={"file": ("image.png", resized, "image/png")},
            data={"width": "1024", "height": "1536"},
            timeout=30
        )
        if resp.status_code in [200, 201]:
            return resp.json()['data']['data']['id']
    except Exception as e:
        print(f"Upload error: {e}")
    return None


# --- Task Workers ---

def process_image_task(task_id, params, api_key_id):
    account = None
    try:
        db.update_task_status(task_id, 'running')

        token, account = login_with_retry(api_key_id)
        if not token:
            db.update_task_status(task_id, 'failed')
            db.add_task_log(task_id, "All accounts failed to login.")
            return

        # Login başarılı → hesabı kaydet.
        # Submit öncesi çökme → external_task_id NULL kalır, orphan cleanup bu hesabı used=0 yapar.
        # Submit sonrası çökme → external_task_id dolu, recovery devam eder, hesap serbest kalmaz.
        db.update_task_account(task_id, account['email'])

        headers = {"authorization": f"Bearer {token}", **DEVICE_HEADERS}

        user_image_ids = []
        images = params.get('images', [])
        if not images and params.get('image'):
            images = [params.get('image')]

        for img_base64 in images:
            img_id = upload_image(token, base64.b64decode(img_base64))
            if img_id:
                user_image_ids.append(img_id)
            else:
                db.update_task_status(task_id, 'failed')
                db.add_task_log(task_id, "Image upload failed.")
                db.release_account(api_key_id, account['email'])
                return

        model_version = params.get('model', 'MODEL_FOUR_NANO_BANANA_PRO')
        payload = {
            "prompt": params.get('prompt', ''),
            "imageSize": params.get('imageSize', 'SIXTEEN_BY_NINE'),
            "count": 1,
            "modelType": "MODEL_FOUR",
            "modelVersion": model_version
        }
        if model_version == 'MODEL_FOUR_NANO_BANANA_PRO':
            payload["resolution"] = params.get('resolution', '2K')
        if user_image_ids:
            payload["userImageIds"] = user_image_ids

        resp = requests.post(URL_SUBMIT_IMG, headers=headers, json=payload, timeout=30)
        resp_json = resp.json()

        error = resp_json.get('error')
        if error and error.get('code') != 0:
            db.update_task_status(task_id, 'failed')
            db.add_task_log(task_id, f"Submit error: {resp_json}")
            db.release_account(api_key_id, account['email'])
            return

        api_task_id = str(resp_json['data']['data']['taskId'])
        db.update_task_external_data(task_id, api_task_id, token)
        db.add_task_log(task_id, f"API Task ID: {api_task_id}")

        for _ in range(IMAGE_POLL_ITERS):
            time.sleep(IMAGE_POLL_INTERVAL)
            try:
                poll = requests.get(URL_ASSETS, headers=headers, timeout=15).json()
                for group in poll.get('data', {}).get('data', {}).get('groups', []):
                    for item in group.get('items', []):
                        creation = item.get('detail', {}).get('creation', {})
                        if str(creation.get('taskId')) == api_task_id:
                            if creation.get('taskState') == 'SUCCESS':
                                urls = creation.get('noWaterMarkImageUrl', [])
                                if urls:
                                    db.update_task_status(task_id, 'completed', urls[0])
                                    db.release_account(api_key_id, account['email'])
                                    return
                            elif creation.get('taskState') == 'FAIL':
                                db.update_task_status(task_id, 'failed')
                                db.release_account(api_key_id, account['email'])
                                return
            except Exception as poll_err:
                print(f"Image poll error (task {task_id}): {poll_err}")

        db.update_task_status(task_id, 'timeout')
        db.release_account(api_key_id, account['email'])

    except Exception as e:
        print(f"process_image_task unhandled error: {e}")
        db.update_task_status(task_id, 'error')
        db.add_task_log(task_id, str(e))
        if account:
            db.release_account(api_key_id, account['email'])


def process_video_task(task_id, params, api_key_id):
    account = None
    try:
        db.update_task_status(task_id, 'running')

        token, account = login_with_retry(api_key_id)
        if not token:
            db.update_task_status(task_id, 'failed')
            return

        # Login başarılı → hesabı kaydet.
        # Submit öncesi çökme → external_task_id NULL kalır, orphan cleanup bu hesabı used=0 yapar.
        # Submit sonrası çökme → external_task_id dolu, recovery devam eder, hesap serbest kalmaz.
        db.update_task_account(task_id, account['email'])

        headers = {"authorization": f"Bearer {token}", **DEVICE_HEADERS}

        model = params.get('model', 'SORA2')
        is_i2v = params.get('image') is not None

        if model == 'VEO_3_1':
            payload = {
                "prompt": params.get('prompt', ''),
                "resolution": "720p",
                "lengthOfSecond": 8,
                "aiPromptEnhance": True,
                "size": params.get('size', 'SIXTEEN_BY_NINE'),
                "addEndFrame": False,
                "modelType": "MODEL_FIVE",
                "modelVersion": "MODEL_FIVE_FAST_3"
            }
            if is_i2v:
                img_id = upload_image(token, base64.b64decode(params['image']))
                if not img_id:
                    db.update_task_status(task_id, 'failed')
                    db.release_account(api_key_id, account['email'])
                    return
                payload["userImageId"] = int(str(img_id).strip())
                url_submit = URL_SUBMIT_VIDEO
            else:
                url_submit = URL_SUBMIT_TXT_VIDEO
        else:
            payload = {
                "prompt": params.get('prompt', ''),
                "resolution": "720p",
                "lengthOfSecond": 10,
                "aiPromptEnhance": True,
                "size": params.get('size', 'SIXTEEN_BY_NINE'),
                "addEndFrame": False
            }
            if is_i2v:
                img_id = upload_image(token, base64.b64decode(params['image']))
                if not img_id:
                    db.update_task_status(task_id, 'failed')
                    db.release_account(api_key_id, account['email'])
                    return
                payload["userImageId"] = int(str(img_id).strip())
                payload["modelVersion"] = "MODEL_ELEVEN_IMAGE_TO_VIDEO_V2"
                url_submit = URL_SUBMIT_VIDEO
            else:
                payload["modelType"] = "MODEL_ELEVEN"
                payload["modelVersion"] = "MODEL_ELEVEN_TEXT_TO_VIDEO_V2"
                url_submit = URL_SUBMIT_TXT_VIDEO

        resp = requests.post(url_submit, headers=headers, json=payload, timeout=30)
        resp_json = resp.json()

        error = resp_json.get('error')
        if error and error.get('code') != 0:
            db.update_task_status(task_id, 'failed')
            db.add_task_log(task_id, f"Submit error: {resp_json}")
            db.release_account(api_key_id, account['email'])
            return

        api_task_id = str(resp_json['data']['data']['taskId'])
        db.update_task_external_data(task_id, api_task_id, token)
        db.add_task_log(task_id, f"API Task ID: {api_task_id}")

        for _ in range(VIDEO_POLL_ITERS):
            time.sleep(VIDEO_POLL_INTERVAL)
            try:
                poll = requests.get(URL_VIDEO_TASKS, headers=headers, timeout=15).json()
                video_list = poll.get('data', {}).get('data', {}).get('data', [])
                if not video_list and isinstance(poll.get('data', {}).get('data'), list):
                    video_list = poll['data']['data']
                for v in video_list:
                    if str(v.get('taskId')) == api_task_id:
                        if v.get('taskState') == 'SUCCESS':
                            url = v.get('noWaterMarkVideoUrl') or v.get('noWatermarkVideoUrl')
                            if isinstance(url, list) and url:
                                url = url[0]
                            if url:
                                db.update_task_status(task_id, 'completed', url)
                                db.release_account(api_key_id, account['email'])
                                return
                        elif v.get('taskState') == 'FAIL':
                            db.update_task_status(task_id, 'failed')
                            db.release_account(api_key_id, account['email'])
                            return
            except Exception as poll_err:
                print(f"Video poll error (task {task_id}): {poll_err}")

        db.update_task_status(task_id, 'timeout')
        db.release_account(api_key_id, account['email'])

    except Exception as e:
        print(f"process_video_task unhandled error: {e}")
        db.update_task_status(task_id, 'error')
        db.add_task_log(task_id, str(e))
        if account:
            db.release_account(api_key_id, account['email'])


def process_tts_task(task_id, params):
    try:
        db.update_task_status(task_id, 'running')

        if not ELEVENLABS_API_KEY:
            db.update_task_status(task_id, 'failed')
            db.add_task_log(task_id, "ElevenLabs API key not configured.")
            return

        text = params.get('text', '')
        if not text:
            db.update_task_status(task_id, 'failed')
            db.add_task_log(task_id, "Text is required.")
            return

        voice_id = params.get('voice_id', 'EXAVITQu4vr4xnSDxMaL')
        payload = {
            "text": text,
            "model_id": params.get('model_id', 'eleven_multilingual_v2'),
            "voice_settings": {
                "stability": params.get('stability', 0.5),
                "similarity_boost": params.get('similarity_boost', 0.75),
                "style": params.get('style', 0.0),
                "use_speaker_boost": params.get('use_speaker_boost', True)
            }
        }
        speed = params.get('speed', 1.0)
        if speed != 1.0:
            payload["voice_settings"]["speed"] = speed

        db.add_task_log(task_id, f"Generating TTS with voice: {voice_id}")
        response = requests.post(
            f"{ELEVENLABS_TTS_URL}/{voice_id}",
            json=payload,
            headers={"Accept": "audio/mpeg", "Content-Type": "application/json", "xi-api-key": ELEVENLABS_API_KEY},
            timeout=60
        )

        if response.status_code == 200:
            audio_b64 = base64.b64encode(response.content).decode('utf-8')
            db.update_task_status(task_id, 'completed', f"data:audio/mpeg;base64,{audio_b64}")
            db.add_task_log(task_id, "TTS generation successful.")
        else:
            db.update_task_status(task_id, 'failed')
            db.add_task_log(task_id, f"ElevenLabs API error: {response.status_code} - {response.text}")

    except Exception as e:
        print(f"process_tts_task unhandled error: {e}")
        db.update_task_status(task_id, 'error')
        db.add_task_log(task_id, str(e))


# --- Recovery Logic ---

def poll_image_recovery(task_id, api_task_id, api_key_id):
    """Fresh login ile image task recovery."""
    account = None
    try:
        token, account = login_with_retry(api_key_id)
        if not token:
            db.update_task_status(task_id, 'failed')
            db.add_task_log(task_id, "Recovery: tüm hesaplar login başarısız.")
            return

        headers = {"authorization": f"Bearer {token}", **DEVICE_HEADERS}

        for _ in range(IMAGE_POLL_ITERS):
            time.sleep(IMAGE_POLL_INTERVAL)
            try:
                poll = requests.get(URL_ASSETS, headers=headers, timeout=15).json()
                for group in poll.get('data', {}).get('data', {}).get('groups', []):
                    for item in group.get('items', []):
                        creation = item.get('detail', {}).get('creation', {})
                        if str(creation.get('taskId')) == api_task_id:
                            if creation.get('taskState') == 'SUCCESS':
                                urls = creation.get('noWaterMarkImageUrl', [])
                                if urls:
                                    db.update_task_status(task_id, 'completed', urls[0])
                                    db.release_account(api_key_id, account['email'])
                                    return
                            elif creation.get('taskState') == 'FAIL':
                                db.update_task_status(task_id, 'failed')
                                db.release_account(api_key_id, account['email'])
                                return
            except Exception as e:
                print(f"Image recovery poll error (task {task_id}): {e}")

        db.update_task_status(task_id, 'timeout')
        db.release_account(api_key_id, account['email'])

    except Exception as e:
        db.add_task_log(task_id, f"Recovery error: {e}")
        db.update_task_status(task_id, 'failed')
        if account:
            db.release_account(api_key_id, account['email'])


def poll_video_recovery(task_id, api_task_id, api_key_id):
    """Fresh login ile video task recovery."""
    account = None
    try:
        token, account = login_with_retry(api_key_id)
        if not token:
            db.update_task_status(task_id, 'failed')
            db.add_task_log(task_id, "Recovery: tüm hesaplar login başarısız.")
            return

        headers = {"authorization": f"Bearer {token}", **DEVICE_HEADERS}

        for _ in range(VIDEO_POLL_ITERS):
            time.sleep(VIDEO_POLL_INTERVAL)
            try:
                poll = requests.get(URL_VIDEO_TASKS, headers=headers, timeout=15).json()
                video_list = poll.get('data', {}).get('data', {}).get('data', [])
                if not video_list and isinstance(poll.get('data', {}).get('data'), list):
                    video_list = poll['data']['data']
                for v in video_list:
                    if str(v.get('taskId')) == api_task_id:
                        if v.get('taskState') == 'SUCCESS':
                            url = v.get('noWaterMarkVideoUrl') or v.get('noWatermarkVideoUrl')
                            if isinstance(url, list) and url:
                                url = url[0]
                            if url:
                                db.update_task_status(task_id, 'completed', url)
                                db.release_account(api_key_id, account['email'])
                                return
                        elif v.get('taskState') == 'FAIL':
                            db.update_task_status(task_id, 'failed')
                            db.release_account(api_key_id, account['email'])
                            return
            except Exception as e:
                print(f"Video recovery poll error (task {task_id}): {e}")

        db.update_task_status(task_id, 'timeout')
        db.release_account(api_key_id, account['email'])

    except Exception as e:
        db.add_task_log(task_id, f"Recovery error: {e}")
        db.update_task_status(task_id, 'failed')
        if account:
            db.release_account(api_key_id, account['email'])


def resume_incomplete_tasks():
    """
    Startup:
    1. Orphan taskler (external_task_id NULL) → failed + kayıtlı account_email serbest
    2. external_task_id olan taskler → fresh login ile recovery thread
    """
    print("Checking for incomplete tasks to recover...")
    try:
        db.cleanup_orphan_tasks()

        tasks = db.get_incomplete_tasks()
        if not tasks:
            print("Recovery: kurtarılacak task yok.")
            return

        for t in tasks:
            task_id     = t['task_id']
            mode        = t['mode']
            external_id = t['external_task_id']
            api_key_id  = t['api_key_id']

            print(f"Recovering task {task_id} ({mode}) - External ID: {external_id}")

            if mode == 'image':
                threading.Thread(
                    target=poll_image_recovery,
                    args=(task_id, external_id, api_key_id),
                    daemon=True
                ).start()
            elif mode == 'video':
                threading.Thread(
                    target=poll_video_recovery,
                    args=(task_id, external_id, api_key_id),
                    daemon=True
                ).start()

    except Exception as e:
        print(f"Error during task recovery: {e}")


# --- API Routes ---

@app.route('/api/generate/image', methods=['POST'])
def generate_image():
    api_key_id = verify_api_key()
    if not api_key_id:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.json
    if not data or 'prompt' not in data:
        return jsonify({"error": "Prompt required"}), 400

    if db.get_account_count(api_key_id) == 0:
        return jsonify({"error": "No accounts available"}), 503

    running_count = db.get_running_task_count()
    if running_count >= MAX_CONCURRENT_TASKS:
        return jsonify({"error": "Maximum concurrent tasks reached", "message": f"Currently {running_count}/{MAX_CONCURRENT_TASKS} tasks running."}), 429

    task_id = str(uuid.uuid4())
    db.create_task(api_key_id, task_id, 'image')
    threading.Thread(target=process_image_task, args=(task_id, data, api_key_id), daemon=True).start()
    return jsonify({"task_id": task_id})


@app.route('/api/generate/video', methods=['POST'])
def generate_video():
    api_key_id = verify_api_key()
    if not api_key_id:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.json
    if not data or 'prompt' not in data:
        return jsonify({"error": "Prompt required"}), 400

    if db.get_account_count(api_key_id) == 0:
        return jsonify({"error": "No accounts available"}), 503

    running_count = db.get_running_task_count()
    if running_count >= MAX_CONCURRENT_TASKS:
        return jsonify({"error": "Maximum concurrent tasks reached", "message": f"Currently {running_count}/{MAX_CONCURRENT_TASKS} tasks running."}), 429

    task_id = str(uuid.uuid4())
    db.create_task(api_key_id, task_id, 'video')
    threading.Thread(target=process_video_task, args=(task_id, data, api_key_id), daemon=True).start()
    return jsonify({"task_id": task_id})


@app.route('/api/generate/tts', methods=['POST'])
def generate_tts():
    api_key_id = verify_api_key()
    if not api_key_id:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.json
    if not data or 'text' not in data:
        return jsonify({"error": "Text required"}), 400

    if not ELEVENLABS_API_KEY:
        return jsonify({"error": "ElevenLabs API key not configured"}), 500

    running_count = db.get_running_task_count()
    if running_count >= MAX_CONCURRENT_TASKS:
        return jsonify({"error": "Maximum concurrent tasks reached", "message": f"Currently {running_count}/{MAX_CONCURRENT_TASKS} tasks running."}), 429

    task_id = str(uuid.uuid4())
    db.create_task(api_key_id, task_id, 'tts')
    threading.Thread(target=process_tts_task, args=(task_id, data), daemon=True).start()
    return jsonify({"task_id": task_id})


@app.route('/api/elevenlabs/voices', methods=['GET'])
def get_elevenlabs_voices():
    api_key_id = verify_api_key()
    if not api_key_id:
        return jsonify({"error": "Unauthorized"}), 401

    try:
        response = requests.get(ELEVENLABS_VOICES_URL, headers={"xi-api-key": ELEVENLABS_API_KEY}, timeout=15)
        if response.status_code == 200:
            voices = [{"name": v.get("name"), "voice_id": v.get("voice_id")} for v in response.json().get("voices", [])]
            return jsonify({"voices": voices})
        return jsonify({"error": f"Failed to fetch voices: {response.text}"}), response.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/status/<task_id>', methods=['GET'])
def get_task_status(task_id):
    api_key_id = verify_api_key()
    if not api_key_id:
        return jsonify({"error": "Unauthorized"}), 401

    task = db.get_task(api_key_id, task_id)
    if not task:
        return jsonify({"error": "Task not found"}), 404
    return jsonify(task)


@app.route('/api/status', methods=['GET'])
def get_all_tasks_status():
    api_key_id = verify_api_key()
    if not api_key_id:
        return jsonify({"error": "Unauthorized"}), 401

    running_count = db.get_running_task_count()
    return jsonify({"tasks": db.get_all_tasks(api_key_id), "running_tasks": running_count, "max_concurrent": MAX_CONCURRENT_TASKS})


@app.route('/api/quota', methods=['GET'])
def get_quota():
    api_key_id = verify_api_key()
    if not api_key_id:
        return jsonify({"error": "Unauthorized"}), 401

    running_count = db.get_running_task_count()
    return jsonify({
        "quota": db.get_account_count(api_key_id),
        "running_tasks": running_count,
        "max_concurrent": MAX_CONCURRENT_TASKS,
        "available_slots": MAX_CONCURRENT_TASKS - running_count
    })


@app.route('/api/accounts/add', methods=['POST'])
def add_accounts():
    api_key_id = verify_api_key()
    if not api_key_id:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.json
    if not data or 'accounts' not in data:
        return jsonify({"error": "accounts field required"}), 400

    added = failed = 0
    for acc_str in data['accounts']:
        if ':' in acc_str:
            parts = acc_str.split(':')
            if len(parts) >= 2:
                if db.add_account(api_key_id, parts[0].strip(), parts[1].strip()):
                    added += 1
                else:
                    failed += 1

    return jsonify({"message": f"Added {added} accounts, {failed} failed (duplicates)", "total_accounts": db.get_account_count(api_key_id)})


@app.route('/api/accounts', methods=['GET'])
def get_accounts():
    api_key_id = verify_api_key()
    if not api_key_id:
        return jsonify({"error": "Unauthorized"}), 401

    accounts = db.get_all_accounts(api_key_id)
    return jsonify({"accounts": accounts, "total": len(accounts), "available": sum(1 for a in accounts if not a['used'])})


@app.route('/api/accounts/<email>', methods=['DELETE'])
def delete_account(email):
    api_key_id = verify_api_key()
    if not api_key_id:
        return jsonify({"error": "Unauthorized"}), 401

    if db.delete_account(api_key_id, email):
        return jsonify({"message": f"Account {email} deleted"})
    return jsonify({"error": "Account not found"}), 404


# --- Startup ---
db.init_db()
resume_incomplete_tasks()

if __name__ == '__main__':
    print(f"Maximum concurrent tasks: {MAX_CONCURRENT_TASKS}")
    print("API ready.")
    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)
