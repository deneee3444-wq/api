import os
import json
import time
import uuid
import threading
import atexit
import requests
import base64
from io import BytesIO
from PIL import Image
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import database as db

app = Flask(__name__)
CORS(app)

# Graceful shutdown: polling thread'leri temiz kapansın
_shutdown_event = threading.Event()
atexit.register(lambda: _shutdown_event.set())

# --- Configuration & Constants ---
API_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.ewogICJyb2xlIjogImFub24iLAogICJpc3MiOiAic3VwYWJhc2UiLAogICJpYXQiOiAxNzM0OTY5NjAwLAogICJleHAiOiAxODkyNzM2MDAwCn0.4NnK23LGYvKPGuKI5rwQn2KbLMzzdE4jXpHwbGCqPqY"

# Maximum concurrent tasks
MAX_CONCURRENT_TASKS = 10

# Deevid URLs
URL_AUTH = "https://sp.deevid.ai/auth/v1/token?grant_type=password"
URL_UPLOAD = "https://api.deevid.ai/file-upload/image"
URL_SUBMIT_IMG = "https://api.deevid.ai/text-to-image/task/submit"
URL_SUBMIT_VIDEO = "https://api.deevid.ai/image-to-video/task/submit"
URL_SUBMIT_TXT_VIDEO = "https://api.deevid.ai/text-to-video/task/submit"
URL_ASSETS = "https://api.deevid.ai/my-assets?limit=50&assetType=All&filter=CREATION"
URL_VIDEO_TASKS = "https://api.deevid.ai/video/tasks?page=1&size=20"
URL_QUOTA = "https://api.deevid.ai/subscription/plan"

# ElevenLabs Configuration
ELEVENLABS_API_KEY = "sk_d7cd9c0991b928ab3a7b9f04b0dedfcd7d56d790f2cca302"
ELEVENLABS_TTS_URL = "https://api.elevenlabs.io/v1/text-to-speech"
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
    """Verifies the API key from request headers and returns api_key_id."""
    auth_header = request.headers.get('Authorization')
    if not auth_header:
        return None
    
    # Support both "Bearer <key>" and direct key
    if auth_header.startswith('Bearer '):
        provided_key = auth_header[7:]
    else:
        provided_key = auth_header
    
    # Get API key in database - only existing keys are allowed
    api_key_id = db.get_api_key_id(provided_key)
    return api_key_id

def can_start_new_task():
    """Checks if a new task can be started (max concurrent limit)."""
    return db.get_running_task_count() < MAX_CONCURRENT_TASKS

def refresh_quota(token):
    """Optional but might be required to activate session."""
    headers = {"authorization": f"Bearer {token}", **DEVICE_HEADERS}
    try:
        requests.get(URL_QUOTA, headers=headers)
    except:
        pass

def login_with_retry(api_key_id, task_id=None):
    """Tries logging in with available accounts until one succeeds.
    
    task_id parametresi verildiğinde, hesap alındığı milisaniyede veritabanında
    task ile atomik olarak eşleştirilir (çökme koruması için).
    """
    tried_count = 0
    max_tries = db.get_account_count(api_key_id)
    
    if max_tries == 0:
        print("No accounts loaded!")
        return None, None
    
    while tried_count < max_tries:
        # DEĞİŞİKLİK: task_id'yi de gönderiyoruz.
        # Böylece hesap alındığı milisaniyede veritabanında task ile eşleşiyor.
        account = db.get_next_account(api_key_id, task_id)
        if not account:
            break
        
        tried_count += 1
        headers = {
            "apikey": API_KEY,
        }
        payload = {
            "email": account['email'].strip(),
            "password": account['password'].strip(),
            "gotrue_meta_security": {}
        }
        try:
            resp = requests.post(URL_AUTH, json=payload, headers=headers, timeout=10)
            if resp.status_code == 200:
                token = resp.json().get('access_token')
                if token:
                    refresh_quota(token)
                    return token, account
            print(f"Login failed for {account['email']}: {resp.status_code} - {resp.text}")
            # Login başarısızsa hesabı hemen bırak
            db.release_account(api_key_id, account['email'])
        except Exception as e:
            print(f"Login error for {account['email']}: {e}")
            # Hata durumunda da bırak
            db.release_account(api_key_id, account['email'])
            
    return None, None

def resize_image(image_bytes):
    """Resizes image if it exceeds 3000px on any side."""
    try:
        img = Image.open(BytesIO(image_bytes))
        width, height = img.size
        max_dim = max(width, height)
        if max_dim > 3000:
            scale = 3000 / max_dim
            img = img.resize((round(width * scale), round(height * scale)), Image.LANCZOS)
        
        out = BytesIO()
        img.save(out, format="PNG")
        out.seek(0)
        return out
    except Exception as e:
        print(f"Resize error: {e}")
        return None

def upload_image(token, image_bytes):
    """Uploads image to API and returns image ID."""
    headers = {"authorization": f"Bearer {token}", **DEVICE_HEADERS}
    resized = resize_image(image_bytes)
    if not resized: return None
    
    files = {"file": ("image.png", resized, "image/png")}
    data = {"width": "1024", "height": "1536"} 
    try:
        resp = requests.post(URL_UPLOAD, headers=headers, files=files, data=data)
        if resp.status_code in [200, 201]:
            return resp.json()['data']['data']['id']
    except Exception as e:
        print(f"Upload error: {e}")
    return None

def process_image_task(task_id, params, api_key_id):
    """Worker for image generation."""
    try:
        db.update_task_status(task_id, 'running')
        try:
            # task_id gönderiyoruz: hesap alındığı anda atomik olarak task'e yazılır (çökme koruması)
            token, account = login_with_retry(api_key_id, task_id=task_id)
            if not token:
                db.update_task_status(task_id, 'failed')
                db.add_task_log(task_id, "All accounts failed to login.")
                return

            # NOT: db.update_task_account() artık burada çağrılmıyor.
            # get_next_account() zaten task_id ile atomik olarak account_email'i yazdı.

            headers = {"authorization": f"Bearer {token}", **DEVICE_HEADERS}
            
            user_image_ids = []
            images = params.get('images', [])
            if not images and params.get('image'):  # Geriye dönük uyumluluk
                images = [params.get('image')]

            for img_base64 in images:
                img_data = base64.b64decode(img_base64)
                img_id = upload_image(token, img_data)
                if img_id:
                    user_image_ids.append(img_id)
                else:
                    db.update_task_status(task_id, 'failed')
                    db.add_task_log(task_id, "Image upload failed.")
                    # Release account on upload failure
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

            # Save token BEFORE submit so crash during submit can still recover
            db.update_task_token(task_id, token)

            resp = requests.post(URL_SUBMIT_IMG, headers=headers, json=payload)
            resp_json = resp.json()
            
            error = resp_json.get('error')
            if error and error.get('code') != 0:
                db.update_task_status(task_id, 'failed')
                db.add_task_log(task_id, f"Submit error: {resp_json}")
                # Release account on submission failure
                db.release_account(api_key_id, account['email'])
                return

            api_task_id = str(resp_json['data']['data']['taskId'])
            db.update_task_external_data(task_id, api_task_id, token)
            db.add_task_log(task_id, f"API Task ID: {api_task_id}")

            for _ in range(300):
                if _shutdown_event.wait(2):
                    return  # Shutdown — task 'running' kalır, recovery halleder
                try:
                    poll = requests.get(URL_ASSETS, headers=headers).json()
                    groups = poll.get('data', {}).get('data', {}).get('groups', [])
                    for group in groups:
                        for item in group.get('items', []):
                            creation = item.get('detail', {}).get('creation', {})
                            if str(creation.get('taskId')) == api_task_id:
                                if creation.get('taskState') == 'SUCCESS':
                                    urls = creation.get('noWaterMarkImageUrl', [])
                                    if urls:
                                        db.update_task_status(task_id, 'completed', urls[0])
                                        return
                                elif creation.get('taskState') == 'FAIL':
                                    db.update_task_status(task_id, 'failed')
                                    # Release account on task FAIL
                                    db.release_account(api_key_id, account['email'])
                                    return
                except:
                    pass
            db.update_task_status(task_id, 'timeout')
            db.release_account(api_key_id, account['email'])
        except Exception as e:
            db.update_task_status(task_id, 'error')
            db.add_task_log(task_id, str(e))
            if 'account' in locals() and account:
                db.release_account(api_key_id, account['email'])
    except Exception:
        db.update_task_status(task_id, 'error')

def process_video_task(task_id, params, api_key_id):
    """Worker for video generation."""
    try:
        db.update_task_status(task_id, 'running')
        try:
            # task_id gönderiyoruz: hesap alındığı anda atomik olarak task'e yazılır (çökme koruması)
            token, account = login_with_retry(api_key_id, task_id=task_id)
            if not token:
                db.update_task_status(task_id, 'failed')
                return

            # NOT: db.update_task_account() artık burada çağrılmıyor.
            # get_next_account() zaten task_id ile atomik olarak account_email'i yazdı.

            headers = {"authorization": f"Bearer {token}", **DEVICE_HEADERS}
            
            # Model parametresini al (varsayılan: SORA2)
            model = params.get('model', 'SORA2')
            is_i2v = params.get('image') is not None
            
            # VEO_3_1 modeli için
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
                    img_data = base64.b64decode(params['image'])
                    img_id = upload_image(token, img_data)
                    if not img_id:
                        db.update_task_status(task_id, 'failed')
                        db.release_account(api_key_id, account['email'])
                        return
                    payload["userImageId"] = int(str(img_id).strip())
                    url_submit = URL_SUBMIT_VIDEO
                else:
                    url_submit = URL_SUBMIT_TXT_VIDEO
            
            # SORA2 modeli için (varsayılan)
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
                    img_data = base64.b64decode(params['image'])
                    img_id = upload_image(token, img_data)
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

            # Save token BEFORE submit so crash during submit can still recover
            db.update_task_token(task_id, token)

            resp = requests.post(url_submit, headers=headers, json=payload)
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
            
            for _ in range(600):
                if _shutdown_event.wait(5):
                    return  # Shutdown — task 'running' kalır, recovery halleder
                try:
                    poll = requests.get(URL_VIDEO_TASKS, headers=headers).json()
                    video_list = poll.get('data', {}).get('data', {}).get('data', [])
                    if not video_list and isinstance(poll.get('data', {}).get('data'), list):
                        video_list = poll['data']['data']
                        
                    for v in video_list:
                        if str(v.get('taskId')) == api_task_id:
                            if v.get('taskState') == 'SUCCESS':
                                url = v.get('noWaterMarkVideoUrl') or v.get('noWatermarkVideoUrl')
                                if isinstance(url, list) and url: url = url[0]
                                if url:
                                    db.update_task_status(task_id, 'completed', url)
                                    return
                            elif v.get('taskState') == 'FAIL':
                                db.update_task_status(task_id, 'failed')
                                db.release_account(api_key_id, account['email'])
                                return
                except:
                    pass
            db.update_task_status(task_id, 'timeout')
            db.release_account(api_key_id, account['email'])
        except Exception as e:
            db.update_task_status(task_id, 'error')
            db.add_task_log(task_id, str(e))
            if 'account' in locals() and account:
                db.release_account(api_key_id, account['email'])
    except Exception:
        db.update_task_status(task_id, 'error')

def process_tts_task(task_id, params):
    """Worker for ElevenLabs TTS generation."""
    try:
        db.update_task_status(task_id, 'running')
        try:
            if not ELEVENLABS_API_KEY:
                db.update_task_status(task_id, 'failed')
                db.add_task_log(task_id, "ElevenLabs API key not configured.")
                return

            voice_id = params.get('voice_id', 'EXAVITQu4vr4xnSDxMaL')  # Default: Bella
            text = params.get('text', '')
            
            if not text:
                db.update_task_status(task_id, 'failed')
                db.add_task_log(task_id, "Text is required.")
                return

            # Voice settings
            stability = params.get('stability', 0.5)
            similarity_boost = params.get('similarity_boost', 0.75)
            style = params.get('style', 0.0)
            speed = params.get('speed', 1.0)
            
            url = f"{ELEVENLABS_TTS_URL}/{voice_id}"
            
            headers = {
                "Accept": "audio/mpeg",
                "Content-Type": "application/json",
                "xi-api-key": ELEVENLABS_API_KEY
            }
            
            payload = {
                "text": text,
                "model_id": params.get('model_id', 'eleven_multilingual_v2'),
                "voice_settings": {
                    "stability": stability,
                    "similarity_boost": similarity_boost,
                    "style": style,
                    "use_speaker_boost": params.get('use_speaker_boost', True)
                }
            }
            
            if speed != 1.0:
                payload["voice_settings"]["speed"] = speed

            db.add_task_log(task_id, f"Generating TTS with voice: {voice_id}")
            
            response = requests.post(url, json=payload, headers=headers, timeout=60)
            
            if response.status_code == 200:
                audio_base64 = base64.b64encode(response.content).decode('utf-8')
                
                db.update_task_status(task_id, 'completed', f"data:audio/mpeg;base64,{audio_base64}")
                db.add_task_log(task_id, "TTS generation successful.")
            else:
                db.update_task_status(task_id, 'failed')
                db.add_task_log(task_id, f"ElevenLabs API error: {response.status_code} - {response.text}")
                
        except Exception as e:
            db.update_task_status(task_id, 'error')
            db.add_task_log(task_id, str(e))
    except Exception:
        db.update_task_status(task_id, 'error')

# --- Recovery Logic ---

def check_deevid_for_task(task_id, mode, token, account_email=None, api_key_id=None):
    """Checks Deevid API for a task that may have been submitted before crash.
    Uses the saved token to check recent assets/tasks.
    If found: saves external_task_id and starts polling.
    If not found: marks task as failed and releases account.
    """
    try:
        headers = {"authorization": f"Bearer {token}", **DEVICE_HEADERS}
        
        if mode == 'image':
            # Check recent image assets
            try:
                poll = requests.get(URL_ASSETS, headers=headers, timeout=15).json()
                groups = poll.get('data', {}).get('data', {}).get('groups', [])
                for group in groups:
                    for item in group.get('items', []):
                        creation = item.get('detail', {}).get('creation', {})
                        task_state = creation.get('taskState')
                        api_task_id = creation.get('taskId')
                        
                        if api_task_id and task_state in ('PENDING', 'RUNNING', 'SUBMITTED'):
                            # Found an active task — save and poll
                            api_task_id = str(api_task_id)
                            db.update_task_external_data(task_id, api_task_id, token)
                            db.add_task_log(task_id, f"[RECOVERY] Found active task on Deevid: {api_task_id}")
                            print(f"  [RECOVERY] Task {task_id}: found active Deevid task {api_task_id}, resuming polling")
                            threading.Thread(
                                target=poll_image_recovery,
                                args=(task_id, api_task_id, token, account_email, api_key_id)
                            ).start()
                            return
                        elif api_task_id and task_state == 'SUCCESS':
                            urls = creation.get('noWaterMarkImageUrl', [])
                            if urls:
                                db.update_task_status(task_id, 'completed', urls[0])
                                print(f"  [RECOVERY] Task {task_id}: found completed result on Deevid")
                                return
            except Exception as e:
                print(f"  [RECOVERY] Task {task_id}: Deevid check failed: {e}")
        
        elif mode == 'video':
            # Check recent video tasks
            try:
                poll = requests.get(URL_VIDEO_TASKS, headers=headers, timeout=15).json()
                video_list = poll.get('data', {}).get('data', {}).get('data', [])
                if not video_list and isinstance(poll.get('data', {}).get('data'), list):
                    video_list = poll['data']['data']
                
                for v in video_list:
                    task_state = v.get('taskState')
                    api_task_id = v.get('taskId')
                    
                    if api_task_id and task_state in ('PENDING', 'RUNNING', 'SUBMITTED'):
                        api_task_id = str(api_task_id)
                        db.update_task_external_data(task_id, api_task_id, token)
                        db.add_task_log(task_id, f"[RECOVERY] Found active video task on Deevid: {api_task_id}")
                        print(f"  [RECOVERY] Task {task_id}: found active Deevid video task {api_task_id}, resuming polling")
                        threading.Thread(
                            target=poll_video_recovery,
                            args=(task_id, api_task_id, token, account_email, api_key_id)
                        ).start()
                        return
                    elif api_task_id and task_state == 'SUCCESS':
                        url = v.get('noWaterMarkVideoUrl') or v.get('noWatermarkVideoUrl')
                        if isinstance(url, list) and url: url = url[0]
                        if url:
                            db.update_task_status(task_id, 'completed', url)
                            print(f"  [RECOVERY] Task {task_id}: found completed video on Deevid")
                            return
            except Exception as e:
                print(f"  [RECOVERY] Task {task_id}: Deevid video check failed: {e}")
        
        # Nothing found on Deevid — submit never went through
        db.update_task_status(task_id, 'failed')
        db.add_task_log(task_id, "[RECOVERY] No active task found on Deevid after crash — submit likely never completed.")
        if account_email and api_key_id:
            db.release_account(api_key_id, account_email)
            print(f"  [RECOVERY] Task {task_id}: no Deevid task found, account {account_email} released")
        else:
            print(f"  [RECOVERY] Task {task_id}: no Deevid task found, marked failed")
    except Exception as e:
        print(f"  [RECOVERY] Task {task_id}: check_deevid error: {e}")
        db.update_task_status(task_id, 'failed')
        if account_email and api_key_id:
            db.release_account(api_key_id, account_email)

def poll_image_recovery(task_id, api_task_id, token, account_email=None, api_key_id=None):
    """Polling worker for recovered image tasks."""
    try:
        headers = {"authorization": f"Bearer {token}", **DEVICE_HEADERS}
        for _ in range(300):
            if _shutdown_event.wait(5):
                return  # Shutdown — task 'running' kalır, recovery halleder
            try:
                poll = requests.get(URL_ASSETS, headers=headers).json()
                groups = poll.get('data', {}).get('data', {}).get('groups', [])
                for group in groups:
                    for item in group.get('items', []):
                        creation = item.get('detail', {}).get('creation', {})
                        if str(creation.get('taskId')) == api_task_id:
                            if creation.get('taskState') == 'SUCCESS':
                                urls = creation.get('noWaterMarkImageUrl', [])
                                if urls:
                                    db.update_task_status(task_id, 'completed', urls[0])
                                    return
                            elif creation.get('taskState') == 'FAIL':
                                db.update_task_status(task_id, 'failed')
                                if account_email and api_key_id:
                                    db.release_account(api_key_id, account_email)
                                return
            except:
                pass
        db.update_task_status(task_id, 'timeout')
        if account_email and api_key_id:
            db.release_account(api_key_id, account_email)
    except Exception as e:
        db.add_task_log(task_id, f"Recovery error: {str(e)}")
        db.update_task_status(task_id, 'failed')
        if account_email and api_key_id:
            db.release_account(api_key_id, account_email)

def poll_video_recovery(task_id, api_task_id, token, account_email=None, api_key_id=None):
    """Polling worker for recovered video tasks."""
    try:
        headers = {"authorization": f"Bearer {token}", **DEVICE_HEADERS}
        for _ in range(600):
            if _shutdown_event.wait(10):
                return  # Shutdown — task 'running' kalır, recovery halleder
            try:
                poll = requests.get(URL_VIDEO_TASKS, headers=headers).json()
                video_list = poll.get('data', {}).get('data', {}).get('data', [])
                if not video_list and isinstance(poll.get('data', {}).get('data'), list):
                    video_list = poll['data']['data']
                    
                for v in video_list:
                    if str(v.get('taskId')) == api_task_id:
                        if v.get('taskState') == 'SUCCESS':
                            url = v.get('noWaterMarkVideoUrl') or v.get('noWatermarkVideoUrl')
                            if isinstance(url, list) and url: url = url[0]
                            if url:
                                db.update_task_status(task_id, 'completed', url)
                                return
                        elif v.get('taskState') == 'FAIL':
                            db.update_task_status(task_id, 'failed')
                            if account_email and api_key_id:
                                db.release_account(api_key_id, account_email)
                            return
            except:
                pass
        db.update_task_status(task_id, 'timeout')
        if account_email and api_key_id:
            db.release_account(api_key_id, account_email)
    except Exception as e:
        db.add_task_log(task_id, f"Recovery error: {str(e)}")
        db.update_task_status(task_id, 'failed')
        if account_email and api_key_id:
            db.release_account(api_key_id, account_email)

def resume_incomplete_tasks():
    """Recovers stale tasks and resumes polling for submitted ones."""
    print("=" * 50)
    print("[STARTUP] Starting crash recovery...")
    
    # Phase 1: Clean up truly stale tasks + get tasks that need Deevid check
    try:
        recovery_result = db.recover_stale_tasks()
        if recovery_result['failed_count'] > 0:
            print(f"[STARTUP] Marked {recovery_result['failed_count']} tasks as failed (never logged in)")
    except Exception as e:
        print(f"[STARTUP] Error during stale task recovery: {e}")
        recovery_result = {'needs_check': []}
    
    # Phase 2: Check Deevid API for tasks that had token but no external_task_id
    needs_check = recovery_result.get('needs_check', [])
    if needs_check:
        print(f"[STARTUP] Checking Deevid API for {len(needs_check)} tasks that may have been submitted...")
        for t in needs_check:
            threading.Thread(
                target=check_deevid_for_task,
                args=(t['task_id'], t['mode'], t['token'], t.get('account_email'), t.get('api_key_id'))
            ).start()
    
    # Phase 3: Resume polling for tasks that WERE confirmed submitted (have external_task_id)
    try:
        tasks = db.get_incomplete_tasks()
        if tasks:
            print(f"[STARTUP] Resuming polling for {len(tasks)} confirmed submitted tasks...")
        else:
            print(f"[STARTUP] No confirmed tasks to resume.")
            
        for t in tasks:
            task_id = t['task_id']
            mode = t['mode']
            external_id = t['external_task_id']
            token = t['token']
            account_email = t.get('account_email')
            api_key_id = t.get('api_key_id')
            
            print(f"  [RESUME] Task {task_id} ({mode}) - External ID: {external_id}")
            if mode == 'image':
                threading.Thread(
                    target=poll_image_recovery,
                    args=(task_id, external_id, token, account_email, api_key_id)
                ).start()
            elif mode == 'video':
                threading.Thread(
                    target=poll_video_recovery,
                    args=(task_id, external_id, token, account_email, api_key_id)
                ).start()
    except Exception as e:
        print(f"[STARTUP] Error during task resume: {e}")
    
    print("[STARTUP] Crash recovery complete.")
    print("=" * 50)

# --- API Routes ---

@app.errorhandler(Exception)
def handle_exception(e):
    print(f"[ERROR] {request.method} {request.path} → {type(e).__name__}: {e}")
    return jsonify({"error": "Internal server error"}), 500

@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')

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
        return jsonify({
            "error": "Maximum concurrent tasks reached",
            "message": f"Currently {running_count}/{MAX_CONCURRENT_TASKS} tasks running. Please wait."
        }), 429
    
    task_id = str(uuid.uuid4())
    db.create_task(api_key_id, task_id, 'image')
    
    threading.Thread(target=process_image_task, args=(task_id, data, api_key_id)).start()
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
        return jsonify({
            "error": "Maximum concurrent tasks reached",
            "message": f"Currently {running_count}/{MAX_CONCURRENT_TASKS} tasks running. Please wait."
        }), 429
    
    task_id = str(uuid.uuid4())
    db.create_task(api_key_id, task_id, 'video')
    
    threading.Thread(target=process_video_task, args=(task_id, data, api_key_id)).start()
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
        return jsonify({
            "error": "Maximum concurrent tasks reached",
            "message": f"Currently {running_count}/{MAX_CONCURRENT_TASKS} tasks running. Please wait."
        }), 429
    
    task_id = str(uuid.uuid4())
    db.create_task(api_key_id, task_id, 'tts')
    
    threading.Thread(target=process_tts_task, args=(task_id, data)).start()
    return jsonify({"task_id": task_id})

@app.route('/api/elevenlabs/voices', methods=['GET'])
def get_elevenlabs_voices():
    api_key_id = verify_api_key()
    if not api_key_id:
        return jsonify({"error": "Unauthorized"}), 401
    
    if not ELEVENLABS_API_KEY:
        return jsonify({"error": "ElevenLabs API key not configured"}), 500
    
    try:
        headers = {"xi-api-key": ELEVENLABS_API_KEY}
        response = requests.get(ELEVENLABS_VOICES_URL, headers=headers)
        
        if response.status_code == 200:
            voices_data = response.json()
            simplified_voices = [
                {
                    "name": voice.get("name"),
                    "voice_id": voice.get("voice_id")
                }
                for voice in voices_data.get("voices", [])
            ]
            return jsonify({"voices": simplified_voices})
        else:
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
    return jsonify({
        "tasks": db.get_all_tasks(api_key_id),
        "running_tasks": running_count,
        "max_concurrent": MAX_CONCURRENT_TASKS
    })

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
    
    added = 0
    failed = 0
    for acc_str in data['accounts']:
        if ':' in acc_str:
            parts = acc_str.split(':')
            if len(parts) >= 2:
                email = parts[0].strip()
                password = parts[1].strip()
                if db.add_account(api_key_id, email, password):
                    added += 1
                else:
                    failed += 1
    
    return jsonify({
        "message": f"Added {added} accounts, {failed} failed (duplicates)",
        "total_accounts": db.get_account_count(api_key_id)
    })

@app.route('/api/accounts', methods=['GET'])
def get_accounts():
    api_key_id = verify_api_key()
    if not api_key_id:
        return jsonify({"error": "Unauthorized"}), 401
    
    accounts = db.get_all_accounts(api_key_id)
    return jsonify({
        "accounts": accounts,
        "total": len(accounts),
        "available": sum(1 for a in accounts if not a['used'])
    })

@app.route('/api/accounts/<email>', methods=['DELETE'])
def delete_account(email):
    api_key_id = verify_api_key()
    if not api_key_id:
        return jsonify({"error": "Unauthorized"}), 401
    
    if db.delete_account(api_key_id, email):
        return jsonify({"message": f"Account {email} deleted"})
    else:
        return jsonify({"error": "Account not found"}), 404
