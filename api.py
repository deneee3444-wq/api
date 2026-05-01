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
URL_SUBMIT_CHARACTER_VIDEO = "https://api.deevid.ai/character-to-video/task/submit"
URL_SUBMIT_TTS = "https://api.deevid.ai/text-to-speech/task/submit"
URL_SUBMIT_MULTIMODAL_VIDEO = "https://api.deevid.ai/video/multimodal/task"
URL_SUBMIT_QUALITY_V2_5 = "https://api.deevid.ai/generation/task"
URL_PRESIGN_MP3 = "https://api.deevid.ai/file-upload/presign/mp3"
URL_CONFIRM_MP3 = "https://api.deevid.ai/file-upload/confirm/mp3"
URL_TTS_VOICES = "https://api.deevid.ai/public-voices"
URL_ASSETS = "https://api.deevid.ai/my-assets?limit=50&assetType=All&filter=CREATION"
URL_VIDEO_TASKS = "https://api.deevid.ai/video/tasks?page=1&size=20"
URL_QUOTA = "https://api.deevid.ai/subscription/plan"

# TTS frontend model name → Deevid modelVersion mapping
TTS_MODEL_MAP = {
    'MINIMAX':       'MODEL_SEVEN_SPEECH_26_HD',
    'MINIMAX-TURBO': 'MODEL_SEVEN_SPEECH_26_TURBO',
}

# Music frontend model name → Deevid selection model mapping
MUSIC_MODEL_MAP = {
    'SUNO': 'quality V1.0',
}

# Frontend model name → Deevid model version mapping
IMAGE_MODEL_MAP = {
    'NANO_BANANA_PRO': 'MODEL_FOUR_NANO_BANANA_PRO',
    'NANO_BANANA':     'MODEL_FOUR_NANO_BANANA',
    'NANO_BANANA_2':   'MODEL_FOUR_NANO_BANANA_2',
}


# Frontend size value → Deevid size value mapping
SIZE_MAP = {
    '16:9': 'SIXTEEN_BY_NINE',
    '9:16': 'NINE_BY_SIXTEEN',
    '1:1':  'ONE_BY_ONE',
    '3:4':  'THREE_BY_FOUR',
    '4:3':  'FOUR_BY_THREE',
    '3:2':  'THREE_BY_TWO',
}

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

def can_start_new_task(api_key_id):
    """Checks if a new task can be started (max concurrent limit per user)."""
    return db.get_running_task_count(api_key_id) < MAX_CONCURRENT_TASKS

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

def upload_image(token, image_bytes, use_asset_id=False, return_url=False):
    """Uploads image to API and returns image ID.
    use_asset_id=True: SEEDANCE_2_0 için assetId döner, aksi halde id döner.
    return_url=True: (assetId, assetUrl) tuple döner — QUALITY_V2_5 için.
    """
    headers = {"authorization": f"Bearer {token}", **DEVICE_HEADERS}
    resized = resize_image(image_bytes)
    if not resized: return (None, None) if return_url else None
    
    files = {"file": ("image.png", resized, "image/png")}
    data = {"width": "1024", "height": "1536"} 
    try:
        resp = requests.post(URL_UPLOAD, headers=headers, files=files, data=data)
        if resp.status_code in [200, 201]:
            d = resp.json()['data']['data']
            if return_url:
                asset_id = str(d['assetId'])
                asset_url = d.get('url') or f"https://cdn2.deevid.ai/user-image/{d['imageName']}" #asset_url = d.get('url', '')
                return asset_id, asset_url
            key = 'assetId' if use_asset_id else 'id'
            return d[key]
    except Exception as e:
        print(f"Upload error: {e}")
    return (None, None) if return_url else None

def upload_audio(token, audio_bytes):
    """Uploads an MP3 audio file via presign → PUT → confirm flow.
    Returns (asset_id, asset_url) tuple or (None, None) on failure.
    """
    try:
        headers = {"authorization": f"Bearer {token}", **DEVICE_HEADERS}

        # Step 1: Get presigned URL
        presign_resp = requests.post(URL_PRESIGN_MP3, headers=headers, timeout=15)
        if presign_resp.status_code not in [200, 201]:
            print(f"Presign failed: {presign_resp.status_code} {presign_resp.text}")
            return None, None
        presign_data = presign_resp.json()['data']['data']
        presigned_url = presign_data['presignedUrl']
        file_name = presign_data['fileName']

        # Step 2: PUT audio bytes to presigned URL
        put_resp = requests.put(
            presigned_url,
            data=audio_bytes,
            headers={"Content-Type": "audio/mpeg"},
            timeout=60
        )
        if put_resp.status_code not in [200, 201, 204]:
            print(f"Audio PUT failed: {put_resp.status_code} {put_resp.text}")
            return None, None

        # Step 3: Confirm upload
        confirm_resp = requests.post(
            URL_CONFIRM_MP3,
            json={"fileName": file_name},
            headers=headers,
            timeout=15
        )
        if confirm_resp.status_code not in [200, 201]:
            print(f"Confirm failed: {confirm_resp.status_code} {confirm_resp.text}")
            return None, None
        confirm_data = confirm_resp.json()['data']['data']
        asset_id = str(confirm_data['assetId'])
        asset_url = confirm_data['url']
        return asset_id, asset_url

    except Exception as e:
        print(f"Audio upload error: {e}")
        return None, None


def process_music_task(task_id, params, api_key_id):
    """Worker for SUNO music generation via Deevid."""
    try:
        db.update_task_status(task_id, 'running')
        try:
            token, account = login_with_retry(api_key_id, task_id=task_id)
            if not token:
                db.update_task_status(task_id, 'failed')
                db.add_task_log(task_id, "Login failed.")
                return

            headers = {"authorization": f"Bearer {token}", **DEVICE_HEADERS}

            # Optional: upload reference audio
            asset_ids = []
            asset_urls = []
            audio_b64 = params.get('audio_base64')
            if audio_b64:
                audio_bytes = base64.b64decode(audio_b64)
                aid, aurl = upload_audio(token, audio_bytes)
                if not aid:
                    db.update_task_status(task_id, 'failed')
                    db.add_task_log(task_id, "Audio upload failed.")
                    db.release_account(api_key_id, account['email'])
                    return
                asset_ids.append(aid)
                asset_urls.append(aurl)
                db.update_task_reference_audio(task_id, aurl)

            prompt = params.get('prompt', '')
            style = params.get('style', '')
            lyrics = params.get('lyrics', '')
            instrumental = params.get('instrumental', False)
            model_key = MUSIC_MODEL_MAP.get(params.get('model', 'SUNO'), 'quality V1.0')

            inputs = {"prompt": prompt}
            if asset_ids:
                inputs["assetIds"] = asset_ids
                inputs["assetUrls"] = asset_urls

            audio_usage = params.get('audioUsage', 'TEXT')
            music_params = {
                "instrumental": instrumental,
                "audioUsage": audio_usage,
            }
            if style:
                music_params["style"] = style
            if lyrics and not instrumental:
                music_params["lyrics"] = lyrics

            payload = {
                "selection": {
                    "modality": "audio",
                    "capability": "music",
                    "model": model_key
                },
                "inputs": inputs,
                "params": music_params
            }

            db.update_task_token(task_id, token)

            resp = requests.post(URL_SUBMIT_QUALITY_V2_5, headers=headers, json=payload, timeout=30)
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

            poll_headers = {"authorization": f"Bearer {token}", **DEVICE_HEADERS}
            for _ in range(600):  # max ~30 minutes
                if _shutdown_event.wait(3):
                    return
                try:
                    poll = requests.get(URL_ASSETS, headers=poll_headers).json()
                    groups = poll.get('data', {}).get('data', {}).get('groups', [])
                    for group in groups:
                        for item in group.get('items', []):
                            creation = item.get('detail', {}).get('creation', {})
                            if str(creation.get('taskId')) != api_task_id:
                                continue
                            state = creation.get('taskState')
                            if state == 'SUCCESS':
                                music_name = creation.get('musicName', '')
                                music_urls = creation.get('musicUrls', [])
                                cover_urls = creation.get('coverImageUrls', [])
                                if not music_urls:
                                    continue
                                tracks = []
                                for i, murl in enumerate(music_urls):
                                    tracks.append({
                                        "musicName": music_name,
                                        "musicUrl": murl,
                                        "coverImageUrl": cover_urls[i] if i < len(cover_urls) else None,
                                        "version": i + 1
                                    })
                                db.update_task_status(task_id, 'completed', json.dumps(tracks))
                                db.add_task_log(task_id, f"Music generation successful. {len(tracks)} tracks.")
                                return
                            elif state == 'FAIL':
                                db.update_task_status(task_id, 'failed')
                                db.add_task_log(task_id, "Music task failed on Deevid.")
                                db.release_account(api_key_id, account['email'])
                                return
                except Exception:
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


def process_image_task(task_id, params, api_key_id):
    """Worker for image generation."""
    try:
        db.update_task_status(task_id, 'running')
        try:
            # task_id gönderiyoruz: hesap alındığı anda atomik olarak task'e yazılır (çökme koruması)
            token, account = login_with_retry(api_key_id, task_id=task_id)
            if not token:
                db.update_task_status(task_id, 'failed')
                db.add_task_log(task_id, "Insufficient quota.")
                return

            # NOT: db.update_task_account() artık burada çağrılmıyor.
            # get_next_account() zaten task_id ile atomik olarak account_email'i yazdı.

            headers = {"authorization": f"Bearer {token}", **DEVICE_HEADERS}
            
            model_version_raw = params.get('model', 'NANO_BANANA_PRO')

            # GPT_IMAGE_2: yeni endpoint ve payload yapısı
            if model_version_raw == 'GPT_IMAGE_2':
                asset_ids = []
                asset_urls = []
                for img_base64 in params.get('reference_images', []):
                    img_data = base64.b64decode(img_base64)
                    aid, aurl = upload_image(token, img_data, return_url=True)
                    if not aid:
                        db.update_task_status(task_id, 'failed')
                        db.add_task_log(task_id, "Image upload failed.")
                        db.release_account(api_key_id, account['email'])
                        return
                    asset_ids.append(aid)
                    asset_urls.append(aurl)

                inputs = {"prompt": params.get('prompt', '')}
                if asset_ids:
                    inputs["assetIds"] = asset_ids
                    inputs["assetUrls"] = asset_urls

                payload = {
                    "selection": {"modality": "image", "capability": "image-to-image", "model": "gpt-image-2"},
                    "inputs": inputs,
                    "params": {
                        "aspect_ratio": params.get('size', '16:9'),
                        "resolution": params.get('resolution', '1K'),
                        "count": 1
                    }
                }

                db.update_task_token(task_id, token)
                resp = requests.post(URL_SUBMIT_QUALITY_V2_5, headers=headers, json=payload)
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

                if asset_urls:
                    db.update_task_reference_urls(task_id, asset_urls)

                for _ in range(1000):
                    if _shutdown_event.wait(2):
                        return
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
                                        db.release_account(api_key_id, account['email'])
                                        return
                    except:
                        pass
                db.update_task_status(task_id, 'timeout')
                db.release_account(api_key_id, account['email'])
                return

            user_image_ids = []
            images = params.get('reference_images', [])

            for img_base64 in images:
                img_data = base64.b64decode(img_base64)
                img_id = upload_image(token, img_data)
                if img_id:
                    user_image_ids.append(img_id)
                else:
                    db.update_task_status(task_id, 'failed')
                    db.add_task_log(task_id, "Image upload failed.")
                    db.release_account(api_key_id, account['email'])
                    return

            model_version_raw = params.get('model', 'NANO_BANANA_PRO')
            model_version = IMAGE_MODEL_MAP.get(model_version_raw, model_version_raw)
            image_size_raw = params.get('size', '16:9')
            image_size = SIZE_MAP.get(image_size_raw, image_size_raw)
            payload = {
                "prompt": params.get('prompt', ''),
                "imageSize": image_size,
                "count": 1,
                "modelType": "MODEL_FOUR",
                "modelVersion": model_version
            }
            
            if model_version in ('MODEL_FOUR_NANO_BANANA_PRO', 'MODEL_FOUR_NANO_BANANA_2'):
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

            ref_urls = resp_json['data']['data'].get('inputUserImageUrls') or []
            if ref_urls:
                db.update_task_reference_urls(task_id, ref_urls)

            for _ in range(1000):
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
            
            # Model parametresini al (frontend'den VEO_3 veya SORA_2 gelir)
            model = params.get('model', 'SORA_2')
            size_raw = params.get('size', '16:9')
            size = SIZE_MAP.get(size_raw, size_raw)
            is_i2v = params.get('start_frame') is not None
            
            # VEO_3 modeli için
            if model == 'VEO_3':
                end_frame = params.get('end_frame')
                payload = {
                    "prompt": params.get('prompt', ''),
                    "resolution": "720p",
                    "lengthOfSecond": 8,
                    "aiPromptEnhance": params.get('aiPromptEnhance', True),
                    "size": size,
                    "addEndFrame": bool(end_frame),
                    "modelType": "MODEL_FIVE",
                    "modelVersion": "MODEL_FIVE_FAST_3"
                }
                
                if is_i2v:
                    img_data = base64.b64decode(params['start_frame'])
                    img_id = upload_image(token, img_data)
                    if not img_id:
                        db.update_task_status(task_id, 'failed')
                        db.release_account(api_key_id, account['email'])
                        return
                    payload["userImageId"] = int(str(img_id).strip())
                    url_submit = URL_SUBMIT_VIDEO
                else:
                    url_submit = URL_SUBMIT_TXT_VIDEO

                if end_frame:
                    end_frame_data = base64.b64decode(end_frame)
                    end_frame_id = upload_image(token, end_frame_data)
                    if not end_frame_id:
                        db.update_task_status(task_id, 'failed')
                        db.add_task_log(task_id, "End frame upload failed.")
                        db.release_account(api_key_id, account['email'])
                        return
                    payload["endFrameUserImageId"] = int(str(end_frame_id).strip())

                reference_images = params.get("reference_images", [])
                if reference_images:
                    ref_ids = []
                    for ref_b64 in reference_images:
                        ref_data = base64.b64decode(ref_b64)
                        ref_id = upload_image(token, ref_data)
                        if not ref_id:
                            db.update_task_status(task_id, "failed")
                            db.add_task_log(task_id, "Reference image upload failed.")
                            db.release_account(api_key_id, account["email"])
                            return
                        ref_ids.append(int(str(ref_id).strip()))
                    payload = {
                        "prompt": params.get('prompt', ''),
                        "resolution": "720p",
                        "duration": 8,
                        "size": size,
                        "aiPromptEnhance": params.get('aiPromptEnhance', True),
                        "modelVersion": "MODEL_FIVE_FAST_3",
                        "userImageIds": ref_ids
                    }
                    url_submit = URL_SUBMIT_CHARACTER_VIDEO
            
            # VIDU_Q3 modeli için (sadece img2vid, 5sn, 720p)
            elif model == 'VIDU_Q3':
                if not is_i2v:
                    db.update_task_status(task_id, 'failed')
                    db.add_task_log(task_id, "VIDU_Q3 model only supports image-to-video.")
                    db.release_account(api_key_id, account['email'])
                    return

                img_data = base64.b64decode(params['start_frame'])
                img_id = upload_image(token, img_data)
                if not img_id:
                    db.update_task_status(task_id, 'failed')
                    db.add_task_log(task_id, "Start frame upload failed.")
                    db.release_account(api_key_id, account['email'])
                    return

                vidu_duration = int(params.get('duration', 5))
                if vidu_duration not in [5, 10]:
                    vidu_duration = 5
                vidu_resolution = params.get('resolution', '720p')
                if vidu_resolution not in ['720p', '512p']:
                    vidu_resolution = '720p'
                # 720p yalnızca 5s destekler
                if vidu_resolution == '720p' and vidu_duration == 10:
                    vidu_duration = 5

                payload = {
                    "userImageId": int(str(img_id).strip()),
                    "prompt": params.get('prompt', ''),
                    "lengthOfSecond": vidu_duration,
                    "resolution": vidu_resolution,
                    "aiPromptEnhance": False,
                    "addEndFrame": False,
                    "modelVersion": "MODEL_TWO_Q_3_PRO"
                }
                url_submit = URL_SUBMIT_VIDEO

            # QUALITY_V2_5 modeli için (sadece img2vid, 5sn/720p veya 10sn/480p)
            elif model == 'QUALITY_V2_5':
                if not is_i2v:
                    db.update_task_status(task_id, 'failed')
                    db.add_task_log(task_id, "QUALITY_V2_5 model only supports image-to-video.")
                    db.release_account(api_key_id, account['email'])
                    return

                img_data = base64.b64decode(params['start_frame'])
                asset_id, asset_url = upload_image(token, img_data, return_url=True)
                if not asset_id:
                    db.update_task_status(task_id, 'failed')
                    db.add_task_log(task_id, "Start frame upload failed.")
                    db.release_account(api_key_id, account['email'])
                    return

                db.update_task_frame_urls(task_id, start_frame_url=asset_url, end_frame_url=None)

                qv_duration = int(params.get('duration', 5))
                if qv_duration not in [5, 10]:
                    qv_duration = 5
                qv_resolution = "480p" if qv_duration == 10 else "720p"

                payload = {
                    "selection": {
                        "model": "quality-v2.5",
                        "modality": "video",
                        "capability": "start-image"
                    },
                    "inputs": {
                        "prompt": params.get('prompt', ''),
                        "assetIds": [asset_id],
                        "assetUrls": [asset_url]
                    },
                    "params": {
                        "duration": str(qv_duration),
                        "resolution": qv_resolution
                    }
                }
                url_submit = URL_SUBMIT_QUALITY_V2_5
            elif model == 'SEEDANCE_2_0':
                reference_images = params.get('reference_images', [])

                if is_i2v:
                    # IMAGE2VIDEO: start frame + opsiyonel end frame
                    asset_ids = []
                    img_data = base64.b64decode(params['start_frame'])
                    img_id = upload_image(token, img_data, use_asset_id=True)
                    if not img_id:
                        db.update_task_status(task_id, 'failed')
                        db.add_task_log(task_id, "Start frame upload failed.")
                        db.release_account(api_key_id, account['email'])
                        return
                    asset_ids.append(int(str(img_id).strip()))

                    end_frame = params.get('end_frame')
                    if end_frame:
                        end_frame_data = base64.b64decode(end_frame)
                        end_frame_id = upload_image(token, end_frame_data, use_asset_id=True)
                        if not end_frame_id:
                            db.update_task_status(task_id, 'failed')
                            db.add_task_log(task_id, "End frame upload failed.")
                            db.release_account(api_key_id, account['email'])
                            return
                        asset_ids.append(int(str(end_frame_id).strip()))

                    payload = {
                        "assetIds": asset_ids,
                        "prompt": params.get('prompt', ''),
                        "resolution": "480p",
                        "duration": 5,
                        "type": "IMAGE2VIDEO"
                    }

                elif reference_images:
                    # CHARACTER2VIDEO: reference images + ratio
                    asset_ids = []
                    for ref_b64 in reference_images:
                        ref_data = base64.b64decode(ref_b64)
                        ref_id = upload_image(token, ref_data, use_asset_id=True)
                        if not ref_id:
                            db.update_task_status(task_id, 'failed')
                            db.add_task_log(task_id, "Reference image upload failed.")
                            db.release_account(api_key_id, account['email'])
                            return
                        asset_ids.append(int(str(ref_id).strip()))

                    size_raw = params.get('size', '16:9')
                    ratio = SIZE_MAP.get(size_raw, 'SIXTEEN_BY_NINE')
                    payload = {
                        "assetIds": asset_ids,
                        "prompt": params.get('prompt', ''),
                        "resolution": "480p",
                        "duration": 5,
                        "ratio": ratio,
                        "type": "CHARACTER2VIDEO"
                    }

                else:
                    # TXT2VIDEO
                    size_raw = params.get('size', '16:9')
                    ratio = SIZE_MAP.get(size_raw, 'SIXTEEN_BY_NINE')
                    payload = {
                        "prompt": params.get('prompt', ''),
                        "resolution": "480p",
                        "duration": 5,
                        "ratio": ratio,
                        "type": "TXT2VIDEO"
                    }

                url_submit = URL_SUBMIT_MULTIMODAL_VIDEO

            # SORA_2 modeli için (varsayılan)
            else:
                payload = {
                    "prompt": params.get('prompt', ''),
                    "resolution": "720p",
                    "lengthOfSecond": 10,
                    "aiPromptEnhance": True,
                    "size": size,
                    "addEndFrame": False
                }

                if is_i2v:
                    img_data = base64.b64decode(params['start_frame'])
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

            data_obj = resp_json['data']['data']
            orig_urls = data_obj.get('originalImageNameUrls') or []
            end_frame_resp_url = data_obj.get('endFrameUserImageUrl')

            reference_images = params.get("reference_images", [])
            if reference_images:
                if orig_urls:
                    db.update_task_reference_urls(task_id, orig_urls)
            else:
                start_url = orig_urls[0] if orig_urls else None
                end_url = end_frame_resp_url if end_frame_resp_url else None
                if start_url or end_url:
                    db.update_task_frame_urls(task_id, start_frame_url=start_url, end_frame_url=end_url)

            # QUALITY_V2_5: URL_ASSETS ile poll et (groups/items/creation yapısı)
            if model == 'QUALITY_V2_5':
                for _ in range(1000):
                    if _shutdown_event.wait(5):
                        return
                    try:
                        poll = requests.get(URL_ASSETS, headers=headers).json()
                        groups = poll.get('data', {}).get('data', {}).get('groups', [])
                        for group in groups:
                            for item in group.get('items', []):
                                creation = item.get('detail', {}).get('creation', {})
                                if str(creation.get('taskId')) == api_task_id:
                                    # start_frame_url'yi buradan kaydet
                                    poll_orig_urls = creation.get('originalImageNameUrls') or []
                                    if poll_orig_urls:
                                        db.update_task_frame_urls(task_id, start_frame_url=poll_orig_urls[0], end_frame_url=None)
                                    if creation.get('taskState') == 'SUCCESS':
                                        url = creation.get('noWaterMarkVideoUrl') or creation.get('noWatermarkVideoUrl')
                                        if isinstance(url, list) and url: url = url[0]
                                        if url:
                                            db.update_task_status(task_id, 'completed', url)
                                            return
                                    elif creation.get('taskState') == 'FAIL':
                                        db.update_task_status(task_id, 'failed')
                                        db.release_account(api_key_id, account['email'])
                                        return
                    except:
                        pass
                db.update_task_status(task_id, 'timeout')
                db.release_account(api_key_id, account['email'])
                return

            for _ in range(1000):
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

def process_tts_task(task_id, params, api_key_id):
    """Worker for Deevid TTS generation."""
    try:
        db.update_task_status(task_id, 'running')
        try:
            token, account = login_with_retry(api_key_id, task_id=task_id)
            if not token:
                db.update_task_status(task_id, 'failed')
                db.add_task_log(task_id, "Login failed.")
                return

            text = params.get('text', '')
            if not text:
                db.update_task_status(task_id, 'failed')
                db.add_task_log(task_id, "Text is required.")
                db.release_account(api_key_id, account['email'])
                return

            headers = {"authorization": f"Bearer {token}", "Content-Type": "application/json"}

            payload = {
                "text": text,
                "voiceId": params.get('voiceId', 'English_expressive_narrator'),
                "speed": params.get('speed', 1.0),
                "pitch": params.get('pitch', 0),
                "volume": params.get('volume', 1.0),
                "modelVersion": TTS_MODEL_MAP.get(params.get('model', 'MINIMAX-TURBO'), 'MODEL_SEVEN_SPEECH_26_TURBO'),
            }
            emotion = params.get('emotion', 'auto')
            if emotion and emotion != 'auto':
                payload['emotion'] = emotion

            db.add_task_log(task_id, f"Submitting TTS with voice: {payload['voiceId']}")

            resp = requests.post(URL_SUBMIT_TTS, json=payload, headers=headers, timeout=30)
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

            poll_headers = {"authorization": f"Bearer {token}"}
            for _ in range(600):  # max ~30 dakika
                if _shutdown_event.wait(3):
                    return
                try:
                    poll = requests.get(URL_ASSETS, headers=poll_headers).json()
                    groups = poll.get('data', {}).get('data', {}).get('groups', [])
                    for group in groups:
                        for item in group.get('items', []):
                            creation = item.get('detail', {}).get('creation', {})
                            if str(creation.get('taskId')) == api_task_id:
                                state = creation.get('taskState')
                                if state == 'SUCCESS':
                                    speech_url = creation.get('speechUrl')
                                    if speech_url:
                                        db.update_task_status(task_id, 'completed', speech_url)
                                        db.add_task_log(task_id, "TTS generation successful.")
                                        return
                                elif state == 'FAIL':
                                    db.update_task_status(task_id, 'failed')
                                    db.add_task_log(task_id, "TTS task failed on Deevid.")
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
        for _ in range(1000):
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
        for _ in range(1000):
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

TASK_FIELDS_BY_MODE = {
    'image': ['task_id', 'mode', 'status', 'result_url', 'prompt', 'model', 'size', 'resolution', 'reference_image_urls', 'logs', 'created_at'],
    'video': ['task_id', 'mode', 'status', 'result_url', 'prompt', 'model', 'size', 'resolution', 'duration', 'start_frame_url', 'end_frame_url', 'reference_image_urls', 'logs', 'created_at'],
    'tts':   ['task_id', 'mode', 'status', 'result_url', 'prompt', 'model', 'voice_id', 'speed', 'pitch', 'volume', 'emotion', 'logs', 'created_at'],
    'music': ['task_id', 'mode', 'status', 'result_url', 'prompt', 'model', 'style', 'lyrics', 'instrumental', 'audio_usage', 'reference_audio_url', 'logs', 'created_at'],
}

def filter_task_fields(task):
    """Filters task dict fields based on mode."""
    if not task:
        return task
    mode = task.get('mode')
    fields = TASK_FIELDS_BY_MODE.get(mode, list(task.keys()))
    result = {k: task[k] for k in fields if k in task}
    # Convert instrumental integer to boolean for music tasks
    if mode == 'music' and 'instrumental' in result:
        result['instrumental'] = bool(result.get('instrumental'))
    return result

@app.errorhandler(Exception)
def handle_exception(e):
    print(f"[ERROR] {request.method} {request.path} → {type(e).__name__}: {e}")
    return jsonify({"error": "Internal server error"}), 500

@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')

@app.route('/api-doc', methods=['GET'])
def api_doc():
    return render_template('apiDocNoTTS.html')

@app.route('/api/generate/image', methods=['POST'])
def generate_image():
    api_key_id = verify_api_key()
    if not api_key_id:
        return jsonify({"error": "Unauthorized"}), 401
    
    data = request.json
    if not data or 'prompt' not in data:
        return jsonify({"error": "Prompt required"}), 400
    
    images = data.get('reference_images', [])
    if isinstance(images, list) and len(images) > 5:
        return jsonify({"error": "Maximum 5 images allowed"}), 400

    if len(data.get('prompt', '')) > 4000:
        return jsonify({"error": "Prompt must be 4000 characters or less"}), 400

    model = data.get('model', 'NANO_BANANA_PRO')
    if model in ('NANO_BANANA_PRO', 'NANO_BANANA_2', 'GPT_IMAGE_2'):
        resolution = data.get('resolution', '2K')
        if resolution not in ['1K', '2K', '4K']:
            return jsonify({"error": "Invalid resolution. Must be one of: 1K, 2K, 4K"}), 400

    if db.get_account_count(api_key_id) == 0:
        return jsonify({"error": "No quota available"}), 503
    
    running_count = db.get_running_task_count(api_key_id)
    if running_count >= MAX_CONCURRENT_TASKS:
        return jsonify({
            "error": "Maximum concurrent tasks reached",
            "message": f"Currently {running_count}/{MAX_CONCURRENT_TASKS} tasks running. Please wait."
        }), 429
    
    task_id = str(uuid.uuid4())
    size = data.get('size', '16:9')
    resolution = data.get('resolution', '2K') if model in ('NANO_BANANA_PRO', 'NANO_BANANA_2', 'GPT_IMAGE_2') else None
    db.create_task(api_key_id, task_id, 'image',
                   prompt=data.get('prompt'),
                   model=model,
                   size=size,
                   resolution=resolution,
                   duration=None)
    
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

    if len(data.get('prompt', '')) > 2000:
        return jsonify({"error": "Prompt must be 2000 characters or less"}), 400

    if db.get_account_count(api_key_id) == 0:
        return jsonify({"error": "No quota available"}), 503

    if data.get('model') == 'VIDU_Q3':
        if not data.get('start_frame'):
            return jsonify({"error": "VIDU_Q3 model requires a start frame (image)"}), 400
        if data.get('end_frame'):
            return jsonify({"error": "VIDU_Q3 model does not support end_frame"}), 400
        if data.get('reference_images'):
            return jsonify({"error": "VIDU_Q3 model does not support reference_images"}), 400

    if data.get('model') == 'SORA_2':
        if data.get('end_frame'):
            return jsonify({"error": "SORA_2 model does not support end_frame"}), 400
        if data.get('reference_images'):
            return jsonify({"error": "SORA_2 model does not support reference_images"}), 400

    if data.get('model') == 'QUALITY_V2_5':
        if not data.get('start_frame'):
            return jsonify({"error": "QUALITY_V2_5 model requires a start frame (image)"}), 400
        if data.get('end_frame'):
            return jsonify({"error": "QUALITY_V2_5 model does not support end_frame"}), 400
        if data.get('reference_images'):
            return jsonify({"error": "QUALITY_V2_5 model does not support reference_images"}), 400

    if data.get('model') == 'VEO_3' and data.get('end_frame') and not data.get('start_frame'):
        return jsonify({"error": "end_frame requires image (start frame) to be provided"}), 400

    if data.get('model') == 'VEO_3':
        reference_images = data.get('reference_images', [])
        if isinstance(reference_images, list) and len(reference_images) > 3:
            return jsonify({"error": "Maximum 3 reference images allowed"}), 400
        if reference_images and (data.get('start_frame') or data.get('end_frame')):
            return jsonify({"error": "reference_images cannot be used together with image or end_frame"}), 400

    if data.get('model') == 'SEEDANCE_2_0' and data.get('end_frame') and not data.get('start_frame'):
        return jsonify({"error": "end_frame requires image (start frame) to be provided"}), 400

    if data.get('model') == 'SEEDANCE_2_0':
        reference_images = data.get('reference_images', [])
        if isinstance(reference_images, list) and len(reference_images) > 3:
            return jsonify({"error": "Maximum 3 reference images allowed"}), 400
        if reference_images and (data.get('start_frame') or data.get('end_frame')):
            return jsonify({"error": "reference_images cannot be used together with image or end_frame"}), 400
    
    running_count = db.get_running_task_count(api_key_id)
    if running_count >= MAX_CONCURRENT_TASKS:
        return jsonify({
            "error": "Maximum concurrent tasks reached",
            "message": f"Currently {running_count}/{MAX_CONCURRENT_TASKS} tasks running. Please wait."
        }), 429
    
    task_id = str(uuid.uuid4())
    model = data.get('model', 'SORA_2')
    size = 'AUTO' if model in ('VIDU_Q3', 'QUALITY_V2_5') else data.get('size', '16:9')
    if model == 'VIDU_Q3':
        duration = int(data.get('duration', 5))
        if duration not in [5, 10]:
            duration = 5
        resolution = data.get('resolution', '720p')
        if resolution not in ['720p', '512p']:
            resolution = '720p'
        # 720p yalnızca 5s destekler
        if resolution == '720p' and duration == 10:
            duration = 5
    elif model == 'QUALITY_V2_5':
        duration = int(data.get('duration', 5))
        if duration not in [5, 10]:
            duration = 5
        resolution = '480p' if duration == 10 else '720p'
    elif model == 'VEO_3':
        duration = 8
        resolution = '720p'
    elif model == 'SEEDANCE_2_0':
        duration = 5
        resolution = '480p'
        size = data.get('size', '16:9')
    else:
        duration = 10
        resolution = '720p'
    db.create_task(api_key_id, task_id, 'video',
                   prompt=data.get('prompt'),
                   model=model,
                   size=size,
                   resolution=resolution,
                   duration=duration)
    
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

    if db.get_account_count(api_key_id) == 0:
        return jsonify({"error": "No quota available"}), 503
    
    running_count = db.get_running_task_count(api_key_id)
    if running_count >= MAX_CONCURRENT_TASKS:
        return jsonify({
            "error": "Maximum concurrent tasks reached",
            "message": f"Currently {running_count}/{MAX_CONCURRENT_TASKS} tasks running. Please wait."
        }), 429
    
    task_id = str(uuid.uuid4())
    db.create_task(api_key_id, task_id, 'tts',
                   prompt=data.get('text'),
                   model=data.get('model', 'MINIMAX-TURBO'),
                   voice_id=data.get('voiceId'),
                   speed=data.get('speed'),
                   pitch=data.get('pitch'),
                   volume=data.get('volume'),
                   emotion=data.get('emotion'))
    
    threading.Thread(target=process_tts_task, args=(task_id, data, api_key_id)).start()
    return jsonify({"task_id": task_id})

@app.route('/api/generate/music', methods=['POST'])
def generate_music():
    api_key_id = verify_api_key()
    if not api_key_id:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.json
    if not data or 'prompt' not in data:
        return jsonify({"error": "Prompt required"}), 400

    if db.get_account_count(api_key_id) == 0:
        return jsonify({"error": "No quota available"}), 503

    running_count = db.get_running_task_count(api_key_id)
    if running_count >= MAX_CONCURRENT_TASKS:
        return jsonify({
            "error": "Maximum concurrent tasks reached",
            "message": f"Currently {running_count}/{MAX_CONCURRENT_TASKS} tasks running. Please wait."
        }), 429

    task_id = str(uuid.uuid4())
    model = data.get('model', 'SUNO')

    # Frontend → API mapping (tıpkı IMAGE_MODEL_MAP, SIZE_MAP gibi)
    main_mode = data.get('main_mode', 'Vocal')
    instrumental = 1 if main_mode == 'Instrumental' else 0
    audio_usage = data.get('mode', 'TEXT')

    db.create_task(api_key_id, task_id, 'music',
                   prompt=data.get('prompt'),
                   model=model,
                   style=data.get('style'),
                   lyrics=data.get('lyrics'),
                   instrumental=instrumental,
                   audio_usage=audio_usage)

    # Worker'a Deevid uyumlu parametreler gönder
    worker_data = {
        'prompt': data.get('prompt', ''),
        'model': model,
        'style': data.get('style', ''),
        'lyrics': data.get('lyrics', ''),
        'instrumental': main_mode == 'Instrumental',
        'audioUsage': audio_usage,
        'audio_base64': data.get('audio'),
    }

    threading.Thread(target=process_music_task, args=(task_id, worker_data, api_key_id)).start()
    return jsonify({"task_id": task_id})

@app.route('/api/tts/voices', methods=['GET'])
def get_tts_voices():
    api_key_id = verify_api_key()
    if not api_key_id:
        return jsonify({"error": "Unauthorized"}), 401

    token, account = login_with_retry(api_key_id)
    if not token:
        return jsonify({"error": "No quota available"}), 503

    try:
        headers = {"authorization": f"Bearer {token}"}
        # İlk istek: total sayısını al
        r = requests.get(f"{URL_TTS_VOICES}?page=1&pageSize=30&source=minimax", headers=headers)
        total = r.json()["data"]["data"]["total"]
        # İkinci istek: hepsini çek
        r2 = requests.get(f"{URL_TTS_VOICES}?page=1&pageSize={total}&source=minimax", headers=headers)
        voices = r2.json()["data"]["data"]["data"]
        db.release_account(api_key_id, account['email'])
        return jsonify({"voices": voices})
    except Exception as e:
        if 'account' in locals() and account:
            db.release_account(api_key_id, account['email'])
        return jsonify({"error": str(e)}), 500

@app.route('/api/status/<task_id>', methods=['GET'])
def get_task_status(task_id):
    api_key_id = verify_api_key()
    if not api_key_id:
        return jsonify({"error": "Unauthorized"}), 401
    
    task = db.get_task(api_key_id, task_id)
    if not task:
        return jsonify({"error": "Task not found"}), 404
    
    result = filter_task_fields(task)
    
    return jsonify(result)
    
@app.route('/api/status', methods=['GET'])
def get_all_tasks_status():
    api_key_id = verify_api_key()
    if not api_key_id:
        return jsonify({"error": "Unauthorized"}), 401
    
    running_count = db.get_running_task_count(api_key_id)

    page_param = request.args.get('page')
    if page_param is not None:
        try:
            page = max(1, int(page_param))
        except ValueError:
            return jsonify({"error": "Invalid page parameter"}), 400

        per_page_param = request.args.get('per_page', 6)
        try:
            per_page = max(1, int(per_page_param))
        except ValueError:
            return jsonify({"error": "Invalid per_page parameter"}), 400

        tasks_raw, total = db.get_tasks_paginated(api_key_id, page, per_page)
        tasks = [filter_task_fields(t) for t in tasks_raw]
        import math
        total_pages = math.ceil(total / per_page) if total > 0 else 1

        return jsonify({
            "tasks": tasks,
            "running_tasks": running_count,
            "max_concurrent": MAX_CONCURRENT_TASKS,
            "page": page,
            "per_page": per_page,
            "total": total,
            "total_pages": total_pages
        })

    tasks_raw = db.get_all_tasks(api_key_id)
    tasks = [filter_task_fields(t) for t in tasks_raw]
    return jsonify({
        "tasks": tasks,
        "running_tasks": running_count,
        "max_concurrent": MAX_CONCURRENT_TASKS
    })

@app.route('/api/quota', methods=['GET'])
def get_quota():
    api_key_id = verify_api_key()
    if not api_key_id:
        return jsonify({"error": "Unauthorized"}), 401
    
    running_count = db.get_running_task_count(api_key_id)
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
