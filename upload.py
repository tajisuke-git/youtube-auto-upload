"""
YouTube 自動アップロード スクリプト
GitHub Actions から実行されます
"""

import os
import io
import re
import time
from datetime import datetime, timezone, timedelta

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload


# ── 設定 ──────────────────────────────────────────────────────

CONFIG = {
    'DRIVE_PARENT_FOLDER_ID': '1H0qF6GXiM1Reu1eBiQINbJ5L_c45jitj',
    'SPREADSHEET_ID':         '1EIbRCn23USVsR3kFXP2AtcUFZYA-keJEKLHx7RdMQhM',
    'SHEET_NAME':             '処理履歴',
    'GMAIL_PROCESSED_LABEL':  'YT処理済み',
    'YOUTUBE_CATEGORY_ID':    '27',
    'SEARCH_DAYS':            30,
}

# ── 認証 ──────────────────────────────────────────────────────

def build_credentials():
    """環境変数からOAuth認証情報を構築"""
    import json
    token_json = os.environ['GOOGLE_TOKEN_JSON']
    token_data = json.loads(token_json)

    creds = Credentials(
        token=token_data.get('token'),
        refresh_token=token_data.get('refresh_token'),
        token_uri='https://oauth2.googleapis.com/token',
        client_id=token_data.get('client_id'),
        client_secret=token_data.get('client_secret'),
        scopes=token_data.get('scopes'),
    )

    # トークンが期限切れなら自動更新
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())

    return creds


def build_services(creds):
    gmail   = build('gmail',   'v1', credentials=creds)
    drive   = build('drive',   'v3', credentials=creds)
    youtube = build('youtube', 'v3', credentials=creds)
    sheets  = build('sheets',  'v4', credentials=creds)
    docs    = build('docs',    'v1', credentials=creds)
    return gmail, drive, youtube, sheets, docs


# ── Gmail ─────────────────────────────────────────────────────

def get_or_create_label(gmail, label_name):
    labels = gmail.users().labels().list(userId='me').execute().get('labels', [])
    for l in labels:
        if l['name'] == label_name:
            return l['id']
    result = gmail.users().labels().create(
        userId='me',
        body={'name': label_name, 'labelListVisibility': 'labelShow', 'messageListVisibility': 'show'}
    ).execute()
    print(f'  ラベル作成: {label_name}')
    return result['id']


def search_unprocessed_emails(gmail):
    query = f"-label:{CONFIG['GMAIL_PROCESSED_LABEL']} newer_than:{CONFIG['SEARCH_DAYS']}d"
    result = gmail.users().threads().list(userId='me', q=query, maxResults=50).execute()
    threads = result.get('threads', [])

    targets = []
    for t in threads:
        detail = gmail.users().threads().get(userId='me', id=t['id']).execute()
        messages = detail.get('messages', [])
        last_msg = messages[-1]
        headers = {h['name']: h['value'] for h in last_msg['payload']['headers']}
        subject = headers.get('Subject', '').strip()

        match = re.match(r'^(B|YT)(\d{4})JP\b', subject, re.IGNORECASE)
        if match:
            targets.append({
                'thread_id': t['id'],
                'subject':   subject,
                'full_code': match.group(0).upper(),
                'digits':    match.group(2),
            })
    return targets


def add_label_to_thread(gmail, thread_id, label_id):
    gmail.users().threads().modify(
        userId='me', id=thread_id,
        body={'addLabelIds': [label_id]}
    ).execute()


# ── Drive ─────────────────────────────────────────────────────

def find_folder(drive, digits):
    query = f"'{CONFIG['DRIVE_PARENT_FOLDER_ID']}' in parents and mimeType='application/vnd.google-apps.folder'"
    result = drive.files().list(q=query, fields='files(id, name)').execute()
    for folder in result.get('files', []):
        if folder['name'].startswith(digits):
            return folder
    return None


def find_files_in_folder(drive, folder_id, full_code):
    result = drive.files().list(
        q=f"'{folder_id}' in parents",
        fields='files(id, name, mimeType, size)'
    ).execute()

    video_file = None
    doc_file   = None

    for f in result.get('files', []):
        name = f['name']
        mime = f['mimeType']

        # 動画: スペースを除いて比較
        if name.upper().replace(' ', '') == f'{full_code}.MP4':
            video_file = f
            size_mb = int(f.get('size', 0)) / 1024 / 1024
            print(f'  動画: "{name}" ({size_mb:.1f} MB)')

        # Google ドキュメント
        if mime == 'application/vnd.google-apps.document' and name.upper() == full_code:
            doc_file = f
            print(f'  ドキュメント: "{name}"')

    return video_file, doc_file


# ── Google ドキュメント ────────────────────────────────────────

def get_doc_content(docs, doc_id):
    doc = docs.documents().get(documentId=doc_id).execute()
    full_text = ''
    for elem in doc.get('body', {}).get('content', []):
        para = elem.get('paragraph')
        if para:
            for e in para.get('elements', []):
                text_run = e.get('textRun')
                if text_run:
                    full_text += text_run.get('content', '')

    full_text = full_text.strip()
    lines = [l for l in full_text.split('\n') if l.strip()]
    title = lines[0].strip() if lines else doc_id
    return title, full_text


# ── YouTube ───────────────────────────────────────────────────

def upload_to_youtube(drive, youtube, video_file_id, title, description):
    # Drive からダウンロード
    request = drive.files().get_media(fileId=video_file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request, chunksize=20 * 1024 * 1024)

    print('  ダウンロード中...')
    done = False
    while not done:
        status, done = downloader.next_chunk()
        if status:
            print(f'  ダウンロード: {int(status.progress() * 100)}%')

    fh.seek(0)
    print('  YouTubeにアップロード中...')

    body = {
        'snippet': {
            'title':       title,
            'description': description,
            'categoryId':  CONFIG['YOUTUBE_CATEGORY_ID'],
        },
        'status': {
            'privacyStatus':            'private',
            'selfDeclaredMadeForKids':  False,
        },
    }

    media = MediaIoBaseUpload(fh, mimetype='video/mp4', chunksize=20 * 1024 * 1024, resumable=True)
    upload_req = youtube.videos().insert(part='snippet,status', body=body, media_body=media)

    response = None
    while response is None:
        status, response = upload_req.next_chunk()
        if status:
            print(f'  アップロード: {int(status.progress() * 100)}%')

    video_id = response['id']
    print(f'  ✅ 完了: https://studio.youtube.com/video/{video_id}/edit')
    return video_id


# ── スプレッドシート ───────────────────────────────────────────

def log_to_sheet(sheets, code, subject, status, note):
    jst = timezone(timedelta(hours=9))
    now = datetime.now(jst).strftime('%Y/%m/%d %H:%M:%S')
    sheets.spreadsheets().values().append(
        spreadsheetId=CONFIG['SPREADSHEET_ID'],
        range=f"{CONFIG['SHEET_NAME']}!A:E",
        valueInputOption='RAW',
        body={'values': [[now, code, subject, status, note]]}
    ).execute()


# ── メイン ────────────────────────────────────────────────────

def main():
    print('=' * 50)
    print('YouTube 自動アップロード 開始')
    print('=' * 50)

    creds = build_credentials()
    gmail, drive, youtube, sheets, docs = build_services(creds)

    label_id = get_or_create_label(gmail, CONFIG['GMAIL_PROCESSED_LABEL'])
    targets  = search_unprocessed_emails(gmail)

    if not targets:
        print('処理対象メールなし')
        return

    print(f'{len(targets)} 件の処理対象メールが見つかりました\n')

    for item in targets:
        full_code = item['full_code']
        digits    = item['digits']
        subject   = item['subject']

        print(f'▶ 処理開始: {full_code}（件名: "{subject}"）')

        try:
            folder = find_folder(drive, digits)
            if not folder:
                note = f'"{digits}" で始まるフォルダが見つかりません'
                print(f'  ⚠ {note}')
                log_to_sheet(sheets, full_code, subject, 'スキップ', note)
                add_label_to_thread(gmail, item['thread_id'], label_id)
                continue
            print(f'  フォルダ: "{folder["name"]}"')

            video_file, doc_file = find_files_in_folder(drive, folder['id'], full_code)

            if not video_file:
                note = f'動画ファイルなし（期待: {full_code}.mp4）'
                print(f'  ⚠ {note}')
                log_to_sheet(sheets, full_code, subject, 'スキップ', note)
                add_label_to_thread(gmail, item['thread_id'], label_id)
                continue

            if not doc_file:
                note = f'ドキュメントなし（期待: {full_code}）'
                print(f'  ⚠ {note}')
                log_to_sheet(sheets, full_code, subject, 'スキップ', note)
                add_label_to_thread(gmail, item['thread_id'], label_id)
                continue

            title, description = get_doc_content(docs, doc_file['id'])
            print(f'  タイトル: {title}')

            video_id = upload_to_youtube(drive, youtube, video_file['id'], title, description)

            add_label_to_thread(gmail, item['thread_id'], label_id)
            note = f'動画ID: {video_id} | https://studio.youtube.com/video/{video_id}/edit'
            log_to_sheet(sheets, full_code, subject, '✅ 成功', note)
            print(f'  ✅ 完了: {note}\n')

        except Exception as e:
            print(f'  ❌ エラー ({full_code}): {e}\n')
            log_to_sheet(sheets, full_code, subject, '❌ エラー', str(e))

        time.sleep(2)

    print('=' * 50)
    print('処理完了')
    print('=' * 50)


if __name__ == '__main__':
    main()
