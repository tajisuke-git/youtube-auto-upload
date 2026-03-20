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

    # ハッシュタグ
    'HASHTAGS_YT': '#eclairemd #healthtalk #chatgerry',
    'HASHTAGS_B':  '#eclairemd #healthtalk',

    # ChatGerry 再生リスト ID（YTのときのみ追加）
    'PLAYLIST_ID_YT': 'PLIFI7HAWh1jACD5oXv3f6U_mkBv7KN-Lr',

    # スプレッドシート①: Youtube_Language_Checklist
    # B→A列キー→C列に✔, YT→G列キー→I列に✔（黄色）6行目から
    'CHECKLIST_SS_ID': '1n3dtkiY3XugTRvTt2eSYkBskNVr4IbPdXibQdOuyoac',
    'CHECKLIST_SHEET': 'progress tracking',
    'CHECKLIST_DATA_START_ROW': 6,
    'CHECKLIST_B_KEY_COL': 1,
    'CHECKLIST_B_VAL_COL': 3,
    'CHECKLIST_YT_KEY_COL': 7,
    'CHECKLIST_YT_VAL_COL': 9,

    # スプレッドシート②: 石井瑠海_作成管理表
    # B→A列キー→B列に日付, YT→D列キー→E列に日付（6行目から）
    'MGMT_SS_ID': '1erw-9Sv7X0cNcF322Y8yx8d4CYTEOgl8YtPgChMSFF4',
    'MGMT_SHEET': 'progress tracking',
    'MGMT_DATA_START_ROW': 6,
    'MGMT_B_KEY_COL': 1,
    'MGMT_B_VAL_COL': 2,
    'MGMT_YT_KEY_COL': 4,
    'MGMT_YT_VAL_COL': 5
}

# ── 認証 ──────────────────────────────────────────────────────

def build_credentials():
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
                'prefix':    match.group(1).upper(),
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
    page_token = None
    while True:
        result = drive.files().list(
            q=query,
            fields='nextPageToken, files(id, name)',
            pageToken=page_token,
            pageSize=100
        ).execute()
        for folder in result.get('files', []):
            if folder['name'].startswith(digits):
                return folder
        page_token = result.get('nextPageToken')
        if not page_token:
            break
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

        if name.upper().replace(' ', '') == f'{full_code}.MP4':
            video_file = f
            size_mb = int(f.get('size', 0)) / 1024 / 1024
            print(f'  動画: "{name}" ({size_mb:.1f} MB)')

        if mime == 'application/vnd.google-apps.document' and name.upper() == full_code:
            doc_file = f
            print(f'  ドキュメント: "{name}"')

    return video_file, doc_file


# ── Google ドキュメント ────────────────────────────────────────

def get_doc_content(docs, doc_id, prefix):
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
    full_text = full_text.replace('\x00', '')
    full_text = full_text.replace('\u200b', '')
    full_text = ''.join(c for c in full_text if ord(c) >= 32 or c in '\n\t')

    # タイトル: "Gerald C. Hsu" が現れる行の直前まで（空白行含む1〜4行目が上限）
    all_lines = full_text.split('\n')
    title_lines = []
    for line in all_lines[:8]:  # 最大8行まで走査
        if 'Gerald' in line or 'gerald' in line:
            break
        title_lines.append(line)
    title = ' '.join(l.strip() for l in title_lines if l.strip())
    # YouTubeで使えない文字を除去
    title = title.replace('<', '').replace('>', '')
    title = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', title)
    title = title.strip()
    # 文字数を安全に100文字以内に収める（日本語対応）
    if len(title) > 100:
        title = title[:97] + '...'
    # 念のため前後の空白を除去
    title = title.strip()
    # タイトルが空の場合はデフォルト値
    if not title:
        title = 'No Title'
    # YT の場合: タイトル冒頭の "ChatGerry" を "【ChatGerry】" に変換
    if prefix == 'YT':
        title = re.sub(r'^ChatGerry', '【ChatGerry】', title, flags=re.IGNORECASE)

    # 説明文: 全文 + ハッシュタグ
    hashtags = CONFIG['HASHTAGS_YT'] if prefix == 'YT' else CONFIG['HASHTAGS_B']
    description = full_text + '\n\n' + hashtags
    description = description[:5000]

    return title, description


# ── YouTube ───────────────────────────────────────────────────

def upload_to_youtube(drive, youtube, video_file_id, title, description, prefix):
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
            'privacyStatus':           'private',
            'selfDeclaredMadeForKids': False,
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
    print(f'  ✅ アップロード完了: https://studio.youtube.com/video/{video_id}/edit')

    # YT の場合は再生リストに追加
    if prefix == 'YT':
        try:
            youtube.playlistItems().insert(
                part='snippet',
                body={
                    'snippet': {
                        'playlistId': CONFIG['PLAYLIST_ID_YT'],
                        'resourceId': {
                            'kind':    'youtube#video',
                            'videoId': video_id,
                        },
                    }
                }
            ).execute()
            print(f'  ✅ 再生リスト（ChatGerry）に追加完了')
        except Exception as e:
            print(f'  ⚠ 再生リスト追加エラー: {e}')

    return video_id


# ── スプレッドシート操作 ───────────────────────────────────────

def find_row_by_digits(sheets, spreadsheet_id, sheet_name, digits, start_row, key_col=1):
    """指定列から4桁数字に一致する行番号を返す"""
    col = col_letter(key_col)
    result = sheets.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f'{sheet_name}!{col}{start_row}:{col}2000'
    ).execute()
    values = result.get('values', [])
    for i, row in enumerate(values):
        if row and str(row[0]).strip() == digits:
            return start_row + i
    return None


def col_letter(col_num):
    """列番号（1始まり）をアルファベットに変換 例: 1→A, 3→C, 9→I"""
    result = ''
    while col_num > 0:
        col_num, remainder = divmod(col_num - 1, 26)
        result = chr(65 + remainder) + result
    return result


def update_checklist(sheets, digits, prefix):
    """スプレッドシート①: 該当行のC列またはI列に✔を黄色背景で入力"""
    ss_id      = CONFIG['CHECKLIST_SS_ID']
    sheet_name = CONFIG['CHECKLIST_SHEET']
    start_row  = CONFIG['CHECKLIST_DATA_START_ROW']
    key_col = CONFIG['CHECKLIST_B_KEY_COL'] if prefix == 'B' else CONFIG['CHECKLIST_YT_KEY_COL']
    col_num = CONFIG['CHECKLIST_B_VAL_COL'] if prefix == 'B' else CONFIG['CHECKLIST_YT_VAL_COL']

    row = find_row_by_digits(sheets, ss_id, sheet_name, digits, start_row, key_col)
    if not row:
        print(f'  ⚠ チェックリスト①: {digits} の行が見つかりません')
        return

    cell = f'{sheet_name}!{col_letter(col_num)}{row}'

    # 値を入力
    sheets.spreadsheets().values().update(
        spreadsheetId=ss_id,
        range=cell,
        valueInputOption='RAW',
        body={'values': [['✔']]}
    ).execute()

    # 黄色背景を設定
    sheets.spreadsheets().batchUpdate(
        spreadsheetId=ss_id,
        body={
            'requests': [{
                'repeatCell': {
                    'range': {
                        'sheetId': get_sheet_id(sheets, ss_id, sheet_name),
                        'startRowIndex': row - 1,
                        'endRowIndex': row,
                        'startColumnIndex': col_num - 1,
                        'endColumnIndex': col_num,
                    },
                    'cell': {
                        'userEnteredFormat': {
                            'backgroundColor': {
                                'red': 1.0, 'green': 1.0, 'blue': 0.0
                            }
                        }
                    },
                    'fields': 'userEnteredFormat.backgroundColor'
                }
            }]
        }
    ).execute()
    print(f'  ✅ チェックリスト①: {digits}行 {col_letter(col_num)}列 に✔（黄色）を記入')


def update_mgmt(sheets, digits, prefix):
    """スプレッドシート②: 該当行のB列またはE列に今日の日付を入力"""
    ss_id      = CONFIG['MGMT_SS_ID']
    sheet_name = CONFIG['MGMT_SHEET']
    start_row  = CONFIG['MGMT_DATA_START_ROW']
    key_col = CONFIG['MGMT_B_KEY_COL'] if prefix == 'B' else CONFIG['MGMT_YT_KEY_COL']
    col_num = CONFIG['MGMT_B_VAL_COL'] if prefix == 'B' else CONFIG['MGMT_YT_VAL_COL']

    row = find_row_by_digits(sheets, ss_id, sheet_name, digits, start_row, key_col)
    if not row:
        print(f'  ⚠ 管理表②: {digits} の行が見つかりません')
        return

    jst = timezone(timedelta(hours=9))
    today = datetime.now(jst).strftime('%-Y/%-m/%-d')
    cell = f'{sheet_name}!{col_letter(col_num)}{row}'

    sheets.spreadsheets().values().update(
        spreadsheetId=ss_id,
        range=cell,
        valueInputOption='USER_ENTERED',
        body={'values': [[today]]}
    ).execute()
    print(f'  ✅ 管理表②: {digits}行 {col_letter(col_num)}列 に {today} を記入')


def get_sheet_id(sheets, spreadsheet_id, sheet_name):
    """シート名からシートIDを取得"""
    info = sheets.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    for s in info.get('sheets', []):
        if s['properties']['title'] == sheet_name:
            return s['properties']['sheetId']
    return 0


# ── 処理履歴スプレッドシート ──────────────────────────────────

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
        prefix    = item['prefix']

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

            title, description = get_doc_content(docs, doc_file['id'], prefix)
            print(f'  タイトル: {title}')
            print(f'  種別: {"YT（ChatGerry再生リストに追加）" if prefix == "YT" else "B"}')

            # YouTube アップロード
            video_id = upload_to_youtube(drive, youtube, video_file['id'], title, description, prefix)

            # スプレッドシート①②に記録
            update_checklist(sheets, digits, prefix)
            update_mgmt(sheets, digits, prefix)

            # 処理済みラベル・履歴記録
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
