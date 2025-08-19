# -*- coding: utf-8 -*-
"""
Yahooニュースの転記（コピー元→出力先）に加えて、
出力先シート（当日名のタブ）の B列タイトルを Gemini で一括分類し、
M列（ポジネガ）/ N列（カテゴリ）にバルク書き込みする。

認証:
- Sheets: 環境変数 GCP_SA_KEY（サービスアカウントJSONの中身）を使用。無ければ key.json を読む。
- Gemini: 環境変数 GEMINI_API_KEY（AI Studioで発行したGenerative Language APIキー）

実行の流れ:
1) 既存ロジックでコピー元シートから期間内（JST 15:00〜翌日14:59:59）のニュースを抽出
2) 出力先スプレッドシートの当日タブ（yyMMdd）を用意し、重複を避けて A〜L 列へ追記
3) その当日タブの B列・L列を読み取り、Gemini（失敗時はルールベース）で M/N 列を書き込み
"""

import os
import datetime
import json
import re
import time
from typing import List, Dict, Tuple

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# === Generative Language API (Gemini) ===
import google.generativeai as genai


# ========= ルールベース（フォールバック） =========
NISSAN_WORDS = ["ニッサン","日産","NISSAN","Nissan"]
OTHER_MAKERS = ["トヨタ","TOYOTA","Toyota","ホンダ","HONDA","Honda","スバル","SUBARU","Subaru",
                "マツダ","MAZDA","Mazda","スズキ","SUZUKI","Suzuki","ミツビシ","三菱","MITSUBISHI","Mitsubishi","ダイハツ","DAIHATSU","Daihatsu"]
POS_KW = ["受注開始","発売","発表","好発進","優勝","開催へ","出会える","目指す","わかった","メリット","最適","ナイス","選ばれたワケ","参戦","総合優勝へ"]
NEG_KW = ["事故","リコール","リストラ","値上げ","中止","苦戦","不正","炎上","失業","没個性？","なぜ進化しない","問題","課題"]

def fallback_sentiment(title: str) -> str:
    t = title or ""
    if any(k in t for k in POS_KW) and not any(k in t for k in NEG_KW):
        return "ポジティブ"
    if any(k in t for k in NEG_KW) and not any(k in t for k in POS_KW):
        return "ネガティブ"
    return "ニュートラル"

def fallback_category(title: str) -> str:
    t = title or ""
    # モータースポーツ
    if any(k in t for k in ["F1","フォーミュラE","ラリー","WRC","Super GT","スーパーＧＴ","スプリントレース","参戦"]):
        return "モータースポーツ"
    # 技術系
    if any(k in t for k in ["EV化","電気自動車"," EV","EV ","バッテリー","電動","充電","ソリッドステート"]):
        return "技術（EV）"
    if any(k in t for k in ["e-POWER","e POWER","ePOWER"]):
        return "技術（e-POWER）"
    if any(k in t for k in ["e-4ORCE","E-4ORCE","4WD","AWD","2WD"]):
        return "技術（e-4ORCE）"
    if any(k in t for k in ["自動運転","ADAS","運転支援","先進運転支援","L2","L3","プラットフォーム","空力","エアロ","技術"]):
        return "技術"
    # 車（競合 or 日産）
    if re.search(r"(RAV4|CX-[0-9]|GR\s?\w+|シルビア|フォレスター|ウルス|スープラ|マイクラ|スカイライン|セレナ|ノート)", t, re.I):
        if any(w in t for w in NISSAN_WORDS):
            if "新型" in t: return "車（新型）"
            if "現行" in t: return "車（現行）"
            if "旧型" in t: return "車（旧型）"
            return "車"
        return "車（競合）"
    # 会社
    if any(w in t for w in NISSAN_WORDS):
        return "会社（ニッサン）"
    for comp in OTHER_MAKERS:
        if comp in t:
            return f"会社（{comp}）"
    # 株式/政治経済/スポーツ
    if any(k in t for k in ["株価","上場","発行株式","投資家","決算","通期見通し"]):
        return "株式"
    if any(k in t for k in ["政治","選挙","税","経済","景気","物価"]):
        return "政治・経済"
    if any(k in t for k in ["野球","サッカー","バレーボール","ラグビー","W杯","五輪"]):
        return "スポーツ"
    return "その他"


# ========= Geminiユーティリティ =========
MODEL_NAME = "gemini-1.5-flash"
TEMPERATURE = 0.2
BATCH_SIZE = 100
SLEEP_SEC = 0.5

def gemini_smoke_test() -> bool:
    """最小トークンで接続確認。失敗なら False。"""
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        print("ℹ️ GEMINI_API_KEY 未設定（ルールベースへフォールバック）")
        return False
    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel(MODEL_NAME)
        r = model.generate_content("OKだけ", generation_config={"temperature": 0, "max_output_tokens": 4, "response_mime_type": "text/plain"})
        ok = (r.text or "").strip()
        if ok:
            print("✅ Gemini 接続OK（Generative Language API）")
            return True
        print("⚠️ Gemini応答が空。フォールバックします。")
        return False
    except Exception as e:
        print(f"⚠️ Gemini接続失敗: {e} → フォールバック")
        return False

def build_prompt(items: List[Dict]) -> str:
    taxonomy = """
【目的】
日本語のニュース「タイトル」から以下2点を出力してください。
1) ポジネガ判定：「ポジティブ」「ネガティブ」「ニュートラル」
2) カテゴリ（最も関連の高い1つのみ）：
   - 会社（例：会社（ニッサン）/会社（トヨタ）/他社名）
   - 車：クルマの名称が含まれているもの（会社名のみは対象外）
       日産車は「車（新型◯◯/現行◯◯/旧型◯◯）」、日産以外は「車（競合）」
   - 技術（EV / e-POWER / e-4ORCE / AD/ADAS）/ 技術（上記以外）
   - モータースポーツ / 株式 / 政治・経済 / スポーツ / その他

【制約】
- 出力は JSON 配列のみ。コメント禁止。
- 各要素: {"idx": <識別子>, "sentiment": "...", "category": "..."}
"""
    lines = ["入力:"]
    for it in items:
        lines.append(f"- idx:{it['idx']} | title:{it['title']}")
    return taxonomy + "\n" + "\n".join(lines)

def ensure_json_array(text: str) -> List[Dict]:
    m = re.search(r"\[\s*{.*}\s*\]", text, re.S)
    if not m:
        raise ValueError("JSON配列が見つかりませんでした。")
    return json.loads(m.group(0))

def gemini_batch_classify(items: List[Dict]) -> List[Dict]:
    model = genai.GenerativeModel(MODEL_NAME)
    prompt = build_prompt(items)
    resp = model.generate_content(
        prompt,
        generation_config={
            "temperature": TEMPERATURE,
            "max_output_tokens": 2048,
            "response_mime_type": "application/json",
        },
    )
    return ensure_json_array(resp.text or "")


# ========= 本体：転記＋分類 =========
def transfer_yahoo_news_from_source_sheet():
    """
    既存の転記ロジック（コピー元→出力先） + 当日タブのB列を分類してM/N列へ出力
    """

    # --- 設定（既存のIDそのまま使用） ---
    SOURCE_SPREADSHEET_ID = '1RglATeTbLU1SqlfXnNToJqhXLdNoHCdePldioKDQgU8'
    DESTINATION_SPREADSHEET_ID = '19c6yIGr5BiI7XwstYhUPptFGksPPXE4N1bEq5iFoPok'
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

    # --- 認証 ---
    try:
        creds_json = os.environ.get('GCP_SA_KEY')
        if not creds_json:
            with open('key.json', 'r', encoding='utf-8') as f:
                creds_info = json.load(f)
        else:
            creds_info = json.loads(creds_json)
        creds = service_account.Credentials.from_service_account_info(creds_info, scopes=SCOPES)
        service = build('sheets', 'v4', credentials=creds)
    except Exception as e:
        print(f"エラー: Google Sheets APIの認証に失敗しました。詳細: {e}")
        return

    # --- JST日付＆当日タブ名 ---
    today = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9)))
    yesterday = today - datetime.timedelta(days=1)
    start_time = yesterday.replace(hour=15, minute=0, second=0, microsecond=0)
    end_time = today.replace(hour=14, minute=59, second=59, microsecond=0)
    destination_sheet_name = today.strftime('%y%m%d')

    print(f"出力先シート名: {destination_sheet_name}")
    print(f"期間: {start_time.strftime('%Y/%m/%d %H:%M:%S')} 〜 {end_time.strftime('%Y/%m/%d %H:%M:%S')}")

    # --- 出力先タブの存在確認＆取得 ---
    existing_data_in_destination = []
    header_exists = False
    try:
        spreadsheet_info = service.spreadsheets().get(spreadsheetId=DESTINATION_SPREADSHEET_ID).execute()
        sheets = spreadsheet_info.get('sheets', [])
        sheet_exists = any(sheet['properties']['title'] == destination_sheet_name for sheet in sheets)
        if not sheet_exists:
            print(f"出力先にタブ「{destination_sheet_name}」を新規作成")
            body = {'requests': [{'addSheet': {'properties': {'title': destination_sheet_name}}}]}
            service.spreadsheets().batchUpdate(spreadsheetId=DESTINATION_SPREADSHEET_ID, body=body).execute()

        # 既存データ取得
        destination_sheet_range = f"'{destination_sheet_name}'!A:L"
        result = service.spreadsheets().values().get(
            spreadsheetId=DESTINATION_SPREADSHEET_ID, range=destination_sheet_range
        ).execute()
        existing_data_in_destination = result.get('values', [])
    except HttpError as e:
        print(f"エラー: 出力先へのアクセス失敗: {e}")
        return
    except Exception as e:
        print(f"エラー: 不明なエラー: {e}")
        return

    existing_urls_in_destination = set()
    if existing_data_in_destination:
        if existing_data_in_destination[0] and existing_data_in_destination[0][0] == 'ソース':
            header_exists = True
            for row in existing_data_in_destination[1:]:
                if len(row) > 2 and row[2]:
                    existing_urls_in_destination.add(row[2])

    print(f"出力先タブの既存ニュース: {len(existing_urls_in_destination)} 件（URL重複判定）")

    # --- コピー元（Yahoo）から抽出 ---
    source_sheet_name = 'Yahoo'
    try:
        source_sheet_range = f"'{source_sheet_name}'!A:D"
        result = service.spreadsheets().values().get(
            spreadsheetId=SOURCE_SPREADSHEET_ID, range=source_sheet_range
        ).execute()
        data = result.get('values', [])
    except Exception as e:
        print(f"エラー: コピー元シート「{source_sheet_name}」へアクセス不可: {e}")
        data = []

    collected_news = []
    if data:
        print(f"コピー元から {len(data)-1} 件（ヘッダー除く）読み込み。")
        for i, row in enumerate(data):
            if i == 0:
                continue
            try:
                title = row[0]
                url = row[1]
                post_date_raw = row[2]
                source = row[3]

                post_date = None
                if isinstance(post_date_raw, str):
                    try:
                        d = datetime.datetime.strptime(post_date_raw, '%m/%d %H:%M')
                        post_date = d.replace(year= today.year)
                    except ValueError:
                        try:
                            post_date = datetime.datetime.strptime(post_date_raw, '%Y/%m/%d %H:%M:%S')
                        except ValueError:
                            pass
                elif isinstance(post_date_raw, float):
                    epoch = datetime.datetime(1899, 12, 30)
                    post_date = epoch + datetime.timedelta(days=post_date_raw)
                elif isinstance(post_date_raw, datetime.date):
                    post_date = datetime.datetime.combine(post_date_raw, datetime.time())

                if post_date:
                    post_date = post_date.replace(tzinfo=datetime.timezone(datetime.timedelta(hours=9)))
                    if start_time <= post_date <= end_time and url not in existing_urls_in_destination:
                        new_row = [source_sheet_name, title, url, post_date.strftime('%Y/%m/%d'), source]
                        collected_news.append(new_row)
            except Exception as e:
                print(f"警告: 行 {i+1} の処理をスキップ: {e}")

    # --- 追記（必要ならヘッダーも） ---
    if not header_exists:
        header_row = ['ソース','タイトル','URL','投稿日','引用元','コメント数','ポジネガ','カテゴリー','有料記事','J列(ダブりチェック)','K列（タイトル抜粋）','L列（番号）']
        service.spreadsheets().values().append(
            spreadsheetId=DESTINATION_SPREADSHEET_ID,
            range=f"'{destination_sheet_name}'!A1",
            valueInputOption='USER_ENTERED',
            body={'values': [header_row]}
        ).execute()
        print("ヘッダーを追加しました。")

    if collected_news:
        # 追記後の最終行を予測（Jの数式/K/L用）
        last_row_after_append = len(existing_data_in_destination) + len(collected_news) + (0 if header_exists else 1)
        start_l_number = 0
        if header_exists and len(existing_data_in_destination) > 1:
            try:
                last_row_data = existing_data_in_destination[-1]
                if len(last_row_data) > 11:
                    start_l_number = int(last_row_data[11])
            except Exception:
                pass

        data_to_append = []
        for i, row in enumerate(collected_news):
            # A〜E
            row_data = row[:5]
            # F(コメント数), G(ポジネガ), H(カテゴリ), I(有料記事)
            row_data.extend(['', '', '', ''])
            # J（ダブりチェック）式
            current_row_num = len(existing_data_in_destination) + 1 + i + (0 if header_exists else 1)
            j_formula = f'=IF(ISERROR(VLOOKUP(K{current_row_num},K{current_row_num+1}:L{last_row_after_append},2,FALSE)),"ダブり無し",VLOOKUP(K{current_row_num},K{current_row_num+1}:L{last_row_after_append},2,FALSE))'
            row_data.append(j_formula)
            # K（タイトル抜粋）: 記号類を除去し先頭20
            processed_title = row[1].translate(str.maketrans({c: "" for c in " ,.-_<>【】「」()"}))
            k_value = processed_title[:20]
            row_data.append(k_value)
            # L（番号）
            l_value = start_l_number + i + 1
            row_data.append(l_value)

            data_to_append.append(row_data)

        service.spreadsheets().values().append(
            spreadsheetId=DESTINATION_SPREADSHEET_ID,
            range=f"'{destination_sheet_name}'!A:L",
            valueInputOption='USER_ENTERED',
            insertDataOption='INSERT_ROWS',
            body={'values': data_to_append}
        ).execute()
        print(f"新規 {len(data_to_append)} 件を追記しました。")
    else:
        print("期間内の新規ニュースはありません（分類は実行します）。")

    # ===== ここから分類 =====
    # B列（タイトル）/ L列（番号）を取得（2行目以降）
    res_title = service.spreadsheets().values().get(
        spreadsheetId=DESTINATION_SPREADSHEET_ID, range=f"'{destination_sheet_name}'!B2:B"
    ).execute()
    titles = [r[0] for r in res_title.get('values', [])] if res_title.get('values') else []

    res_num = service.spreadsheets().values().get(
        spreadsheetId=DESTINATION_SPREADSHEET_ID, range=f"'{destination_sheet_name}'!L2:L"
    ).execute()
    numbers = [r[0] for r in res_num.get('values', [])] if res_num.get('values') else []

    if not titles:
        print("分類対象のタイトルがありません。処理終了。")
        return

    # Gemini使用可否
    use_gemini = gemini_smoke_test()

    # 推論アイテム作成
    items: List[Dict] = []
    for i, title in enumerate(titles, start=2):
        if not (title or "").strip():
            continue
        idx = numbers[i-2].strip() if i-2 < len(numbers) and (numbers[i-2] or "").strip() else str(i)
        items.append({"idx": idx, "title": title})

    # 分類
    results_map: Dict[str, Tuple[str, str]] = {}
    if use_gemini:
        print("Geminiで一括分類します...")
        for s in range(0, len(items), BATCH_SIZE):
            batch = items[s:s+BATCH_SIZE]
            try:
                out = gemini_batch_classify(batch)
                got = {o["idx"]: (o["sentiment"], o["category"]) for o in out if "idx" in o and "sentiment" in o and "category" in o}
                for it in batch:
                    results_map[it["idx"]] = got.get(it["idx"], (fallback_sentiment(it["title"]), fallback_category(it["title"])))
            except Exception as e:
                print(f"⚠️ バッチ失敗（フォールバック）: {e}")
                for it in batch:
                    results_map[it["idx"]] = (fallback_sentiment(it["title"]), fallback_category(it["title"]))
            time.sleep(SLEEP_SEC)
    else:
        print("Gemini未使用（フォールバック実行）")
        for it in items:
            results_map[it["idx"]] = (fallback_sentiment(it["title"]), fallback_category(it["title"]))

    # 行順に M/N の値を作成
    m_values, n_values = [], []
    for i, title in enumerate(titles, start=2):
        if not (title or "").strip():
            m_values.append([""])
            n_values.append([""])
            continue
        idx = numbers[i-2].strip() if i-2 < len(numbers) and (numbers[i-2] or "").strip() else str(i)
        sentiment, category = results_map.get(idx, (fallback_sentiment(title), fallback_category(title)))
        m_values.append([sentiment])
        n_values.append([category])

    end_row = 1 + len(m_values)
    service.spreadsheets().values().update(
        spreadsheetId=DESTINATION_SPREADSHEET_ID,
        range=f"'{destination_sheet_name}'!M2:M{end_row}",
        valueInputOption='USER_ENTERED',
        body={'values': m_values}
    ).execute()
    service.spreadsheets().values().update(
        spreadsheetId=DESTINATION_SPREADSHEET_ID,
        range=f"'{destination_sheet_name}'!N2:N{end_row}",
        valueInputOption='USER_ENTERED',
        body={'values': n_values}
    ).execute()

    print(f"✅ 分類を書き込み：M2:M{end_row}, N2:N{end_row}")


if __name__ == '__main__':
    transfer_yahoo_news_from_source_sheet()
