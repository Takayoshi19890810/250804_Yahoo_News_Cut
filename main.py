# -*- coding: utf-8 -*-
import os
import datetime
import json
import re
import time
from typing import List, Dict

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# --- 追加: Gemini ---
try:
    import google.generativeai as genai
except ImportError:
    genai = None  # 後でフォールバック


# ========= Gemini / ルールベース =========
MODEL_NAME = "gemini-1.5-flash"  # 無料枠・低コスト向け
TEMP = 0.2
BATCH_SIZE = 100  # まとめて判定

NISSAN_WORDS = ["ニッサン","日産","NISSAN","Nissan"]
OTHER_MAKERS = ["トヨタ","TOYOTA","Toyota","ホンダ","HONDA","Honda","スバル","SUBARU","Subaru",
                "マツダ","MAZDA","Mazda","スズキ","SUZUKI","Suzuki",
                "ミツビシ","三菱","MITSUBISHI","Mitsubishi","ダイハツ","DAIHATSU","Daihatsu"]
POS_KW = ["受注開始","発売","発表","好発進","優勝","開催へ","出会える","目指す","わかった","メリット","最適","ナイス","選ばれたワケ","参戦","総合優勝へ"]
NEG_KW = ["事故","リコール","リストラ","値上げ","中止","苦戦","不正","炎上","失業","没個性？","なぜ進化しない","問題","課題"]

def _fallback_sentiment(title: str) -> str:
    t = title or ""
    if any(k in t for k in POS_KW) and not any(k in t for k in NEG_KW):
        return "ポジティブ"
    if any(k in t for k in NEG_KW) and not any(k in t for k in POS_KW):
        return "ネガティブ"
    return "ニュートラル"

def _fallback_category(title: str) -> str:
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
    # 車名の痕跡（競合寄り）
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

def _build_prompt(items: List[Dict]) -> str:
    taxonomy = """
【目的】
日本語のニュース「タイトル」から以下2点を出力してください。
1) ポジネガ判定：「ポジティブ」「ネガティブ」「ニュートラル」のいずれか
2) カテゴリ（最も関連の高い1つのみ）：
   - 会社（例：会社（ニッサン）/会社（トヨタ）/他社名）
   - 車：日産なら「車（新型◯◯/現行◯◯/旧型◯◯）」、日産以外は「車（競合）」
   - 技術（EV / e-POWER / e-4ORCE / AD/ADAS） / 技術
   - モータースポーツ / 株式 / 政治・経済 / スポーツ / その他

【制約】
- 出力は JSON 配列のみ（コメント不要）
- 各要素: {"idx": <行識別>, "sentiment": "...", "category": "..."}
"""
    lines = ["入力:"]
    for it in items:
        lines.append(f"- idx:{it['idx']} | title:{it['title']}")
    return taxonomy + "\n" + "\n".join(lines)

def _ensure_json(text: str) -> List[Dict]:
    m = re.search(r"\[\s*{.*}\s*\]", text, re.S)
    if not m:
        raise ValueError("JSON配列が見つかりませんでした。")
    return json.loads(m.group(0))

def _gemini_batch(items: List[Dict]) -> List[Dict]:
    if genai is None:
        raise RuntimeError("google-generativeai が未インストールです。")
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        raise RuntimeError("環境変数 GEMINI_API_KEY が設定されていません。")
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel(MODEL_NAME)
    prompt = _build_prompt(items)
    resp = model.generate_content(
        prompt,
        generation_config={
            "temperature": TEMP,
            "max_output_tokens": 2048,
            "response_mime_type": "application/json",
        },
    )
    return _ensure_json(resp.text or "")

def classify_titles_with_gemini(titles: List[str], idx_list: List[str]) -> Dict[str, Dict]:
    """
    titles: B2～のタイトル配列
    idx_list: 同じ行の L列（番号）。空なら行番号文字列をidxにする
    戻り値: {idx: {"sentiment": "...", "category": "..."}}
    """
    results: Dict[str, Dict] = {}
    items: List[Dict] = []
    # データ作成
    for i, t in enumerate(titles, start=2):  # 実シート行=2始まり
        if not (t or "").strip():
            continue
        idx = (idx_list[i-2].strip() if i-2 < len(idx_list) and (idx_list[i-2] or "").strip() else str(i))
        items.append({"idx": idx, "title": t})

    if not items:
        return results

    # バッチ推論（無料枠配慮）
    for s in range(0, len(items), BATCH_SIZE):
        batch = items[s:s+BATCH_SIZE]
        try:
            out = _gemini_batch(batch)
            # マッピング
            got = {o["idx"]: o for o in out if "idx" in o and "sentiment" in o and "category" in o}
            for it in batch:
                if it["idx"] in got:
                    results[it["idx"]] = {"sentiment": got[it["idx"]]["sentiment"], "category": got[it["idx"]]["category"]}
                else:
                    results[it["idx"]] = {"sentiment": _fallback_sentiment(it["title"]), "category": _fallback_category(it["title"])}
        except Exception:
            # 失敗時は全件フォールバック
            for it in batch:
                results[it["idx"]] = {"sentiment": _fallback_sentiment(it["title"]), "category": _fallback_category(it["title"])}
        time.sleep(0.5)  # 軽いレート調整
    return results


def transfer_yahoo_news_from_source_sheet():
    """
    既存の転記処理 → その後にGeminiでM/N列を自動分類してバルク書き込み
    """
    # --- 設定 ---
    SOURCE_SPREADSHEET_ID = '1RglATeTbLU1SqlfXnNToJqhXLdNoHCdePldioKDQgU8'
    DESTINATION_SPREADSHEET_ID = '19c6yIGr5BiI7XwstYhUPptFGksPPXE4N1bEq5iFoPok'
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

    # --- 認証 ---
    try:
        creds_json = os.environ.get('GCP_SA_KEY')
        if not creds_json:
            with open('key.json', 'r') as f:
                creds_info = json.load(f)
        else:
            creds_info = json.loads(creds_json)
        creds = service_account.Credentials.from_service_account_info(creds_info, scopes=SCOPES)
        service = build('sheets', 'v4', credentials=creds)
    except Exception as e:
        print(f"エラー: Google Sheets APIの認証に失敗しました。詳細: {e}")
        return

    # --- 日付範囲（JSTの 15:00〜翌14:59:59） ---
    today = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9)))
    yesterday = today - datetime.timedelta(days=1)
    start_time = yesterday.replace(hour=15, minute=0, second=0, microsecond=0)
    end_time = today.replace(hour=14, minute=59, second=59, microsecond=0)
    destination_sheet_name = today.strftime('%y%m%d')

    print(f"出力先シート名: {destination_sheet_name}")
    print(f"期間: {start_time.strftime('%Y/%m/%d %H:%M:%S')} 〜 {end_time.strftime('%Y/%m/%d %H:%M:%S')}")

    # --- 出力先シートの準備＆既存URL収集 ---
    existing_data_in_destination = []
    header_exists = False
    try:
        spreadsheet_info = service.spreadsheets().get(spreadsheetId=DESTINATION_SPREADSHEET_ID).execute()
        sheets = spreadsheet_info.get('sheets', [])
        sheet_exists = any(sheet['properties']['title'] == destination_sheet_name for sheet in sheets)
        if not sheet_exists:
            print(f"シート「{destination_sheet_name}」を新規作成")
            body = {'requests': [{'addSheet': {'properties': {'title': destination_sheet_name}}}]}
            service.spreadsheets().batchUpdate(spreadsheetId=DESTINATION_SPREADSHEET_ID, body=body).execute()

        # 既存データ
        destination_sheet_range = f"'{destination_sheet_name}'!A:L"
        result = service.spreadsheets().values().get(
            spreadsheetId=DESTINATION_SPREADSHEET_ID, range=destination_sheet_range
        ).execute()
        existing_data_in_destination = result.get('values', [])
    except HttpError as e:
        print(f"エラー: 出力先シートへのアクセス失敗: {e}")
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

    print(f"出力先シートに既存ニュース {len(existing_urls_in_destination)} 件（URL重複判定）")

    # --- コピー元から取得 ---
    source_sheet_name = 'Yahoo'
    try:
        source_sheet_range = f"'{source_sheet_name}'!A:D"
        result = service.spreadsheets().values().get(
            spreadsheetId=SOURCE_SPREADSHEET_ID, range=source_sheet_range
        ).execute()
        data = result.get('values', [])
    except Exception as e:
        print(f"エラー: コピー元シート「{source_sheet_name}」へアクセス不可: {e}")
        return

    if not data:
        print(f"エラー: コピー元シート「{source_sheet_name}」にデータ無し。")
        return

    print(f"コピー元から {len(data) - 1} 件（ヘッダ除く）読み込み。")

    # --- 抽出＆追記 ---
    today_jst = today
    collected_news = []
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
                    post_date = d.replace(year=today_jst.year)
                except ValueError:
                    try:
                        post_date = datetime.datetime.strptime(post_date_raw, '%Y/%m/%d %H:%M:%S')
                    except ValueError:
                        pass
            elif isinstance(post_date_raw, float):
                epoch = datetime.datetime(1899, 12, 30)
                post_date = epoch + datetime.timedelta(days=post_date_raw)

            if post_date:
                post_date = post_date.replace(tzinfo=datetime.timezone(datetime.timedelta(hours=9)))
                if start_time <= post_date <= end_time and url not in existing_urls_in_destination:
                    new_row = [source_sheet_name, title, url, post_date.strftime('%Y/%m/%d'), source]
                    collected_news.append(new_row)
        except Exception as e:
            print(f"警告: 行 {i+1} の処理をスキップ: {e}")
            continue

    if not collected_news:
        print("期間内の新規ニュースはありません。分類処理のみ実行します。")

    # --- ヘッダが無ければ作成 ---
    if not header_exists:
        header_row = ['ソース','タイトル','URL','投稿日','引用元','コメント数','ポジネガ','カテゴリー','有料記事',
                      'J列(ダブりチェック)','K列（タイトル抜粋）','L列（番号）']
        service.spreadsheets().values().append(
            spreadsheetId=DESTINATION_SPREADSHEET_ID,
            range=f"'{destination_sheet_name}'!A1",
            valueInputOption='USER_ENTERED',
            body={'values': [header_row]}
        ).execute()
        print(f"ヘッダーを追加: {destination_sheet_name}")

    # --- 追記 ---
    if collected_news:
        # 追加する行にJ/K/L も付与
        # 既存データを再取得して行数・番号付与用に利用
        result = service.spreadsheets().values().get(
            spreadsheetId=DESTINATION_SPREADSHEET_ID,
            range=f"'{destination_sheet_name}'!A:L"
        ).execute()
        existing_data_in_destination = result.get('values', [])
        header_exists = bool(existing_data_in_destination and existing_data_in_destination[0][0] == 'ソース')
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
            row_data = row[:5]
            row_data.extend(['', '', '', ''])  # コメント数/ポジネガ/カテゴリ/有料記事 の空欄
            current_row_num = len(existing_data_in_destination) + 1 + i + (0 if header_exists else 1)
            j_formula = f'=IF(ISERROR(VLOOKUP(K{current_row_num},K{current_row_num+1}:L{last_row_after_append},2,FALSE)),"ダブり無し",VLOOKUP(K{current_row_num},K{current_row_num+1}:L{last_row_after_append},2,FALSE))'
            processed_title = row[1].translate(str.maketrans({c: "" for c in " ,.-_<>【】「」()"}))
            k_value = processed_title[:20]
            l_value = start_l_number + i + 1
            row_data.append(j_formula)
            row_data.append(k_value)
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

    # ===== ここから分類処理 =====
    # B列（タイトル）と L列（番号）を取得（2行目以降）
    result_titles = service.spreadsheets().values().get(
        spreadsheetId=DESTINATION_SPREADSHEET_ID,
        range=f"'{destination_sheet_name}'!B2:B"
    ).execute()
    titles = [r[0] for r in result_titles.get('values', [])] if result_titles.get('values') else []

    result_numbers = service.spreadsheets().values().get(
        spreadsheetId=DESTINATION_SPREADSHEET_ID,
        range=f"'{destination_sheet_name}'!L2:L"
    ).execute()
    numbers = [r[0] for r in result_numbers.get('values', [])] if result_numbers.get('values') else []

    if not titles:
        print("分類対象のタイトルがありません。処理終了。")
        return

    # Gemini（無ければルールベース）
    use_gemini = (genai is not None) and bool(os.environ.get("GEMINI_API_KEY"))
    if use_gemini:
        print("Geminiで一括分類します（無料枠配慮のバッチ推論）...")
        result_map = classify_titles_with_gemini(titles, numbers)
    else:
        print("GEMINI_API_KEYが無いかライブラリ未導入のため、ルールベースで分類します。")
        result_map = {}
        for i, t in enumerate(titles, start=2):
            idx = numbers[i-2].strip() if i-2 < len(numbers) and (numbers[i-2] or "").strip() else str(i)
            result_map[idx] = {"sentiment": _fallback_sentiment(t), "category": _fallback_category(t)}

    # 行順に M/N 列の配列を作成
    m_values, n_values = [], []
    for i, t in enumerate(titles, start=2):
        idx = numbers[i-2].strip() if i-2 < len(numbers) and (numbers[i-2] or "").strip() else str(i)
        res = result_map.get(idx, {"sentiment": _fallback_sentiment(t), "category": _fallback_category(t)})
        m_values.append([res["sentiment"]])
        n_values.append([res["category"]])

    end_row = 1 + len(m_values)  # M2〜M{end_row}
    if end_row >= 2:
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
