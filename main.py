# -*- coding: utf-8 -*-
"""
Google Sheets にニュースを転記し、
B列タイトルを Gemini (Generative Language API) で分類して
M列（ポジネガ判定）と N列（カテゴリ）に追記するスクリプト。

Secrets 必須:
- GOOGLE_CREDENTIALS: サービスアカウントJSONの中身
- GEMINI_API_KEY: AI Studioで発行したAPIキー
"""

import os
import json
import re
import time
from typing import List, Dict, Tuple

# Google Sheets API
from google.oauth2 import service_account
from googleapiclient.discovery import build

# Generative Language API (AI Studio)
import google.generativeai as genai


# ==========================
# 設定（必要に応じて変更）
# ==========================
SPREADSHEET_ID = "YOUR_SPREADSHEET_ID_HERE"  # スプレッドシートIDを指定
SHEET_NAME     = "Yahoo"                      # 対象シート名
START_ROW      = 2                            # データ開始行（ヘッダ行=1行目）
BATCH_SIZE     = 50                           # まとめて推論する件数
MODEL_NAME     = "gemini-1.5-flash"           # 無料枠向け
TEMPERATURE    = 0.2                          # 安定性重視
SLEEP_SEC      = 0.5                          # バッチ間のレート制御


# ==========================
# フォールバック判定（ルールベース）
# ==========================
NISSAN_WORDS = ["ニッサン", "日産", "NISSAN", "Nissan"]
OTHER_MAKERS = ["トヨタ","TOYOTA","ホンダ","HONDA","スバル","SUBARU",
                "マツダ","MAZDA","スズキ","SUZUKI","三菱","MITSUBISHI","ダイハツ","DAIHATSU"]

POS_KW = ["受注開始","発売","発表","好発進","優勝","開催へ","出会える","目指す",
          "わかった","メリット","最適","ナイス","選ばれたワケ","参戦","総合優勝へ"]
NEG_KW = ["事故","リコール","リストラ","値上げ","中止","苦戦","不正","炎上","失業",
          "没個性？","なぜ進化しない","問題","課題"]

def fallback_sentiment(title: str) -> str:
    t = title or ""
    if any(k in t for k in POS_KW) and not any(k in t for k in NEG_KW):
        return "ポジティブ"
    if any(k in t for k in NEG_KW) and not any(k in t for k in POS_KW):
        return "ネガティブ"
    return "ニュートラル"

def fallback_category(title: str) -> str:
    t = title or ""
    if any(k in t for k in ["F1","フォーミュラE","ラリー","WRC","Super GT","スーパーＧＴ","参戦"]):
        return "モータースポーツ"
    if any(k in t for k in ["EV化","電気自動車"," EV","EV ","バッテリー","電動","充電"]):
        return "技術（EV）"
    if any(k in t for k in ["e-POWER","e POWER","ePOWER"]):
        return "技術（e-POWER）"
    if any(k in t for k in ["e-4ORCE","E-4ORCE","4WD","AWD","2WD"]):
        return "技術（e-4ORCE）"
    if any(k in t for k in ["自動運転","ADAS","運転支援","先進運転支援","L2","L3","プラットフォーム","空力","技術"]):
        return "技術"
    if re.search(r"(RAV4|CX-[0-9]|シルビア|フォレスター|ウルス|スープラ|マイクラ|スカイライン|セレナ|ノート)", t, re.I):
        if any(w in t for w in NISSAN_WORDS):
            if "新型" in t: return "車（新型◯◯）"
            if "現行" in t: return "車（現行◯◯）"
            if "旧型" in t: return "車（旧型◯◯）"
            return "車"
        return "車（競合）"
    if any(w in t for w in NISSAN_WORDS):
        return "会社（ニッサン）"
    for comp in OTHER_MAKERS:
        if comp in t:
            return f"会社（{comp}）"
    if any(k in t for k in ["株価","上場","投資家","決算","通期見通し"]):
        return "株式"
    if any(k in t for k in ["政治","選挙","税","経済","景気","物価"]):
        return "政治・経済"
    if any(k in t for k in ["野球","サッカー","バレーボール","ラグビー","五輪"]):
        return "スポーツ"
    return "その他"


# ==========================
# Sheets 認証
# ==========================
def build_sheets_service():
    creds_info = None
    env_json = os.environ.get("GOOGLE_CREDENTIALS")
    if env_json:
        creds_info = json.loads(env_json)
    else:
        if not os.path.exists("key.json"):
            raise RuntimeError("GOOGLE_CREDENTIALS も key.json も見つかりません。")
        with open("key.json", "r", encoding="utf-8") as f:
            creds_info = json.load(f)

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = service_account.Credentials.from_service_account_info(creds_info, scopes=scopes)
    return build("sheets", "v4", credentials=creds)


# ==========================
# Gemini ユーティリティ
# ==========================
def gemini_smoke_test() -> bool:
    """接続検査。失敗したら False。"""
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        print("ℹ️ GEMINI_API_KEY 未設定（ルールベースにフォールバック）")
        return False
    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel(MODEL_NAME)
        r = model.generate_content(
            "OKだけ返してください",
            generation_config={"temperature": 0, "max_output_tokens": 4, "response_mime_type": "text/plain"},
        )
        txt = (r.text or "").strip()
        if txt:
            print("✅ Gemini接続OK")
            return True
        return False
    except Exception as e:
        print(f"⚠️ Gemini接続失敗: {e}")
        return False

def build_prompt(items: List[Dict]) -> str:
    taxonomy = """
【目的】
ニュース記事タイトルから以下を出力:
1) sentiment: ポジティブ/ネガティブ/ニュートラル
2) category: 以下のいずれか
   - 会社（◯◯）/車（新型◯◯/現行◯◯/旧型◯◯/競合）
   - 技術（EV/e-POWER/e-4ORCE/AD/ADAS/その他）
   - モータースポーツ/株式/政治・経済/スポーツ/その他

【制約】
- 出力はJSON配列のみ。コメント禁止。
- 各要素は {"idx": <id>, "sentiment": "...", "category": "..."}
"""
    lines = ["入力:"]
    for it in items:
        lines.append(f"- idx:{it['idx']} | title:{it['title']}")
    return taxonomy + "\n" + "\n".join(lines)

def ensure_json_array(text: str) -> List[Dict]:
    m = re.search(r"\[\s*{.*}\s*\]", text, re.S)
    if not m:
        raise ValueError("JSON配列が見つかりません")
    return json.loads(m.group(0))

def gemini_batch_classify(items: List[Dict]) -> List[Dict]:
    model = genai.GenerativeModel(MODEL_NAME)
    prompt = build_prompt(items)
    resp = model.generate_content(
        prompt,
        generation_config={"temperature": TEMPERATURE,
                           "max_output_tokens": 2048,
                           "response_mime_type": "application/json"},
    )
    return ensure_json_array(resp.text or "")


# ==========================
# メイン処理
# ==========================
def main():
    service = build_sheets_service()

    # B列（タイトル）, L列（番号）
    res_title = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=f"'{SHEET_NAME}'!B{START_ROW}:B"
    ).execute()
    titles = [r[0] for r in res_title.get("values", [])] if res_title.get("values") else []

    res_num = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=f"'{SHEET_NAME}'!L{START_ROW}:L"
    ).execute()
    numbers = [r[0] for r in res_num.get("values", [])] if res_num.get("values") else []

    if not titles:
        print("タイトルが見つかりません。終了。")
        return

    use_gemini = gemini_smoke_test()
    work_items: List[Dict] = []
    for i, title in enumerate(titles, start=START_ROW):
        if not (title or "").strip():
            continue
        idx = numbers[i-START_ROW].strip() if i-START_ROW < len(numbers) and (numbers[i-START_ROW] or "").strip() else str(i)
        work_items.append({"idx": idx, "title": title})

    results_map: Dict[str, Tuple[str, str]] = {}

    if use_gemini:
        print("Geminiで分類開始...")
        for s in range(0, len(work_items), BATCH_SIZE):
            batch = work_items[s:s+BATCH_SIZE]
            try:
                out = gemini_batch_classify(batch)
                got = {o["idx"]: (o["sentiment"], o["category"]) for o in out}
                for it in batch:
                    results_map[it["idx"]] = got.get(it["idx"], (fallback_sentiment(it["title"]), fallback_category(it["title"])))
            except Exception as e:
                print(f"⚠️ Geminiバッチ失敗: {e}")
                for it in batch:
                    results_map[it["idx"]] = (fallback_sentiment(it["title"]), fallback_category(it["title"]))
            time.sleep(SLEEP_SEC)
    else:
        for it in work_items:
            results_map[it["idx"]] = (fallback_sentiment(it["title"]), fallback_category(it["title"]))

    # 書き込み用データ
    m_values, n_values = [], []
    for i, title in enumerate(titles, start=START_ROW):
        idx = numbers[i-START_ROW].strip() if i-START_ROW < len(numbers) and (numbers[i-START_ROW] or "").strip() else str(i)
        sentiment, category = results_map.get(idx, (fallback_sentiment(title), fallback_category(title)))
        m_values.append([sentiment])
        n_values.append([category])

    end_row = START_ROW + len(m_values) - 1
    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{SHEET_NAME}'!M{START_ROW}:M{end_row}",
        valueInputOption="USER_ENTERED",
        body={"values": m_values},
    ).execute()
    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"'{SHEET_NAME}'!N{START_ROW}:N{end_row}",
        valueInputOption="USER_ENTERED",
        body={"values": n_values},
    ).execute()

    print(f"✅ 完了: M{START_ROW}:M{end_row}, N{START_ROW}:N{end_row}")


if __name__ == "__main__":
    main()
