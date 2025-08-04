import os
import datetime
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

def transfer_yahoo_news_from_source_sheet():
    """
    Google Apps Scriptの機能をPythonに置き換えた関数。
    Google Sheets APIを使ってスプレッドシートのデータを転送する。
    """

    # --- 設定 ---
    # コピー元スプレッドシートのID
    SOURCE_SPREADSHEET_ID = '1RglATeTbLU1SqlfXnNToJqhXLdNoHCdePldioKDQgU8'
    # 出力先スプレッドシートのID
    DESTINATION_SPREADSHEET_ID = '19c6yIGr5BiI7XwstYhUPptFGksPPXE4N1bEq5iFoPok'
    # スコープ
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

    # --- 認証情報の取得 ---
    try:
        # GitHub Actionsの環境変数からサービスアカウントキーを読み込む
        creds_json = os.environ.get('GCP_SA_KEY')
        if not creds_json:
            # ローカルテスト用: key.jsonファイルから読み込む
            with open('key.json', 'r') as f:
                creds_info = json.load(f)
        else:
            # GitHub Actionsの実行環境
            creds_info = json.loads(creds_json)
            
        creds = service_account.Credentials.from_service_account_info(creds_info, scopes=SCOPES)
        service = build('sheets', 'v4', credentials=creds)
    except Exception as e:
        print(f"エラー: Google Sheets APIの認証に失敗しました。詳細: {e}")
        return

    # --- 日付範囲の設定 ---
    # 日本時間で今日を取得
    today = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9))) 
    yesterday = today - datetime.timedelta(days=1)

    # GASの15時〜14:59の範囲を模倣
    start_time = yesterday.replace(hour=15, minute=0, second=0, microsecond=0)
    end_time = today.replace(hour=14, minute=59, second=59, microsecond=0)

    # 出力先シート名
    destination_sheet_name = today.strftime('%y%m%d')

    print(f"出力先シート名: {destination_sheet_name}")
    print(f"期間: {start_time.strftime('%Y/%m/%d %H:%M:%S')} 〜 {end_time.strftime('%Y/%m/%d %H:%M:%S')}")

    # --- 出力先シートの準備と既存URLの収集 ---
    existing_data_in_destination = []
    header_exists = False
    
    try:
        # スプレッドシートのメタデータを取得
        spreadsheet_info = service.spreadsheets().get(spreadsheetId=DESTINATION_SPREADSHEET_ID).execute()
        sheets = spreadsheet_info.get('sheets', [])
        
        # 目的のシートが存在するか確認
        sheet_exists = any(sheet['properties']['title'] == destination_sheet_name for sheet in sheets)
        
        if not sheet_exists:
            print(f"出力先スプレッドシートに新しいシート「{destination_sheet_name}」を作成します。")
            body = {
                'requests': [{
                    'addSheet': {
                        'properties': {
                            'title': destination_sheet_name
                        }
                    }
                }]
            }
            service.spreadsheets().batchUpdate(
                spreadsheetId=DESTINATION_SPREADSHEET_ID,
                body=body
            ).execute()
            print(f"新しいシート「{destination_sheet_name}」を作成しました。")
            
        # 既存データを取得
        destination_sheet_range = f"'{destination_sheet_name}'!A:L"
        result = service.spreadsheets().values().get(
            spreadsheetId=DESTINATION_SPREADSHEET_ID,
            range=destination_sheet_range
        ).execute()
        existing_data_in_destination = result.get('values', [])
        
    except HttpError as e:
        # APIエラーが発生した場合（権限不足など）
        print(f"エラー: 出力先スプレッドシートへのアクセスに失敗しました。詳細: {e}")
        return
    except Exception as e:
        print(f"エラー: 不明なエラーが発生しました。詳細: {e}")
        return
        
    existing_urls_in_destination = set()
    if existing_data_in_destination:
        # ヘッダー行が存在するか確認
        if existing_data_in_destination[0] and existing_data_in_destination[0][0] == 'ソース':
            header_exists = True
            for row in existing_data_in_destination[1:]:
                if len(row) > 2 and row[2]:
                    existing_urls_in_destination.add(row[2])
    
    print(f"出力先シートに既存のニュースが {len(existing_urls_in_destination)} 件あります（URLで重複を判定）。")

    # --- コピー元シートからニュースを抽出・収集 ---
    source_sheet_name = 'Yahoo'
    try:
        source_sheet_range = f"'{source_sheet_name}'!A:D"
        result = service.spreadsheets().values().get(
            spreadsheetId=SOURCE_SPREADSHEET_ID,
            range=source_sheet_range
        ).execute()
        data = result.get('values', [])
    except Exception as e:
        print(f"エラー: コピー元シート「{source_sheet_name}」にアクセスできませんでした。詳細: {e}")
        return

    if not data:
        print(f"エラー: コピー元シート「{source_sheet_name}」にデータがありません。")
        return

    print(f"シート「{source_sheet_name}」から {len(data) - 1} 件のニュースを読み込みました（ヘッダーを除く）。")
    
    collected_news = []
    for i, row in enumerate(data):
        if i == 0:
            continue
        
        try:
            title = row[0]
            url = row[1]
            post_date_raw = row[2]
            source = row[3]
            
            # 日付の形式を適切に処理
            post_date = None
            if isinstance(post_date_raw, str):
                try:
                    # '7/31 14:30' 形式を試す
                    post_date_without_year = datetime.datetime.strptime(post_date_raw, '%m/%d %H:%M')
                    # 年情報を補完
                    post_date = post_date_without_year.replace(year=today.year)
                except ValueError:
                    try:
                        # 'YYYY/MM/DD HH:MM:SS' 形式を試す
                        post_date = datetime.datetime.strptime(post_date_raw, '%Y/%m/%d %H:%M:%S')
                    except ValueError:
                        pass
            elif isinstance(post_date_raw, float):
                # Excel/Googleスプレッドシートのシリアル値の場合
                epoch = datetime.datetime(1899, 12, 30)
                post_date = epoch + datetime.timedelta(days=post_date_raw)
            elif isinstance(post_date_raw, datetime.date):
                # dateオブジェクトの場合
                post_date = datetime.datetime.combine(post_date_raw, datetime.time())
            
            if post_date:
                # タイムゾーン情報を付与
                post_date = post_date.replace(tzinfo=datetime.timezone(datetime.timedelta(hours=9)))
                
                if start_time <= post_date <= end_time:
                    # 重複チェック
                    if url not in existing_urls_in_destination:
                        # 投稿日の形式をYYYY/MM/DDに変更
                        new_row = [source_sheet_name, title, url, post_date.strftime('%Y/%m/%d'), source]
                        collected_news.append(new_row)

        except (IndexError, ValueError) as e:
            print(f"警告: 行 {i+1} のデータの処理中にエラーが発生しました。行をスキップします。詳細: {e}")
            continue

    if not collected_news:
        print("期間内に新しいニュースは見つかりませんでした。")
        return

    print(f"新規に追記するニュースの数: {len(collected_news)}")

    # --- 収集したデータを目的のシートに書き込み ---
    data_to_append = []
    
    # 最後の行番号を取得して'L'列の開始番号を決定
    last_row_after_append = len(existing_data_in_destination) + len(collected_news) + (0 if header_exists else 1)
    
    start_l_number = 0
    if header_exists and existing_data_in_destination:
        try:
            last_row_data = existing_data_in_destination[-1]
            if len(last_row_data) > 11:
                start_l_number = int(last_row_data[11])
        except (ValueError, IndexError):
            pass

    for i, row in enumerate(collected_news):
        row_data = row[:5]
        row_data.extend(['', '', '', ''])
        current_row_num = len(existing_data_in_destination) + 1 + i + (0 if header_exists else 1)
        j_formula = f'=IF(ISERROR(VLOOKUP(K{current_row_num},K{current_row_num+1}:L{last_row_after_append},2,FALSE)),"ダブり無し",VLOOKUP(K{current_row_num},K{current_row_num+1}:L{last_row_after_append},2,FALSE))'
        row_data.append(j_formula)
        processed_title = row[1].replace(' ', '').replace(',', '').replace('.', '').replace('-', '').replace('_', '').replace('<', '').replace('>', '').replace('【', '').replace('】', '').replace('「', '').replace('」', '').replace('(', '').replace(')', '')
        k_value = processed_title[:20]
        row_data.append(k_value)
        l_value = start_l_number + i + 1
        row_data.append(l_value)
        
        data_to_append.append(row_data)

    # ヘッダーがなければ追加
    if not header_exists:
        header_row = [
            'ソース', 'タイトル', 'URL', '投稿日', '引用元',
            'コメント数', 'ポジネガ', 'カテゴリー', '有料記事',
            'J列(ダブりチェック)', 'K列（タイトル抜粋）', 'L列（番号）'
        ]
        try:
            body = {'values': [header_row]}
            service.spreadsheets().values().append(
                spreadsheetId=DESTINATION_SPREADSHEET_ID,
                range=f"'{destination_sheet_name}'!A1",
                valueInputOption='USER_ENTERED',
                body=body
            ).execute()
            print(f"シート「{destination_sheet_name}」にヘッダーを追加しました。")
        except Exception as e:
            print(f"エラー: ヘッダーの追加に失敗しました。詳細: {e}")
            return
    
    # データを追記
    if data_to_append:
        try:
            body = {'values': data_to_append}
            service.spreadsheets().values().append(
                spreadsheetId=DESTINATION_SPREADSHEET_ID,
                range=f"'{destination_sheet_name}'!A:L",
                valueInputOption='USER_ENTERED',
                insertDataOption='INSERT_ROWS',
                body=body
            ).execute()
            print(f"スプレッドシート「{DESTINATION_SPREADSHEET_ID}」のシート「{destination_sheet_name}」に {len(data_to_append)} 件の新しいニュースを追記しました。")
        except Exception as e:
            print(f"エラー: データの追記に失敗しました。詳細: {e}")
            return

if __name__ == '__main__':
    transfer_yahoo_news_from_source_sheet()
