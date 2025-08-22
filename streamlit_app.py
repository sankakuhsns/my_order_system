# -*- coding: utf-8 -*-
# =============================================================================
# 📦 Streamlit 식자재 발주 시스템 (v16.0 - 생산/재고 관리 기능 통합)
#
# - 주요 변경 사항 (v15.0 -> v16.0):
#   - 신규 시트 '재고로그', '현재고' 연동 및 관련 로직 추가
#   - 관리자용 '일일 생산 보고' 탭 신설 (생산량 입력 기능)
#   - 관리자용 '생산/재고 관리' 탭 신설 (현재고, 변동내역, 재고조정 기능)
#   - 발주 승인 시, 재고 부족을 차단하는 대신 '경고 메시지' 표시로 로직 변경
#   - 증빙서류 다운로드 UI 개선 및 생산/재고 관련 리포트 추가
#   - 매출 순위 시각화를 표 내부 막대그래프 스타일로 변경
#   - pivot table 관련 'ArrowInvalid' 오류 해결
# =============================================================================

from io import BytesIO
from datetime import datetime, date, timedelta
from typing import Dict, Any, List
from zoneinfo import ZoneInfo
import math
import pandas as pd
import streamlit as st
import gspread
from google.oauth2 import service_account
import xlsxwriter

# =============================================================================
# 0) 기본 설정 및 유틸리티 함수
# =============================================================================
st.set_page_config(page_title="산카쿠 식자재 발주 시스템", page_icon="📦", layout="wide")
THEME = { "BORDER": "#e8e8ee", "PRIMARY": "#1C6758", "BG": "#f7f8fa", "TEXT": "#222" }
# (스타일 설정은 이전과 동일)
st.markdown(f"""<br><style>
    .stTabs [data-baseweb="tab-list"] {{ gap: 12px; }}
    .stTabs [data-baseweb="tab"] {{ height: 42px; border: 1px solid {THEME['BORDER']}; border-radius: 12px; background-color: #fff; padding: 10px 14px; box-shadow: 0 1px 6px rgba(0,0,0,0.04); }}
    .stTabs [aria-selected="true"] {{ border-color: {THEME['PRIMARY']}; color: {THEME['PRIMARY']}; box-shadow: 0 6px 16px rgba(28,103,88,0.18); font-weight: 700; }}
    html, body, [data-testid="stAppViewContainer"] {{ background: {THEME['BG']}; color: {THEME['TEXT']}; }}
    .block-container {{ padding-top: 2.4rem; padding-bottom: 1.6rem; }}
    [data-testid="stAppViewContainer"] .main .block-container {{ max-width: 1050px; margin: 0 auto; padding: 0 12px; }}
    .stTabs [data-baseweb="tab-highlight"], .stTabs [data-baseweb="tab-border"] {{ display: none; }}
</style>""", unsafe_allow_html=True)

KST = ZoneInfo("Asia/Seoul")
SUPPLIER_INFO = {
    "상호명": "(주)산카쿠컴퍼니",
    "사업자등록번호": "123-45-67890",
    "대표자명": "김대표",
    "사업장주소": "대전광역시 중구 중앙로 123번길 45, 1층",
    "업태": "도소매",
    "종목": "식자재"
}

def now_kst_str(fmt: str = "%Y-%m-%d %H:%M:%S") -> str: return datetime.now(KST).strftime(fmt)

def display_feedback():
    if "success_message" in st.session_state and st.session_state.success_message:
        st.success(st.session_state.success_message)
        st.session_state.success_message = ""
    if "error_message" in st.session_state and st.session_state.error_message:
        st.error(st.session_state.error_message)
        st.session_state.error_message = ""
    if "warning_message" in st.session_state and st.session_state.warning_message:
        st.warning(st.session_state.warning_message)
        st.session_state.warning_message = ""

def v_spacer(height: int):
    st.markdown(f"<div style='height:{height}px'></div>", unsafe_allow_html=True)

# =============================================================================
# 1) 시트/스키마 정의
# =============================================================================
# --- 기존 시트 ---
SHEET_NAME_STORES = "지점마스터"
SHEET_NAME_MASTER = "상품마스터"
SHEET_NAME_ORDERS = "발주"
SHEET_NAME_BALANCE = "잔액마스터"
SHEET_NAME_CHARGE_REQ = "충전요청"
SHEET_NAME_TRANSACTIONS = "거래내역"
# --- 신규 재고 시트 ---
SHEET_NAME_INVENTORY_LOG = "재고로그"
SHEET_NAME_CURRENT_INVENTORY = "현재고"

# --- 기존 스키마 ---
STORES_COLUMNS = ["지점ID", "지점PW", "역할", "지점명", "사업자등록번호", "상호명", "대표자명", "사업장주소", "업태", "종목"]
MASTER_COLUMNS = ["품목코드", "품목명", "품목규격", "분류", "단위", "단가", "과세구분", "활성"]
ORDERS_COLUMNS = ["주문일시", "발주번호", "지점ID", "지점명", "품목코드", "품목명", "단위", "수량", "단가", "공급가액", "세액", "합계금액", "비고", "상태", "처리일시", "처리자", "반려사유"]
CART_COLUMNS = ["품목코드", "품목명", "단위", "단가", "단가(VAT포함)", "수량", "합계금액(VAT포함)"]
BALANCE_COLUMNS = ["지점ID", "지점명", "선충전잔액", "여신한도", "사용여신액"]
CHARGE_REQ_COLUMNS = ["요청일시", "지점ID", "지점명", "입금자명", "입금액", "종류", "상태", "처리사유"]
TRANSACTIONS_COLUMNS = ["일시", "지점ID", "지점명", "구분", "내용", "금액", "처리후선충전잔액", "처리후사용여신액", "관련발주번호", "처리자"]
# --- 신규 재고 스키마 ---
INVENTORY_LOG_COLUMNS = ["로그일시", "품목코드", "품목명", "구분", "수량변경", "처리후재고", "관련번호", "처리자", "사유"]
CURRENT_INVENTORY_COLUMNS = ["품목코드", "품목명", "현재고수량", "최종갱신일시"]

# =============================================================================
# 2) Google Sheets 연결 및 I/O (재고 관련 로직 추가)
# =============================================================================
@st.cache_resource(show_spinner=False)
def get_gs_client():
    creds_info = st.secrets["google"]
    creds_dict = dict(creds_info)
    if "\\n" in creds_dict.get("private_key", ""):
        creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")
    creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return gspread.authorize(creds)

@st.cache_resource(show_spinner=False)
def open_spreadsheet():
    key = st.secrets["google"]["SPREADSHEET_KEY"]
    try: return get_gs_client().open_by_key(key)
    except Exception as e: st.error(f"스프레드시트 열기 실패: {e}"); st.stop()

@st.cache_data(ttl=30)
def load_data(sheet_name: str, columns: List[str] = None) -> pd.DataFrame:
    try:
        ws = open_spreadsheet().worksheet(sheet_name)
        records = ws.get_all_records(empty2zero=False, head=1)
        if not records:
            return pd.DataFrame(columns=columns) if columns else pd.DataFrame()
        
        df = pd.DataFrame(records)
        df = df.astype(str)
        
        numeric_cols = {
            SHEET_NAME_BALANCE: ['선충전잔액', '여신한도', '사용여신액'],
            SHEET_NAME_CHARGE_REQ: ['입금액'],
            SHEET_NAME_TRANSACTIONS: ['금액', '처리후선충전잔액', '처리후사용여신액'],
            SHEET_NAME_ORDERS: ["수량", "단가", "공급가액", "세액", "합계금액"],
            SHEET_NAME_MASTER: ["단가"],
            SHEET_NAME_INVENTORY_LOG: ["수량변경", "처리후재고"],
            SHEET_NAME_CURRENT_INVENTORY: ["현재고수량"]
        }
        if sheet_name in numeric_cols:
            for col in numeric_cols[sheet_name]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)

        if columns:
            for col in columns:
                if col not in df.columns:
                    default_value = 0 if col in [nc for sublist in numeric_cols.values() for nc in sublist] else ''
                    df[col] = default_value
            df = df[columns]
            
        sort_key_map = {'주문일시': "주문일시", '요청일시': "요청일시", '일시': "일시", '로그일시': "로그일시"}
        for col, key in sort_key_map.items():
            if col in df.columns:
                try:
                    df[key] = pd.to_datetime(df[key])
                    df = df.sort_values(by=key, ascending=False)
                except Exception:
                    pass
                break
        return df
    except gspread.WorksheetNotFound:
        st.warning(f"'{sheet_name}' 시트를 찾을 수 없습니다. 시트를 먼저 생성해주세요.")
        return pd.DataFrame(columns=columns) if columns else pd.DataFrame()

def save_df_to_sheet(sheet_name: str, df: pd.DataFrame):
    try:
        ws = open_spreadsheet().worksheet(sheet_name)
        ws.clear()
        df_filled = df.fillna('')
        ws.update([df_filled.columns.values.tolist()] + df_filled.values.tolist(), value_input_option='USER_ENTERED')
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"'{sheet_name}' 시트에 데이터를 저장하는 중 오류 발생: {e}")
        return False
        
def append_rows_to_sheet(sheet_name: str, rows_data: List[Dict], columns_order: List[str]):
    try:
        ws = open_spreadsheet().worksheet(sheet_name)
        values_to_append = [[row.get(col, "") for col in columns_order] for row in rows_data]
        ws.append_rows(values_to_append, value_input_option='USER_ENTERED')
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"'{sheet_name}' 시트에 데이터를 추가하는 중 오류 발생: {e}")
        return False

def update_balance_sheet(store_id: str, updates: Dict):
    try:
        ws = open_spreadsheet().worksheet(SHEET_NAME_BALANCE)
        cell = ws.find(store_id, in_column=1)
        if not cell:
            st.error(f"'{SHEET_NAME_BALANCE}' 시트에서 지점ID '{store_id}'를 찾을 수 없습니다.")
            return False
        header = ws.row_values(1)
        for key, value in updates.items():
            if key in header:
                col_idx = header.index(key) + 1
                ws.update_cell(cell.row, col_idx, int(value))
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"잔액/여신 정보 업데이트 중 오류 발생: {e}")
        return False
        
def update_order_status(selected_ids: List[str], new_status: str, handler: str, reason: str = "") -> bool:
    if not selected_ids: return True
    try:
        ws = open_spreadsheet().worksheet(SHEET_NAME_ORDERS)
        all_data = ws.get_all_values()
        header = all_data[0]
        id_col_idx = header.index("발주번호")
        status_col_idx = header.index("상태")
        handler_col_idx = header.index("처리자")
        timestamp_col_idx = header.index("처리일시")
        reason_col_idx = header.index("반려사유") if "반려사유" in header else -1
        
        cells_to_update = []
        now_str = now_kst_str() if new_status != '요청' else ''
        handler_name = handler if new_status != '요청' else ''
        
        for i, row in enumerate(all_data[1:], start=2):
            if row[id_col_idx] in selected_ids:
                cells_to_update.append(gspread.Cell(i, status_col_idx + 1, new_status))
                cells_to_update.append(gspread.Cell(i, handler_col_idx + 1, handler_name))
                cells_to_update.append(gspread.Cell(i, timestamp_col_idx + 1, now_str))
                if reason_col_idx != -1:
                    reason_text = reason if new_status == "반려" else ""
                    cells_to_update.append(gspread.Cell(i, reason_col_idx + 1, reason_text))

        if cells_to_update: ws.update_cells(cells_to_update, value_input_option='USER_ENTERED')
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"발주 상태 업데이트 중 오류가 발생했습니다: {e}")
        return False

# =============================================================================
# 3) 로그인 및 인증 (지점마스터 시트 기반)
# =============================================================================
def authenticate_user(uid, pwd, store_master_df):
    if uid and pwd:
        user_info = store_master_df[store_master_df['지점ID'] == uid]
        if not user_info.empty:
            stored_pw = user_info.iloc[0]['지점PW']
            if pwd == stored_pw:
                role = user_info.iloc[0]['역할']
                name = user_info.iloc[0]['지점명']
                return {"login": True, "user_id": uid, "name": name, "role": role}
    return {"login": False, "message": "아이디 또는 비밀번호가 올바르지 않습니다."}

def require_login():
    if st.session_state.get("auth", {}).get("login"):
        user = st.session_state.auth
        st.sidebar.markdown(f"### 로그인 정보")
        st.sidebar.markdown(f"**{user['name']}** ({user['role']})님 환영합니다.")
        if st.sidebar.button("로그아웃"):
            del st.session_state.auth
            st.rerun()
        return True
    
    store_master_df = load_data(SHEET_NAME_STORES, STORES_COLUMNS)
    if store_master_df.empty:
        st.error("'지점마스터' 시트를 찾을 수 없거나 비어있습니다. 관리자에게 문의하세요.")
        st.stop()

    st.markdown('<div style="text-align:center; font-size:42px; font-weight:800; margin:16px 0 12px;">식자재 발주 시스템</div>', unsafe_allow_html=True)
    _, mid, _ = st.columns([3, 2, 3])
    with mid.form("login_form"):
        uid = st.text_input("아이디 (지점ID)", key="login_uid")
        pwd = st.text_input("비밀번호", type="password", key="login_pw")
        
        if st.form_submit_button("로그인", use_container_width=True):
            auth_result = authenticate_user(uid, pwd, store_master_df)
            if auth_result["login"]:
                st.session_state["auth"] = auth_result
                st.rerun()
            else:
                st.error(auth_result.get("message", "로그인 실패"))
    return False

# =============================================================================
# 4) Excel 생성 (생산/재고 리포트 추가)
# =============================================================================
def make_order_id(store_id: str) -> str: return f"{datetime.now(KST):%Y%m%d%H%M%S}{store_id}"

def get_vat_inclusive_price(row: pd.Series) -> int:
    price = int(row.get('단가', 0))
    tax_type = row.get('과세구분', '과세')
    return int(price * 1.1) if tax_type == '과세' else price

# [개선사항 3] 거래명세서 서식 전면 개선
def make_item_transaction_statement_excel(order_df: pd.DataFrame, store_info: pd.Series) -> BytesIO:
    output = BytesIO()
    if order_df.empty: return output

    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        workbook = writer.book
        worksheet = workbook.add_worksheet("품목거래명세서")
        
        # --- 서식 정의 ---
        fmt_title = workbook.add_format({'bold': True, 'font_size': 20, 'align': 'center', 'valign': 'vcenter', 'border': 1})
        fmt_h2 = workbook.add_format({'bold': True, 'font_size': 11, 'bg_color': '#F2F2F2', 'align': 'center', 'valign': 'vcenter', 'border': 1})
        fmt_info = workbook.add_format({'font_size': 10, 'border': 1, 'align': 'left', 'valign': 'vcenter'})
        fmt_header = workbook.add_format({'bold': True, 'bg_color': '#DDEBF7', 'border': 1, 'align': 'center', 'valign': 'vcenter'})
        fmt_money = workbook.add_format({'num_format': '#,##0', 'border': 1})
        fmt_border = workbook.add_format({'border': 1})
        fmt_border_c = workbook.add_format({'border': 1, 'align': 'center'})
        fmt_total = workbook.add_format({'bold': True, 'bg_color': '#DDEBF7', 'border': 1, 'num_format': '#,##0'})

        # --- 레이아웃 설정 ---
        worksheet.set_column('A:A', 5); worksheet.set_column('B:B', 25); worksheet.set_column('C:D', 10)
        worksheet.set_column('E:H', 15)

        # --- 제목 ---
        worksheet.merge_range('A1:H2', '품 목 거 래 명 세 서', fmt_title)
        
        # --- 발주 정보 ---
        order_info = order_df.iloc[0]
        worksheet.write('F4', '발주번호', fmt_h2)
        worksheet.merge_range('G4:H4', order_info['발주번호'], fmt_info)
        worksheet.write('F5', '발주일시', fmt_h2)
        worksheet.merge_range('G5:H5', order_info['주문일시'], fmt_info)

        # --- 공급자/공급받는자 정보 ---
        for i in range(7, 12):
            worksheet.set_row(i, 20)
        
        worksheet.merge_range('A7:A11', '공\n급\n하\n는\n자', fmt_h2)
        worksheet.write('B7', '사업자등록번호', fmt_h2); worksheet.merge_range('C7:E7', SUPPLIER_INFO['사업자등록번호'], fmt_info)
        worksheet.write('B8', '상호', fmt_h2); worksheet.write('C8', SUPPLIER_INFO['상호명'], fmt_info)
        worksheet.write('D8', '대표', fmt_h2); worksheet.write('E8', SUPPLIER_INFO['대표자명'], fmt_info)
        worksheet.write('B9', '사업장 주소', fmt_h2); worksheet.merge_range('C9:E9', SUPPLIER_INFO['사업장주소'], fmt_info)
        worksheet.write('B10', '업태', fmt_h2); worksheet.write('C10', SUPPLIER_INFO['업태'], fmt_info)
        worksheet.write('D10', '종목', fmt_h2); worksheet.write('E10', SUPPLIER_INFO['종목'], fmt_info)

        worksheet.merge_range('F7:F11', '공\n급\n받\n는\n자', fmt_h2)
        worksheet.write('G7', '상호', fmt_h2); worksheet.write('H7', store_info.get('상호명', ''), fmt_info)
        worksheet.write('G8', '사업장 주소', fmt_h2); worksheet.write('H8', store_info.get('사업장주소', ''), fmt_info)
        worksheet.write('G9', '대표', fmt_h2); worksheet.write('H9', store_info.get('대표자명', ''), fmt_info)
        worksheet.write('G10', '업태', fmt_h2); worksheet.write('H10', store_info.get('업태', ''), fmt_info)
        worksheet.write('G11', '종목', fmt_h2); worksheet.write('H11', store_info.get('종목', ''), fmt_info)
        
        # --- 품목 리스트 헤더 ---
        headers = ["No", "품목명", "단위", "수량", "단가", "공급가액", "세액", "합계금액"]
        worksheet.write_row('A13', headers, fmt_header)
        
        # --- 품목 데이터 ---
        row_num = 13
        order_df_reset = order_df.reset_index(drop=True)
        for i, record in order_df_reset.iterrows():
            row_num += 1
            worksheet.write(f'A{row_num}', i + 1, fmt_border_c)
            worksheet.write(f'B{row_num}', record['품목명'], fmt_border)
            worksheet.write(f'C{row_num}', record['단위'], fmt_border_c)
            worksheet.write(f'D{row_num}', record['수량'], fmt_money)
            worksheet.write(f'E{row_num}', record['단가'], fmt_money)
            worksheet.write(f'F{row_num}', record['공급가액'], fmt_money)
            worksheet.write(f'G{row_num}', record['세액'], fmt_money)
            worksheet.write(f'H{row_num}', record['합계금액'], fmt_money)

        # --- 합계 ---
        start_row = 14
        total_row = row_num + 1
        worksheet.merge_range(f'A{total_row}:D{total_row}', '합계', fmt_total)
        # [개선사항 3] 합계금액 0 문제 해결 (SUM 범위 동적 계산)
        worksheet.write_formula(f'E{total_row}', f"=SUM(E{start_row}:E{row_num})", fmt_total)
        worksheet.write_formula(f'F{total_row}', f"=SUM(F{start_row}:F{row_num})", fmt_total)
        worksheet.write_formula(f'G{total_row}', f"=SUM(G{start_row}:G{row_num})", fmt_total)
        worksheet.write_formula(f'H{total_row}', f"=SUM(H{start_row}:H{row_num})", fmt_total)

    output.seek(0)
    return output

# [개선사항 3] 기간별 거래명세서 서식 전면 개선
def make_multi_date_item_statement_excel(orders_df: pd.DataFrame, store_info: pd.Series, dt_from: date, dt_to: date) -> BytesIO:
    output = BytesIO()
    if orders_df.empty: return output

    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        workbook = writer.book
        worksheet = workbook.add_worksheet("기간별_품목거래명세서")
        
        fmt_title = workbook.add_format({'bold': True, 'font_size': 20, 'align': 'center', 'valign': 'vcenter', 'border': 1})
        fmt_h2 = workbook.add_format({'bold': True, 'font_size': 11, 'bg_color': '#F2F2F2', 'align': 'center', 'valign': 'vcenter', 'border': 1})
        fmt_info = workbook.add_format({'font_size': 10, 'border': 1, 'align': 'left', 'valign': 'vcenter'})
        fmt_header = workbook.add_format({'bold': True, 'bg_color': '#DDEBF7', 'border': 1, 'align': 'center', 'valign': 'vcenter'})
        fmt_money = workbook.add_format({'num_format': '#,##0', 'border': 1})
        fmt_border = workbook.add_format({'border': 1})
        fmt_border_c = workbook.add_format({'border': 1, 'align': 'center'})
        fmt_date_header = workbook.add_format({'bold': True, 'font_size': 12, 'bg_color': '#FFF2CC', 'border': 1})
        fmt_daily_total = workbook.add_format({'bold': True, 'bg_color': '#FFF2CC', 'border': 1, 'num_format': '#,##0'})
        fmt_grand_total = workbook.add_format({'bold': True, 'font_size': 13, 'bg_color': '#DDEBF7', 'border': 1, 'num_format': '#,##0'})

        worksheet.set_column('A:A', 5); worksheet.set_column('B:B', 20); worksheet.set_column('C:C', 18)
        worksheet.set_column('D:E', 8); worksheet.set_column('F:I', 14)

        worksheet.merge_range('A1:I2', '기간별 품목 거래명세서', fmt_title)
        worksheet.write('F5', '거래기간', fmt_h2)
        worksheet.merge_range('G5:I5', f"{dt_from.strftime('%Y-%m-%d')} ~ {dt_to.strftime('%Y-%m-%d')}", fmt_info)
        
        # 공급자/받는자 정보 (단일 명세서와 동일)
        # ... (생략, 필요시 단일 명세서 코드 복사)
        
        headers = ["No", "품목명", "발주번호", "단위", "수량", "단가", "공급가액", "세액", "합계금액"]
        
        orders_df['주문일'] = pd.to_datetime(orders_df['주문일시']).dt.date
        
        row_num = 8
        grand_total_supply = 0
        grand_total_tax = 0
        grand_total_amount = 0

        for order_date, group in orders_df.sort_values(by=['주문일', '발주번호']).groupby('주문일'):
            worksheet.merge_range(f'A{row_num}:I{row_num}', f"▶ 거래일자: {order_date.strftime('%Y-%m-%d')}", fmt_date_header)
            row_num += 1
            worksheet.write_row(f'A{row_num}', headers, fmt_header)
            
            start_row_daily = row_num + 1
            group = group.reset_index(drop=True)
            for i, record in group.iterrows():
                row_num += 1
                worksheet.write(f'A{row_num}', i + 1, fmt_border_c)
                worksheet.write(f'B{row_num}', record['품목명'], fmt_border)
                worksheet.write(f'C{row_num}', record['발주번호'], fmt_border_c)
                worksheet.write(f'D{row_num}', record['단위'], fmt_border_c)
                worksheet.write(f'E{row_num}', record['수량'], fmt_money)
                worksheet.write(f'F{row_num}', record['단가'], fmt_money)
                worksheet.write(f'G{row_num}', record['공급가액'], fmt_money)
                worksheet.write(f'H{row_num}', record['세액'], fmt_money)
                worksheet.write(f'I{row_num}', record['합계금액'], fmt_money)

            # 일별 합계
            daily_total_row = row_num + 1
            worksheet.merge_range(f'A{daily_total_row}:F{daily_total_row}', '일계', fmt_daily_total)
            worksheet.write_formula(f'G{daily_total_row}', f"=SUM(G{start_row_daily}:G{row_num})", fmt_daily_total)
            worksheet.write_formula(f'H{daily_total_row}', f"=SUM(H{start_row_daily}:H{row_num})", fmt_daily_total)
            worksheet.write_formula(f'I{daily_total_row}', f"=SUM(I{start_row_daily}:I{row_num})", fmt_daily_total)
            row_num += 2
            
            grand_total_supply += group['공급가액'].sum()
            grand_total_tax += group['세액'].sum()
            grand_total_amount += group['합계금액'].sum()

        # 총 합계
        grand_total_row = row_num + 1
        worksheet.merge_range(f'A{grand_total_row}:F{grand_total_row}', '총계', fmt_grand_total)
        worksheet.write(f'G{grand_total_row}', grand_total_supply, fmt_grand_total)
        worksheet.write(f'H{grand_total_row}', grand_total_tax, fmt_grand_total)
        worksheet.write(f'I{grand_total_row}', grand_total_amount, fmt_grand_total)

    output.seek(0)
    return output

def make_full_transaction_statement_excel(df_transactions: pd.DataFrame, store_info: pd.Series) -> BytesIO:
    output = BytesIO()
    if df_transactions.empty: return output

    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        workbook = writer.book
        worksheet = workbook.add_worksheet(f"{store_info['지점명']} 금전거래")

        fmt_title = workbook.add_format({'bold': True, 'font_size': 18, 'align': 'center', 'valign': 'vcenter'})
        fmt_header = workbook.add_format({'bold': True, 'bg_color': '#F2F2F2', 'border': 1, 'align': 'center', 'valign': 'vcenter'})
        fmt_money = workbook.add_format({'num_format': '#,##0', 'border': 1})
        fmt_border_c = workbook.add_format({'border': 1, 'align': 'center', 'valign': 'vcenter'})
        fmt_border_l = workbook.add_format({'border': 1, 'align': 'left', 'valign': 'vcenter'})

        worksheet.set_paper(9); worksheet.set_landscape(); worksheet.fit_to_pages(1, 1)
        worksheet.set_margins(left=0.5, right=0.5, top=0.5, bottom=0.5)
        col_widths = {'A': 20, 'B': 12, 'C': 35, 'D': 15, 'E': 15, 'F': 15}
        for col, width in col_widths.items(): worksheet.set_column(f'{col}:{col}', width)

        worksheet.merge_range('A1:F1', f"{store_info['지점명']} 금전 거래 상세 명세서", fmt_title)
        headers = ['일시', '구분', '내용', '금액', '선충전 잔액', '사용 여신액']
        worksheet.write_row('A3', headers, fmt_header)
        
        df_sorted = df_transactions.sort_values(by='일시', ascending=True).reset_index(drop=True)
        
        for idx, row in df_sorted.iterrows():
            row_num = idx + 4
            worksheet.write(f'A{row_num}', row.get('일시', ''), fmt_border_c)
            worksheet.write(f'B{row_num}', row.get('구분', ''), fmt_border_c)
            worksheet.write(f'C{row_num}', row.get('내용', ''), fmt_border_l)
            worksheet.write(f'D{row_num}', row.get('금액', 0), fmt_money)
            worksheet.write(f'E{row_num}', row.get('처리후선충전잔액', 0), fmt_money)
            worksheet.write(f'F{row_num}', row.get('처리후사용여신액', 0), fmt_money)
    
    output.seek(0)
    return output
# --- 신규 엑셀 생성 함수 ---
def make_inventory_report_excel(df_report: pd.DataFrame, report_type: str, dt_from: date, dt_to: date) -> BytesIO:
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        workbook = writer.book
        worksheet = workbook.add_worksheet(report_type)
        
        fmt_h1 = workbook.add_format({'bold': True, 'font_size': 18, 'align': 'center', 'valign': 'vcenter'})
        fmt_header = workbook.add_format({'bold': True, 'bg_color': '#F2F2F2', 'border': 1, 'align': 'center'})
        
        worksheet.merge_range(0, 0, 0, len(df_report.columns) - 1, f"{report_type} ({dt_from} ~ {dt_to})", fmt_h1)
        
        for col_num, value in enumerate(df_report.columns.values):
            worksheet.write(2, col_num, value, fmt_header)
        
        df_report.to_excel(writer, sheet_name=report_type, index=False, startrow=3, header=False)
        worksheet.set_column(0, len(df_report.columns), 15)
        
    output.seek(0)
    return output

# [개선사항 8] 매출 정산표 서식 개선
def make_sales_summary_excel(daily_pivot: pd.DataFrame, monthly_pivot: pd.DataFrame, summary_data: dict, filter_info: dict) -> BytesIO:
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        workbook = writer.book
        # --- 서식 정의 ---
        fmt_h1 = workbook.add_format({'bold': True, 'font_size': 18, 'align': 'center', 'valign': 'vcenter'})
        fmt_h2 = workbook.add_format({'bold': True, 'font_size': 12, 'bg_color': '#F2F2F2'})
        fmt_money = workbook.add_format({'num_format': '#,##0'})
        fmt_header = workbook.add_format({'bold': True, 'bg_color': '#DDEBF7', 'border': 1, 'align': 'center'})
        fmt_pivot_money = workbook.add_format({'num_format': '#,##0', 'border': 1})
        
        # --- 1. 종합 분석 시트 ---
        ws_summary = workbook.add_worksheet('종합 분석')
        ws_summary.set_column('A:A', 20); ws_summary.set_column('B:B', 25)
        ws_summary.merge_range('A1:B1', '매출 종합 분석', fmt_h1)
        
        ws_summary.write('A3', '조회 조건', fmt_h2)
        ws_summary.write('A4', '조회 기간'); ws_summary.write('B4', filter_info['period'])
        ws_summary.write('A5', '조회 지점'); ws_summary.write('B5', filter_info['store'])
        
        ws_summary.write('A7', '주요 지표', fmt_h2)
        ws_summary.write('A8', '총 매출 (VAT 포함)'); ws_summary.write('B8', summary_data['total_sales'], fmt_money)
        ws_summary.write('A9', '공급가액'); ws_summary.write('B9', summary_data['total_supply'], fmt_money)
        ws_summary.write('A10', '부가세액'); ws_summary.write('B10', summary_data['total_tax'], fmt_money)
        ws_summary.write('A11', '총 발주 건수'); ws_summary.write('B11', summary_data['total_orders'])

        # --- 2. 일별/월별 매출 시트 ---
        for name, pivot_df in [('일별매출현황', daily_pivot), ('월별매출현황', monthly_pivot)]:
            pivot_df.to_excel(writer, sheet_name=name, index=True, startrow=2)
            worksheet = writer.sheets[name]
            worksheet.set_zoom(90)
            df_for_format = pivot_df.reset_index()
            worksheet.merge_range(0, 0, 0, len(df_for_format.columns) - 1, f"거래처별 {name}", fmt_h1)
            for col_num, value in enumerate(df_for_format.columns.values):
                worksheet.write(2, col_num, value, fmt_header)
            worksheet.set_column(0, len(df_for_format.columns), 14)
            worksheet.conditional_format(3, 1, len(df_for_format) + 2, len(df_for_format.columns), {'type': 'no_blanks', 'format': fmt_pivot_money})

    output.seek(0)
    return output

# =============================================================================
# 5) 유틸리티 함수 (재고 관련 추가)
# =============================================================================
def init_session_state():
    defaults = {
        "cart": pd.DataFrame(columns=CART_COLUMNS), 
        "store_editor_ver": 0, 
        "production_cart": pd.DataFrame(),
        "production_date_to_log": date.today(), # 👈 이 줄 추가
        "production_editor_ver": 0,
        "success_message": "",
        "error_message": "",
        "warning_message": "",
        "store_orders_selection": {},
        "admin_orders_selection": {}
    }
    for key, value in defaults.items():
        if key not in st.session_state: st.session_state[key] = value

def coerce_cart_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in CART_COLUMNS:
        if col not in out.columns: out[col] = 0 if '금액' in col or '단가' in col or '수량' in col else ""
    out["수량"] = pd.to_numeric(out["수량"], errors="coerce").fillna(0).astype(int)
    out["단가"] = pd.to_numeric(out["단가"], errors="coerce").fillna(0).astype(int)
    out["단가(VAT포함)"] = pd.to_numeric(out["단가(VAT포함)"], errors="coerce").fillna(0).astype(int)
    out["합계금액(VAT포함)"] = out["단가(VAT포함)"] * out["수량"]
    return out[CART_COLUMNS]

def add_to_cart(rows_df: pd.DataFrame, master_df: pd.DataFrame):
    add_with_qty = rows_df[rows_df["수량"] > 0].copy()
    if add_with_qty.empty: return

    add_merged = pd.merge(add_with_qty, master_df[['품목코드', '과세구분']], on='품목코드', how='left')
    add_merged['단가(VAT포함)'] = add_merged.apply(get_vat_inclusive_price, axis=1)
    
    cart = st.session_state.cart.copy()
    
    merged = pd.concat([cart, add_merged]).groupby("품목코드", as_index=False).agg({
        "품목명": "last", 
        "단위": "last", 
        "단가": "last", 
        "단가(VAT포함)": "last",
        "수량": "sum"
    })
    
    # [개선사항 1] 품목별 합계금액 계산
    merged["합계금액(VAT포함)"] = merged["단가(VAT포함)"] * merged["수량"]
    st.session_state.cart = merged[CART_COLUMNS]

# --- 신규 재고 처리 함수 ---
def update_inventory(items_to_update: pd.DataFrame, change_type: str, handler: str, ref_id: str = "", reason: str = ""):
    if items_to_update.empty:
        return True
    
    current_inventory_df = load_data(SHEET_NAME_CURRENT_INVENTORY, CURRENT_INVENTORY_COLUMNS)
    log_rows = []
    
    for _, item in items_to_update.iterrows():
        item_code = item['품목코드']
        item_name = item['품목명']
        quantity_change = int(item['수량변경'])
        
        # 현재고 조회
        current_stock_series = current_inventory_df[current_inventory_df['품목코드'] == item_code]
        if current_stock_series.empty:
            current_stock = 0
            # 현재고 시트에 없는 품목이면 새로 추가
            new_inv_row = pd.DataFrame([{'품목코드': item_code, '품목명': item_name, '현재고수량': 0, '최종갱신일시': ''}])
            current_inventory_df = pd.concat([current_inventory_df, new_inv_row], ignore_index=True)
        else:
            current_stock = int(current_stock_series.iloc[0]['현재고수량'])
            
        new_stock = current_stock + quantity_change
        
        # 재고로그 기록 준비
        log_rows.append({
            "로그일시": now_kst_str(),
            "품목코드": item_code,
            "품목명": item_name,
            "구분": change_type,
            "수량변경": quantity_change,
            "처리후재고": new_stock,
            "관련번호": ref_id,
            "처리자": handler,
            "사유": reason
        })
        
        # 현재고 데이터프레임 업데이트
        current_inventory_df.loc[current_inventory_df['품목코드'] == item_code, '현재고수량'] = new_stock
        current_inventory_df.loc[current_inventory_df['품목코드'] == item_code, '최종갱신일시'] = now_kst_str()

    # 시트에 한 번에 저장
    if append_rows_to_sheet(SHEET_NAME_INVENTORY_LOG, log_rows, INVENTORY_LOG_COLUMNS):
        if save_df_to_sheet(SHEET_NAME_CURRENT_INVENTORY, current_inventory_df):
            return True
    return False

# =============================================================================
# 6) 지점 페이지
# =============================================================================
def page_store_register_confirm(master_df: pd.DataFrame, balance_info: pd.Series):
    st.subheader("🛒 발주 요청")
    user = st.session_state.auth
    
    prepaid_balance = int(balance_info.get('선충전잔액', 0))
    credit_limit = int(balance_info.get('여신한도', 0))
    used_credit = int(balance_info.get('사용여신액', 0))
    available_credit = credit_limit - used_credit
    
    with st.container(border=True):
        c1, c2 = st.columns(2)
        c1.metric("선충전 잔액", f"{prepaid_balance:,.0f}원")
        c2.metric("사용 가능 여신", f"{available_credit:,.0f}원", delta=f"한도: {credit_limit:,.0f}원", delta_color="off")
    if credit_limit > 0 and (available_credit / credit_limit) < 0.2 :
        st.warning("⚠️ 여신 한도가 20% 미만으로 남았습니다.")
    v_spacer(10)
    
    with st.container(border=True):
        st.markdown("##### 🧾 발주 수량 입력")
        l, r = st.columns([2, 1])
        keyword = l.text_input("품목 검색(이름/코드)", placeholder="오이, P001 등", key="store_reg_keyword")
        cat_opt = ["(전체)"] + sorted(master_df["분류"].dropna().unique().tolist())
        cat_sel = r.selectbox("분류(선택)", cat_opt, key="store_reg_category")
        
        df_view = master_df[master_df['활성'].astype(str).str.lower() == 'true'].copy()
        if keyword: df_view = df_view[df_view.apply(lambda row: keyword.strip().lower() in str(row["품목명"]).lower() or keyword.strip().lower() in str(row["품목코드"]).lower(), axis=1)]
        if cat_sel != "(전체)": df_view = df_view[df_view["분류"] == cat_sel]
        
        df_view['단가(VAT포함)'] = df_view.apply(get_vat_inclusive_price, axis=1)

        with st.form(key="add_to_cart_form"):
            df_edit = df_view.copy()
            df_edit["수량"] = 0
            
            # [개선사항 10] 새로고침 문제 완화 (상태유지 키)
            edited_disp = st.data_editor(
                df_edit[["품목코드", "품목명", "단위", "단가", "단가(VAT포함)", "수량"]], 
                key=f"editor_v{st.session_state.store_editor_ver}", 
                hide_index=True, 
                disabled=["품목코드", "품목명", "단위", "단가", "단가(VAT포함)"], 
                use_container_width=True, 
                column_config={
                    "단가": st.column_config.NumberColumn(format="%d원"), 
                    "단가(VAT포함)": st.column_config.NumberColumn(format="%d원"),
                    "수량": st.column_config.NumberColumn(min_value=0)
                }
            )
            
            if st.form_submit_button("장바구니 추가", use_container_width=True, type="primary"):
                items_to_add = coerce_cart_df(edited_disp)
                if not items_to_add[items_to_add["수량"] > 0].empty:
                    add_to_cart(items_to_add, master_df)
                    st.session_state.store_editor_ver += 1
                    st.session_state.success_message = "선택한 품목이 장바구니에 추가되었습니다."
                st.rerun()

    v_spacer(16)
    
    with st.container(border=True):
        st.markdown("##### 🧺 장바구니 및 최종 확인")
        cart_now = coerce_cart_df(st.session_state.cart)
        
        if cart_now.empty:
            st.info("장바구니가 비어 있습니다.")
        else:
            # [개선사항 1] 장바구니 UI 개선
            st.dataframe(
                cart_now[["품목코드", "품목명", "단위", "단가(VAT포함)", "수량", "합계금액(VAT포함)"]], 
                hide_index=True, 
                use_container_width=True,
                column_config={
                    "단가(VAT포함)": st.column_config.NumberColumn(format="%d원"), 
                    "합계금액(VAT포함)": st.column_config.NumberColumn(format="%d원")
                }
            )
            
            cart_with_master = pd.merge(cart_now, master_df[['품목코드', '과세구분']], on='품목코드', how='left')
            cart_with_master['공급가액'] = cart_with_master['단가'] * cart_with_master['수량']
            cart_with_master['세액'] = cart_with_master.apply(lambda r: math.ceil(r['공급가액'] * 0.1) if r['과세구분'] == '과세' else 0, axis=1)
            cart_with_master['합계금액_final'] = cart_with_master['공급가액'] + cart_with_master['세액']
            
            total_final_amount_sum = int(cart_with_master['합계금액_final'].sum())
            st.markdown(f"<h4 style='text-align: right;'>최종 합계금액 (VAT 포함): {total_final_amount_sum:,.0f}원</h4>", unsafe_allow_html=True)

            with st.form("submit_form"):
                memo = st.text_area("요청 사항(선택)", height=80, placeholder="예: 2025-12-25 에 출고 부탁드립니다")
                
                can_prepaid = prepaid_balance >= total_final_amount_sum
                can_credit = available_credit >= total_final_amount_sum
                payment_options = []
                if can_prepaid: payment_options.append("선충전 잔액 결제")
                if can_credit: payment_options.append("여신 결제")

                if not payment_options:
                    st.error(f"결제 가능한 수단이 없습니다. 잔액 또는 여신 한도를 확인해주세요.")
                
                payment_method = st.radio("결제 방식 선택", payment_options, key="payment_method", horizontal=True) if payment_options else None
                
                c1, c2 = st.columns(2)
                with c1:
                    if st.form_submit_button("📦 발주 제출 및 결제", type="primary", use_container_width=True, disabled=not payment_method):
                        order_id = make_order_id(user["user_id"])
                        rows = []
                        
                        for _, r in cart_with_master.iterrows():
                            rows.append({"주문일시": now_kst_str(), "발주번호": order_id, "지점ID": user["user_id"], "지점명": user["name"], "품목코드": r["품목코드"], "품목명": r["품목명"], "단위": r["단위"], "수량": r["수량"], "단가": r["단가"], "공급가액": r['공급가액'], "세액": r['세액'], "합계금액": r['합계금액_final'], "비고": memo, "상태": "요청", "처리자": "", "처리일시": "", "반려사유":""})
                        
                        if append_rows_to_sheet(SHEET_NAME_ORDERS, rows, ORDERS_COLUMNS):
                            new_balance, new_used_credit, trans_desc = prepaid_balance, used_credit, ""
                            if payment_method == "선충전 잔액 결제":
                                new_balance -= total_final_amount_sum
                                trans_desc = "선충전결제"
                            else: # 여신 결제
                                new_used_credit += total_final_amount_sum
                                trans_desc = "여신결제"
                            
                            update_balance_sheet(user["user_id"], {"선충전잔액": new_balance, "사용여신액": new_used_credit})
                            
                            transaction_record = {
                                "일시": now_kst_str(), "지점ID": user["user_id"], "지점명": user["name"],
                                "구분": trans_desc, "내용": f"{cart_now.iloc[0]['품목명']} 등 {len(cart_now)}건 발주",
                                "금액": -total_final_amount_sum, "처리후선충전잔액": new_balance,
                                "처리후사용여신액": new_used_credit, "관련발주번호": order_id, "처리자": user["name"]
                            }
                            append_rows_to_sheet(SHEET_NAME_TRANSACTIONS, [transaction_record], TRANSACTIONS_COLUMNS)
                            
                            st.session_state.success_message = "발주 및 결제가 성공적으로 완료되었습니다."
                            st.session_state.cart = pd.DataFrame(columns=CART_COLUMNS)
                            st.rerun()
                        else:
                            st.session_state.error_message = "발주 제출 중 오류가 발생했습니다."
                with c2:
                    if st.form_submit_button("🗑️ 장바구니 비우기", use_container_width=True):
                        st.session_state.cart = pd.DataFrame(columns=CART_COLUMNS)
                        st.session_state.success_message = "장바구니를 비웠습니다."
                        st.rerun()

def page_store_balance(charge_requests_df: pd.DataFrame, balance_info: pd.Series):
    st.subheader("💰 결제 관리")
    user = st.session_state.auth

    prepaid_balance = int(balance_info.get('선충전잔액', 0))
    credit_limit = int(balance_info.get('여신한도', 0))
    used_credit = int(balance_info.get('사용여신액', 0))
    available_credit = credit_limit - used_credit
    
    with st.container(border=True):
        c1, c2, c3 = st.columns(3)
        c1.metric("선충전 잔액", f"{prepaid_balance:,.0f}원")
        c2.metric("사용 여신액", f"{used_credit:,.0f}원")
        c3.metric("사용 가능 여신", f"{available_credit:,.0f}원", delta=f"한도: {credit_limit:,.0f}원", delta_color="off")
        if credit_limit > 0 and used_credit > 0 and (available_credit / credit_limit) < 0.2:
            st.warning("⚠️ 여신 한도가 20% 미만으로 남았습니다.")
    
    st.info("**입금 계좌: OOO은행 123-456-789 (주)산카쿠**\n\n위 계좌로 입금하신 후, 아래 양식을 작성하여 '알림 보내기' 버튼을 눌러주세요.")
    with st.form("charge_request_form", border=True):
        st.markdown("##### 입금 완료 알림 보내기")
        c1, c2, c3 = st.columns(3)
        depositor_name = c1.text_input("입금자명")
        charge_amount = c2.number_input("입금액", min_value=1000, step=1000, format="%d")
        charge_type = c3.radio("종류", ["선충전", "여신상환"], horizontal=True)
        
        if st.form_submit_button("알림 보내기", type="primary"):
            if depositor_name and charge_amount > 0:
                new_request = {
                    "요청일시": now_kst_str(), "지점ID": user["user_id"], "지점명": user["name"],
                    "입금자명": depositor_name, "입금액": charge_amount, "종류": charge_type, "상태": "요청", "처리사유": ""
                }
                if append_rows_to_sheet(SHEET_NAME_CHARGE_REQ, [new_request], CHARGE_REQ_COLUMNS):
                    st.session_state.success_message = "관리자에게 입금 완료 알림을 보냈습니다. 확인 후 처리됩니다."
                else: st.session_state.error_message = "알림 전송에 실패했습니다."
            else: st.warning("입금자명과 입금액을 모두 입력해주세요.")
            st.rerun()
            
    st.markdown("---")
    st.markdown("##### 나의 충전/상환 요청 현황")
    my_requests = charge_requests_df[charge_requests_df['지점ID'] == user['user_id']]
    st.dataframe(my_requests, use_container_width=True, hide_index=True)

def page_store_orders_change(store_info_df: pd.DataFrame, master_df: pd.DataFrame):
    st.subheader("🧾 발주 조회")
    
    df_all_orders = load_data(SHEET_NAME_ORDERS, ORDERS_COLUMNS)
    df_all_transactions = load_data(SHEET_NAME_TRANSACTIONS, TRANSACTIONS_COLUMNS)
    df_balance = load_data(SHEET_NAME_BALANCE, BALANCE_COLUMNS)
    user = st.session_state.auth
    
    df_user = df_all_orders[df_all_orders["지점ID"] == user["user_id"]]
    if df_user.empty:
        st.info("발주 데이터가 없습니다.")
        return
    
    c1, c2, c3 = st.columns(3)
    dt_from = c1.date_input("조회 시작일", date.today() - timedelta(days=30), key="store_orders_from")
    dt_to = c2.date_input("조회 종료일", date.today(), key="store_orders_to")
    order_id_search = c3.text_input("발주번호로 검색", key="store_orders_search", placeholder="전체 또는 일부 입력")
    
    df_filtered = df_user.copy()
    if order_id_search:
        df_filtered = df_filtered[df_filtered["발주번호"].str.contains(order_id_search, na=False)]
    else:
        df_filtered['주문일시_dt'] = pd.to_datetime(df_filtered['주문일시']).dt.date
        df_filtered = df_filtered[(df_filtered['주문일시_dt'] >= dt_from) & (df_filtered['주문일시_dt'] <= dt_to)]
    
    orders = df_filtered.groupby("발주번호").agg(
        주문일시=("주문일시", "first"), 
        건수=("품목코드", "count"), 
        합계금액=("합계금액", "sum"), 
        상태=("상태", "first"), 
        처리일시=("처리일시", "first"),
        반려사유=("반려사유", "first")
    ).reset_index().sort_values("주문일시", ascending=False)
    
    pending = orders[orders["상태"] == "요청"].copy()
    shipped = orders[orders["상태"].isin(["승인", "출고완료"])].copy()
    rejected = orders[orders["상태"] == "반려"].copy()

    tab1, tab2, tab3 = st.tabs([f"요청 ({len(pending)}건)", f"승인/출고 ({len(shipped)}건)", f"반려 ({len(rejected)}건)"])
    
    with tab1:
        pending_display = pending.copy()
        pending_display.insert(0, '선택', pending['발주번호'].apply(lambda x: st.session_state.store_orders_selection.get(x, False)))
        edited_pending = st.data_editor(
            pending_display[['선택', '주문일시', '발주번호', '건수', '합계금액', '상태']], 
            hide_index=True, 
            use_container_width=True, 
            key="pending_editor", 
            disabled=pending.columns
        )
        for _, row in edited_pending.iterrows():
            st.session_state.store_orders_selection[row['발주번호']] = row['선택']
        
        selected_to_cancel = [oid for oid, selected in st.session_state.store_orders_selection.items() if selected and oid in pending['발주번호'].values]
        
        if st.button("선택한 발주 요청 취소하기", disabled=not selected_to_cancel, type="primary"):
            with st.spinner("발주 취소 및 환불 처리 중..."):
                for order_id in selected_to_cancel:
                    original_transaction = df_all_transactions[df_all_transactions['관련발주번호'] == order_id]
                    if not original_transaction.empty:
                        trans_info = original_transaction.iloc[0]
                        refund_amount = abs(int(trans_info['금액']))
                        
                        balance_info = df_balance[df_balance['지점ID'] == user['user_id']].iloc[0]
                        new_prepaid, new_used_credit = int(balance_info['선충전잔액']), int(balance_info['사용여신액'])

                        credit_refund = min(refund_amount, new_used_credit)
                        new_used_credit -= credit_refund
                        new_prepaid += (refund_amount - credit_refund)

                        update_balance_sheet(user["user_id"], {"선충전잔액": new_prepaid, "사용여신액": new_used_credit})
                        
                        refund_record = {
                            "일시": now_kst_str(), "지점ID": user["user_id"], "지점명": user["name"],
                            "구분": "발주취소", "내용": f"발주번호 {order_id} 취소 환불",
                            "금액": refund_amount, "처리후선충전잔액": new_prepaid,
                            "처리후사용여신액": new_used_credit, "관련발주번호": order_id, "처리자": user["name"]
                        }
                        append_rows_to_sheet(SHEET_NAME_TRANSACTIONS, [refund_record], TRANSACTIONS_COLUMNS)
                
                update_order_status(selected_to_cancel, "취소", user["name"])
                st.session_state.success_message = f"{len(selected_to_cancel)}건의 발주가 취소되고 환불 처리되었습니다."
                st.session_state.store_orders_selection = {}
                st.rerun()

    with tab2:
        shipped_display = shipped.copy()
        shipped_display.insert(0, '선택', [st.session_state.store_orders_selection.get(x, False) for x in shipped['발주번호']])
        edited_shipped = st.data_editor(shipped_display[['선택', '주문일시', '발주번호', '건수', '합계금액', '상태', '처리일시']], hide_index=True, use_container_width=True, key="shipped_editor", disabled=shipped.columns)
        for _, row in edited_shipped.iterrows():
            st.session_state.store_orders_selection[row['발주번호']] = row['선택']
        
    with tab3:
        rejected_display = rejected.copy()
        rejected_display.insert(0, '선택', [st.session_state.store_orders_selection.get(x, False) for x in rejected['발주번호']])
        edited_rejected = st.data_editor(rejected_display[['선택', '주문일시', '발주번호', '건수', '합계금액', '상태', '반려사유']], hide_index=True, use_container_width=True, key="rejected_editor", disabled=rejected.columns)
        for _, row in edited_rejected.iterrows():
            st.session_state.store_orders_selection[row['발주번호']] = row['선택']

    v_spacer(16)
    with st.container(border=True):
        st.markdown("##### 📄 발주 품목 상세 조회")
        selected_ids = [k for k, v in st.session_state.store_orders_selection.items() if v]
        if len(selected_ids) == 1:
            target_id = selected_ids[0]
            target_df = df_user[df_user["발주번호"] == target_id]
            total_amount = target_df['합계금액'].sum()
            
            # [개선사항 2] 상세조회 정보 강화
            st.markdown(f"**선택된 발주번호:** `{target_id}` / **총 합계금액(VAT포함):** `{total_amount:,.0f}원`")
            
            # VAT 포함 단가 계산을 위해 master_df와 merge
            display_df = pd.merge(target_df, master_df[['품목코드', '과세구분']], on='품목코드', how='left')
            display_df['단가(VAT포함)'] = display_df.apply(get_vat_inclusive_price, axis=1)
            display_df.rename(columns={'합계금액': '합계금액(VAT포함)'}, inplace=True)
            
            st.dataframe(display_df[["품목코드", "품목명", "단위", "수량", "단가(VAT포함)", "합계금액(VAT포함)"]], hide_index=True, use_container_width=True)

            if target_df.iloc[0]['상태'] in ["승인", "출고완료"]:
                my_store_info = store_info_df[store_info_df['지점ID'] == user['user_id']].iloc[0]
                buf = make_item_transaction_statement_excel(target_df, my_store_info)
                st.download_button("📄 품목 거래명세서 다운로드", data=buf, file_name=f"품목거래명세서_{user['name']}_{target_id}.xlsx", mime="application/vnd.ms-excel", use_container_width=True, type="primary")

        else:
            st.info("상세 내용을 보려면 위 목록에서 발주를 **하나만** 선택하세요.")

def page_store_documents(store_info_df: pd.DataFrame):
    st.subheader("📑 증빙서류 다운로드")
    user = st.session_state.auth
    
    c1, c2, c3, _ = st.columns(4)
    dt_from = c1.date_input("조회 시작일", date.today() - timedelta(days=30), key="store_doc_from")
    dt_to = c2.date_input("조회 종료일", date.today(), key="store_doc_to")
    
    # [개선사항 4] 세금계산서 삭제
    doc_type = c3.selectbox("서류 종류", ["금전 거래내역서", "품목 거래명세서"])
    
    my_store_info = store_info_df[store_info_df['지점ID'] == user['user_id']].iloc[0]

    if doc_type == "금전 거래내역서":
        transactions_df = load_data(SHEET_NAME_TRANSACTIONS, TRANSACTIONS_COLUMNS)
        my_transactions = transactions_df[transactions_df['지점ID'] == user['user_id']]
        if my_transactions.empty: st.info("거래 내역이 없습니다."); return
        
        my_transactions['일시_dt'] = pd.to_datetime(my_transactions['일시']).dt.date
        mask = (my_transactions['일시_dt'] >= dt_from) & (my_transactions['일시_dt'] <= dt_to)
        dfv = my_transactions[mask].copy()
        if dfv.empty: st.warning("해당 기간의 거래 내역이 없습니다."); return
        st.dataframe(dfv.drop(columns=['일시_dt']), use_container_width=True, hide_index=True)
        
        buf = make_full_transaction_statement_excel(dfv, my_store_info)
        st.download_button("엑셀 다운로드", data=buf, file_name=f"금전거래명세서_{user['name']}_{dt_from}_to_{dt_to}.xlsx", mime="application/vnd.ms-excel", use_container_width=True, type="primary")
    
    elif doc_type == "품목 거래명세서":
        orders_df = load_data(SHEET_NAME_ORDERS, ORDERS_COLUMNS)
        my_orders = orders_df[(orders_df['지점ID'] == user['user_id']) & (orders_df['상태'].isin(['승인', '출고완료']))]
        
        if my_orders.empty:
            st.warning("승인/출고된 발주 내역이 없습니다.")
            return

        my_orders['주문일시_dt'] = pd.to_datetime(my_orders['주문일시']).dt.date
        filtered_orders = my_orders[my_orders['주문일시_dt'].between(dt_from, dt_to)]
        
        if filtered_orders.empty:
            st.warning("선택한 기간 내에 승인/출고된 발주 내역이 없습니다.")
            return

        # [개선사항 4] 다운로드 방식 선택 UI
        dl_col1, dl_col2 = st.columns(2)
        with dl_col1:
            buf = make_multi_date_item_statement_excel(filtered_orders, my_store_info, dt_from, dt_to)
            st.download_button(f"'{dt_from}~{dt_to}' 기간 전체 다운로드", data=buf, file_name=f"기간별_거래명세서_{user['name']}.xlsx", mime="application/vnd.ms-excel", use_container_width=True, type="primary")

        with dl_col2:
            order_options = filtered_orders['발주번호'].unique().tolist()
            selected_order_id = st.selectbox("개별 발주번호 선택 다운로드", ["-"] + order_options)
            if selected_order_id != "-":
                order_to_print = filtered_orders[filtered_orders['발주번호'] == selected_order_id]
                buf_single = make_item_transaction_statement_excel(order_to_print, my_store_info)
                st.download_button(f"'{selected_order_id}' 다운로드", data=buf_single, file_name=f"거래명세서_{user['name']}_{selected_order_id}.xlsx", mime="application/vnd.ms-excel", use_container_width=True, type="primary")

def page_store_master_view(master_df: pd.DataFrame):
    st.subheader("🏷️ 품목 단가 조회")
    l, r = st.columns([2, 1])
    keyword = l.text_input("품목 검색(이름/코드)", placeholder="오이, P001 등", key="store_master_keyword")
    cat_opt = ["(전체)"] + sorted(master_df["분류"].dropna().unique().tolist())
    cat_sel = r.selectbox("분류(선택)", cat_opt, key="store_master_category")
    
    df_view = master_df[master_df['활성'].astype(str).str.lower() == 'true'].copy()
    if keyword: df_view = df_view[df_view.apply(lambda row: keyword.strip().lower() in str(row["품목명"]).lower() or keyword.strip().lower() in str(row["품목코드"]).lower(), axis=1)]
    if cat_sel != "(전체)": df_view = df_view[df_view["분류"] == cat_sel]

    df_view['단가(VAT포함)'] = df_view.apply(get_vat_inclusive_price, axis=1)
    df_view.rename(columns={'단가': '단가(원)'}, inplace=True)
    
    st.dataframe(df_view[['품목코드', '분류', '품목명', '단위', '단가(원)', '단가(VAT포함)']], use_container_width=True, hide_index=True)

# =============================================================================
# 7) 관리자 페이지 (대폭 수정 및 신설)
# =============================================================================

### 🏭 7-1) 신규: 일일 생산 보고
def page_admin_daily_production(master_df: pd.DataFrame):
    st.subheader("📝 일일 생산 보고")
    user = st.session_state.auth
    
    with st.container(border=True):
        st.markdown("##### 📦 생산 수량 입력")
        
        with st.form(key="add_production_form"):
            production_date = st.date_input("생산일자")
            change_reason = "" # 사유 초기화

            # --- [수정] 생산일자 변경 시 사유 입력 로직 ---
            if production_date != date.today():
                change_reason = st.text_input("생산일자 변경 사유 (필수)", placeholder="예: 어제 누락분 입력")

            df_producible = master_df[master_df['활성'].astype(str).str.lower() == 'true'].copy()
            df_producible['생산수량'] = 0
            
            edited_production = st.data_editor(
                df_producible[['품목코드', '품목명', '단위', '생산수량']],
                key=f"production_editor_{st.session_state.production_editor_ver}",
                use_container_width=True, hide_index=True,
                disabled=['품목코드', '품목명', '단위'],
                column_config={"생산수량": st.column_config.NumberColumn(min_value=0, step=1)}
            )

            if st.form_submit_button("생산 목록에 추가", type="primary", use_container_width=True):
                if production_date != date.today() and not change_reason:
                    st.warning("생산일자를 변경한 경우, 변경 사유를 반드시 입력해야 합니다.")
                    st.stop()

                items_to_add = edited_production[edited_production['생산수량'] > 0]
                if not items_to_add.empty:
                    current_cart = st.session_state.production_cart
                    updated_cart = pd.concat([current_cart, items_to_add]).groupby('품목코드').agg({
                        '품목명': 'last', '단위': 'last', '생산수량': 'sum'
                    }).reset_index()
                    st.session_state.production_cart = updated_cart
                    st.session_state.production_editor_ver += 1
                    st.session_state.production_date_to_log = production_date
                    st.session_state.production_change_reason = change_reason # 사유도 세션에 저장
                    st.session_state.success_message = "생산 목록에 추가되었습니다."
                else:
                    st.session_state.warning_message = "생산수량을 입력한 품목이 없습니다."
                st.rerun()

    v_spacer(16)

    with st.container(border=True):
        production_cart = st.session_state.production_cart
        
        if production_cart.empty:
            st.markdown("##### 📦 최종 생산 기록 목록")
            st.info("기록할 생산 목록이 없습니다.")
        else:
            production_log_date = st.session_state.production_date_to_log
            st.markdown(f"##### 📦 최종 생산 기록 목록 ({production_log_date.strftime('%Y년 %m월 %d일')})")
            
            st.dataframe(production_cart[['품목코드', '품목명', '단위', '생산수량']], use_container_width=True, hide_index=True)
            
            with st.form("finalize_production_form"):
                btn_cols = st.columns(2)
                with btn_cols[0]:
                    if st.form_submit_button("✅ 최종 생산 기록 저장", type="primary", use_container_width=True):
                        items_to_log = production_cart.copy()
                        items_to_log.rename(columns={'생산수량': '수량변경'}, inplace=True)
                        
                        change_reason_final = st.session_state.get("production_change_reason", "")
                        
                        with st.spinner("생산 기록 및 재고 업데이트 중..."):
                            if update_inventory(items_to_log, "생산입고", user['name'], reason=change_reason_final):
                                st.session_state.success_message = f"{len(items_to_log)}개 품목의 생산 기록이 저장되었습니다."
                                st.session_state.production_cart = pd.DataFrame() # 목록 비우기
                                st.rerun()
                            else:
                                st.session_state.error_message = "생산 기록 저장 중 오류가 발생했습니다."
                
                with btn_cols[1]:
                    if st.form_submit_button("🗑️ 목록 비우기", use_container_width=True):
                        st.session_state.production_cart = pd.DataFrame()
                        st.session_state.success_message = "생산 목록을 모두 삭제했습니다."
                        st.rerun()

### 🏭 7-2) 신규: 생산/재고 관리
def page_admin_inventory_management(master_df: pd.DataFrame):
    st.subheader("📊 생산/재고 관리")

    inventory_tabs = st.tabs(["현재고 현황", "재고 변동 내역", "재고 수동 조정"])

    with inventory_tabs[0]: # 현재고 현황
        st.markdown("##### 📦 현재고 현황")
        
        # --- [수정] '전체 현황'과 '재고 보유 현황' 탭으로 분리 ---
        inv_status_tabs = st.tabs(["전체 현황", "재고 보유 현황"])
        
        # 데이터 계산은 한 번만 수행
        current_inv_df = load_data(SHEET_NAME_CURRENT_INVENTORY, CURRENT_INVENTORY_COLUMNS)
        orders_df = load_data(SHEET_NAME_ORDERS, ORDERS_COLUMNS)
        active_master_df = master_df[master_df['활성'].astype(str).str.lower() == 'true']
        
        pending_orders = orders_df[orders_df['상태'] == '요청']
        pending_qty = pending_orders.groupby('품목코드')['수량'].sum().reset_index().rename(columns={'수량': '출고 대기 수량'})

        # 상품 마스터를 기준으로 Left Join하여 모든 품목 표시
        display_inv = pd.merge(active_master_df[['품목코드', '품목명']], current_inv_df[['품목코드', '현재고수량']], on='품목코드', how='left')
        display_inv = pd.merge(display_inv, pending_qty, on='품목코드', how='left').fillna(0)
        
        display_inv['현재고수량'] = pd.to_numeric(display_inv['현재고수량'], errors='coerce').fillna(0).astype(int)
        display_inv['출고 대기 수량'] = pd.to_numeric(display_inv['출고 대기 수량'], errors='coerce').fillna(0).astype(int)
        
        display_inv['실질 가용 재고'] = display_inv['현재고수량'] - display_inv['출고 대기 수량']
        
        with inv_status_tabs[0]: # 전체 현황
            st.dataframe(display_inv, use_container_width=True, hide_index=True)
            
        with inv_status_tabs[1]: # 재고 보유 현황
            st.dataframe(display_inv[display_inv['현재고수량'] > 0], use_container_width=True, hide_index=True)

    with inventory_tabs[1]: # 재고 변동 내역
        st.markdown("##### 📜 재고 변동 내역")
        log_df = load_data(SHEET_NAME_INVENTORY_LOG, INVENTORY_LOG_COLUMNS)
        
        # --- 오류 수정: log_df가 비어있을 경우를 먼저 처리 ---
        if log_df.empty:
            st.info("재고 변동 기록이 없습니다.")
        else:
            c1, c2, c3 = st.columns(3)
            dt_from = c1.date_input("조회 시작일", date.today() - timedelta(days=7), key="log_from")
            dt_to = c2.date_input("조회 종료일", date.today(), key="log_to")
            
            item_list = ["(전체)"] + master_df['품목명'].unique().tolist()
            item_filter = c3.selectbox("품목 필터", item_list, key="log_item_filter")
            
            filtered_log = log_df.copy()
            
            filtered_log['로그일시_dt'] = pd.to_datetime(filtered_log['로그일시']).dt.date
            filtered_log = filtered_log[(filtered_log['로그일시_dt'] >= dt_from) & (filtered_log['로그일시_dt'] <= dt_to)]
            if item_filter != "(전체)":
                filtered_log = filtered_log[filtered_log['품목명'] == item_filter]
            
            # 이 블록 안에서만 drop을 실행하므로 안전함
            st.dataframe(filtered_log.drop(columns=['로그일시_dt']), use_container_width=True, hide_index=True)

    with inventory_tabs[2]: # 재고 수동 조정
        st.markdown("##### ✍️ 재고 수동 조정")
        st.warning("이 기능은 전산 재고와 실물 재고가 맞지 않을 때만 사용하세요. 모든 조정 내역은 영구적으로 기록됩니다.")

        with st.form("adj_form", border=True):
            item_list = master_df['품목명'].unique().tolist()
            c1, c2, c3 = st.columns(3)
            selected_item = c1.selectbox("조정할 품목 선택", item_list)
            adj_qty = c2.number_input("조정 수량 (+/-)", step=1, help="증가시키려면 양수, 감소시키려면 음수를 입력하세요.")
            adj_reason = c3.text_input("조정 사유 (필수)", placeholder="예: 실사 재고 오차, 파손 폐기 등")
            
            if st.form_submit_button("재고 조정 실행", type="primary"):
                if not (selected_item and adj_reason and adj_qty != 0):
                    st.warning("모든 필드를 올바르게 입력해주세요.")
                else:
                    item_info = master_df[master_df['품목명'] == selected_item].iloc[0]
                    item_to_update = pd.DataFrame([{
                        '품목코드': item_info['품목코드'],
                        '품목명': selected_item,
                        '수량변경': adj_qty
                    }])
                    
                    if update_inventory(item_to_update, "재고조정", st.session_state.auth['name'], reason=adj_reason):
                        st.session_state.success_message = f"'{selected_item}'의 재고가 성공적으로 조정되었습니다."
                        st.rerun()
                    else:
                        st.session_state.error_message = "재고 조정 중 오류가 발생했습니다."


### 📋 7-3) 기존: 발주요청 조회 (재고 연동 강화)
def page_admin_unified_management(df_all: pd.DataFrame, store_info_df: pd.DataFrame, master_df: pd.DataFrame):
    st.subheader("📋 발주요청 조회·수정")
    
    if df_all.empty:
        st.info("발주 데이터가 없습니다.")
        return
    
    # --- 필터링 UI ---
    c1, c2, c3, c4 = st.columns(4)
    dt_from = c1.date_input("시작일", date.today() - timedelta(days=7), key="admin_mng_from")
    dt_to = c2.date_input("종료일", date.today(), key="admin_mng_to")
    stores = ["(전체)"] + sorted(df_all["지점명"].dropna().unique().tolist())
    store = c3.selectbox("지점", stores, key="admin_mng_store")
    order_id_search = c4.text_input("발주번호로 검색", key="admin_mng_order_id", placeholder="전체 또는 일부 입력")
    
    # --- 데이터 필터링 ---
    df = df_all.copy()
    if order_id_search:
        df = df[df["발주번호"].str.contains(order_id_search, na=False)]
    else:
        # '주문일시'가 datetime 객체인지 확인하고 변환
        if not pd.api.types.is_datetime64_any_dtype(df['주문일시']):
            df['주문일시'] = pd.to_datetime(df['주문일시'])
        df['주문일시_dt'] = df['주문일시'].dt.date
        df = df[(df['주문일시_dt'] >= dt_from) & (df['주문일시_dt'] <= dt_to)]
        if store != "(전체)":
            df = df[df["지점명"] == store]
    
    # --- 상태별 데이터 분리 ---
    orders = df.groupby("발주번호").agg(
        주문일시=("주문일시", "first"), 
        지점명=("지점명", "first"), 
        건수=("품목코드", "count"), 
        합계금액=("합계금액", "sum"), 
        상태=("상태", "first"), 
        처리일시=("처리일시", "first"),
        반려사유=("반려사유", "first")
    ).reset_index().sort_values(by="주문일시", ascending=False)
    
    orders.rename(columns={"합계금액": "합계금액(원)"}, inplace=True)
    pending = orders[orders["상태"] == "요청"].copy()
    shipped = orders[orders["상태"].isin(["승인", "출고완료"])].copy()
    rejected = orders[orders["상태"] == "반려"].copy()
    
    tab1, tab2, tab3 = st.tabs([f"📦 발주 요청 ({len(pending)}건)", f"✅ 승인/출고 ({len(shipped)}건)", f"❌ 반려 ({len(rejected)}건)"])
    
    with tab1:
        pending_display = pending.copy()
        pending_display.insert(0, '선택', [st.session_state.admin_orders_selection.get(x, False) for x in pending['발주번호']])
        edited_pending = st.data_editor(pending_display, key="admin_pending_editor", hide_index=True, disabled=pending_display.columns.drop("선택"), column_order=("선택", "주문일시", "발주번호", "지점명", "건수", "합계금액(원)", "상태"))
        for _, row in edited_pending.iterrows():
            st.session_state.admin_orders_selection[row['발주번호']] = row['선택']
        selected_pending_ids = [oid for oid, selected in st.session_state.admin_orders_selection.items() if selected and oid in pending['발주번호'].values]
        
        st.markdown("---")
        st.markdown("##### 📦 선택한 발주 처리")
        btn_cols = st.columns([1, 1, 2])
        
        with btn_cols[0]:
            if st.button("✅ 선택 발주 승인", disabled=not selected_pending_ids, key="admin_approve_btn", use_container_width=True, type="primary"):
                # --- [수정] 재고 부족 시 승인 차단 및 상세 내역 표시 로직 ---
                current_inv_df = load_data(SHEET_NAME_CURRENT_INVENTORY, CURRENT_INVENTORY_COLUMNS)
                all_pending_orders = load_data(SHEET_NAME_ORDERS, ORDERS_COLUMNS).query("상태 == '요청'")
                
                other_pending_orders = all_pending_orders[~all_pending_orders['발주번호'].isin(selected_pending_ids)]
                pending_qty = other_pending_orders.groupby('품목코드')['수량'].sum().reset_index().rename(columns={'수량': '출고 대기 수량'})
                
                inventory_check = pd.merge(current_inv_df, pending_qty, on='품목코드', how='left').fillna(0)
                inventory_check['실질 가용 재고'] = inventory_check['현재고수량'] - inventory_check['출고 대기 수량']
                
                lacking_items_details = []
                orders_to_approve_df = df_all[df_all['발주번호'].isin(selected_pending_ids)]
                items_needed = orders_to_approve_df.groupby('품목코드')['수량'].sum().reset_index()
                
                for _, needed in items_needed.iterrows():
                    item_code = needed['품목코드']
                    needed_qty = needed['수량']
                    stock_info = inventory_check.query(f"품목코드 == '{item_code}'")
                    available_stock = int(stock_info.iloc[0]['실질 가용 재고']) if not stock_info.empty else 0
                    
                    if needed_qty > available_stock:
                        item_name = master_df.loc[master_df['품목코드'] == item_code, '품목명'].iloc[0]
                        shortfall = needed_qty - available_stock
                        lacking_items_details.append(f"- **{item_name}** (부족: **{shortfall}**개 / 필요: {needed_qty}개 / 가용: {available_stock}개)")
                
                if lacking_items_details:
                    details_str = "\n".join(lacking_items_details)
                    st.error(f"🚨 재고 부족으로 승인할 수 없습니다:\n{details_str}")
                else:
                    with st.spinner("발주 승인 및 재고 차감 처리 중..."):
                        # 선 재고 변경, 후 상태 업데이트
                        items_to_deduct = orders_to_approve_df.groupby(['품목코드', '품목명'])['수량'].sum().reset_index()
                        items_to_deduct['수량변경'] = -items_to_deduct['수량']
                        ref_id = ", ".join(selected_pending_ids)
                        
                        if update_inventory(items_to_deduct, "발주출고", "system_auto", ref_id=ref_id):
                            if update_order_status(selected_pending_ids, "승인", st.session_state.auth["name"]):
                                st.session_state.success_message = f"{len(selected_pending_ids)}건이 승인 처리되고 재고가 차감되었습니다."
                                st.session_state.admin_orders_selection.clear()
                                st.rerun()
                            else:
                                # 재고는 차감했으나 상태 변경 실패 시 복구 로직 (심화) - 여기서는 에러 메시지로 대체
                                st.session_state.error_message = "치명적 오류: 재고는 차감되었으나 발주 상태 변경에 실패했습니다. 관리자에게 문의하세요."
                        else:
                            st.session_state.error_message = "발주 승인 중 재고 차감 단계에서 오류가 발생했습니다."
                        st.rerun()
                        
        with btn_cols[1]:
            # --- 오류 수정: 누락된 '반려' 로직 시작 ---
            if st.button("❌ 선택 발주 반려", disabled=not selected_pending_ids, key="admin_reject_btn", use_container_width=True):
                rejection_reason = st.session_state.get("rejection_reason_input", "")
                if not rejection_reason:
                    st.warning("반려 사유를 반드시 입력해야 합니다.")
                else:
                    with st.spinner("발주 반려 및 환불 처리 중..."):
                        balance_df = load_data(SHEET_NAME_BALANCE, BALANCE_COLUMNS)
                        transactions_df = load_data(SHEET_NAME_TRANSACTIONS, TRANSACTIONS_COLUMNS)
                        
                        for order_id in selected_pending_ids:
                            order_items = df_all[df_all['발주번호'] == order_id]
                            store_id = order_items.iloc[0]['지점ID']
                            
                            original_tx = transactions_df[transactions_df['관련발주번호'] == order_id]
                            if original_tx.empty:
                                st.error(f"발주번호 {order_id}의 원거래 내역을 찾을 수 없어 환불 처리에 실패했습니다.")
                                continue

                            tx_info = original_tx.iloc[0]
                            refund_amount = abs(int(tx_info['금액']))
                            balance_info = balance_df[balance_df['지점ID'] == store_id].iloc[0]
                            new_prepaid = int(balance_info['선충전잔액'])
                            new_used_credit = int(balance_info['사용여신액'])
                            credit_refund = min(refund_amount, new_used_credit)
                            new_used_credit -= credit_refund
                            prepaid_refund = refund_amount - credit_refund
                            new_prepaid += prepaid_refund
                            update_balance_sheet(store_id, {'선충전잔액': new_prepaid, '사용여신액': new_used_credit})
                            
                            refund_record = {
                                "일시": now_kst_str(), "지점ID": store_id, "지점명": tx_info['지점명'],
                                "구분": "발주반려", "내용": f"발주 반려 환불 ({order_id})",
                                "금액": refund_amount, "처리후선충전잔액": new_prepaid,
                                "처리후사용여신액": new_used_credit, "관련발주번호": order_id, "처리자": st.session_state.auth["name"]
                            }
                            append_rows_to_sheet(SHEET_NAME_TRANSACTIONS, [refund_record], TRANSACTIONS_COLUMNS)

                        update_order_status(selected_pending_ids, "반려", st.session_state.auth["name"], reason=rejection_reason)
                        st.session_state.success_message = f"{len(selected_pending_ids)}건이 반려 처리되고 환불되었습니다."
                        st.session_state.admin_orders_selection.clear()
                        st.rerun()
        with btn_cols[2]:
            st.text_input("반려 사유 (반려 시 필수)", key="rejection_reason_input", placeholder="예: 재고 부족")

    with tab2: # 승인/출고 탭
        shipped_display = shipped.copy()
        shipped_display.insert(0, '선택', [st.session_state.admin_orders_selection.get(x, False) for x in shipped['발주번호']])
        edited_shipped = st.data_editor(shipped_display[['선택', '주문일시', '발주번호', '지점명', '건수', '합계금액(원)', '상태', '처리일시']], key="admin_shipped_editor", hide_index=True, disabled=shipped.columns)
        for _, row in edited_shipped.iterrows():
            st.session_state.admin_orders_selection[row['발주번호']] = row['선택']
        
        selected_shipped_ids = [oid for oid, selected in st.session_state.admin_orders_selection.items() if selected and oid in shipped['발주번호'].values]
        
        if st.button("↩️ 선택 건 요청 상태로 되돌리기", key="revert_shipped", disabled=not selected_shipped_ids, use_container_width=True):
            with st.spinner("승인 취소 및 재고 복원 중..."):
                orders_to_revert_df = df_all[df_all['발주번호'].isin(selected_shipped_ids)]
                items_to_restore = orders_to_revert_df.groupby(['품목코드', '품목명'])['수량'].sum().reset_index()
                items_to_restore['수량변경'] = items_to_restore['수량'] # 양수로 복원

                ref_id = ", ".join(selected_shipped_ids)
                
                if update_inventory(items_to_restore, "승인취소", st.session_state.auth['name'], ref_id=ref_id):
                    update_order_status(selected_shipped_ids, "요청", "")
                    st.session_state.success_message = f"{len(selected_shipped_ids)}건이 '요청' 상태로 변경되고 재고가 복원되었습니다."
                else:
                    st.session_state.error_message = "승인 취소 중 재고 복원 오류가 발생했습니다."

                st.session_state.admin_orders_selection.clear()
                st.rerun()

    with tab3: # 반려 탭
        rejected_display = rejected.copy()
        rejected_display.insert(0, '선택', [st.session_state.admin_orders_selection.get(x, False) for x in rejected['발주번호']])
        edited_rejected = st.data_editor(rejected_display[['선택', '주문일시', '발주번호', '지점명', '건수', '합계금액(원)', '상태', '반려사유']], key="admin_rejected_editor", hide_index=True, disabled=rejected.columns)
        for _, row in edited_rejected.iterrows():
            st.session_state.admin_orders_selection[row['발주번호']] = row['선택']
            
        selected_rejected_ids = [oid for oid, selected in st.session_state.admin_orders_selection.items() if selected and oid in rejected['발주번호'].values]

        if st.button("↩️ 선택 건 요청 상태로 되돌리기", key="revert_rejected", disabled=not selected_rejected_ids, use_container_width=True):
            # 반려 되돌리기는 재고 변동이 없었으므로 상태만 변경
            update_order_status(selected_rejected_ids, "요청", "")
            st.session_state.success_message = f"{len(selected_rejected_ids)}건이 '요청' 상태로 변경되었습니다."
            st.session_state.admin_orders_selection.clear()
            st.rerun()

    v_spacer(16)
    with st.container(border=True):
        st.markdown("##### 📄 발주 품목 상세 조회")
        selected_ids = [k for k, v in st.session_state.admin_orders_selection.items() if v]
        if len(selected_ids) == 1:
            target_id = selected_ids[0]
            target_df = df_all[df_all["발주번호"] == target_id]
            total_amount = target_df['합계금액'].sum()
            
            st.markdown(f"**선택된 발주번호:** `{target_id}` / **총 합계금액(VAT포함):** `{total_amount:,.0f}원`")
            
            display_df = pd.merge(target_df, master_df[['품목코드', '과세구분']], on='품목코드', how='left')
            display_df['단가(VAT포함)'] = display_df.apply(get_vat_inclusive_price, axis=1)
            display_df.rename(columns={'합계금액': '합계금액(VAT포함)'}, inplace=True)
            
            st.dataframe(display_df[["품목코드", "품목명", "단위", "수량", "단가(VAT포함)", "합계금액(VAT포함)"]], hide_index=True, use_container_width=True)

            if target_df.iloc[0]['상태'] in ["승인", "출고완료"]:
                store_name = target_df.iloc[0]['지점명']
                store_info = store_info_df[store_info_df['지점명'] == store_name].iloc[0]
                buf = make_item_transaction_statement_excel(target_df, store_info)
                st.download_button("📄 품목 거래명세서 다운로드", data=buf, file_name=f"품목거래명세서_{store_name}_{target_id}.xlsx", mime="application/vnd.ms-excel", use_container_width=True, type="primary")
        else:
            st.info("상세 내용을 보려면 위 목록에서 발주를 **하나만** 선택하세요.")

### 📈 7-4) 기존: 매출 조회 (오류 수정 및 시각화 변경)
def page_admin_sales_inquiry(master_df: pd.DataFrame):
    st.subheader("📈 매출 조회")
    df_orders = load_data(SHEET_NAME_ORDERS, ORDERS_COLUMNS)
    df_sales_raw = df_orders[df_orders['상태'].isin(['승인', '출고완료'])].copy()
    if df_sales_raw.empty: 
        st.info("매출 데이터가 없습니다.")
        return

    c1, c2, c3 = st.columns(3)
    dt_from = c1.date_input("조회 시작일", date.today().replace(day=1), key="admin_sales_from")
    dt_to = c2.date_input("조회 종료일", date.today(), key="admin_sales_to")
    stores = ["(전체 통합)"] + sorted(df_sales_raw["지점명"].dropna().unique().tolist())
    store_sel = c3.selectbox("조회 지점", stores, key="admin_sales_store")
    
    if not pd.api.types.is_datetime64_any_dtype(df_sales_raw['주문일시']):
        df_sales_raw['주문일시'] = pd.to_datetime(df_sales_raw['주문일시'])

    df_sales_raw['주문일시_dt'] = df_sales_raw['주문일시'].dt.date
    mask = (df_sales_raw['주문일시_dt'] >= dt_from) & (df_sales_raw['주문일시_dt'] <= dt_to)
    if store_sel != "(전체 통합)": 
        mask &= (df_sales_raw["지점명"] == store_sel)
    df_sales = df_sales_raw[mask].copy()
    
    if df_sales.empty: 
        st.warning("해당 조건의 매출 데이터가 없습니다.")
        return
    
    total_sales = df_sales["합계금액"].sum()
    total_supply = df_sales["공급가액"].sum()
    total_tax = df_sales["세액"].sum()
    total_orders_count = df_sales['발주번호'].nunique()

    with st.container(border=True):
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("총 매출 (VAT 포함)", f"{total_sales:,.0f}원")
        m2.metric("공급가액", f"{total_supply:,.0f}원")
        m3.metric("부가세액", f"{total_tax:,.0f}원")
        m4.metric("총 발주 건수", f"{total_orders_count} 건")

    st.divider()
    
    sales_tab1, sales_tab2, sales_tab3 = st.tabs(["📊 종합 분석", "📅 일별 상세", "🗓️ 월별 상세"])
    with sales_tab1:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("##### 🏢 **지점별 매출 순위**")
            store_sales = df_sales.groupby("지점명")["합계금액"].sum().nlargest(10).reset_index()
            st.dataframe(store_sales, use_container_width=True, hide_index=True)
        with col2:
            st.markdown("##### 🍔 **품목별 판매 순위 (Top 10)**")
            item_sales = df_sales.groupby("품목명").agg(수량=('수량', 'sum'), 매출액=('합계금액', 'sum')).nlargest(10, '매출액').reset_index()
            item_sales.rename(columns={'매출액': '매출액(원)'}, inplace=True)
            if total_sales > 0:
                item_sales['매출액(%)'] = (item_sales['매출액(원)'] / total_sales * 100)
            else:
                item_sales['매출액(%)'] = 0
            
            st.dataframe(
                item_sales,
                column_config={
                    "매출액(%)": st.column_config.ProgressColumn(
                        "매출액(%)", format="%.1f%%",
                        min_value=0, max_value=item_sales['매출액(%)'].max(),
                    ),
                },
                use_container_width=True, hide_index=True
            )

    df_sales['연'] = df_sales['주문일시'].dt.strftime('%y')
    df_sales['월'] = df_sales['주문일시'].dt.month
    df_sales['일'] = df_sales['주문일시'].dt.day

    daily_pivot = df_sales.pivot_table(index=['연', '월', '일'], columns='지점명', values='합계금액', aggfunc='sum', fill_value=0, margins=True, margins_name='합계')
    monthly_pivot = df_sales.pivot_table(index=['연', '월'], columns='지점명', values='합계금액', aggfunc='sum', fill_value=0, margins=True, margins_name='합계')
    
    with sales_tab2:
        st.markdown("##### 📅 일별 매출 상세")
        daily_display_df = daily_pivot.reset_index()
        
        # --- [오류 수정] 숫자 형식의 열에만 서식 적용 ---
        numeric_cols = daily_display_df.columns.drop(['연', '월', '일'])
        st.dataframe(daily_display_df.style.format("{:,.0f}", subset=numeric_cols))
        
    with sales_tab3:
        st.markdown("##### 🗓️ 월별 매출 상세")
        monthly_display_df = monthly_pivot.reset_index()
        
        # --- [오류 수정] 숫자 형식의 열에만 서식 적용 ---
        numeric_cols = monthly_display_df.columns.drop(['연', '월'])
        st.dataframe(monthly_display_df.style.format("{:,.0f}", subset=numeric_cols))

    st.divider()
    summary_data = {
        'total_sales': total_sales, 'total_supply': total_supply,
        'total_tax': total_tax, 'total_orders': total_orders_count
    }
    filter_info = {
        'period': f"{dt_from.strftime('%Y-%m-%d')} ~ {dt_to.strftime('%Y-%m-%d')}",
        'store': store_sel
    }
    excel_buffer = make_sales_summary_excel(daily_pivot, monthly_pivot, summary_data, filter_info)
    st.download_button(label="📥 매출 정산표 다운로드", data=excel_buffer, file_name=f"매출정산표_{dt_from}_to_{dt_to}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)

### 📑 7-5) 기존: 증빙서류 다운로드 (UI 개선 및 재고 리포트 추가)
def page_admin_documents(store_info_df: pd.DataFrame):
    st.subheader("📑 증빙서류 다운로드")
    
    c1, c2, c3, c4 = st.columns(4)
    dt_from = c1.date_input("조회 시작일", date.today() - timedelta(days=30), key="admin_doc_from")
    
    all_stores = sorted(store_info_df["지점명"].dropna().unique().tolist())
    store_selection_list = ["대전 가공장 (Admin)"] + [s for s in all_stores if s != '대전 가공장']
    
    selected_entity = c3.selectbox("지점/관리 선택", store_selection_list, key="admin_doc_entity_select")

    if selected_entity == "대전 가공장 (Admin)":
        doc_type = c4.selectbox("서류 종류", ["품목 생산 보고서", "품목 재고 변동 보고서", "현재고 현황 보고서"], key="admin_doc_type_admin")
        
        # --- [수정] '현재고 현황 보고서' 선택 시 종료일 자동 변경 ---
        dt_to_value = dt_from if doc_type == "현재고 현황 보고서" else date.today()
        dt_to_disabled = True if doc_type == "현재고 현황 보고서" else False
        dt_to = c2.date_input("조회 종료일", dt_to_value, key="admin_doc_to", disabled=dt_to_disabled)
        
        if doc_type == "품목 생산 보고서":
            log_df = load_data(SHEET_NAME_INVENTORY_LOG, INVENTORY_LOG_COLUMNS)
            production_log = log_df[log_df['구분'] == '생산입고'].copy()
            if not production_log.empty:
                production_log['로그일시_dt'] = pd.to_datetime(production_log['로그일시']).dt.date
                report_df = production_log[(production_log['로그일시_dt'] >= dt_from) & (production_log['로그일시_dt'] <= dt_to)]
                st.dataframe(report_df.drop(columns=['로그일시_dt']), use_container_width=True, hide_index=True)
                
                if not report_df.empty:
                    buf = make_inventory_report_excel(report_df, "품목 생산 보고서", dt_from, dt_to)
                    st.download_button("엑셀 다운로드", data=buf, file_name=f"품목생산보고서_{dt_from}_to_{dt_to}.xlsx", mime="application/vnd.ms-excel", use_container_width=True, type="primary")
            else:
                st.info("생산 기록이 없습니다.")

        elif doc_type == "품목 재고 변동 보고서":
            log_df = load_data(SHEET_NAME_INVENTORY_LOG, INVENTORY_LOG_COLUMNS)
            if not log_df.empty:
                log_df['로그일시_dt'] = pd.to_datetime(log_df['로그일시']).dt.date
                report_df = log_df[(log_df['로그일시_dt'] >= dt_from) & (log_df['로그일시_dt'] <= dt_to)]
                st.dataframe(report_df.drop(columns=['로그일시_dt']), use_container_width=True, hide_index=True)

                if not report_df.empty:
                    buf = make_inventory_report_excel(report_df, "품목 재고 변동 보고서", dt_from, dt_to)
                    st.download_button("엑셀 다운로드", data=buf, file_name=f"품목재고변동보고서_{dt_from}_to_{dt_to}.xlsx", mime="application/vnd.ms-excel", use_container_width=True, type="primary")
            else:
                st.info("재고 변동 기록이 없습니다.")

        elif doc_type == "현재고 현황 보고서":
            st.info(f"{dt_from.strftime('%Y-%m-%d')} 기준의 현재고 현황을 조회합니다.")
            log_df = load_data(SHEET_NAME_INVENTORY_LOG, INVENTORY_LOG_COLUMNS)
            if not log_df.empty:
                log_df['로그일시_dt'] = pd.to_datetime(log_df['로그일시']).dt.date
                filtered_log = log_df[log_df['로그일시_dt'] <= dt_from]
                
                if not filtered_log.empty:
                    report_df = filtered_log.groupby(['품목코드', '품목명'])['수량변경'].sum().reset_index()
                    report_df.rename(columns={'수량변경': '현재고수량'}, inplace=True)
                    st.dataframe(report_df, use_container_width=True, hide_index=True)

                    buf = make_inventory_report_excel(report_df, "현재고 현황 보고서", dt_from, dt_from)
                    st.download_button("엑셀 다운로드", data=buf, file_name=f"현재고현황보고서_{dt_from}.xlsx", mime="application/vnd.ms-excel", use_container_width=True, type="primary")
                else:
                    st.info("해당 날짜까지의 재고 기록이 없습니다.")
            else:
                st.info("재고 기록이 없습니다.")    

    else: # 일반 지점 선택 시
        dt_to = c2.date_input("조회 종료일", date.today(), key="admin_doc_to_store")        
        doc_type = c4.selectbox("서류 종류", ["금전 거래내역서", "품목 거래명세서"], key="admin_doc_type_store")
        selected_store_info = store_info_df[store_info_df['지점명'] == selected_entity].iloc[0]
        
        if doc_type == "금전 거래내역서":
            transactions_df = load_data(SHEET_NAME_TRANSACTIONS, TRANSACTIONS_COLUMNS)
            store_transactions = transactions_df[transactions_df['지점명'] == selected_entity]
            
            if not store_transactions.empty:
                store_transactions['일시_dt'] = pd.to_datetime(store_transactions['일시']).dt.date
                mask = (store_transactions['일시_dt'] >= dt_from) & (store_transactions['일시_dt'] <= dt_to)
                dfv = store_transactions[mask].copy()

                st.dataframe(dfv.drop(columns=['일시_dt']), use_container_width=True, hide_index=True)
                if not dfv.empty:
                    buf = make_full_transaction_statement_excel(dfv, selected_store_info)
                    st.download_button("엑셀 다운로드", data=buf, file_name=f"금전거래명세서_{selected_entity}_{dt_from}_to_{dt_to}.xlsx", mime="application/vnd.ms-excel", use_container_width=True, type="primary")
            else:
                st.info(f"'{selected_entity}' 지점의 거래 내역이 없습니다.")

        elif doc_type == "품목 거래명세서":
            orders_df = load_data(SHEET_NAME_ORDERS, ORDERS_COLUMNS)
            store_orders = orders_df[(orders_df['지점명'] == selected_entity) & (orders_df['상태'].isin(['승인', '출고완료']))]
            
            if store_orders.empty:
                st.warning(f"'{selected_entity}' 지점의 승인/출고된 발주 내역이 없습니다.")
                return

            store_orders['주문일시_dt'] = pd.to_datetime(store_orders['주문일시']).dt.date
            filtered_orders = store_orders[store_orders['주문일시_dt'].between(dt_from, dt_to)]

            if filtered_orders.empty:
                st.warning(f"선택한 기간 내 '{selected_entity}' 지점의 승인/출고된 발주 내역이 없습니다.")
                return

            order_options = ["(기간 전체)"] + filtered_orders['발주번호'].unique().tolist()
            selected_order_id = st.selectbox("발주번호 선택", order_options, key="admin_doc_order_select")

            preview_df = filtered_orders
            if selected_order_id != "(기간 전체)":
                preview_df = filtered_orders[filtered_orders['발주번호'] == selected_order_id]

            st.dataframe(preview_df, use_container_width=True, hide_index=True)

            if not preview_df.empty:
                buf = make_multi_date_item_statement_excel(preview_df, selected_store_info, dt_from, dt_to)
                st.download_button("엑셀 다운로드", data=buf, file_name=f"품목거래명세서_{selected_entity}_{selected_order_id}.xlsx", mime="application/vnd.ms-excel", use_container_width=True, type="primary")

def page_admin_balance_management(store_info_df: pd.DataFrame):
    st.subheader("💰 결제 관리")
    
    charge_requests_df = load_data(SHEET_NAME_CHARGE_REQ, CHARGE_REQ_COLUMNS)
    balance_df = load_data(SHEET_NAME_BALANCE, BALANCE_COLUMNS)
    pending_requests = charge_requests_df[charge_requests_df['상태'] == '요청']
    
    st.markdown("##### 💳 충전/상환 요청 처리")
    if pending_requests.empty:
        st.info("처리 대기 중인 요청이 없습니다.")
    else:
        st.dataframe(pending_requests, hide_index=True, use_container_width=True)
        
        c1, c2, c3 = st.columns(3)
        req_options = {f"{row['요청일시']} / {row['지점명']} / {int(row['입금액']):,}원": row for _, row in pending_requests.iterrows()}
        selected_req_str = c1.selectbox("처리할 요청 선택", req_options.keys())
        action = c2.selectbox("처리 방식", ["승인", "반려"])
        reason = c3.text_input("반려 사유 (반려 시 필수)")

        if st.button("처리 실행", type="primary", use_container_width=True):
            selected_req = req_options[selected_req_str]
            if action == "반려" and not reason:
                st.warning("반려 시 사유를 입력해야 합니다.")
            else:
                store_id = selected_req['지점ID']
                
                all_charge_requests = load_data(SHEET_NAME_CHARGE_REQ, CHARGE_REQ_COLUMNS)
                req_index = all_charge_requests[(all_charge_requests['요청일시'] == selected_req['요청일시']) & (all_charge_requests['지점ID'] == store_id)].index

                if action == "승인":
                    current_balance_info = balance_df[balance_df['지점ID'] == store_id]
                    if current_balance_info.empty:
                        st.session_state.error_message = f"'{selected_req['지점명']}'의 잔액 정보가 없습니다."
                        st.rerun()

                    current_balance = current_balance_info.iloc[0]
                    new_prepaid = int(current_balance['선충전잔액'])
                    new_used_credit = int(current_balance['사용여신액'])
                    amount = int(selected_req['입금액'])
                    trans_record = {}

                    if selected_req['종류'] == '선충전':
                        new_prepaid += amount
                        trans_record = {"구분": "선충전승인", "내용": f"선충전 입금 확인 ({selected_req['입금자명']})"}
                    else: # 여신상환
                        new_used_credit -= amount
                        trans_record = {"구분": "여신상환승인", "내용": f"여신 상환 입금 확인 ({selected_req['입금자명']})"}
                        if new_used_credit < 0:
                            new_prepaid += abs(new_used_credit)
                            new_used_credit = 0
                    
                    # [개선사항 4] 잔액 변경과 함께 거래내역 자동 기록
                    if update_balance_sheet(store_id, {'선충전잔액': new_prepaid, '사용여신액': new_used_credit}):
                        full_trans_record = {
                            "일시": now_kst_str(), "지점ID": store_id, "지점명": selected_req['지점명'],
                            "금액": amount, "처리후선충전잔액": new_prepaid,
                            "처리후사용여신액": new_used_credit, "관련발주번호": "", "처리자": st.session_state.auth["name"],
                            **trans_record
                        }
                        append_rows_to_sheet(SHEET_NAME_TRANSACTIONS, [full_trans_record], TRANSACTIONS_COLUMNS)
                        
                        all_charge_requests.loc[req_index, '상태'] = '승인'
                        st.session_state.success_message = "요청이 승인 처리되고 거래내역에 기록되었습니다."
                    else:
                        st.session_state.error_message = "잔액 정보 업데이트에 실패했습니다."

                else: # 반려
                    all_charge_requests.loc[req_index, '상태'] = '반려'
                    all_charge_requests.loc[req_index, '처리사유'] = reason
                    st.session_state.success_message = "요청이 반려 처리되었습니다."
                
                save_df_to_sheet(SHEET_NAME_CHARGE_REQ, all_charge_requests)
                st.rerun()

    st.markdown("---")
    st.markdown("##### 🏢 지점별 잔액 현황")
    st.dataframe(balance_df, hide_index=True, use_container_width=True)
    
    with st.expander("✍️ 잔액/여신 수동 조정"):
        with st.form("manual_adjustment_form"):
            # --- [수정] '대전 가공장'을 제외한 지점 목록 생성 ---
            store_info_filtered = store_info_df[store_info_df['지점명'] != '대전 가공장']
            stores = sorted(store_info_filtered["지점명"].dropna().unique().tolist())
            
            if not stores:
                st.warning("조정할 지점이 없습니다.")
            else:
                c1, c2, c3 = st.columns(3)
                selected_store = c1.selectbox("조정 대상 지점", stores)
                adj_type = c2.selectbox("조정 항목", ["선충전잔액", "여신한도", "사용여신액"])
                adj_amount = c3.number_input("조정할 금액 (+/-)", format="%d", step=1000)
                adj_reason = st.text_input("조정 사유 (거래내역에 기록됩니다)")
                
                if st.form_submit_button("조정 실행", type="primary"):
                    if not (selected_store and adj_reason and adj_amount != 0):
                        st.warning("모든 필드를 올바르게 입력해주세요.")
                    else:
                        store_id = store_info_df[store_info_df['지점명'] == selected_store]['지점ID'].iloc[0]
                        current_balance_query = balance_df[balance_df['지점ID'] == store_id]
                        
                        if current_balance_query.empty:
                            st.error(f"'{selected_store}'의 잔액 정보가 '잔액마스터' 시트에 없습니다. 먼저 잔액 정보를 등록해주세요.")
                        else:
                            current_balance = current_balance_query.iloc[0]
                            
                            if adj_type == "여신한도":
                                new_limit = int(current_balance['여신한도']) + adj_amount
                                update_balance_sheet(store_id, {adj_type: new_limit})
                                st.session_state.success_message = f"'{selected_store}'의 여신한도가 조정되었습니다. (거래내역에 기록되지 않음)"
                            else:
                                current_prepaid = int(current_balance['선충전잔액'])
                                current_used_credit = int(current_balance['사용여신액'])
                                
                                new_prepaid, new_used_credit = current_prepaid, current_used_credit
                                trans_record = {"금액": adj_amount, "내용": adj_reason}

                                if adj_type == "선충전잔액":
                                    new_prepaid += adj_amount
                                    update_balance_sheet(store_id, {adj_type: new_prepaid})
                                    trans_record.update({"구분": "수동조정(충전)", "처리후선충전잔액": new_prepaid, "처리후사용여신액": new_used_credit})
                                
                                elif adj_type == "사용여신액":
                                    new_used_credit += adj_amount
                                    update_balance_sheet(store_id, {adj_type: new_used_credit})
                                    trans_record.update({"구분": "수동조정(여신)", "처리후선충전잔액": current_prepaid, "처리후사용여신액": new_used_credit})

                                full_trans_record = {
                                    **trans_record, 
                                    "일시": now_kst_str(), 
                                    "지점ID": store_id, 
                                    "지점명": selected_store, 
                                    "처리자": st.session_state.auth['name']
                                }
                                append_rows_to_sheet(SHEET_NAME_TRANSACTIONS, [full_trans_record], TRANSACTIONS_COLUMNS)
                                st.session_state.success_message = f"'{selected_store}'의 {adj_type}이(가) 조정되고 거래내역에 기록되었습니다."
                            st.rerun()

def page_admin_settings(store_info_df_raw: pd.DataFrame, master_df_raw: pd.DataFrame):
    st.subheader("🛠️ 관리 설정")
    tab1, tab2 = st.tabs(["품목 관리", "지점 관리"])

    with tab1:
        st.markdown("##### 🏷️ 품목 정보 설정")
        edited_master_df = st.data_editor(master_df_raw, num_rows="dynamic", use_container_width=True, key="master_editor")
        if st.button("품목 정보 저장", type="primary", key="save_master"):
            if save_df_to_sheet(SHEET_NAME_MASTER, edited_master_df):
                st.session_state.success_message = "품목 정보가 성공적으로 저장되었습니다."
                st.rerun()

    with tab2:
        st.markdown("##### 🏢 지점(사용자) 정보 설정")
        
        # [수정] 신규 ID 추가 시 주의 문구 추가
        st.info("ℹ️ 신규 지점 추가 시 ID는 신중히 입력해주세요. 저장 후에는 변경할 수 없습니다.")
        
        # [수정] 기존 지점 ID를 수정하지 못하도록 disabled 처리
        edited_store_df = st.data_editor(
            store_info_df_raw, 
            num_rows="dynamic", 
            use_container_width=True, 
            key="store_editor",
            disabled=["지점ID"] 
        )
        
        if st.button("지점 정보 저장", type="primary", key="save_stores"):
            if save_df_to_sheet(SHEET_NAME_STORES, edited_store_df):
                # 신규 지점 잔액 마스터 자동 추가 로직 (기존과 동일)
                balance_df = load_data(SHEET_NAME_BALANCE, BALANCE_COLUMNS)
                
                store_ids_set = set(edited_store_df['지점ID'].unique())
                balance_ids_set = set(balance_df['지점ID'].unique())
                new_store_ids = store_ids_set - balance_ids_set
                
                new_stores_added = 0
                if new_store_ids:
                    new_balance_rows = []
                    for new_id in new_store_ids:
                        if new_id: # 빈 ID는 제외
                            store_info = edited_store_df[edited_store_df['지점ID'] == new_id].iloc[0]
                            new_balance_rows.append({
                                "지점ID": new_id,
                                "지점명": store_info['지점명'],
                                "선충전잔액": 0,
                                "여신한도": 0,
                                "사용여신액": 0
                            })
                    if new_balance_rows:
                        append_rows_to_sheet(SHEET_NAME_BALANCE, new_balance_rows, BALANCE_COLUMNS)
                        new_stores_added = len(new_balance_rows)

                success_msg = "지점 정보가 성공적으로 저장되었습니다."
                if new_stores_added > 0:
                    success_msg += f" {new_stores_added}개의 신규 지점이 잔액 마스터에 자동 추가되었습니다."
                st.session_state.success_message = success_msg
                st.rerun()
# =============================================================================
# 8) 라우팅
# =============================================================================
if __name__ == "__main__":
    if not require_login(): st.stop()
    init_session_state()
    st.title("📦 식자재 발주 시스템")
    display_feedback()
    
    user = st.session_state.auth
    
    # --- 오류 수정: 공통 데이터 로드를 if문 밖으로 이동 ---
    master_df = load_data(SHEET_NAME_MASTER, MASTER_COLUMNS)
    store_info_df_raw = load_data(SHEET_NAME_STORES, STORES_COLUMNS)
    orders_df = load_data(SHEET_NAME_ORDERS, ORDERS_COLUMNS)
    balance_df = load_data(SHEET_NAME_BALANCE, BALANCE_COLUMNS)
    charge_requests_df = load_data(SHEET_NAME_CHARGE_REQ, CHARGE_REQ_COLUMNS)
    
    if user["role"] == "admin":
        # --- 통합 관리자 탭 ---
        tabs = st.tabs(["🏭 일일 생산 보고", "📊 생산/재고 관리", "📋 발주요청 조회", "📈 매출 조회", "💰 결제 관리", "📑 증빙서류 다운로드", "🛠️ 관리 설정"])
        with tabs[0]: page_admin_daily_production(master_df)
        with tabs[1]: page_admin_inventory_management(master_df)
        with tabs[2]: page_admin_unified_management(orders_df, store_info_df_raw, master_df)
        with tabs[3]: page_admin_sales_inquiry(master_df)
        with tabs[4]: page_admin_balance_management(store_info_df_raw)
        with tabs[5]: page_admin_documents(store_info_df_raw)
        with tabs[6]: page_admin_settings(store_info_df_raw, master_df)

    else: # store
        tabs = st.tabs(["🛒 발주 요청", "🧾 발주 조회", "💰 결제 관리", "📑 증빙서류 다운로드", "🏷️ 품목 단가 조회"])
        
        # 이제 balance_df가 존재하므로 이 코드가 정상적으로 작동합니다.
        my_balance_series = balance_df[balance_df['지점ID'] == user['user_id']]
        my_balance_info = my_balance_series.iloc[0] if not my_balance_series.empty else pd.Series(dtype='object')
        
        my_store_info = store_info_df_raw[store_info_df_raw['지점ID'] == user['user_id']]

        with tabs[0]: page_store_register_confirm(master_df, my_balance_info)
        with tabs[1]: page_store_orders_change(my_store_info, master_df)
        with tabs[2]: page_store_balance(charge_requests_df, my_balance_info)
        with tabs[3]: page_store_documents(my_store_info)
        with tabs[4]: page_store_master_view(master_df)
