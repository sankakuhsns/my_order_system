# -*- coding: utf-8 -*-
# =============================================================================
# 📦 Streamlit 식자재 발주 시스템 (v20.0 - 최종 안정화 버전)
#
# - 주요 변경 사항 (v20.0):
#   - (버그 수정) 전역적으로 발생하던 Excel 생성 오류(AttributeError, KeyError) 최종 해결
#   - (기능 복원) 관리자 페이지의 재고 관련 서류 다운로드 기능 복원
#   - (기능 개선) 지점 페이지의 다운로드 기능 또한 새로운 통합 양식으로 모두 교체 완료
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
import hashlib
import random
import string
import time

# =============================================================================
# 0) 기본 설정 및 CONFIG
# =============================================================================
st.set_page_config(page_title="산카쿠 식자재 발주 시스템", page_icon="📦", layout="wide")
THEME = { "BORDER": "#e8e8ee", "PRIMARY": "#1C6758", "BG": "#f7f8fa", "TEXT": "#222" }
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

CONFIG = {
    'STORES': { 'name': "지점마스터", 'cols': ["지점ID", "지점PW", "역할", "지점명", "사업자등록번호", "상호명", "대표자명", "사업장주소", "업태", "종목", "활성"] },
    'MASTER': { 'name': "상품마스터", 'cols': ["품목코드", "품목명", "품목규격", "분류", "단위", "단가", "과세구분", "활성"] },
    'ORDERS': { 'name': "발주", 'cols': ["주문일시", "발주번호", "지점ID", "지점명", "품목코드", "품목명", "단위", "수량", "단가", "공급가액", "세액", "합계금액", "비고", "상태", "처리일시", "처리자", "반려사유"] },
    'BALANCE': { 'name': "잔액마스터", 'cols': ["지점ID", "지점명", "선충전잔액", "여신한도", "사용여신액"] },
    'CHARGE_REQ': { 'name': "충전요청", 'cols': ["요청일시", "지점ID", "지점명", "입금자명", "입금액", "종류", "상태", "처리사유"] },
    'TRANSACTIONS': { 'name': "거래내역", 'cols': ["일시", "지점ID", "지점명", "구분", "내용", "금액", "처리후선충전잔액", "처리후사용여신액", "관련발주번호", "처리자"] },
    'AUDIT_LOG': { 'name': "활동로그", 'cols': ["로그일시", "변경자 ID", "변경자 이름", "작업 종류", "대상 ID", "대상 이름", "변경 항목", "이전 값", "새로운 값", "사유"] },
    'INVENTORY_LOG': { 'name': "재고로그", 'cols': ["로그일시", "작업일자", "품목코드", "품목명", "구분", "수량변경", "처리후재고", "관련번호", "처리자", "사유"] },
    'PRICE_HISTORY': { 'name': "가격변경이력", 'cols': ["변경일시", "품목코드", "품목명", "이전단가", "새단가"] },
    'CART': { 'cols': ["품목코드", "분류", "품목명", "단위", "단가", "단가(VAT포함)", "수량", "합계금액(VAT포함)"] },
    'ROLES': { 'ADMIN': 'admin', 'STORE': 'store' },
    'ORDER_STATUS': { 'PENDING': '요청', 'APPROVED': '승인', 'SHIPPED': '출고완료', 'REJECTED': '반려', 'CANCELED_STORE': '취소', 'CANCELED_ADMIN': '승인취소', 'MODIFIED': '변동출고' },
    'INV_CHANGE_TYPE': { 'PRODUCE': '생산입고', 'SHIPMENT': '발주출고', 'ADJUSTMENT': '재고조정', 'CANCEL_SHIPMENT': '승인취소' }
}

# =============================================================================
# 0-1) 기본 유틸리티 함수
# =============================================================================
def now_kst_str(fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    return datetime.now(KST).strftime(fmt)

def display_feedback():
    if "success_message" in st.session_state and st.session_state.success_message:
        st.success(st.session_state.success_message, icon="✅")
        st.session_state.success_message = ""
    if "error_message" in st.session_state and st.session_state.error_message:
        st.error(st.session_state.error_message, icon="🚨")
        st.session_state.error_message = ""
    if "warning_message" in st.session_state and st.session_state.warning_message:
        st.warning(st.session_state.warning_message, icon="⚠️")
        st.session_state.warning_message = ""

def v_spacer(height: int):
    st.markdown(f"<div style='height:{height}px'></div>", unsafe_allow_html=True)

def render_paginated_ui(total_items, page_size, key_prefix):
    page_number_key = f"{key_prefix}_page_number"
    if page_number_key not in st.session_state:
        st.session_state[page_number_key] = 1
        
    total_pages = math.ceil(total_items / page_size)
    if total_pages <= 1:
        return 1

    c1, c2, c3 = st.columns([2, 1, 2])
    with c1:
        if st.button("⬅️ 이전", key=f"{key_prefix}_prev", use_container_width=True, disabled=(st.session_state[page_number_key] <= 1)):
            st.session_state[page_number_key] -= 1
            st.rerun()
    with c2:
        st.markdown(f"<div style='text-align:center; margin-top: 8px;'>{st.session_state[page_number_key]} / {total_pages}</div>", unsafe_allow_html=True)
    with c3:
        if st.button("다음 ➡️", key=f"{key_prefix}_next", use_container_width=True, disabled=(st.session_state[page_number_key] >= total_pages)):
            st.session_state[page_number_key] += 1
            st.rerun()
    
    return st.session_state[page_number_key]

def add_audit_log(user_id: str, user_name: str, action_type: str, target_id: str, target_name: str = "", changed_item: str = "", before_value: Any = "", after_value: Any = "", reason: str = ""):
    log_sheet_name = CONFIG['AUDIT_LOG']['name']
    log_columns = CONFIG['AUDIT_LOG']['cols']
    
    new_log_entry = {
        "로그일시": now_kst_str(), "변경자 ID": user_id, "변경자 이름": user_name, "작업 종류": action_type,
        "대상 ID": target_id, "대상 이름": target_name, "변경 항목": str(changed_item),
        "이전 값": str(before_value), "새로운 값": str(after_value), "사유": reason
    }
    
    try:
        ws = open_spreadsheet().worksheet(log_sheet_name)
        values_to_append = [[new_log_entry.get(col, "") for col in log_columns]]
        ws.append_rows(values_to_append, value_input_option='USER_ENTERED')
    except gspread.WorksheetNotFound:
        sh = open_spreadsheet()
        ws = sh.add_worksheet(title=log_sheet_name, rows="1", cols=len(log_columns))
        ws.append_row(log_columns, value_input_option='USER_ENTERED')
        ws.append_rows(values_to_append, value_input_option='USER_ENTERED')
    except Exception as e:
        print(f"CRITICAL: 감사 로그 기록 실패! - {e}")

# =============================================================================
# 2) Google Sheets 연결 및 I/O
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
    try:
        return get_gs_client().open_by_key(key)
    except Exception as e:
        st.error(f"스프레드시트 열기 실패: {e}")
        st.stop()

@st.cache_data(ttl=300)
def load_data(sheet_name: str, columns: List[str] = None) -> pd.DataFrame:
    try:
        ws = open_spreadsheet().worksheet(sheet_name)
        records = ws.get_all_records(empty2zero=False, head=1)
        if not records:
            return pd.DataFrame(columns=columns) if columns else pd.DataFrame()
        
        df = pd.DataFrame(records)
        df = df.astype(str)
        
        numeric_cols_map = {
            CONFIG['BALANCE']['name']: ['선충전잔액', '여신한도', '사용여신액'],
            CONFIG['CHARGE_REQ']['name']: ['입금액'],
            CONFIG['TRANSACTIONS']['name']: ['금액', '처리후선충전잔액', '처리후사용여신액'],
            CONFIG['ORDERS']['name']: ["수량", "단가", "공급가액", "세액", "합계금액"],
            CONFIG['MASTER']['name']: ["단가"],
            CONFIG['INVENTORY_LOG']['name']: ["수량변경", "처리후재고"],
        }
        numeric_cols = numeric_cols_map.get(sheet_name, [])
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)

        if columns:
            for col in columns:
                if col not in df.columns:
                    is_numeric = any(col in num_list for num_list in numeric_cols_map.values())
                    df[col] = 0 if is_numeric else ''
            df = df[columns]
            
        df = convert_datetime_columns(df)
        
        sort_key_map = {'로그일시': "로그일시", '주문일시': "주문일시", '요청일시': "요청일시", '일시': "일시"}
        for col in sort_key_map:
            if col in df.columns and pd.api.types.is_datetime64_any_dtype(df[col]):
                df = df.sort_values(by=col, ascending=False).reset_index(drop=True)
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
        balance_df = get_balance_df() # 캐시된 DataFrame 로드
        ws = open_spreadsheet().worksheet(CONFIG['BALANCE']['name'])
        header = ws.row_values(1)

        # DataFrame에서 해당 지점의 인덱스를 찾음
        target_indices = balance_df.index[balance_df['지점ID'] == store_id].tolist()
        if not target_indices:
            st.error(f"'{CONFIG['BALANCE']['name']}' 시트에서 지점ID '{store_id}'를 찾을 수 없습니다.")
            return False
        
        # gspread는 1-based index, pandas는 0-based. 헤더 행(+1)과 인덱스(+1) 고려
        sheet_row_index = target_indices[0] + 2 

        cells_to_update = []
        for key, value in updates.items():
            if key in header:
                col_idx = header.index(key) + 1
                cells_to_update.append(gspread.Cell(sheet_row_index, col_idx, int(value)))
        
        if cells_to_update:
            ws.update_cells(cells_to_update, value_input_option='USER_ENTERED')

        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"잔액/여신 정보 업데이트 중 오류 발생: {e}")
        return False

def update_order_status(selected_ids: List[str], new_status: str, handler: str, reason: str = "") -> bool:
    if not selected_ids: return True
    try:
        orders_df = get_orders_df()
        user = st.session_state.auth
        
        for order_id in selected_ids:
            order_info = orders_df[orders_df['발주번호'] == order_id]
            if not order_info.empty:
                old_status = order_info['상태'].iloc[0]
                add_audit_log(
                    user_id=user['user_id'], user_name=user['name'],
                    action_type="주문 상태 변경", target_id=order_id,
                    target_name=order_info['지점명'].iloc[0], changed_item="상태",
                    before_value=old_status, after_value=new_status, reason=reason
                )

        ws = open_spreadsheet().worksheet(CONFIG['ORDERS']['name'])
        all_data = ws.get_all_values()
        header = all_data[0]
        id_col_idx = header.index("발주번호")
        status_col_idx = header.index("상태")
        handler_col_idx = header.index("처리자")
        timestamp_col_idx = header.index("처리일시")
        reason_col_idx = header.index("반려사유") if "반려사유" in header else -1
        
        cells_to_update = []
        now_str = now_kst_str() if new_status != CONFIG['ORDER_STATUS']['PENDING'] else ''
        handler_name = handler if new_status != CONFIG['ORDER_STATUS']['PENDING'] else ''
        
        for i, row in enumerate(all_data[1:], start=2):
            if row[id_col_idx] in selected_ids:
                cells_to_update.append(gspread.Cell(i, status_col_idx + 1, new_status))
                cells_to_update.append(gspread.Cell(i, handler_col_idx + 1, handler_name))
                cells_to_update.append(gspread.Cell(i, timestamp_col_idx + 1, now_str))
                if reason_col_idx != -1:
                    reason_text = reason if new_status == CONFIG['ORDER_STATUS']['REJECTED'] else ""
                    cells_to_update.append(gspread.Cell(i, reason_col_idx + 1, reason_text))

        if cells_to_update:
            ws.update_cells(cells_to_update, value_input_option='USER_ENTERED')
            time.sleep(2)
        
        st.cache_data.clear()
        return True
        
    except Exception as e:
        st.error(f"발주 상태 업데이트 중 오류가 발생했습니다: {e}")
        return False
        
# =============================================================================
# 3) 로그인, 인증 및 데이터 로더
# =============================================================================
def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()

def authenticate_user(uid, pwd, store_master_df):
    if uid and pwd:
        user_info = store_master_df[store_master_df['지점ID'] == uid]
        if not user_info.empty:
            user_record = user_info.iloc[0]
            stored_pw_hash = user_record['지점PW']
            input_pw_hash = hash_password(pwd)
            if stored_pw_hash.strip() == input_pw_hash.strip():
                if str(user_record['활성']).upper() != 'TRUE':
                    return {"login": False, "message": "비활성화된 계정입니다."}
                role = user_record['역할']
                name = user_record['지점명']
                return {"login": True, "user_id": uid, "name": name, "role": role}
    return {"login": False, "message": "아이디 또는 비밀번호가 올바르지 않습니다."}
    
def convert_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in ['주문일시', '요청일시', '처리일시', '일시', '로그일시', '작업일자']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    return df

def clear_data_cache():
    for key in list(st.session_state.keys()):
        if key.endswith('_df'):
            del st.session_state[key]
    st.cache_data.clear()

def get_master_df():
    if 'master_df' not in st.session_state:
        st.session_state.master_df = load_data(CONFIG['MASTER']['name'], CONFIG['MASTER']['cols'])
    return st.session_state.master_df

def get_stores_df():
    if 'stores_df' not in st.session_state:
        st.session_state.stores_df = load_data(CONFIG['STORES']['name'], CONFIG['STORES']['cols'])
    return st.session_state.stores_df

def get_orders_df():
    if 'orders_df' not in st.session_state:
        st.session_state.orders_df = load_data(CONFIG['ORDERS']['name'], CONFIG['ORDERS']['cols'])
    return st.session_state.orders_df

def get_balance_df():
    if 'balance_df' not in st.session_state:
        st.session_state.balance_df = load_data(CONFIG['BALANCE']['name'], CONFIG['BALANCE']['cols'])
    return st.session_state.balance_df

def get_charge_requests_df():
    if 'charge_requests_df' not in st.session_state:
        st.session_state.charge_requests_df = load_data(CONFIG['CHARGE_REQ']['name'], CONFIG['CHARGE_REQ']['cols'])
    return st.session_state.charge_requests_df

def get_transactions_df():
    if 'transactions_df' not in st.session_state:
        st.session_state.transactions_df = load_data(CONFIG['TRANSACTIONS']['name'], CONFIG['TRANSACTIONS']['cols'])
    return st.session_state.transactions_df

def get_inventory_log_df():
    if 'inventory_log_df' not in st.session_state:
        st.session_state.inventory_log_df = load_data(CONFIG['INVENTORY_LOG']['name'], CONFIG['INVENTORY_LOG']['cols'])
    return st.session_state.inventory_log_df

def get_price_history_df():
    if 'price_history_df' not in st.session_state:
        st.session_state.price_history_df = load_data(CONFIG['PRICE_HISTORY']['name'], CONFIG['PRICE_HISTORY']['cols'])
    return st.session_state.price_history_df

def require_login():
    if st.session_state.get("auth", {}).get("login"):
        user = st.session_state.auth
        st.sidebar.markdown(f"### 로그인 정보")
        st.sidebar.markdown(f"**{user['name']}** ({user['role']})님 환영합니다.")
        if st.sidebar.button("로그아웃"):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()
        return True
    
    store_master_df = get_stores_df()
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
# 가격 및 보고서 유틸리티
# =============================================================================
# [최종 수정 코드] get_price_at_date 함수
@st.cache_data(ttl=300)
def get_price_at_date(item_code: str, target_date: date, price_history_df: pd.DataFrame, master_df: pd.DataFrame) -> int:
    """특정 날짜 기준의 품목 단가를 가격 이력에서 조회 (수정된 로직)"""
    target_datetime = datetime.combine(target_date, datetime.max.time())

    item_history = price_history_df[price_history_df['품목코드'] == item_code].copy()
    
    if not item_history.empty:
        item_history['변경일시_dt'] = pd.to_datetime(item_history['변경일시'], errors='coerce')
        item_history.dropna(subset=['변경일시_dt'], inplace=True)

        # 1. target_date 이전에 변경된 기록이 있는지 확인
        past_history = item_history[item_history['변경일시_dt'] <= target_datetime]
        if not past_history.empty:
            # 있다면, 가장 최신 기록의 '새단가'를 반환
            latest_price_row = past_history.sort_values(by='변경일시_dt', ascending=False).iloc[0]
            return int(latest_price_row['새단가'])
        
        # 2. [개선] 과거 기록이 없다면, target_date 이후의 기록을 찾음
        future_history = item_history[item_history['변경일시_dt'] > target_datetime]
        if not future_history.empty:
            # 있다면, 가장 오래된 기록(첫 변경)의 '이전단가'를 반환
            first_change_row = future_history.sort_values(by='변경일시_dt', ascending=True).iloc[0]
            return int(first_change_row['이전단가'])

    # 3. 가격 변경 이력이 전혀 없는 품목이라면, 상품 마스터의 현재 단가를 반환
    current_price_series = master_df.loc[master_df['품목코드'] == item_code, '단가']
    return int(current_price_series.iloc[0]) if not current_price_series.empty else 0

# =============================================================================
# 4) Excel 생성 (통합 양식 v2.1 - 최종 수정본)
# =============================================================================

def make_order_id(store_id: str) -> str: return f"{datetime.now(KST):%Y%m%d%H%M%S}{store_id}"

def get_vat_inclusive_price(row: pd.Series) -> int:
    price = int(row.get('단가', 0))
    tax_type = row.get('과세구분', '과세')
    return int(price * 1.1) if tax_type == '과세' else price

def get_col_widths(dataframe: pd.DataFrame):
    """컬럼 너비를 데이터 길이에 맞게 자동 계산하는 헬퍼 함수"""
    widths = [max(len(str(s)) for s in dataframe[col].astype(str).values) for col in dataframe.columns]
    return [max(len(str(col)), width) + 2 for col, width in zip(dataframe.columns, widths)]

def create_unified_item_statement(orders_df: pd.DataFrame, supplier_info: pd.Series, customer_info: pd.Series) -> BytesIO:
    output = BytesIO()
    if orders_df.empty:
        return output

    # 1. 데이터 사전 처리
    df = orders_df.copy()
    df['거래일자'] = pd.to_datetime(df['주문일시']).dt.date
    if '세액' not in df.columns: df['세액'] = 0
    aggregation_rules = {'수량': 'sum', '공급가액': 'sum', '세액': 'sum', '합계금액': 'sum'}
    grouping_keys = ['거래일자', '품목코드', '품목명', '단위', '단가']
    df_agg = df.groupby(grouping_keys).agg(aggregation_rules).reset_index()
    df_agg = df_agg.sort_values(by=['거래일자', '품목명'])

    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        workbook = writer.book
        worksheet = workbook.add_worksheet("품목거래내역서")

        worksheet.fit_to_pages(1, 0)

        # 2. Excel 서식 정의
        fmt_title = workbook.add_format({'bold': True, 'font_size': 22, 'align': 'center', 'valign': 'vcenter', 'border': 1, 'bg_color': '#4F81BD', 'font_color': 'white'})
        fmt_subtitle = workbook.add_format({'bold': True, 'font_size': 11, 'bg_color': '#DDEBF7', 'align': 'center', 'valign': 'vcenter', 'border': 1})
        fmt_info_label = workbook.add_format({'bold': True, 'font_size': 9, 'bg_color': '#F2F2F2', 'align': 'center', 'valign': 'vcenter', 'border': 1})
        fmt_info_data = workbook.add_format({'font_size': 9, 'align': 'left', 'valign': 'vcenter', 'border': 1, 'text_wrap': True})
        fmt_summary_header = workbook.add_format({'bold': True, 'bg_color': '#DDEBF7', 'border': 1, 'align': 'center', 'valign': 'vcenter'})
        fmt_summary_data = workbook.add_format({'font_size': 9, 'border': 1, 'align': 'center', 'valign': 'vcenter'})
        fmt_summary_money = workbook.add_format({'bold': True, 'font_size': 9, 'num_format': '#,##0 "원"', 'bg_color': '#DDEBF7', 'border': 1, 'align': 'center', 'valign': 'vcenter'})
        fmt_date_header = workbook.add_format({'bold': True, 'font_size': 10, 'align': 'left', 'valign': 'vcenter', 'indent': 1, 'font_color': '#404040'})
        fmt_order_id_sub = workbook.add_format({'font_size': 8, 'align': 'left', 'valign': 'vcenter', 'indent': 2, 'font_color': '#808080'})
        fmt_header = workbook.add_format({'bold': True, 'font_size': 9, 'bg_color': '#4F81BD', 'font_color': 'white', 'align': 'center', 'valign': 'vcenter', 'border': 1})
        fmt_text_c = workbook.add_format({'font_size': 9, 'align': 'center', 'valign': 'vcenter', 'border': 1})
        fmt_text_l = workbook.add_format({'font_size': 9, 'align': 'left', 'valign': 'vcenter', 'border': 1})
        fmt_money = workbook.add_format({'font_size': 9, 'num_format': '#,##0', 'align': 'right', 'valign': 'vcenter', 'border': 1})
        fmt_subtotal_label = workbook.add_format({'bold': True, 'font_size': 9, 'bg_color': '#DDEBF7', 'align': 'center', 'valign': 'vcenter', 'border': 1})
        fmt_subtotal_money = workbook.add_format({'bold': True, 'font_size': 9, 'bg_color': '#DDEBF7', 'num_format': '#,##0', 'align': 'right', 'valign': 'vcenter', 'border': 1})
        fmt_print_date = workbook.add_format({'font_size': 8, 'align': 'right', 'font_color': '#777777'})

        # 3. 레이아웃 설정
        col_widths = [7, 7, 40, 7, 7, 10, 10, 10, 10]
        for i, width in enumerate(col_widths):
            worksheet.set_column(i, i, width)

        # 4. 헤더 영역 작성
        worksheet.set_row(0, 50)
        worksheet.merge_range('A1:I1', '품 목 거 래 내 역 서', fmt_title)
        worksheet.merge_range('A2:I2', f"출력일: {datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')}", fmt_print_date)
        
        worksheet.merge_range('A4:C4', '공급하는자', fmt_subtitle)
        worksheet.merge_range('D4:I4', '공급받는자', fmt_subtitle)

        info_data = [('사업자번호', '사업자등록번호'), ('상호', '상호명'), ('대표자', '대표자명'), ('사업장주소', '사업장주소'), ('업태/종목', '업태/종목')]
        
        for i in range(5, 10): worksheet.set_row(i-1, 28)

        for i, (label, key) in enumerate(info_data, 5):
            val_sup = f"{supplier_info.get('업태', '')}/{supplier_info.get('종목', '')}" if key == '업태/종목' else supplier_info.get(key, '')
            val_cus = f"{customer_info.get('업태', '')}/{customer_info.get('종목', '')}" if key == '업태/종목' else customer_info.get(key, '')
            
            worksheet.merge_range(f'A{i}:B{i}', label, fmt_info_label)
            worksheet.write(f'C{i}', val_sup, fmt_info_data)
            worksheet.merge_range(f'D{i}:E{i}', label, fmt_info_label)
            worksheet.merge_range(f'F{i}:I{i}', val_cus, fmt_info_data)
        
        # 5. 거래 요약 정보
        min_date, max_date = df_agg['거래일자'].min(), df_agg['거래일자'].max()
        date_range = max_date.strftime('%Y-%m-%d') if min_date == max_date else f"{min_date.strftime('%Y-%m-%d')} ~ {max_date.strftime('%Y-%m-%d')}"
        grand_total = df_agg['합계금액'].sum()
        worksheet.merge_range('A11:B11', '거래 기간', fmt_summary_header)
        worksheet.write('C11', date_range, fmt_summary_data)
        worksheet.merge_range('D11:E11', '총 합계 금액', fmt_summary_header)
        worksheet.merge_range('F11:I11', grand_total, fmt_summary_money)

        current_row = 13 

        # 6. 본문 데이터 작성
        order_ids_by_date = df.groupby('거래일자')['발주번호'].unique().apply(lambda x: ', '.join(x)).to_dict()

        for trade_date in df_agg['거래일자'].unique():
            worksheet.merge_range(f'A{current_row}:I{current_row}', f"■ 거래일자 : {trade_date.strftime('%Y년 %m월 %d일')}", fmt_date_header)
            current_row += 1
            related_orders = order_ids_by_date.get(trade_date, "")
            worksheet.merge_range(f'A{current_row}:I{current_row}', f"  관련 발주번호: {related_orders}", fmt_order_id_sub)
            current_row += 1

            headers = ['No', '품목코드', '품목명', '단위', '수량', '단가', '공급가액', '세액', '합계금액']
            worksheet.write_row(f'A{current_row}', headers, fmt_header)
            current_row += 1

            row_idx = current_row - 1
            date_df = df_agg[df_agg['거래일자'] == trade_date]
            item_counter = 1
            for _, record in date_df.iterrows():
                worksheet.write(row_idx, 0, item_counter, fmt_text_c)
                worksheet.write(row_idx, 1, record['품목코드'], fmt_text_c)
                worksheet.write(row_idx, 2, record['품목명'], fmt_text_l)
                worksheet.write(row_idx, 3, record['단위'], fmt_text_c)
                worksheet.write(row_idx, 4, record['수량'], fmt_money)
                worksheet.write(row_idx, 5, record['단가'], fmt_money)
                worksheet.write(row_idx, 6, record['공급가액'], fmt_money)
                worksheet.write(row_idx, 7, record['세액'], fmt_money)
                worksheet.write(row_idx, 8, record['합계금액'], fmt_money)
                item_counter += 1
                row_idx += 1

            current_row = row_idx + 1

            # ✨✨✨ 수정된 부분 시작 ✨✨✨
            # 해당 날짜의 요청사항(비고)을 가져와서 '일계' 바로 위에 추가
            memos_series = df[df['거래일자'] == trade_date]['비고'].dropna().unique()
            day_memo = ", ".join([str(memo) for memo in memos_series if str(memo).strip()])

            if day_memo:
                worksheet.merge_range(f'A{current_row}:B{current_row}', '요청사항', fmt_info_label)
                worksheet.merge_range(f'C{current_row}:I{current_row}', day_memo, fmt_info_data)
                current_row += 1
            # ✨✨✨ 수정된 부분 끝 ✨✨✨
            
            # 일계
            worksheet.merge_range(f'A{current_row}:F{current_row}', '일 계', fmt_subtotal_label)
            worksheet.write(f'G{current_row}', date_df['공급가액'].sum(), fmt_subtotal_money)
            worksheet.write(f'H{current_row}', date_df['세액'].sum(), fmt_subtotal_money)
            worksheet.write(f'I{current_row}', date_df['합계금액'].sum(), fmt_subtotal_money)
            current_row += 2

        # 7. 최종 합계
        worksheet.merge_range(f'A{current_row}:F{current_row}', '총 계', fmt_subtotal_label)
        worksheet.write(f'G{current_row}', df_agg['공급가액'].sum(), fmt_subtotal_money)
        worksheet.write(f'H{current_row}', df_agg['세액'].sum(), fmt_subtotal_money)
        worksheet.write(f'I{current_row}', df_agg['합계금액'].sum(), fmt_subtotal_money)

    output.seek(0)
    return output
    
def create_unified_financial_statement(df_transactions_period: pd.DataFrame, df_transactions_all: pd.DataFrame, supplier_info: pd.Series, customer_info: pd.Series) -> BytesIO:
    output = BytesIO()
    if df_transactions_period.empty: return output

    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        workbook = writer.book
        worksheet = workbook.add_worksheet(f"{customer_info.get('지점명', '금전 거래')} 내역서")

        # 인쇄 시 모든 열을 한 페이지에 맞춤
        worksheet.fit_to_pages(1, 0)
        
        # 1. Excel 서식 정의 (품목내역서와 유사하게 통일)
        fmt_title = workbook.add_format({'bold': True, 'font_size': 22, 'align': 'center', 'valign': 'vcenter', 'border': 1, 'bg_color': '#4F81BD', 'font_color': 'white'})
        fmt_subtitle = workbook.add_format({'bold': True, 'font_size': 11, 'bg_color': '#DDEBF7', 'align': 'center', 'valign': 'vcenter', 'border': 1})
        fmt_info_label = workbook.add_format({'bold': True, 'font_size': 9, 'bg_color': '#F2F2F2', 'align': 'center', 'valign': 'vcenter', 'border': 1})
        fmt_info_data = workbook.add_format({'font_size': 9, 'align': 'left', 'valign': 'vcenter', 'border': 1, 'text_wrap': True})
        fmt_summary_header = workbook.add_format({'bold': True, 'bg_color': '#DDEBF7', 'border': 1, 'align': 'center', 'valign': 'vcenter'})
        fmt_summary_data = workbook.add_format({'font_size': 9, 'border': 1, 'align': 'center', 'valign': 'vcenter', 'bg_color': '#DDEBF7', 'bold': True})
        fmt_summary_money = workbook.add_format({'bold': True, 'font_size': 9, 'num_format': '#,##0 "원"', 'bg_color': '#DDEBF7', 'border': 1, 'align': 'center', 'valign': 'vcenter'})
        fmt_header = workbook.add_format({'bold': True, 'font_size': 9, 'bg_color': '#4F81BD', 'font_color': 'white', 'align': 'center', 'valign': 'vcenter', 'border': 1})
        fmt_text_c = workbook.add_format({'font_size': 9, 'align': 'center', 'valign': 'vcenter', 'border': 1})
        fmt_text_l = workbook.add_format({'font_size': 9, 'align': 'left', 'valign': 'vcenter', 'border': 1})
        fmt_money_pos = workbook.add_format({'font_size': 9, 'num_format': '#,##0', 'align': 'right', 'valign': 'vcenter', 'border': 1, 'font_color': 'blue'})
        fmt_money_neg = workbook.add_format({'font_size': 9, 'num_format': '#,##0', 'align': 'right', 'valign': 'vcenter', 'border': 1, 'font_color': 'red'})
        fmt_money_zero = workbook.add_format({'font_size': 9, 'num_format': '#,##0', 'align': 'right', 'valign': 'vcenter', 'border': 1})
        fmt_print_date = workbook.add_format({'font_size': 8, 'align': 'right', 'font_color': '#777777'})
        
        # 2. 레이아웃 설정
        col_widths = [15, 10, 30, 15, 20, 20]
        for i, width in enumerate(col_widths):
            worksheet.set_column(i, i, width)

        # 3. 헤더 영역 작성
        worksheet.set_row(0, 50)
        worksheet.merge_range('A1:F1', '금 전 거 래 내 역 서', fmt_title)
        worksheet.merge_range('A2:F2', f"출력일: {datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')}", fmt_print_date)
        
        # 4. 정보 영역 (공급자/공급받는자 정보 셀 병합 수정)
        worksheet.merge_range('A4:C4', '공급하는자', fmt_subtitle)
        worksheet.merge_range('D4:F4', '공급받는자', fmt_subtitle)

        info_data = [('사업자번호', '사업자등록번호'), ('상호', '상호명'), ('대표자', '대표자명'), ('사업장주소', '사업장주소'), ('업태/종목', '업태/종목')]
        for i in range(5, 10): worksheet.set_row(i-1, 28)

        for i, (label, key) in enumerate(info_data, 5):
            val_sup = f"{supplier_info.get('업태', '')}/{supplier_info.get('종목', '')}" if key == '업태/종목' else supplier_info.get(key, '')
            val_cus = f"{customer_info.get('업태', '')}/{customer_info.get('종목', '')}" if key == '업태/종목' else customer_info.get(key, '')
            
            worksheet.write(f'A{i}', label, fmt_info_label)
            worksheet.merge_range(f'B{i}:C{i}', val_sup, fmt_info_data)
            worksheet.write(f'D{i}', label, fmt_info_label)
            worksheet.merge_range(f'E{i}:F{i}', val_cus, fmt_info_data)
        
        # 5. 거래 요약 정보
        dt_from = pd.to_datetime(df_transactions_period['일시']).min().date()
        dt_to = pd.to_datetime(df_transactions_period['일시']).max().date()

        all_tx = df_transactions_all[df_transactions_all['지점ID'] == customer_info['지점ID']].copy()
        all_tx['일시_dt'] = pd.to_datetime(all_tx['일시']).dt.date
        tx_before = all_tx[all_tx['일시_dt'] < dt_from].sort_values(by='일시', ascending=True)
        opening_balance = tx_before.iloc[-1]['처리후선충전잔액'] if not tx_before.empty else 0
        
        period_income = df_transactions_period[df_transactions_period['금액'] > 0]['금액'].sum()
        period_outcome = df_transactions_period[df_transactions_period['금액'] < 0]['금액'].sum()
        
        df_sorted_period = df_transactions_period.sort_values(by='일시', ascending=True)
        closing_balance = df_sorted_period.iloc[-1]['처리후선충전잔액'] if not df_sorted_period.empty else opening_balance
        
        current_row = 11
        worksheet.merge_range(f'A{current_row}:B{current_row}', '거래 기간', fmt_summary_header)
        worksheet.merge_range(f'C{current_row}:F{current_row}', f"{dt_from} ~ {dt_to}", fmt_summary_data)
        current_row += 1
        
        worksheet.merge_range(f'A{current_row}:B{current_row}', '기초 잔액', fmt_summary_header)
        worksheet.merge_range(f'C{current_row}:F{current_row}', opening_balance, fmt_summary_money)
        current_row += 1
        
        worksheet.merge_range(f'A{current_row}:B{current_row}', '기간 내 입금 (+)', fmt_summary_header)
        worksheet.merge_range(f'C{current_row}:F{current_row}', period_income, fmt_summary_money)
        current_row += 1
        
        worksheet.merge_range(f'A{current_row}:B{current_row}', '기간 내 출금 (-)', fmt_summary_header)
        worksheet.merge_range(f'C{current_row}:F{current_row}', period_outcome, fmt_summary_money)
        current_row += 1
        
        worksheet.merge_range(f'A{current_row}:B{current_row}', '기말 잔액', fmt_summary_header)
        worksheet.merge_range(f'C{current_row}:F{current_row}', closing_balance, fmt_summary_money)
        current_row += 2
        
        # 6. 본문 데이터 작성
        # 요청에 따라 6개 열로 구성
        headers = ['일시', '구분', '내용', '금액', '처리 후 선충전잔액', '처리 후 사용여신액']
        worksheet.write_row(f'A{current_row}', headers, fmt_header)
        current_row += 1
        
        for _, row in df_sorted_period.iterrows():
            amount = row.get('금액', 0)
            fmt = fmt_money_pos if amount > 0 else fmt_money_neg if amount < 0 else fmt_money_zero
            
            worksheet.write(f'A{current_row}', str(row.get('일시', '')), fmt_text_c)
            worksheet.write(f'B{current_row}', row.get('구분', ''), fmt_text_c)
            worksheet.write(f'C{current_row}', row.get('내용', ''), fmt_text_l)
            worksheet.write(f'D{current_row}', amount, fmt)
            worksheet.write(f'E{current_row}', row.get('처리후선충전잔액', 0), fmt_money_zero)
            worksheet.write(f'F{current_row}', row.get('처리후사용여신액', 0), fmt_money_zero)
            current_row += 1

    output.seek(0)
    return output
    
# [정상 코드] make_inventory_production_report_excel 함수
def make_inventory_production_report_excel(df_report: pd.DataFrame, report_type: str, dt_from: date, dt_to: date) -> BytesIO:
    output = BytesIO()
    if df_report.empty:
        return output

    master_df = get_master_df()
    price_history_df = get_price_history_df()
    
    df_report['작업일자_dt'] = pd.to_datetime(df_report['작업일자']).dt.date
    
    df_report['단가'] = df_report.apply(
        lambda row: get_price_at_date(row['품목코드'], row['작업일자_dt'], price_history_df, master_df),
        axis=1
    )

    df_merged = pd.merge(df_report, master_df[['품목코드', '단위']], on='품목코드', how='left')
    df_merged['단가'] = pd.to_numeric(df_merged['단가'], errors='coerce').fillna(0).astype(int)
    df_merged['수량변경'] = pd.to_numeric(df_merged['수량변경'], errors='coerce').fillna(0).astype(int)
    df_merged['총금액'] = df_merged['단가'] * df_merged['수량변경']

    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        workbook = writer.book
        worksheet = workbook.add_worksheet("품목생산보고서")
        worksheet.fit_to_pages(1, 0)
        
        fmt_title = workbook.add_format({'bold': True, 'font_size': 22, 'align': 'center', 'valign': 'vcenter', 'border': 1, 'bg_color': '#4F81BD', 'font_color': 'white'})
        fmt_header = workbook.add_format({'bold': True, 'font_size': 9, 'bg_color': '#4F81BD', 'font_color': 'white', 'align': 'center', 'valign': 'vcenter', 'border': 1})
        fmt_text_c = workbook.add_format({'font_size': 9, 'align': 'center', 'valign': 'vcenter', 'border': 1})
        fmt_text_l = workbook.add_format({'font_size': 9, 'align': 'left', 'valign': 'vcenter', 'border': 1})
        fmt_money_r = workbook.add_format({'font_size': 9, 'num_format': '#,##0', 'align': 'right', 'valign': 'vcenter', 'border': 1})
        fmt_subtotal_label = workbook.add_format({'bold': True, 'font_size': 9, 'bg_color': '#DDEBF7', 'align': 'center', 'valign': 'vcenter', 'border': 1})
        fmt_subtotal_money = workbook.add_format({'bold': True, 'font_size': 9, 'bg_color': '#DDEBF7', 'num_format': '#,##0', 'align': 'right', 'valign': 'vcenter', 'border': 1})
        fmt_date_header = workbook.add_format({'bold': True, 'font_size': 10, 'align': 'left', 'valign': 'vcenter', 'indent': 1, 'bg_color': '#EAF1F8', 'border': 1})
        fmt_grand_total_label = workbook.add_format({'bold': True, 'font_size': 11, 'bg_color': '#4F81BD', 'font_color': 'white', 'align': 'center', 'valign': 'vcenter', 'border': 1})
        fmt_grand_total_money = workbook.add_format({'bold': True, 'font_size': 11, 'bg_color': '#DDEBF7', 'num_format': '#,##0 "원"', 'align': 'right', 'valign': 'vcenter', 'border': 1})
        
        df_display = df_merged.drop(columns=['로그일시', '관련번호', '사유', '구분', '작업일자_dt'], errors='ignore').copy()
        df_display['작업일자'] = pd.to_datetime(df_display['작업일자']).dt.strftime('%Y-%m-%d')
        
        columns_order = ['작업일자', '품목코드', '품목명', '단위', '단가', '수량변경', '총금액', '처리후재고']
        df_display = df_display.reindex(columns=columns_order, fill_value='')

        num_cols = len(df_display.columns)
        last_col_letter = chr(64 + num_cols)
        worksheet.set_row(0, 50)
        worksheet.merge_range(f'A1:{last_col_letter}1', '품 목 생 산 보 고 서', fmt_title)

        current_row = 2
        worksheet.merge_range(f'A{current_row}:{last_col_letter}{current_row}', f"조회 기간: {dt_from.strftime('%Y-%m-%d')} ~ {dt_to.strftime('%Y-%m-%d')}", fmt_date_header)
        current_row += 1
        
        fmt_info_text_right_bold = workbook.add_format({'font_size': 9, 'align': 'right', 'valign': 'top', 'text_wrap': True, 'bold': True})
        worksheet.merge_range(f'A{current_row}:{last_col_letter}{current_row}', "※ 본 보고서는 '생산입고' 내역만 포함하며, 재고 조정 등 다른 항목들은 반영되지 않습니다.", fmt_info_text_right_bold)
        current_row += 2

        grouped_by_date = df_display.groupby('작업일자')
        for date_str, date_group in grouped_by_date:
            worksheet.merge_range(f'A{current_row}:{last_col_letter}{current_row}', f"■ 생산일자: {date_str}", fmt_date_header)
            current_row += 1
            
            headers = ['작업일자', '품목코드', '품목명', '단위', '단가', '생산수량', '총금액', '처리후재고']
            worksheet.write_row(f'A{current_row}', headers, fmt_header)
            current_row += 1

            for _, row in date_group.iterrows():
                worksheet.write(f'A{current_row}', row['작업일자'], fmt_text_c)
                worksheet.write(f'B{current_row}', row['품목코드'], fmt_text_c)
                worksheet.write(f'C{current_row}', row['품목명'], fmt_text_l)
                worksheet.write(f'D{current_row}', row['단위'], fmt_text_c)
                worksheet.write(f'E{current_row}', row['단가'], fmt_money_r)
                worksheet.write(f'F{current_row}', row['수량변경'], fmt_subtotal_money)
                worksheet.write(f'G{current_row}', row['총금액'], fmt_subtotal_money)
                worksheet.write(f'H{current_row}', row['처리후재고'], fmt_money_r)
                current_row += 1
            
            worksheet.merge_range(f'A{current_row}:E{current_row}', '일 계', fmt_subtotal_label)
            worksheet.write(f'F{current_row}', date_group['수량변경'].sum(), fmt_subtotal_money)
            worksheet.write(f'G{current_row}', date_group['총금액'].sum(), fmt_subtotal_money)
            worksheet.write(f'H{current_row}', '', fmt_subtotal_label)
            current_row += 2

        current_row += 1
        grand_total_amount = df_display['총금액'].sum()
        label_text = f"조회기간 ({dt_from.strftime('%Y-%m-%d')} ~ {dt_to.strftime('%Y-%m-%d')}) 총생산평가금액"
        worksheet.merge_range(f'A{current_row}:E{current_row}', label_text, fmt_grand_total_label)
        worksheet.merge_range(f'F{current_row}:{last_col_letter}{current_row}', grand_total_amount, fmt_grand_total_money)
        
        col_widths_final = [12, 10, 30, 8, 10, 10, 12, 10]
        for i, width in enumerate(col_widths_final):
            worksheet.set_column(i, i, width)

    output.seek(0)
    return output

def make_inventory_change_report_excel(df_report: pd.DataFrame, report_type: str, dt_from: date, dt_to: date) -> BytesIO:
    output = BytesIO()
    if df_report.empty:
        return output

    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        workbook = writer.book
        worksheet = workbook.add_worksheet(report_type)
        
        # 인쇄 시 모든 열을 한 페이지에 맞추는 설정을 추가합니다.
        worksheet.fit_to_pages(1, 0)
        
        # 1. Excel 서식 정의
        fmt_title = workbook.add_format({'bold': True, 'font_size': 22, 'align': 'center', 'valign': 'vcenter', 'border': 1, 'bg_color': '#4F81BD', 'font_color': 'white'})
        fmt_header = workbook.add_format({'bold': True, 'font_size': 9, 'bg_color': '#4F81BD', 'font_color': 'white', 'align': 'center', 'valign': 'vcenter', 'border': 1})
        
        # 텍스트 데이터용 왼쪽 정렬 포맷
        fmt_text_l = workbook.add_format({'font_size': 9, 'align': 'left', 'valign': 'vcenter', 'border': 1})
        
        # 텍스트 데이터용 가운데 정렬 포맷
        fmt_text_c = workbook.add_format({'font_size': 9, 'align': 'center', 'valign': 'vcenter', 'border': 1})
        
        # 수량 및 재고 셀에 적용할 배경색 포맷을 테마 색상으로 변경
        fmt_money_bg = workbook.add_format({'font_size': 9, 'num_format': '#,##0', 'align': 'right', 'valign': 'vcenter', 'border': 1, 'bg_color': '#DDEBF7'})

        # 2. 데이터 전처리 및 열 선택
        # '작업일자', '관련번호', '사유' 열 삭제
        df_display = df_report.drop(columns=['작업일자', '관련번호', '사유'], errors='ignore').copy()
        
        # '로그일시'를 '변동일시'로 변경하고 날짜 및 시간 포맷팅
        df_display.rename(columns={'로그일시': '변동일시'}, inplace=True)
        df_display['변동일시'] = pd.to_datetime(df_display['변동일시']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # 열 순서 재정의
        columns_order = ['변동일시', '구분', '품목코드', '품목명', '수량변경', '처리후재고', '단위', '처리자']
        # '단위'는 재고로그에 없으므로, master_df에서 가져오기
        master_df = get_master_df()
        df_merged = pd.merge(df_display, master_df[['품목코드', '단위']], on='품목코드', how='left')
        df_merged['단위'] = df_merged['단위'].fillna('')
        df_display = df_merged.reindex(columns=columns_order, fill_value='')

        # 3. 헤더 영역 작성
        worksheet.set_row(0, 50)
        worksheet.merge_range(0, 0, 0, len(df_display.columns) - 1, '재 고 변 동 보 고 서', fmt_title)
        
        # 4. 보고서 정보 (조회 기간)
        current_row = 2
        fmt_date_header = workbook.add_format({'bold': True, 'font_size': 10, 'align': 'left', 'valign': 'vcenter', 'indent': 1, 'bg_color': '#EAF1F8', 'border': 1})
        worksheet.merge_range(f'A{current_row}:H{current_row}', f"조회 기간: {dt_from} ~ {dt_to}", fmt_date_header)
        current_row += 2

        # 5. 본문 데이터
        headers = ['변동일시', '구분', '품목코드', '품목명', '수량변경', '처리후재고', '단위', '처리자']
        worksheet.write_row(f'A{current_row}', headers, fmt_header)
        current_row += 1
        
        for _, row in df_display.iterrows():
            # 변동일시, 품목코드, 단위, 처리자는 가운데 정렬, 나머지는 왼쪽 정렬
            worksheet.write(f'A{current_row}', row['변동일시'], fmt_text_c)
            worksheet.write(f'B{current_row}', row['구분'], fmt_text_c)
            worksheet.write(f'C{current_row}', row['품목코드'], fmt_text_c)
            worksheet.write(f'D{current_row}', row['품목명'], fmt_text_l)
            worksheet.write(f'E{current_row}', row['수량변경'], fmt_money_bg)
            worksheet.write(f'F{current_row}', row['처리후재고'], fmt_money_bg)
            worksheet.write(f'G{current_row}', row['단위'], fmt_text_c)
            worksheet.write(f'H{current_row}', row['처리자'], fmt_text_c)
            current_row += 1

        # 열 너비 수동 복구
        col_widths_final = [20, 10, 10, 30, 10, 10, 8, 12]
        for i, width in enumerate(col_widths_final):
            worksheet.set_column(i, i, width)
        
    output.seek(0)
    return output
    
def make_inventory_current_report_excel(df_report: pd.DataFrame, report_type: str, dt_from: date, dt_to: date) -> BytesIO:
    output = BytesIO()
    if df_report.empty:
        return output

    master_df = get_master_df()
    price_history_df = get_price_history_df()
    
    # 조회 기준일(dt_to)의 단가를 가져옴
    df_report['단가'] = df_report.apply(
        lambda row: get_price_at_date(row['품목코드'], dt_to, price_history_df, master_df),
        axis=1
    )
    
    df_merged = pd.merge(df_report, master_df[['품목코드', '품목규격', '단위']], on='품목코드', how='left').fillna('')
    
    df_merged['단가'] = pd.to_numeric(df_merged['단가'], errors='coerce').fillna(0).astype(int)
    df_merged['현재고수량'] = pd.to_numeric(df_merged['현재고수량'], errors='coerce').fillna(0).astype(int)
    df_merged['총금액'] = df_merged['단가'] * df_merged['현재고수량']
    
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        workbook = writer.book
        worksheet = workbook.add_worksheet(report_type)
        
        worksheet.fit_to_pages(1, 0)
        
        # 1. Excel 서식 정의
        fmt_title = workbook.add_format({'bold': True, 'font_size': 22, 'align': 'center', 'valign': 'vcenter', 'border': 1, 'bg_color': '#4F81BD', 'font_color': 'white'})
        fmt_header = workbook.add_format({'bold': True, 'font_size': 9, 'bg_color': '#4F81BD', 'font_color': 'white', 'align': 'center', 'valign': 'vcenter', 'border': 1})
        fmt_text_c = workbook.add_format({'font_size': 9, 'align': 'center', 'valign': 'vcenter', 'border': 1})
        fmt_money_c = workbook.add_format({'font_size': 9, 'num_format': '#,##0', 'align': 'center', 'valign': 'vcenter', 'border': 1})
        fmt_money_bg_c = workbook.add_format({'font_size': 9, 'num_format': '#,##0', 'align': 'center', 'valign': 'vcenter', 'border': 1, 'bg_color': '#DDEBF7'})
        fmt_date_header = workbook.add_format({'bold': True, 'font_size': 10, 'align': 'center', 'valign': 'vcenter', 'indent': 1, 'bg_color': '#EAF1F8', 'border': 1})
        
        # ✨ 합계 행을 위한 서식 추가
        fmt_subtotal_label = workbook.add_format({'bold': True, 'font_size': 11, 'bg_color': '#DDEBF7', 'align': 'center', 'valign': 'vcenter', 'border': 1})
        fmt_subtotal_money = workbook.add_format({'bold': True, 'font_size': 11, 'bg_color': '#DDEBF7', 'num_format': '#,##0 "원"', 'align': 'right', 'valign': 'vcenter', 'border': 1})

        # 2. 데이터 및 열 순서 재정의
        columns_order = ['품목코드', '분류', '품목명', '품목규격', '단위', '단가', '현재고수량', '총금액']
        df_display = df_merged[columns_order].copy()

        # 3. 헤더 영역 작성
        worksheet.set_row(0, 50)
        worksheet.merge_range(0, 0, 0, len(df_display.columns) - 1, '현 재 고 현 황 보 고 서', fmt_title)
        
        # 4. 보고서 정보 (조회 기준일)
        current_row = 2
        worksheet.merge_range(f'A{current_row}:{chr(65 + len(df_display.columns) - 1)}{current_row}', f"조회 기준일: {dt_to}", fmt_date_header)
        current_row += 2

        # 5. 본문 데이터
        headers = ['품목코드', '분류', '품목명', '품목규격', '단위', '단가', '현재고수량', '총금액']
        worksheet.write_row(f'A{current_row}', headers, fmt_header)
        current_row += 1
        
        for _, row in df_display.iterrows():
            worksheet.write(f'A{current_row}', row['품목코드'], fmt_text_c)
            worksheet.write(f'B{current_row}', row['분류'], fmt_text_c)
            worksheet.write(f'C{current_row}', row['품목명'], fmt_text_c)
            worksheet.write(f'D{current_row}', row['품목규격'], fmt_text_c)
            worksheet.write(f'E{current_row}', row['단위'], fmt_text_c)
            worksheet.write(f'F{current_row}', row['단가'], fmt_money_c) # 단가 열 추가
            worksheet.write(f'G{current_row}', row['현재고수량'], fmt_money_bg_c)
            worksheet.write(f'H{current_row}', row['총금액'], fmt_money_bg_c) # 총 금액 열 추가
            current_row += 1

        # ✨ 6. 총 평가금액 합계 행 추가
        current_row += 1 # 한 칸 띄우기
        total_valuation = df_display['총금액'].sum()
        worksheet.merge_range(f'A{current_row}:G{current_row}', '총평가금액', fmt_subtotal_label)
        worksheet.write(f'H{current_row}', total_valuation, fmt_subtotal_money)

        # 7. 최종 열 너비 및 행 높이 설정
        col_widths_final = [10, 10, 30, 10, 8, 10, 10, 15]
        for i, width in enumerate(col_widths_final):
            worksheet.set_column(i, i, width)
        
        for i in range(4, len(df_display) + 5): # 헤더 포함한 행 개수만큼
            worksheet.set_row(i, 20)

    output.seek(0)
    return output

def make_sales_summary_excel(sales_df: pd.DataFrame, daily_pivot: pd.DataFrame, monthly_pivot: pd.DataFrame, summary_data: dict, filter_info: dict) -> BytesIO:
    output = BytesIO()

    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        workbook = writer.book
        
        # 1. 엑셀 서식 정의
        fmt_title = workbook.add_format({'bold': True, 'font_size': 22, 'align': 'center', 'valign': 'vcenter', 'border': 1, 'bg_color': '#4F81BD', 'font_color': 'white'})
        fmt_subtitle = workbook.add_format({'bold': True, 'font_size': 11, 'bg_color': '#DDEBF7', 'align': 'center', 'valign': 'vcenter', 'border': 1})
        fmt_header = workbook.add_format({'bold': True, 'font_size': 9, 'bg_color': '#4F81BD', 'font_color': 'white', 'align': 'center', 'valign': 'vcenter', 'border': 1})
        fmt_info_label = workbook.add_format({'bold': True, 'font_size': 9, 'bg_color': '#F2F2F2', 'align': 'center', 'valign': 'vcenter', 'border': 1})
        
        # ✨ 요청사항: 가운데 정렬을 위한 서식 추가
        fmt_info_data_c = workbook.add_format({'font_size': 9, 'align': 'center', 'valign': 'vcenter', 'border': 1, 'text_wrap': True})

        fmt_money_c_bg = workbook.add_format({'font_size': 9, 'num_format': '#,##0', 'align': 'center', 'valign': 'vcenter', 'border': 1, 'bg_color': '#DDEBF7'})
        fmt_text_c_bg = workbook.add_format({'font_size': 9, 'align': 'center', 'valign': 'vcenter', 'border': 1, 'bg_color': '#DDEBF7'})
        fmt_date_c = workbook.add_format({'align': 'center', 'border': 1})
        fmt_money_r = workbook.add_format({'num_format': '#,##0', 'align': 'right', 'border': 1})
        fmt_total_header_c = workbook.add_format({'bold': True, 'bg_color': '#EAEAEA', 'align': 'center', 'border': 1})
        fmt_total_money_r = workbook.add_format({'bold': True, 'bg_color': '#EAEAEA', 'num_format': '#,##0', 'align': 'right', 'border': 1})
        fmt_percent = workbook.add_format({'num_format': '0.00%', 'border': 1})

        # --- 1. 01_종합_현황 시트 ---
        ws_summary = workbook.add_worksheet('01_종합_현황')
        ws_summary.fit_to_pages(1, 0)
        ws_summary.set_column('A:A', 20)
        ws_summary.set_column('B:B', 25)
        
        ws_summary.merge_range('A1:B1', '매 출 정 산 표', fmt_title)
        ws_summary.merge_range('A2:B2', f"출력일: {now_kst_str()}", workbook.add_format({'font_size': 8, 'align': 'right', 'font_color': '#777777'}))
        
        ws_summary.write('A4', '조회 조건', fmt_subtitle)
        ws_summary.write('A5', '조회 기간', fmt_info_label)
        
        # ✨ 수정된 부분: 가운데 정렬 서식 적용
        ws_summary.write('B5', filter_info['period'], fmt_info_data_c) 
        ws_summary.write('A6', '조회 지점', fmt_info_label)
        # ✨ 수정된 부분: 가운데 정렬 서식 적용
        ws_summary.write('B6', filter_info['store'], fmt_info_data_c)

        ws_summary.write('A8', '주요 지표', fmt_subtitle)
        ws_summary.write('A9', '총 매출 (VAT 포함)', fmt_info_label)
        ws_summary.write('B9', summary_data['total_sales'], fmt_money_c_bg)
        ws_summary.write('A10', '공급가액', fmt_info_label)
        ws_summary.write('B10', summary_data['total_supply'], fmt_money_c_bg)
        ws_summary.write('A11', '부가세액', fmt_info_label)
        ws_summary.write('B11', summary_data['total_tax'], fmt_money_c_bg)
        ws_summary.write('A12', '총 발주 건수', fmt_info_label)
        ws_summary.write('B12', summary_data['total_orders'], fmt_text_c_bg)

        # --- 2. 02_일별_매출현황 시트 ---
        ws_daily = workbook.add_worksheet('02_일별_매출현황')
        daily_display_df = daily_pivot.reset_index()
        ws_daily.fit_to_pages(1, 0)
        
        ws_daily.merge_range(0, 0, 0, len(daily_display_df.columns) - 1, '일 별 매 출 현 황', fmt_title)
        ws_daily.merge_range(1, 0, 1, len(daily_display_df.columns) - 1, f"조회 기간: {filter_info['period']}", fmt_subtitle)
        
        start_row = 3 
        ws_daily.write_row(start_row, 0, daily_display_df.columns, fmt_header)
        
        for r_idx, row_data in daily_display_df.iterrows():
            row_num = start_row + 1 + r_idx
            if row_data['연'] == '합계':
                ws_daily.merge_range(row_num, 0, row_num, 2, '합계', fmt_total_header_c)
                for c_idx, cell_data in enumerate(row_data[3:], 3):
                    ws_daily.write(row_num, c_idx, cell_data, fmt_total_money_r)
            else:
                ws_daily.write(row_num, 0, row_data['연'], fmt_date_c)
                ws_daily.write(row_num, 1, row_data['월'], fmt_date_c)
                ws_daily.write(row_num, 2, row_data['일'], fmt_date_c)
                for c_idx, cell_data in enumerate(row_data[3:], 3):
                    ws_daily.write(row_num, c_idx, cell_data, fmt_money_r)

        ws_daily.set_column(0, 2, 5) 
        ws_daily.set_column(3, len(daily_display_df.columns) - 1, 12)

        # --- 3. 03_월별_매출현황 시트 ---
        ws_monthly = workbook.add_worksheet('03_월별_매출현황')
        monthly_display_df = monthly_pivot.reset_index()
        ws_monthly.fit_to_pages(1, 0)
        
        ws_monthly.merge_range(0, 0, 0, len(monthly_display_df.columns) - 1, '월 별 매 출 현 황', fmt_title)
        ws_monthly.merge_range(1, 0, 1, len(monthly_display_df.columns) - 1, f"조회 기간: {filter_info['period']}", fmt_subtitle)
        
        start_row = 3
        ws_monthly.write_row(start_row, 0, monthly_display_df.columns, fmt_header)

        for r_idx, row_data in monthly_display_df.iterrows():
            row_num = start_row + 1 + r_idx
            if row_data['연'] == '합계':
                ws_monthly.merge_range(row_num, 0, row_num, 1, '합계', fmt_total_header_c)
                for c_idx, cell_data in enumerate(row_data[2:], 2):
                    ws_monthly.write(row_num, c_idx, cell_data, fmt_total_money_r)
            else:
                ws_monthly.write(row_num, 0, row_data['연'], fmt_date_c)
                ws_monthly.write(row_num, 1, row_data['월'], fmt_date_c)
                for c_idx, cell_data in enumerate(row_data[2:], 2):
                    ws_monthly.write(row_num, c_idx, cell_data, fmt_money_r)

        ws_monthly.set_column(0, 1, 5)
        ws_monthly.set_column(2, len(monthly_display_df.columns) - 1, 12)

        # --- 4. 04_지점별_매출순위 시트 ---
        ws_store_rank = workbook.add_worksheet('04_지점별_매출순위')
        ws_store_rank.fit_to_pages(1, 0)
        
        store_sales_df = sales_df.groupby("지점명")["합계금액"].sum().nlargest(10).reset_index()
        store_sales_df.columns = ['지점명', '총 매출액']
        
        store_sales_df.insert(0, 'NO', range(1, 1 + len(store_sales_df)))
        total_sales_for_share = summary_data.get('total_sales', 0)
        if total_sales_for_share > 0:
            store_sales_df['매출 비중 (%)'] = store_sales_df['총 매출액'] / total_sales_for_share
        else:
            store_sales_df['매출 비중 (%)'] = 0

        ws_store_rank.merge_range('A1:D1', '지 점 별 매 출 순 위', fmt_title)
        ws_store_rank.merge_range('A2:D2', f"조회 기간: {filter_info['period']}", fmt_subtitle)
        
        ws_store_rank.write_row('A4', store_sales_df.columns, fmt_header)
        
        for row_num, row_data in store_sales_df.iterrows():
            ws_store_rank.write(row_num + 4, 0, row_data['NO'], fmt_date_c)
            ws_store_rank.write(row_num + 4, 1, row_data['지점명'], fmt_date_c)
            ws_store_rank.write(row_num + 4, 2, row_data['총 매출액'], fmt_money_r)
            ws_store_rank.write(row_num + 4, 3, row_data['매출 비중 (%)'], fmt_percent)
        
        ws_store_rank.conditional_format(f'D5:D{4+len(store_sales_df)}', {'type': 'data_bar', 'bar_color': '#4F81BD'})

        ws_store_rank.set_column('A:A', 5)
        ws_store_rank.set_column('B:B', 20)
        ws_store_rank.set_column('C:D', 15)

        # --- 5. 05_품목별_판매순위 시트 ---
        ws_item_rank = workbook.add_worksheet('05_품목별_판매순위')
        ws_item_rank.fit_to_pages(1, 0)

        if not sales_df.empty:
            item_sales_df = sales_df.groupby("품목명").agg(
                총판매수량=('수량', 'sum'), 총매출액=('합계금액', 'sum')
            ).nlargest(10, '총매출액').reset_index()
            
            item_sales_df.insert(0, 'NO', range(1, 1 + len(item_sales_df)))
            if total_sales_for_share > 0:
                item_sales_df['매출 비중 (%)'] = item_sales_df['총매출액'] / total_sales_for_share
            else:
                item_sales_df['매출 비중 (%)'] = 0
        else:
            item_sales_df = pd.DataFrame(columns=['NO', '품목명', '총판매수량', '총매출액', '매출 비중 (%)'])

        ws_item_rank.merge_range('A1:E1', '품 목 별 판 매 순 위', fmt_title)
        ws_item_rank.merge_range('A2:E2', f"조회 기간: {filter_info['period']}", fmt_subtitle)
        ws_item_rank.write_row('A4', item_sales_df.columns, fmt_header)
        
        for row_num, row_data in item_sales_df.iterrows():
            ws_item_rank.write(row_num + 4, 0, row_data['NO'], fmt_date_c)
            ws_item_rank.write(row_num + 4, 1, row_data['품목명'], fmt_date_c)
            ws_item_rank.write(row_num + 4, 2, row_data['총판매수량'], fmt_money_r)
            ws_item_rank.write(row_num + 4, 3, row_data['총매출액'], fmt_money_r)
            ws_item_rank.write(row_num + 4, 4, row_data['매출 비중 (%)'], fmt_percent)

        ws_item_rank.conditional_format(f'E5:E{4+len(item_sales_df)}', {'type': 'data_bar', 'bar_color': '#C0504D'})
        
        ws_item_rank.set_column('A:A', 5)
        ws_item_rank.set_column('B:B', 30)
        ws_item_rank.set_column('C:E', 15)
        
    output.seek(0)
    return output
    
# =============================================================================
# 5) 유틸리티 함수
# =============================================================================
def init_session_state():
    defaults = {
        "cart": pd.DataFrame(columns=CONFIG['CART']['cols']),
        "store_editor_ver": 0, "production_cart": pd.DataFrame(),
        "production_date_to_log": date.today(), "production_change_reason": "",
        "production_editor_ver": 0, "success_message": "", "error_message": "",
        "warning_message": "", "store_orders_selection": {}, "admin_orders_selection": {},
        "charge_type_radio": "선충전", "charge_amount": 1000, "charge_type_index": 0,
        "confirm_action": None, "confirm_data": None,
        "report_df": pd.DataFrame(), "report_info": {}
    }
    for key, value in defaults.items():
        if key not in st.session_state: st.session_state[key] = value

def coerce_cart_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    cart_cols = CONFIG['CART']['cols']
    for col in cart_cols:
        if col not in out.columns: out[col] = 0 if '금액' in col or '단가' in col or '수량' in col else ""
    out["수량"] = pd.to_numeric(out["수량"], errors="coerce").fillna(0).astype(int)
    out["단가"] = pd.to_numeric(out["단가"], errors="coerce").fillna(0).astype(int)
    out["단가(VAT포함)"] = pd.to_numeric(out["단가(VAT포함)"], errors="coerce").fillna(0).astype(int)
    out["합계금액(VAT포함)"] = out["단가(VAT포함)"] * out["수량"]
    return out[cart_cols]

def add_to_cart(rows_df: pd.DataFrame, master_df: pd.DataFrame):
    add_with_qty = rows_df[rows_df["수량"] > 0].copy()
    if add_with_qty.empty: return

    add_merged = pd.merge(add_with_qty, master_df[['품목코드', '과세구분']], on='품목코드', how='left')
    add_merged['단가(VAT포함)'] = add_merged.apply(get_vat_inclusive_price, axis=1)
    
    cart = st.session_state.cart.copy()
    
    merged = pd.concat([cart, add_merged]).groupby("품목코드", as_index=False).agg({
        "분류": "last", "품목명": "last", "단위": "last", "단가": "last", 
        "단가(VAT포함)": "last", "수량": "sum"
    })
    
    merged["합계금액(VAT포함)"] = merged["단가(VAT포함)"] * merged["수량"]
    st.session_state.cart = merged[CONFIG['CART']['cols']]

@st.cache_data(ttl=60)
def get_inventory_from_log(master_df: pd.DataFrame, target_date: date = None) -> pd.DataFrame:
    if target_date is None:
        target_date = date.today()

    log_df = get_inventory_log_df()
    
    if log_df.empty:
        inventory_df = master_df[['품목코드', '분류', '품목명']].copy()
        inventory_df['현재고수량'] = 0
        return inventory_df

    if not pd.api.types.is_datetime64_any_dtype(log_df['작업일자']):
        log_df['작업일자'] = pd.to_datetime(log_df['작업일자'], errors='coerce')

    log_df.dropna(subset=['작업일자'], inplace=True)
    filtered_log = log_df[log_df['작업일자'].dt.date <= target_date]

    if filtered_log.empty:
        inventory_df = master_df[['품목코드', '분류', '품목명']].copy()
        inventory_df['현재고수량'] = 0
        return inventory_df

    calculated_stock = filtered_log.groupby('품목코드')['수량변경'].sum().reset_index()
    calculated_stock.rename(columns={'수량변경': '현재고수량'}, inplace=True)

    final_inventory = pd.merge(
        master_df[['품목코드', '분류', '품목명']],
        calculated_stock,
        on='품목코드',
        how='left'
    )
    final_inventory['현재고수량'] = final_inventory['현재고수량'].fillna(0).astype(int)
    return final_inventory

def update_inventory(items_to_update: pd.DataFrame, change_type: str, handler: str, working_date: date, ref_id: str = "", reason: str = ""):
    if items_to_update.empty:
        return True

    master_df_for_inv = get_master_df()
    inventory_before_change = get_inventory_from_log(master_df_for_inv)
    
    log_rows = []
    
    for _, item in items_to_update.iterrows():
        item_code = item['품목코드']
        item_name = item['품목명']
        quantity_change = int(item['수량변경'])
        
        current_stock_series = inventory_before_change[inventory_before_change['품목코드'] == item_code]
        current_stock = 0
        if not current_stock_series.empty:
            current_stock = current_stock_series.iloc[0]['현재고수량']
            
        new_stock = current_stock + quantity_change
        
        log_rows.append({
            "로그일시": now_kst_str(), "작업일자": working_date.strftime('%Y-%m-%d'),
            "품목코드": item_code, "품목명": item_name, "구분": change_type, 
            "수량변경": int(quantity_change), "처리후재고": int(new_stock), 
            "관련번호": ref_id, "처리자": handler, "사유": reason
        })

    if append_rows_to_sheet(CONFIG['INVENTORY_LOG']['name'], log_rows, CONFIG['INVENTORY_LOG']['cols']):
        clear_data_cache()
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
            
            edited_disp = st.data_editor(
                df_edit[CONFIG['CART']['cols'][:-1]],
                key=f"editor_v{st.session_state.store_editor_ver}", 
                hide_index=True, 
                disabled=["품목코드", "분류", "품목명", "단위", "단가", "단가(VAT포함)"], 
                use_container_width=True 
            )
            
            if st.form_submit_button("장바구니 추가", use_container_width=True, type="primary"):
                edited_df = pd.DataFrame(edited_disp)
                # [개선] 음수 값 입력 방지 로직
                if (pd.to_numeric(edited_df['수량'], errors='coerce') < 0).any():
                    st.session_state.error_message = "발주 수량은 0 이상이어야 합니다. 음수 값을 수정해주세요."
                    st.rerun()
                
                items_to_add = coerce_cart_df(edited_df)
                if not items_to_add[items_to_add["수량"] > 0].empty:
                    add_to_cart(items_to_add, master_df)
                    st.session_state.store_editor_ver += 1
                    st.session_state.success_message = "선택한 품목이 장바구니에 추가되었습니다."
                else:
                    st.session_state.warning_message = "장바구니에 추가할 품목의 수량을 입력해주세요."
                st.rerun()

    v_spacer(16)
    
    with st.container(border=True):
        st.markdown("##### 🧺 장바구니 및 최종 확인")
        cart_now = st.session_state.cart.copy()

        if '분류' not in cart_now.columns and not cart_now.empty:
            cart_now = pd.merge(
                cart_now.drop(columns=['분류'], errors='ignore'),
                master_df[['품목코드', '분류']],
                on='품목코드', how='left'
            )
            cart_now['분류'] = cart_now['분류'].fillna('미지정')
            st.session_state.cart = cart_now.copy()
        
        if cart_now.empty:
            st.info("장바구니가 비어 있습니다.")
        else:
            st.dataframe(cart_now[CONFIG['CART']['cols']], hide_index=True, use_container_width=True)
            
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
                            rows.append({"주문일시": now_kst_str(), "발주번호": order_id, "지점ID": user["user_id"], "지점명": user["name"], "품목코드": r["품목코드"], "품목명": r["품목명"], "단위": r["단위"], "수량": r["수량"], "단가": r["단가"], "공급가액": r['공급가액'], "세액": r['세액'], "합계금액": r['합계금액_final'], "비고": memo, "상태": CONFIG['ORDER_STATUS']['PENDING'], "처리자": "", "처리일시": "", "반려사유":""})

                        if payment_method == "선충전 잔액 결제":
                            new_balance = prepaid_balance - total_final_amount_sum
                            new_used_credit = used_credit
                            trans_desc = "선충전결제"
                        else: # 여신 결제
                            new_balance = prepaid_balance
                            new_used_credit = used_credit + total_final_amount_sum
                            trans_desc = "여신결제"

                        # [안정성] 기록 -> 처리 -> 금액 변경 순서 적용 (발주 요청은 선결제 방식이므로 금액 변경부터)
                        try:
                            # 1. 발주서 기록
                            if not append_rows_to_sheet(CONFIG['ORDERS']['name'], rows, CONFIG['ORDERS']['cols']):
                                raise Exception("발주 기록 실패")
                            
                            # 2. 거래내역 기록
                            transaction_record = {
                                "일시": now_kst_str(), "지점ID": user["user_id"], "지점명": user["name"],
                                "구분": trans_desc, "내용": f"{cart_now.iloc[0]['품목명']} 등 {len(cart_now)}건 발주",
                                "금액": -total_final_amount_sum, "처리후선충전잔액": new_balance,
                                "처리후사용여신액": new_used_credit, "관련발주번호": order_id, "처리자": user["name"]
                            }
                            if not append_rows_to_sheet(CONFIG['TRANSACTIONS']['name'], [transaction_record], CONFIG['TRANSACTIONS']['cols']):
                                 raise Exception("거래내역 기록 실패, 발주 기록 롤백 필요")

                            # 3. 모든 기록 성공 후 실제 잔액 변경
                            if not update_balance_sheet(user["user_id"], {"선충전잔액": new_balance, "사용여신액": new_used_credit}):
                                raise Exception("최종 결제 처리 실패, 수동 확인 필요")

                            st.session_state.success_message = "발주 및 결제가 성공적으로 완료되었습니다."
                            st.session_state.cart = pd.DataFrame(columns=CONFIG['CART']['cols'])
                            clear_data_cache()
                            st.rerun()

                        except Exception as e:
                            st.session_state.error_message = f"발주 처리 중 오류가 발생했습니다: {e}. 문제가 지속되면 관리자에게 문의하세요."
                            # 롤백 로직 추가 가능 (예: 생성된 발주서 삭제 등)
                            st.rerun()
                            
                with c2:
                    if st.form_submit_button("🗑️ 장바구니 비우기", use_container_width=True):
                        st.session_state.cart = pd.DataFrame(columns=CONFIG['CART']['cols'])
                        st.session_state.success_message = "장바구니를 비웠습니다."
                        st.rerun()
                        
def page_store_balance(charge_requests_df: pd.DataFrame, balance_info: pd.Series):
    st.subheader("💰 결제 관리")
    user = st.session_state.auth

    # ### 1번 수정: 입력 필드 초기화 로직을 함수 최상단으로 이동 ###
    # 'reset_form' 요청이 있으면, 위젯이 그려지기 전에 값을 먼저 초기화합니다.
    if st.session_state.get("reset_form"):
        st.session_state.depositor_name_input = ""
        if st.session_state.charge_type_radio == '선충전':
            st.session_state.charge_amount = 0
        # 요청 처리 후 플래그 삭제
        del st.session_state.reset_form

    prepaid_balance = int(balance_info.get('선충전잔액', 0))
    credit_limit = int(balance_info.get('여신한도', 0))
    used_credit = int(balance_info.get('사용여신액', 0))
    available_credit = credit_limit - used_credit
    
    with st.container(border=True):
        c1, c2, c3 = st.columns(3)
        c1.metric("선충전 잔액", f"{prepaid_balance:,}원")
        c2.metric("사용 여신액", f"{used_credit:,}원")
        c3.metric("사용 가능 여신", f"{available_credit:,}원", delta=f"한도: {credit_limit:,}원", delta_color="off")
    
    st.info("**입금 계좌: OOO은행 123-456-789 (주)산카쿠**\n\n위 계좌로 입금하신 후, 아래 양식을 작성하여 '알림 보내기' 버튼을 눌러주세요.")
    
    my_pending_repayments = charge_requests_df[
        (charge_requests_df['지점ID'] == user['user_id']) &
        (charge_requests_df['상태'] == '요청') &
        (charge_requests_df['종류'] == '여신상환')
    ]
    pending_repayment_sum = int(my_pending_repayments['입금액'].sum())
    
    repayable_amount = max(0, used_credit - pending_repayment_sum)

    if pending_repayment_sum > 0:
        st.warning(f"현재 처리 대기 중인 여신상환 요청 금액 {pending_repayment_sum:,.0f}원이 있습니다.\n\n해당 금액을 제외한 **{repayable_amount:,.0f}원**으로 상환 요청이 생성됩니다.")

    def on_charge_type_change():
        if st.session_state.charge_type_radio == '여신상환':
            st.session_state.charge_amount = repayable_amount
        else:
            st.session_state.charge_amount = 0

    charge_type = st.radio(
        "종류 선택", ["선충전", "여신상환"], 
        key="charge_type_radio", 
        horizontal=True,
        on_change=on_charge_type_change
    )

    is_disabled = st.session_state.charge_type_radio == '여신상환'

    with st.form("charge_request_form", border=True):
        st.markdown(f"##### {charge_type} 알림 보내기")
        c1, c2 = st.columns(2)
        
        depositor_name = c1.text_input("입금자명", key="depositor_name_input")
        
        charge_amount = c2.number_input(
            "입금액", min_value=0, step=1000, 
            key="charge_amount", disabled=is_disabled
        )
        
        if st.form_submit_button("알림 보내기", type="primary"):
            if depositor_name and charge_amount >= 0:
                if charge_type == '선충전' and charge_amount == 0:
                    st.warning("선충전 입금액은 0원 이상이어야 합니다.")
                else:
                    new_request = {
                        "요청일시": now_kst_str(), "지점ID": user["user_id"], "지점명": user["name"],
                        "입금자명": depositor_name, "입금액": charge_amount, "종류": charge_type, "상태": "요청", "처리사유": ""
                    }
                    if append_rows_to_sheet(CONFIG['CHARGE_REQ']['name'], [new_request], CONFIG['CHARGE_REQ']['cols']):
                        st.session_state.success_message = "관리자에게 입금 완료 알림을 보냈습니다. 확인 후 처리됩니다."
                        
                        # ### 2번 수정: 직접 값을 바꾸는 대신, 'reset_form' 플래그를 설정 ###
                        st.session_state.reset_form = True
                        
                        clear_data_cache()
                        st.rerun()
                    else: 
                        st.session_state.error_message = "알림 전송에 실패했습니다."
                        st.rerun()
            else: 
                st.warning("입금자명과 입금액을 올바르게 입력해주세요.")
            
    st.markdown("---")
    st.markdown("##### 나의 충전/상환 요청 현황")
    my_requests = charge_requests_df[charge_requests_df['지점ID'] == user['user_id']]
    st.dataframe(my_requests, use_container_width=True, hide_index=True)

def page_store_orders_change(store_info_df: pd.DataFrame, master_df: pd.DataFrame):
    st.subheader("🧾 발주 조회")

    if 'cancel_ids' in st.session_state and st.session_state.cancel_ids:
        with st.spinner("발주 취소 및 환불 처리 중..."):
            ids_to_process = st.session_state.pop('cancel_ids')
            
            df_all_transactions = get_transactions_df()
            df_balance = get_balance_df()
            user = st.session_state.auth
            
            success_count = 0
            fail_count = 0

            for order_id in ids_to_process:
                original_transaction = df_all_transactions[df_all_transactions['관련발주번호'] == order_id]
                if original_transaction.empty:
                    st.session_state.warning_message = f"발주번호 {order_id}의 원본 거래내역이 없어 환불 처리를 건너뜁니다."
                    fail_count += 1
                    continue

                trans_info = original_transaction.iloc[0]
                balance_info_df = df_balance[df_balance['지점ID'] == user['user_id']]
                
                if balance_info_df.empty:
                    st.session_state.error_message = f"'{user['name']}'님의 잔액 정보를 찾을 수 없습니다."
                    fail_count += 1
                    continue

                balance_info = balance_info_df.iloc[0]
                current_prepaid, current_used_credit = int(balance_info['선충전잔액']), int(balance_info['사용여신액'])
                
                refund_amount = abs(int(trans_info['금액']))
                
                new_prepaid, new_used_credit = current_prepaid, current_used_credit
                if trans_info['구분'] == '선충전결제':
                    new_prepaid += refund_amount
                else: # 여신결제
                    new_used_credit -= refund_amount

                refund_record = {
                    "일시": now_kst_str(), "지점ID": user["user_id"], "지점명": user["name"],
                    "구분": "발주취소", "내용": f"발주번호 {order_id} 취소 환불",
                    "금액": refund_amount, "처리후선충전잔액": new_prepaid,
                    "처리후사용여신액": new_used_credit, "관련발주번호": order_id, "처리자": user["name"]
                }
                
                # [안정성] 기록 -> 처리 -> 금액 변경 순서 적용
                try:
                    # 1. 발주 상태 먼저 '취소'로 변경
                    if not update_order_status([order_id], CONFIG['ORDER_STATUS']['CANCELED_STORE'], user["name"]):
                        raise Exception("발주 상태 변경 실패")
                        
                    # 2. 거래내역에 환불 기록 추가
                    if not append_rows_to_sheet(CONFIG['TRANSACTIONS']['name'], [refund_record], CONFIG['TRANSACTIONS']['cols']):
                        update_order_status([order_id], CONFIG['ORDER_STATUS']['PENDING'], "system_rollback") 
                        raise Exception("거래내역 기록 실패")
                        
                    # 3. 모든 기록과 처리가 성공한 후, 최종적으로 실제 잔액(돈) 변경
                    if not update_balance_sheet(user["user_id"], {"선충전잔액": new_prepaid, "사용여신액": new_used_credit}):
                        st.session_state.error_message = f"CRITICAL ERROR: {order_id} 환불 금액이 기록되었으나 잔액 반영에 실패했습니다. 즉시 수동 조치가 필요합니다!"
                        fail_count += 1
                        continue
                    
                    success_count += 1

                except Exception as e:
                    fail_count += 1
                    st.session_state.error_message = f"발주번호 {order_id} 처리 중 오류 발생: {e}. 데이터가 원상 복구되었을 수 있습니다."

            if success_count > 0:
                st.session_state.success_message = f"{success_count}건의 발주가 취소되고 환불 처리되었습니다."
            if fail_count > 0:
                 st.session_state.warning_message = f"{fail_count}건의 발주 취소에 실패했습니다."

            st.session_state.store_orders_selection = {}
            clear_data_cache()
            st.rerun()

    df_all_orders = get_orders_df()
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
        if not pd.api.types.is_datetime64_any_dtype(df_filtered['주문일시']):
            df_filtered['주문일시'] = pd.to_datetime(df_filtered['주문일시'], errors='coerce')
        df_filtered.dropna(subset=['주문일시'], inplace=True)
        df_filtered = df_filtered[(df_filtered['주문일시'].dt.date >= dt_from) & (df_filtered['주문일시'].dt.date <= dt_to)]
    
    orders = df_filtered.groupby("발주번호").agg(
        주문일시=("주문일시", "first"), 건수=("품목코드", "count"), 
        합계금액=("합계금액", "sum"), 상태=("상태", "first"), 
        처리일시=("처리일시", "first"), 반려사유=("반려사유", "first")
    ).reset_index().sort_values("주문일시", ascending=False)
    
    pending = orders[orders["상태"] == "요청"].copy()
    shipped = orders[orders["상태"].isin(["승인", "출고완료"])].copy()
    rejected = orders[orders["상태"].isin(["반려", "취소", "승인취소"])].copy()

    tab1, tab2, tab3 = st.tabs([f"요청 ({len(pending)}건)", f"승인/출고 ({len(shipped)}건)", f"반려/취소 ({len(rejected)}건)"])
    
    def handle_multiselect(key, source_df):
        edits = st.session_state[key].get("edited_rows", {})
        for row_index, changed_data in edits.items():
            if "선택" in changed_data:
                order_id = source_df.iloc[row_index]['발주번호']
                st.session_state.store_orders_selection[order_id] = changed_data["선택"]

    with tab1:
        pending_display = pending.copy()
        pending_display.insert(0, '선택', pending['발주번호'].apply(lambda x: st.session_state.store_orders_selection.get(x, False)))
        st.data_editor(
            pending_display[['선택', '주문일시', '발주번호', '건수', '합계금액', '상태']], 
            hide_index=True, use_container_width=True, key="pending_editor", 
            disabled=pending_display.columns.drop('선택'),
            on_change=handle_multiselect, kwargs={"key": "pending_editor", "source_df": pending}
        )
        
        selected_to_cancel = [oid for oid, selected in st.session_state.store_orders_selection.items() if selected and oid in pending['발주번호'].values]
        
        if st.button("선택한 발주 요청 취소하기", disabled=not selected_to_cancel, type="primary"):
            st.session_state.cancel_ids = selected_to_cancel
            st.rerun()
    
    with tab2:
        shipped_display = shipped.copy()
        shipped_display.insert(0, '선택', [st.session_state.store_orders_selection.get(x, False) for x in shipped['발주번호']])
        st.data_editor(
            shipped_display[['선택', '주문일시', '발주번호', '건수', '합계금액', '상태', '처리일시']], 
            hide_index=True, use_container_width=True, key="shipped_editor", 
            disabled=shipped_display.columns.drop('선택'),
            on_change=handle_multiselect, kwargs={"key": "shipped_editor", "source_df": shipped}
        )

    with tab3:
        rejected_display = rejected.copy()
        rejected_display.insert(0, '선택', [st.session_state.store_orders_selection.get(x, False) for x in rejected['발주번호']])
        st.data_editor(
            rejected_display[['선택', '주문일시', '발주번호', '건수', '합계금액', '상태', '반려사유']], 
            hide_index=True, use_container_width=True, key="rejected_editor", 
            disabled=rejected_display.columns.drop('선택'),
            on_change=handle_multiselect, kwargs={"key": "rejected_editor", "source_df": rejected}
        )

    v_spacer(16)
    
    with st.container(border=True):
        st.markdown("##### 📄 발주 품목 상세 조회")
        
        selected_ids = [k for k, v in st.session_state.store_orders_selection.items() if v]
        
        if len(selected_ids) == 1:
            target_id = selected_ids[0]
            target_df = df_user[df_user["발주번호"] == target_id]
            
            if not target_df.empty:
                total_amount = target_df['합계금액'].sum()
                memo = target_df['비고'].iloc[0] if '비고' in target_df.columns else ""

                st.markdown(f"**선택된 발주번호:** `{target_id}` / **총 합계금액(VAT포함):** `{total_amount:,.0f}원`")

                if pd.notna(memo) and memo.strip():
                    st.markdown("**요청사항:**")
                    st.text_area("", value=memo, height=80, disabled=True, label_visibility="collapsed")
                
                display_df = pd.merge(target_df, master_df[['품목코드', '과세구분']], on='품목코드', how='left')
                display_df['단가(VAT포함)'] = display_df.apply(get_vat_inclusive_price, axis=1)
                display_df.rename(columns={'합계금액': '합계금액(VAT포함)'}, inplace=True)
                
                st.dataframe(display_df[["품목코드", "품목명", "단위", "수량", "단가(VAT포함)", "합계금액(VAT포함)"]], hide_index=True, use_container_width=True)

                if target_df.iloc[0]['상태'] in ["승인", "출고완료"]:
                    supplier_info_df = store_info_df[store_info_df['역할'] == 'admin']
                    customer_info_df = store_info_df[store_info_df['지점ID'] == user['user_id']]
                    if not supplier_info_df.empty and not customer_info_df.empty:
                        supplier_info = supplier_info_df.iloc[0]
                        customer_info = customer_info_df.iloc[0]
                        buf = create_unified_item_statement(target_df, supplier_info, customer_info)
                        
                        st.download_button("📄 품목거래내역서 다운로드", data=buf, file_name=f"품목거래내역서_{user['name']}_{target_id}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True, type="primary")

        elif len(selected_ids) > 1:
            st.info("상세 내용을 보려면 발주를 **하나만** 선택하세요.")
        else:
            st.info("상세 내용을 보려면 위 목록에서 발주를 선택하세요.")
            
def page_store_documents(store_info_df: pd.DataFrame, master_df: pd.DataFrame):
    st.subheader("📑 증빙서류 다운로드")
    user = st.session_state.auth
    
    c1, c2, c3, c4 = st.columns(4)
    dt_from = c1.date_input("조회 시작일", date.today() - timedelta(days=30), key="store_doc_from")
    dt_to = c2.date_input("조회 종료일", date.today(), key="store_doc_to")
    
    doc_type = c3.selectbox("서류 종류", ["금전거래내역서", "품목거래내역서"])
    
    if doc_type == "금전거래내역서":
        c4.empty()
        transactions_df_all = get_transactions_df()
        my_transactions = transactions_df_all[transactions_df_all['지점ID'] == user['user_id']]
        if my_transactions.empty: 
            st.info("거래 내역이 없습니다.")
            return
        
        my_transactions['일시_dt'] = pd.to_datetime(my_transactions['일시'], errors='coerce').dt.date
        my_transactions.dropna(subset=['일시_dt'], inplace=True)
        mask = (my_transactions['일시_dt'] >= dt_from) & (my_transactions['일시_dt'] <= dt_to)
        dfv = my_transactions.loc[mask].copy()
        if dfv.empty: 
            st.warning("해당 기간의 거래 내역이 없습니다.")
            return
            
        st.dataframe(dfv.drop(columns=['일시_dt']), use_container_width=True, hide_index=True)
        
        customer_info_df = store_info_df[store_info_df['지점ID'] == user['user_id']]
        supplier_info_df = store_info_df[store_info_df['역할'] == 'admin']

        if not customer_info_df.empty and not supplier_info_df.empty:
            customer_info = customer_info_df.iloc[0]
            supplier_info = supplier_info_df.iloc[0]

            buf = create_unified_financial_statement(dfv, transactions_df_all, supplier_info, customer_info)
            
            st.download_button("엑셀 다운로드", data=buf, file_name=f"금전거래내역서_{user['name']}_{dt_from}_to_{dt_to}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True, type="primary")
        else:
            st.error("엑셀 생성에 필요한 공급자 또는 지점 정보가 마스터 시트에 없습니다.")

    elif doc_type == "품목거래내역서":
        orders_df = get_orders_df()
        my_orders = orders_df[(orders_df['지점ID'] == user['user_id']) & (orders_df['상태'].isin(['승인', '출고완료']))]
        
        if my_orders.empty:
            st.warning("승인/출고된 발주 내역이 없습니다.")
            return

        my_orders['주문일시_dt'] = pd.to_datetime(my_orders['주문일시'], errors='coerce').dt.date
        my_orders.dropna(subset=['주문일시_dt'], inplace=True)
        filtered_orders = my_orders.loc[my_orders['주문일시_dt'].between(dt_from, dt_to)].copy()
        
        if filtered_orders.empty:
            st.warning("선택한 기간 내에 승인/출고된 발주 내역이 없습니다.")
            return

        order_options = ["(기간 전체)"] + sorted(filtered_orders['발주번호'].unique().tolist())
        selected_order_id = c4.selectbox("발주번호 선택", order_options, key="store_doc_order_select")

        supplier_info_df = store_info_df[store_info_df['역할'] == 'admin']
        customer_info_df = store_info_df[store_info_df['지점ID'] == user['user_id']]
        
        if supplier_info_df.empty or customer_info_df.empty:
            st.error("내역서 출력에 필요한 공급자 또는 지점 정보가 없습니다.")
            return
            
        supplier_info = supplier_info_df.iloc[0]
        customer_info = customer_info_df.iloc[0]

        if selected_order_id == "(기간 전체)":
            preview_df = filtered_orders
        else:
            preview_df = filtered_orders[filtered_orders['발주번호'] == selected_order_id]
        
        st.dataframe(preview_df, use_container_width=True, hide_index=True)
        if not preview_df.empty:
            buf = create_unified_item_statement(preview_df, supplier_info, customer_info)
            download_label = "기간 전체 내역서" if selected_order_id == "(기간 전체)" else f"'{selected_order_id}' 내역서"
            
            # ✨✨✨ 수정된 부분: 닫는 괄호 ')'와 type="primary" 추가 ✨✨✨
            st.download_button(f"{download_label} 다운로드", data=buf, file_name=f"품목거래내역서_{user['name']}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True, type="primary")

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

def page_store_my_info():
    st.subheader("👤 내 정보 관리")
    user = st.session_state.auth
    
    with st.form("change_password_form", border=True):
        st.markdown("##### 🔑 비밀번호 변경")
        current_password = st.text_input("현재 비밀번호", type="password")
        new_password = st.text_input("새 비밀번호", type="password")
        confirm_password = st.text_input("새 비밀번호 확인", type="password")
        
        if st.form_submit_button("비밀번호 변경", type="primary", use_container_width=True):
            if not (current_password and new_password and confirm_password):
                st.warning("모든 필드를 입력해주세요.")
                return

            stores_df = get_stores_df()
            user_info = stores_df[stores_df['지점ID'] == user['user_id']].iloc[0]
            
            if user_info['지점PW'] != hash_password(current_password):
                st.error("현재 비밀번호가 일치하지 않습니다.")
                return
            
            if new_password != confirm_password:
                st.error("새 비밀번호가 일치하지 않습니다.")
                return

            try:
                ws = open_spreadsheet().worksheet(CONFIG['STORES']['name'])
                cell = ws.find(user['user_id'], in_column=1)
                pw_col_index = ws.row_values(1).index('지점PW') + 1
                ws.update_cell(cell.row, pw_col_index, hash_password(new_password))
                
                clear_data_cache()
                st.session_state.success_message = "비밀번호가 성공적으로 변경되었습니다."
                st.rerun()
            except Exception as e:
                st.error(f"비밀번호 변경 중 오류가 발생했습니다: {e}")

# =============================================================================
# 7) 관리자 페이지 (UX 및 코드 품질 개선 적용)
# =============================================================================

# [신규] 관리자 활동 로그 조회 페이지
def page_admin_audit_log():
    st.subheader("📜 활동 로그 조회")

    with st.expander("도움말: 활동 로그는 무엇인가요?"):
        st.markdown("""
        **활동 로그**는 시스템 내에서 발생하는 모든 중요한 데이터 변경 이력을 자동으로 기록하는 공간입니다.
        이를 통해 **'언제, 누가, 무엇을, 어떻게'** 변경했는지 투명하게 추적하여 시스템의 안정성과 보안을 강화할 수 있습니다.
        """)
    
    try:
        audit_log_df = load_data(CONFIG['AUDIT_LOG']['name'], CONFIG['AUDIT_LOG']['cols'])
    except gspread.WorksheetNotFound:
        st.warning("'활동로그' 시트를 찾을 수 없습니다. 로그가 기록되면 자동으로 생성됩니다.")
        return
        
    if audit_log_df.empty:
        st.info("활동 기록이 없습니다.")
        return

    c1, c2, c3 = st.columns(3)
    default_start = audit_log_df['로그일시'].min().date() if not audit_log_df.empty else date.today()
    dt_from = c1.date_input("조회 시작일", default_start, key="audit_log_from")
    dt_to = c2.date_input("조회 종료일", date.today(), key="audit_log_to")

    user_list = ["(전체)"] + sorted(audit_log_df["변경자 이름"].dropna().unique().tolist())
    user_filter = c3.selectbox("변경자 필터", user_list, key="audit_log_user")
    
    filtered_df = audit_log_df[
        (audit_log_df['로그일시'].dt.date >= dt_from) &
        (audit_log_df['로그일시'].dt.date <= dt_to)
    ]
    if user_filter != "(전체)":
        filtered_df = filtered_df[filtered_df["변경자 이름"] == user_filter]

    st.markdown(f"총 **{len(filtered_df)}**개의 기록이 조회되었습니다.")
    
    page_size = 20
    page_number = render_paginated_ui(len(filtered_df), page_size, "audit_log")
    start_idx = (page_number - 1) * page_size
    end_idx = start_idx + page_size
    
    st.dataframe(filtered_df.iloc[start_idx:end_idx], use_container_width=True, hide_index=True)

def perform_initial_audit():
    with st.spinner("시스템 상태를 자동으로 점검하는 중입니다..."):
        stores_df = get_stores_df()
        master_df = get_master_df()
        orders_df = get_orders_df()
        balance_df = get_balance_df()
        transactions_df = get_transactions_df()
        inventory_log_df = get_inventory_log_df()
        charge_req_df = get_charge_requests_df() # ✅ 충전 요청 데이터 불러오기 추가

        results = {}
        # ✅ audit_financial_data에 charge_req_df 전달
        results['financial'] = audit_financial_data(balance_df, transactions_df, charge_req_df)
        results['links'] = audit_transaction_links(transactions_df, orders_df)
        results['inventory'] = audit_inventory_logs(inventory_log_df, orders_df)
        results['integrity'] = audit_data_integrity(orders_df, transactions_df, stores_df, master_df)
        
        st.session_state['audit_results'] = results
        st.session_state['initial_audit_done'] = True
        
def audit_financial_data(balance_df, transactions_df, charge_req_df):
    issues = []
    store_ids = balance_df[balance_df['지점ID'] != ''].dropna(subset=['지점ID'])['지점ID'].unique()
    
    for store_id in store_ids:
        store_balance = balance_df[balance_df['지점ID'] == store_id].iloc[0]
        store_tx = transactions_df[transactions_df['지점ID'] == store_id]
        
        # --- 선충전 잔액 감사: '거래내역' 시트의 '선충전승인' 내역만 합산 ---
        # 수정: '요청' 상태는 포함하지 않고, 'transactions' 시트에 기록된 최종 내역만 계산
        processed_prepaid_tx = store_tx[store_tx['구분'].str.contains('선충전|발주취소|발주반려|수동조정\(충전\)', na=False)]
        calculated_prepaid = processed_prepaid_tx['금액'].sum()
        
        master_prepaid = int(store_balance['선충전잔액'])

        if master_prepaid != calculated_prepaid:
            issues.append(f"- **{store_balance['지점명']}**: 선충전 잔액 불일치 (장부: {master_prepaid: ,}원 / 계산: {calculated_prepaid: ,}원)")
        
        # --- 사용 여신액 감사 로직 (변경 없음) ---
        credit_tx = store_tx[store_tx['구분'].str.contains('여신결제|여신상환|수동조정\(여신\)', na=False)]
        calculated_credit = credit_tx[credit_tx['구분'].str.contains('여신결제', na=False)]['금액'].abs().sum() - credit_tx[credit_tx['구분'].str.contains('여신상환', na=False)]['금액'].abs().sum()
        master_credit = int(store_balance['사용여신액'])

        if master_credit != calculated_credit:
            issues.append(f"- **{store_balance['지점명']}**: 사용 여신액 불일치 (장부: {master_credit: ,}원 / 계산: {calculated_credit: ,}원)")

    if issues:
        return "❌ 오류", issues
    return "✅ 정상", []

def audit_transaction_links(transactions_df, orders_df):
    issues = []
    order_related_tx = transactions_df[transactions_df['구분'].str.contains('발주|여신결제', na=False)]
    valid_order_ids = set(orders_df['발주번호'])
    for _, tx in order_related_tx.iterrows():
        order_id = tx['관련발주번호']
        if not order_id: continue
        if order_id not in valid_order_ids:
            issues.append(f"- **유령 거래:** `거래내역`에 발주번호 `{order_id}`가 있으나, `발주` 시트에는 해당 주문이 없습니다.")
        else:
            order_amount = int(orders_df[orders_df['발주번호'] == order_id]['합계금액'].sum())
            tx_amount = int(abs(tx['금액']))
            if order_amount != tx_amount:
                issues.append(f"- **금액 불일치:** 발주번호 `{order_id}`의 금액이 다릅니다 (발주: {order_amount:,}원 / 거래: {tx_amount:,}원).")
    if issues:
        return "❌ 오류", issues
    return "✅ 정상", []

def audit_inventory_logs(inventory_log_df, orders_df):
    issues = []
    approved_orders = orders_df[orders_df['상태'].isin([CONFIG['ORDER_STATUS']['APPROVED'], CONFIG['ORDER_STATUS']['SHIPPED']])]
    shipped_order_ids = set(inventory_log_df[inventory_log_df['구분'] == CONFIG['INV_CHANGE_TYPE']['SHIPMENT']]['관련번호'].str.split(', ').explode())
    for _, order in approved_orders.iterrows():
        if order['발주번호'] not in shipped_order_ids:
            issues.append(f"- **재고 차감 누락:** 주문 `{order['발주번호']}`({order['지점명']})는 '승인' 상태이나, 재고 출고 기록이 없습니다.")
    if issues:
        return "⚠️ 경고", issues
    return "✅ 정상", []

def audit_data_integrity(orders_df, transactions_df, store_info_df, master_df):
    issues = []
    valid_store_ids = set(store_info_df['지점ID'])
    valid_item_codes = set(master_df['품목코드'])
    for df, name in [(orders_df, '발주'), (transactions_df, '거래내역')]:
        invalid_stores = df[~df['지점ID'].isin(valid_store_ids)]
        if not invalid_stores.empty:
            for _, row in invalid_stores.iterrows():
                issues.append(f"- **잘못된 지점ID:** `{name}` 시트에 존재하지 않는 지점ID `{row['지점ID']}`가 사용되었습니다.")
    invalid_items = orders_df[~orders_df['품목코드'].isin(valid_item_codes)]
    if not invalid_items.empty:
        for _, row in invalid_items.iterrows():
            issues.append(f"- **잘못된 품목코드:** `발주` 시트에 존재하지 않는 품목코드 `{row['품목코드']}`가 사용되었습니다.")
    if issues:
        return "❌ 오류", issues
    return "✅ 정상", []

def page_admin_dashboard(master_df: pd.DataFrame):
    st.subheader("📊 대시보드")

    orders_df = get_orders_df()
    charge_req_df = get_charge_requests_df()
    pending_orders_count = len(orders_df[orders_df['상태'] == CONFIG['ORDER_STATUS']['PENDING']]['발주번호'].unique())
    pending_charge_count = len(charge_req_df[charge_req_df['상태'] == '요청'])
    
    with st.container(border=True):
        st.markdown("##### 🔔 **즉시 처리 필요 항목**")
        c1, c2 = st.columns(2)
        c1.metric("📦 신규 발주 요청", f"{pending_orders_count} 건")
        c2.metric("💳 충전/상환 요청", f"{pending_charge_count} 건")

    st.divider()

    st.markdown("##### 🩺 **시스템 상태 요약**")
    if 'audit_results' in st.session_state:
        results = st.session_state['audit_results']
        cols = st.columns(4)
        status_map = {
            "재무": results['financial'], "거래": results['links'],
            "재고": results['inventory'], "무결성": results['integrity']
        }
        has_issue = False
        for i, (key, (status, issues)) in enumerate(status_map.items()):
            with cols[i]:
                st.metric(
                    f"{key} 점검", status, f"{len(issues)}건 문제" if issues else "정상", 
                    delta_color=("inverse" if "오류" in status else "off") if "정상" not in status else "normal"
                )
                if issues:
                    has_issue = True
        if has_issue:
            st.info("문제가 발견되었습니다. '관리 설정' 탭에서 상세 내역을 확인하세요.")
    else:
        st.info("시스템 점검 데이터가 없습니다. '관리 설정' 탭에서 점검을 실행해주세요.")
    
    st.divider()

    c1, c2 = st.columns([3, 1])
    with c1:
        st.markdown("##### ⚠️ **재고 부족 경고 품목**")
    with c2:
        low_stock_threshold = st.number_input(
            "알림 기준 재고 (이하)", min_value=0, value=5, step=1, 
            key="low_stock_threshold", label_visibility="collapsed"
        )
    
    current_inv_df = get_inventory_from_log(master_df)
    pending_orders = orders_df[orders_df['상태'] == CONFIG['ORDER_STATUS']['PENDING']]
    pending_qty = pending_orders.groupby('품목코드')['수량'].sum().reset_index().rename(columns={'수량': '출고 대기 수량'})
    
    display_inv = pd.merge(current_inv_df, pending_qty, on='품목코드', how='left').fillna(0)
    display_inv['실질 가용 재고'] = pd.to_numeric(display_inv['현재고수량'], errors='coerce').fillna(0) - pd.to_numeric(display_inv['출고 대기 수량'], errors='coerce').fillna(0)

    active_master_df = master_df[master_df['활성'].astype(str).str.upper() == 'TRUE']
    low_stock_df = display_inv[
        (display_inv['실질 가용 재고'] <= low_stock_threshold) &
        (display_inv['품목코드'].isin(active_master_df['품목코드']))
    ].copy()

    if low_stock_df.empty:
        st.info(f"가용 재고가 {low_stock_threshold}개 이하인 품목이 없습니다.")
    else:
        st.dataframe(
            low_stock_df[['품목코드', '품목명', '현재고수량', '출고 대기 수량', '실질 가용 재고']],
            use_container_width=True, hide_index=True
        )

def page_admin_daily_production(master_df: pd.DataFrame):
    st.subheader("📝 일일 생산 보고")
    user = st.session_state.auth
    
    with st.container(border=True):
        st.markdown("##### 📦 생산 수량 입력")
        
        with st.form(key="add_production_form"):
            c1, c2 = st.columns(2)
            production_date = c1.date_input("생산일자")
            
            cat_opt = ["(전체)"] + sorted(master_df["분류"].dropna().unique().tolist())
            cat_sel = c2.selectbox("분류(선택)", cat_opt, key="prod_reg_category")

            change_reason = ""
            if production_date != date.today():
                change_reason = st.text_input("생산일자 변경 사유 (필수)", placeholder="예: 어제 누락분 입력")
            
            df_producible = master_df[master_df['활성'].astype(str).str.lower() == 'true'].copy()
            if cat_sel != "(전체)":
                df_producible = df_producible[df_producible["분류"] == cat_sel]

            df_producible['생산수량'] = 0
            
            edited_production = st.data_editor(
                df_producible[['품목코드', '분류', '품목명', '단위', '생산수량']],
                key=f"production_editor_{st.session_state.production_editor_ver}",
                use_container_width=True, hide_index=True,
                disabled=['품목코드', '분류', '품목명', '단위']
            )

            if st.form_submit_button("생산 목록에 추가", type="primary", use_container_width=True):
                if production_date != date.today() and not change_reason:
                    st.session_state.warning_message = "생산일자를 변경한 경우, 변경 사유를 반드시 입력해야 합니다."
                    st.rerun()
                else:
                    edited_df = pd.DataFrame(edited_production)
                    # [개선] 음수 값 입력 방지 로직
                    if (pd.to_numeric(edited_df['생산수량'], errors='coerce') < 0).any():
                        st.session_state.error_message = "생산수량은 0 이상이어야 합니다. 음수 값을 수정해주세요."
                        st.rerun()
                    
                    items_to_add = edited_df[edited_df['생산수량'] > 0]
                    if not items_to_add.empty:
                        current_cart = st.session_state.production_cart
                        
                        updated_cart = pd.concat([current_cart, items_to_add]).groupby('품목코드').agg({
                            '분류': 'last', '품목명': 'last', '단위': 'last', '생산수량': 'sum'
                        }).reset_index()
                        
                        st.session_state.production_cart = updated_cart
                        st.session_state.production_editor_ver += 1
                        st.session_state.production_date_to_log = production_date
                        st.session_state.production_change_reason = change_reason
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
            st.dataframe(production_cart[['품목코드', '분류', '품목명', '단위', '생산수량']], use_container_width=True, hide_index=True)
            
            with st.form("finalize_production_form"):
                btn_cols = st.columns(2)
                with btn_cols[0]:
                    if st.form_submit_button("✅ 최종 생산 기록 저장", type="primary", use_container_width=True):
                        items_to_log = production_cart.copy()
                        items_to_log.rename(columns={'생산수량': '수량변경'}, inplace=True)
                        change_reason_final = st.session_state.get("production_change_reason", "")
                        
                        with st.spinner("생산 기록 및 재고 업데이트 중..."):
                            if update_inventory(items_to_log, CONFIG['INV_CHANGE_TYPE']['PRODUCE'], user['name'], production_log_date, reason=change_reason_final):
                                st.session_state.success_message = f"{len(items_to_log)}개 품목의 생산 기록이 저장되었습니다."
                                st.session_state.production_cart = pd.DataFrame()
                                st.rerun()
                            else:
                                st.session_state.error_message = "생산 기록 저장 중 오류가 발생했습니다."
                                st.rerun()
                
                with btn_cols[1]:
                    if st.form_submit_button("🗑️ 목록 비우기", use_container_width=True):
                        st.session_state.production_cart = pd.DataFrame()
                        st.session_state.success_message = "생산 목록을 모두 삭제했습니다."
                        st.rerun()
                        
def page_admin_inventory_management(master_df: pd.DataFrame):
    st.subheader("📊 생산/재고 관리")
    inventory_tabs = st.tabs(["현재고 현황", "재고 변동 내역", "재고 수동 조정"])

    current_inv_df = get_inventory_from_log(master_df)

    with inventory_tabs[0]:
        st.markdown("##### 📦 현재고 현황")
        inv_status_tabs = st.tabs(["전체품목 현황", "보유재고 현황"])
        
        orders_df = get_orders_df() 
        active_master_df = master_df[master_df['활성'].astype(str).str.lower() == 'true']
        
        pending_orders = orders_df[orders_df['상태'] == CONFIG['ORDER_STATUS']['PENDING']]
        pending_qty = pending_orders.groupby('품목코드')['수량'].sum().reset_index().rename(columns={'수량': '출고 대기 수량'})

        display_inv = pd.merge(current_inv_df, pending_qty, on='품목코드', how='left').fillna(0)
        
        display_inv['현재고수량'] = pd.to_numeric(display_inv['현재고수량'], errors='coerce').fillna(0).astype(int)
        display_inv['출고 대기 수량'] = pd.to_numeric(display_inv['출고 대기 수량'], errors='coerce').fillna(0).astype(int)
        display_inv['실질 가용 재고'] = display_inv['현재고수량'] - display_inv['출고 대기 수량']
        
        active_codes = active_master_df['품목코드'].tolist()
        display_inv = display_inv[display_inv['품목코드'].isin(active_codes)]
        
        cols_display_order = ['품목코드', '분류', '품목명', '현재고수량', '출고 대기 수량', '실질 가용 재고']
        
        with inv_status_tabs[0]:
            st.dataframe(display_inv[cols_display_order], use_container_width=True, hide_index=True)
            
        with inv_status_tabs[1]:
            st.dataframe(display_inv[display_inv['현재고수량'] > 0][cols_display_order], use_container_width=True, hide_index=True)
            
    with inventory_tabs[1]:
        st.markdown("##### 📜 재고 변동 내역")
        
        log_df = get_inventory_log_df()
        
        if log_df.empty:
            st.info("재고 변동 기록이 없습니다.")
        else:
            c1, c2, c3 = st.columns(3)
            dt_from = c1.date_input("조회 시작일", date.today() - timedelta(days=7), key="log_from")
            dt_to = c2.date_input("조회 종료일", date.today(), key="log_to")
            item_list = ["(전체)"] + sorted(master_df['품목명'].unique().tolist())
            item_filter = c3.selectbox("품목 필터", item_list, key="log_item_filter")
            filtered_log = log_df.copy()
            if '작업일자_dt' not in filtered_log.columns:
                filtered_log['작업일자_dt'] = pd.to_datetime(filtered_log['작업일자'], errors='coerce').dt.date
            filtered_log.dropna(subset=['작업일자_dt'], inplace=True)
            filtered_log = filtered_log[(filtered_log['작업일자_dt'] >= dt_from) & (filtered_log['작업일자_dt'] <= dt_to)]
            if item_filter != "(전체)":
                filtered_log = filtered_log[filtered_log['품목명'] == item_filter]
            
            page_size = 20
            page_number = render_paginated_ui(len(filtered_log), page_size, "inv_log")
            start_idx = (page_number - 1) * page_size
            end_idx = start_idx + page_size
            st.dataframe(filtered_log.iloc[start_idx:end_idx].drop(columns=['작업일자_dt'], errors='ignore'), use_container_width=True, hide_index=True)

    with inventory_tabs[2]:
        st.markdown("##### ✍️ 재고 수동 조정")
        st.warning("이 기능은 전산 재고와 실물 재고가 맞지 않을 때만 사용하세요. 모든 조정 내역은 영구적으로 기록됩니다.")
        c1, c2 = st.columns(2)
        item_list = sorted(master_df['품목명'].unique().tolist())
        selected_item = c1.selectbox("조정할 품목 선택", item_list, key="adj_item_select")
        current_stock = 0
        if selected_item:
            stock_info = current_inv_df[current_inv_df['품목명'] == selected_item]
            if not stock_info.empty:
                current_stock = stock_info.iloc[0]['현재고수량']
        c2.metric("현재고", f"{current_stock} 개")
        with st.form("adj_form", border=True):
            c1, c2 = st.columns(2)
            adj_qty = c1.number_input("조정 수량 (+/-)", step=1, help="증가시키려면 양수, 감소시키려면 음수를 입력하세요.")
            adj_reason = c2.text_input("조정 사유 (필수)", placeholder="예: 실사 재고 오차, 파손 폐기 등")
            if st.form_submit_button("재고 조정 실행", type="primary"):
                if not (selected_item and adj_reason and adj_qty != 0):
                    st.warning("모든 필드를 올바르게 입력해주세요.")
                # ✨ 수정된 부분: st.error 대신 st.session_state.error_message 사용
                elif (current_stock + adj_qty) < 0:
                    st.session_state.error_message = f"조정 후 재고가 음수가 될 수 없습니다. (현재고: {current_stock}개, 조정량: {adj_qty}개)"
                    st.rerun()
                # ✨ 수정 끝
                else:
                    item_info_df = master_df[master_df['품목명'] == selected_item]
                    if not item_info_df.empty:
                        item_info = item_info_df.iloc[0]
                        item_to_update = pd.DataFrame([{'품목코드': item_info['품목코드'], '품목명': selected_item, '수량변경': adj_qty}])
                        if update_inventory(item_to_update, CONFIG['INV_CHANGE_TYPE']['ADJUSTMENT'], st.session_state.auth['name'], date.today(), reason=adj_reason):
                            st.session_state.success_message = f"'{selected_item}'의 재고가 성공적으로 조정되었습니다."
                            st.rerun()
                        else:
                            st.session_state.error_message = "재고 조정 중 오류가 발생했습니다."
                            
def handle_order_action_confirmation(df_all: pd.DataFrame):
    action = st.session_state.get('confirm_action')
    data = st.session_state.get('confirm_data', {})
    
    if not action:
        return False

    if action == "reject_order":
        st.warning(f"**확인 필요**: 선택한 {len(data['ids'])}건의 발주를 정말로 **반려**하시겠습니까?")
        c1, c2 = st.columns(2)
        if c1.button("예, 반려합니다.", key="confirm_yes_reject", type="primary", use_container_width=True):
            with st.spinner("발주 반려 및 환불 처리 중..."):
                success_count = 0
                fail_count = 0
                
                # 최신 데이터로 작업하기 위해 한번 더 불러오기
                balance_df = get_balance_df()
                transactions_df = get_transactions_df()
                
                for order_id in data['ids']:
                    order_items = df_all[df_all['발주번호'] == order_id]
                    if order_items.empty:
                        fail_count += 1
                        continue

                    store_id = order_items.iloc[0]['지점ID']
                    original_tx = transactions_df[transactions_df['관련발주번호'] == order_id]
                    
                    if original_tx.empty:
                        st.session_state.warning_message = f"발주번호 {order_id}의 원본 거래내역이 없어 환불 처리를 건너뜁니다."
                        fail_count += 1
                        # 상태는 반려로 변경
                        update_order_status([order_id], CONFIG['ORDER_STATUS']['REJECTED'], st.session_state.auth["name"], reason=data['reason'])
                        continue

                    # 환불 및 잔액 계산
                    tx_info = original_tx.iloc[0]
                    refund_amount = abs(int(tx_info['금액']))
                    balance_info = balance_df[balance_df['지점ID'] == store_id].iloc[0]
                    new_prepaid = int(balance_info['선충전잔액'])
                    new_used_credit = int(balance_info['사용여신액'])
                    
                    if tx_info['구분'] == '선충전결제':
                        new_prepaid += refund_amount
                    else: # 여신결제
                        new_used_credit -= refund_amount
                    
                    refund_record = {
                        "일시": now_kst_str(), "지점ID": store_id, "지점명": tx_info['지점명'],
                        "구분": "발주반려", "내용": f"발주 반려 환불 ({order_id})", "금액": refund_amount,
                        "처리후선충전잔액": new_prepaid, "처리후사용여신액": new_used_credit,
                        "관련발주번호": order_id, "처리자": st.session_state.auth["name"]
                    }
                    
                    # [안정성] 기록 -> 처리 -> 금액 변경 순서 적용
                    try:
                        # 1. 발주 상태 먼저 '반려'로 변경
                        if not update_order_status([order_id], CONFIG['ORDER_STATUS']['REJECTED'], st.session_state.auth["name"], reason=data['reason']):
                            raise Exception("발주 상태 변경 실패")
                        
                        # 2. 거래 내역에 환불 기록 추가
                        if not append_rows_to_sheet(CONFIG['TRANSACTIONS']['name'], [refund_record], CONFIG['TRANSACTIONS']['cols']):
                            # 롤백: 발주 상태를 다시 '요청'으로 되돌림
                            update_order_status([order_id], CONFIG['ORDER_STATUS']['PENDING'], "system_rollback")
                            raise Exception("거래내역 기록 실패")
                        
                        # 3. 모든 기록이 성공한 후, 최종적으로 잔액(돈) 변경
                        if not update_balance_sheet(store_id, {'선충전잔액': new_prepaid, '사용여신액': new_used_credit}):
                            # 이 경우 수동 조치가 필요함을 명확히 알림
                            st.session_state.error_message = f"CRITICAL ERROR: {order_id}의 환불이 기록되었으나 잔액 반영에 실패했습니다. 즉시 수동 조치가 필요합니다!"
                            fail_count += 1
                            continue
                        
                        success_count += 1

                    except Exception as e:
                        fail_count += 1
                        st.session_state.error_message = f"발주번호 {order_id} 처리 중 오류 발생: {e}"

                if success_count > 0:
                    st.session_state.success_message = f"{success_count}건이 성공적으로 반려 처리되었습니다."
                if fail_count > 0:
                    st.session_state.warning_message = f"{fail_count}건 처리 중 문제가 발생했습니다."
                
                st.session_state.confirm_action = None
                st.session_state.confirm_data = None
                st.session_state.admin_orders_selection.clear()
                clear_data_cache()
                st.rerun()

        if c2.button("아니요, 취소합니다.", key="confirm_no_reject", use_container_width=True):
            st.session_state.confirm_action = None
            st.session_state.confirm_data = None
            st.rerun()
        return True

    elif action == "revert_to_pending":
        st.warning(f"**확인 필요**: 선택한 {len(data['ids'])}건의 발주를 **'요청' 상태로 되돌리시겠습니까?** 승인 시 차감되었던 재고가 다시 복원됩니다.")
        c1, c2 = st.columns(2)
        if c1.button("예, 되돌립니다.", key="confirm_yes_revert", type="primary", use_container_width=True):
            with st.spinner("승인 취소 및 재고 복원 중..."):
                orders_to_revert_df = df_all[df_all['발주번호'].isin(data['ids'])]
                items_to_restore = orders_to_revert_df.groupby(['품목코드', '품목명'])['수량'].sum().reset_index()
                items_to_restore['수량변경'] = items_to_restore['수량']
                ref_id = ", ".join(data['ids'])
                
                if update_inventory(items_to_restore, CONFIG['INV_CHANGE_TYPE']['CANCEL_SHIPMENT'], st.session_state.auth['name'], date.today(), ref_id=ref_id):
                    update_order_status(data['ids'], CONFIG['ORDER_STATUS']['PENDING'], "")
                    st.session_state.success_message = f"{len(data['ids'])}건이 '요청' 상태로 변경되고 재고가 복원되었습니다."
                else:
                    st.session_state.error_message = "승인 취소 중 재고 복원 오류가 발생했습니다."

                st.session_state.confirm_action = None
                st.session_state.confirm_data = None
                st.session_state.admin_orders_selection.clear()
                clear_data_cache()
                st.rerun()

        if c2.button("아니요, 취소합니다.", key="confirm_no_revert", use_container_width=True):
            st.session_state.confirm_action = None
            st.session_state.confirm_data = None
            st.rerun()
        return True

    return False

def render_pending_orders_tab(pending_orders: pd.DataFrame, df_all: pd.DataFrame, master_df: pd.DataFrame):
    # '승인' 버튼 클릭 시 실행되는 로직 (페이지 상단에 위치)
    if st.session_state.get('approve_ids'):
        with st.spinner("발주 승인 및 재고 차감 처리 중..."):
            ids_to_process = st.session_state.pop('approve_ids')

            clear_data_cache()
            current_inv_df = get_inventory_from_log(master_df)
            all_pending_orders = get_orders_df().query(f"상태 == '{CONFIG['ORDER_STATUS']['PENDING']}'")
            
            other_pending_orders = all_pending_orders[~all_pending_orders['발주번호'].isin(ids_to_process)]
            pending_qty = other_pending_orders.groupby('품목코드')['수량'].sum().reset_index().rename(columns={'수량': '출고 대기 수량'})
            inventory_check = pd.merge(current_inv_df, pending_qty, on='품목코드', how='left').fillna(0)
            inventory_check['실질 가용 재고'] = inventory_check['현재고수량'] - inventory_check['출고 대기 수량']
            
            lacking_items_details = []
            orders_to_approve_df = df_all[df_all['발주번호'].isin(ids_to_process)]
            items_needed = orders_to_approve_df.groupby('품목코드')['수량'].sum().reset_index()

            for _, needed in items_needed.iterrows():
                item_code = needed['품목코드']
                needed_qty = needed['수량']
                stock_info = inventory_check.query(f"품목코드 == '{item_code}'")
                available_stock = int(stock_info.iloc[0]['실질 가용 재고']) if not stock_info.empty else 0
                if needed_qty > available_stock:
                    item_name_series = master_df.loc[master_df['품목코드'] == item_code, '품목명']
                    item_name = item_name_series.iloc[0] if not item_name_series.empty else item_code
                    shortfall = needed_qty - available_stock
                    lacking_items_details.append(f"- **{item_name}** (부족: **{shortfall}**개 / 필요: {needed_qty}개 / 가용: {available_stock}개)")

            if lacking_items_details:
                details_str = "\n".join(lacking_items_details)
                st.session_state.error_message = f"🚨 재고 부족으로 승인할 수 없습니다:\n{details_str}"
            else:
                items_to_deduct = orders_to_approve_df.groupby(['품목코드', '품목명'])['수량'].sum().reset_index()
                items_to_deduct['수량변경'] = -items_to_deduct['수량']
                ref_id = ", ".join(ids_to_process)
                
                inventory_success = update_inventory(items_to_deduct, CONFIG['INV_CHANGE_TYPE']['SHIPMENT'], "system_auto", date.today(), ref_id=ref_id)
                status_success = update_order_status(ids_to_process, CONFIG['ORDER_STATUS']['APPROVED'], st.session_state.auth["name"])

                if inventory_success and status_success:
                    st.session_state.success_message = f"{len(ids_to_process)}건이 승인 처리되고 재고가 차감되었습니다."
                    st.session_state.admin_orders_selection.clear()
                else:
                    st.session_state.error_message = "처리 중 오류가 발생했습니다. 재고 또는 주문 상태를 확인해주세요."
            
            clear_data_cache()
            st.rerun()

    # --- UI 렌더링 시작 ---
    page_size = 10
    page_number = render_paginated_ui(len(pending_orders), page_size, "pending_orders")
    start_idx = (page_number - 1) * page_size
    end_idx = start_idx + page_size
    
    pending_display = pending_orders.iloc[start_idx:end_idx].copy()
    pending_display.insert(0, '선택', [st.session_state.admin_orders_selection.get(x, False) for x in pending_display['발주번호']])
    
    edited_pending = st.data_editor(
        pending_display, 
        key="admin_pending_editor", 
        hide_index=True, 
        disabled=pending_display.columns.drop("선택"), 
        column_order=("선택", "주문일시", "발주번호", "지점명", "건수", "합계금액(원)", "상태")
    )
    
    for _, row in edited_pending.iterrows():
        st.session_state.admin_orders_selection[row['발주번호']] = row['선택']
    
    selected_pending_ids = [oid for oid, selected in st.session_state.admin_orders_selection.items() if selected and oid in pending_orders['발주번호'].values]
    
    # [수정] UI 순서 변경: 상세 조회를 먼저 표시
    v_spacer(16)
    render_order_details_section(selected_pending_ids, df_all, get_stores_df(), master_df, context="pending")
    
    v_spacer(16)
    
    # [수정] 처리 섹션을 상세 조회 아래로 이동
    st.markdown("##### 📦 선택한 발주 처리")
    with st.container(border=True):
        btn_cols = st.columns(2)
        with btn_cols[0]:
            if st.button("✅ 선택 발주 승인", disabled=not selected_pending_ids, use_container_width=True, type="primary"):
                st.session_state.approve_ids = selected_pending_ids
                st.rerun()
        with btn_cols[1]:
            rejection_reason = st.text_input("반려 사유 (반려 시 필수)", key="rejection_reason_input", placeholder="예: 재고 부족")
            if st.button("❌ 선택 발주 반려", disabled=not selected_pending_ids, key="admin_reject_btn", use_container_width=True):
                if not rejection_reason:
                    st.session_state.warning_message = "반려 사유를 반드시 입력해야 합니다."
                    st.rerun()
                else:
                    st.session_state.confirm_action = "reject_order"
                    st.session_state.confirm_data = {'ids': selected_pending_ids, 'reason': rejection_reason}
                    st.rerun()

def render_shipped_orders_tab(shipped_orders: pd.DataFrame, df_all: pd.DataFrame, store_info_df: pd.DataFrame, master_df: pd.DataFrame):
    page_size = 10
    page_number = render_paginated_ui(len(shipped_orders), page_size, "shipped_orders")
    start_idx = (page_number - 1) * page_size
    end_idx = start_idx + page_size
    shipped_display = shipped_orders.iloc[start_idx:end_idx].copy()

    shipped_display.insert(0, '선택', [st.session_state.admin_orders_selection.get(x, False) for x in shipped_display['발주번호']])
    
    edited_shipped = st.data_editor(
        shipped_display[['선택', '주문일시', '발주번호', '지점명', '건수', '합계금액(원)', '상태', '처리일시']], 
        key="admin_shipped_editor", 
        hide_index=True, 
        disabled=shipped_display.columns.drop("선택")
    )
    
    for _, row in edited_shipped.iterrows():
        st.session_state.admin_orders_selection[row['발주번호']] = row['선택']
        
    selected_shipped_ids = [oid for oid, selected in st.session_state.admin_orders_selection.items() if selected and oid in shipped_orders['발주번호'].values]
    
    v_spacer(16)
    
    # ▼▼▼ [수정] 변수명을 selected_pending_ids -> selected_shipped_ids 로 수정 ▼▼▼
    render_order_details_section(selected_shipped_ids, df_all, store_info_df, master_df, context="shipped")
    # ▲▲▲ 수정 완료 ▲▲▲
    
    v_spacer(16)
    st.markdown("##### ↩️ 승인 취소")
    with st.container(border=True):
      if st.button("선택 건 요청 상태로 되돌리기", key="revert_shipped", disabled=not selected_shipped_ids, use_container_width=True):
          st.session_state.confirm_action = "revert_to_pending"
          st.session_state.confirm_data = {'ids': selected_shipped_ids}
          st.rerun()

def render_order_edit_modal(order_id: str, df_all: pd.DataFrame, master_df: pd.DataFrame):
    st.warning(f"**수정 모드**: 발주번호 `{order_id}`의 수량을 수정합니다. 수량을 0으로 만들면 해당 품목이 삭제됩니다.")
    
    original_items = df_all[df_all['발주번호'] == order_id].copy()
    
    with st.form(key="edit_order_form"):
        # 원본 데이터에서 수정에 필요한 컬럼만 선택하여 data_editor에 전달
        cols_to_edit = ['품목코드', '품목명', '단위', '수량', '단가']
        edited_items_df = st.data_editor(
            original_items[cols_to_edit],
            key=f"editor_{order_id}",
            num_rows="dynamic", # 품목 추가는 불가, 삭제(수량 0)만 가능
            use_container_width=True,
            disabled=['품목코드', '품목명', '단위', '단가']
        )
        
        c1, c2 = st.columns(2)
        if c1.form_submit_button("💾 수정사항 저장", type="primary", use_container_width=True):
            with st.spinner("변경사항을 계산하고 재고 및 잔액을 업데이트하는 중..."):
                # --- 1. 변경사항 계산 ---
                original_items.set_index('품목코드', inplace=True)
                edited_items = pd.DataFrame(edited_items_df).set_index('품목코드')
                
                comparison = original_items.join(edited_items, lsuffix='_orig', rsuffix='_edit', how='outer').fillna(0)
                
                inventory_changes = []
                price_diff = 0.0

                for code, row in comparison.iterrows():
                    qty_orig = int(row['수량_orig'])
                    qty_edit = int(row['수량_edit'])
                    qty_diff = qty_edit - qty_orig
                    
                    if qty_diff != 0:
                        price = int(row['단가_orig']) if qty_orig > 0 else int(row['단가_edit'])
                        item_name = row['품목명_orig'] if qty_orig > 0 else row['품목명_edit']
                        
                        # VAT 포함 금액으로 차액 계산 (감소: 환불(+), 증가: 추가결제(-))
                        price_diff -= (qty_diff * price * 1.1) 
                        # 재고는 반대로 증감
                        inventory_changes.append({'품목코드': code, '품목명': item_name, '수량변경': -qty_diff})
                
                price_diff = int(round(price_diff, 0)) # 최종 금액을 정수로 변환

                # --- 2. 데이터 처리 (재고 -> 거래/잔액 -> 주문서) ---
                try:
                    user = st.session_state.auth
                    store_name = original_items.iloc[0]['지점명']
                    store_id = original_items.iloc[0]['지점ID']

                    # 2-1. 재고 업데이트
                    if inventory_changes:
                        if not update_inventory(pd.DataFrame(inventory_changes), '재고조정(출고변경)', user['name'], date.today(), ref_id=order_id):
                            raise Exception("재고 업데이트에 실패했습니다.")
                    
                    # 2-2. 거래내역 기록 및 잔액 업데이트
                    if price_diff != 0:
                        balance_df = get_balance_df()
                        balance_info = balance_df[balance_df['지점ID'] == store_id].iloc[0]
                        new_prepaid = int(balance_info['선충전잔액'])
                        new_used_credit = int(balance_info['사용여신액'])

                        trans_type = "부분환불" if price_diff > 0 else "추가 결제"
                        
                        if price_diff > 0: # 환불
                            credit_refund = min(price_diff, new_used_credit)
                            new_used_credit -= credit_refund
                            new_prepaid += (price_diff - credit_refund)
                        else: # 추가 결제
                            payment_amount = abs(price_diff)
                            prepaid_payment = min(payment_amount, new_prepaid)
                            new_prepaid -= prepaid_payment
                            new_used_credit += (payment_amount - prepaid_payment)

                        trans_record = {
                            "일시": now_kst_str(), "지점ID": store_id, "지점명": store_name,
                            "구분": trans_type, "내용": f"발주번호 {order_id} 변경", "금액": price_diff,
                            "처리후선충전잔액": new_prepaid, "처리후사용여신액": new_used_credit,
                            "관련발주번호": order_id, "처리자": user['name']
                        }
                        if not append_rows_to_sheet(CONFIG['TRANSACTIONS']['name'], [trans_record], CONFIG['TRANSACTIONS']['cols']):
                            raise Exception("거래내역 기록에 실패했습니다.")
                        
                        if not update_balance_sheet(store_id, {'선충전잔액': new_prepaid, '사용여신액': new_used_credit}):
                            raise Exception("잔액 정보 업데이트에 실패했습니다. (치명적 오류, 수동 확인 필요)")

                    # 2-3. 주문서 업데이트 (기존 내역 삭제 후, 수정된 내역으로 새로 추가)
                    if not find_and_delete_rows(SHEET_NAMES["ORDERS"], "발주번호", [order_id]):
                        raise Exception("기존 주문서 삭제에 실패했습니다.")
                    
                    final_items_df = pd.DataFrame(edited_items_df).reset_index(drop=True)
                    final_items_df = final_items_df[final_items_df['수량'] > 0] # 수량이 0인 품목은 최종 제외
                    
                    new_order_rows = []
                    if not final_items_df.empty:
                        base_info = original_items.iloc[0]
                        for _, row in final_items_df.iterrows():
                            supply_price = row['단가'] * row['수량']
                            tax = math.ceil(supply_price * 0.1) if base_info['과세구분'] == '과세' else 0
                            total_price = supply_price + tax
                            new_order_rows.append({
                                "주문일시": base_info['주문일시'], "발주번호": order_id, 
                                "지점ID": store_id, "지점명": store_name, 
                                "품목코드": row['품목코드'], "품목명": row['품목명'], "단위": row['단위'], 
                                "수량": row['수량'], "단가": row['단가'], 
                                "공급가액": supply_price, "세액": tax, "합계금액": total_price, 
                                "비고": base_info['비고'], "상태": CONFIG['ORDER_STATUS']['MODIFIED'], 
                                "처리일시": now_kst_str(), "처리자": user['name'], "반려사유": ""
                            })
                    
                    if new_order_rows:
                        if not append_rows_to_sheet(SHEET_NAMES["ORDERS"], new_order_rows, CONFIG['ORDERS']['cols']):
                            raise Exception("수정된 주문서 생성에 실패했습니다.")
                    
                    add_audit_log(user['user_id'], user['name'], "발주 부분 수정", order_id, store_name, reason="관리자 수정")

                    st.session_state.success_message = f"발주번호 {order_id}가 성공적으로 수정되었습니다."
                    st.session_state.editing_order_id = None
                    clear_data_cache()
                    st.rerun()

                except Exception as e:
                    st.session_state.error_message = f"수정 중 오류 발생: {e}"
                    st.rerun()

        if c2.form_submit_button("닫기", use_container_width=True):
            st.session_state.editing_order_id = None
            st.rerun()

def render_modified_orders_tab(modified_orders: pd.DataFrame, df_all: pd.DataFrame, store_info_df: pd.DataFrame, master_df: pd.DataFrame):
    st.info("여기에는 승인 후 수량 등이 변경된 발주 내역이 표시됩니다.")
    
    page_size = 10
    page_number = render_paginated_ui(len(modified_orders), page_size, "modified_orders")
    start_idx = (page_number - 1) * page_size
    end_idx = start_idx + page_size
    modified_display = modified_orders.iloc[start_idx:end_idx].copy()

    modified_display.insert(0, '선택', [st.session_state.admin_orders_selection.get(x, False) for x in modified_display['발주번호']])
    
    edited_modified = st.data_editor(
        modified_display, 
        key="admin_modified_editor", 
        hide_index=True, 
        disabled=modified_display.columns.drop("선택")
    )

    for _, row in edited_modified.iterrows():
        st.session_state.admin_orders_selection[row['발주번호']] = row['선택']

    selected_ids = [oid for oid, selected in st.session_state.admin_orders_selection.items() if selected and oid in modified_orders['발주번호'].values]
    
    v_spacer(16)

    # ▼▼▼ [수정] 변수명과 context 값을 올바르게 수정합니다 ▼▼▼
    render_order_details_section(selected_ids, df_all, store_info_df, master_df, context="modified")
    # ▲▲▲ 수정 완료 ▲▲▲

def render_rejected_orders_tab(rejected_orders: pd.DataFrame):
    page_size = 10
    page_number = render_paginated_ui(len(rejected_orders), page_size, "rejected_orders")
    start_idx = (page_number - 1) * page_size
    end_idx = start_idx + page_size
    rejected_display = rejected_orders.iloc[start_idx:end_idx].copy()

    rejected_display.insert(0, '선택', [st.session_state.admin_orders_selection.get(x, False) for x in rejected_display['발주번호']])

    # ✨ 수정된 부분: 체크박스가 동작하도록 disabled 속성 수정
    edited_rejected = st.data_editor(
        rejected_display[['선택', '주문일시', '발주번호', '지점명', '건수', '합계금액(원)', '상태', '반려사유']], 
        key="admin_rejected_editor", 
        hide_index=True, 
        disabled=rejected_display.columns.drop("선택")
    )

    for _, row in edited_rejected.iterrows():
        st.session_state.admin_orders_selection[row['발주번호']] = row['선택']

# 기존 render_order_details_section 함수를 아래 코드로 전체 교체하세요.

def render_order_details_section(selected_ids: List[str], df_all: pd.DataFrame, store_info_df: pd.DataFrame, master_df: pd.DataFrame, context: str):
    with st.container(border=True):
        st.markdown("##### 📄 발주 품목 상세 조회")
        
        if len(selected_ids) == 1:
            target_id = selected_ids[0]
            target_df = df_all[df_all["발주번호"] == target_id]
            
            if not target_df.empty:
                total_amount = target_df['합계금액'].sum()
                memo = target_df['비고'].iloc[0] if '비고' in target_df.columns and pd.notna(target_df['비고'].iloc[0]) else ""

                st.markdown(f"**선택된 발주번호:** `{target_id}` / **총 합계금액(VAT포함):** `{int(total_amount):,.0f}원`")

                if memo.strip():
                    st.markdown("**요청사항:**")
                    # ▼▼▼ [수정] key에 context를 추가하여 고유성을 보장합니다 ▼▼▼
                    st.text_area(
                        "요청사항_상세", 
                        value=memo, 
                        height=80, 
                        disabled=True, 
                        label_visibility="collapsed",
                        key=f"memo_display_{context}_{target_id}" 
                    )
                    # ▲▲▲ 수정 완료 ▲▲▲
                
                display_df = pd.merge(target_df, master_df[['품목코드', '과세구분']], on='품목코드', how='left')
                display_df['단가(VAT포함)'] = display_df.apply(get_vat_inclusive_price, axis=1)
                display_df.rename(columns={'합계금액': '합계금액(VAT포함)'}, inplace=True)
                
                st.dataframe(
                    display_df[["품목코드", "품목명", "단위", "수량", "단가", "단가(VAT포함)", "합계금액(VAT포함)"]], 
                    hide_index=True, 
                    use_container_width=True
                )
                
                order_status = target_df.iloc[0]['상태']
                
                if order_status in [CONFIG['ORDER_STATUS']['APPROVED'], CONFIG['ORDER_STATUS']['SHIPPED']]:
                    c1, c2 = st.columns(2)
                    with c1:
                        if st.button("✏️ 선택 발주 일부 수정하기", key=f"edit_{target_id}", use_container_width=True):
                            st.session_state.editing_order_id = target_id
                            st.rerun()
                    with c2:
                        # ... (다운로드 버튼 로직은 동일) ...
                        pass # 실제 코드에서는 이 pass를 지우고 기존 다운로드 버튼 코드를 유지하세요.

                elif order_status not in [CONFIG['ORDER_STATUS']['MODIFIED']]:
                    st.download_button("📄 품목거래내역서 다운로드", disabled=True, use_container_width=True)

        elif len(selected_ids) > 1:
            st.info("상세 내용을 보려면 발주를 **하나만** 선택하세요.")
        else:
            st.info("상세 내용을 보려면 위 목록에서 발주를 선택하세요.")

def page_admin_unified_management(df_all: pd.DataFrame, store_info_df: pd.DataFrame, master_df: pd.DataFrame):
    st.subheader("📋 발주요청 조회·수정")

    if handle_order_action_confirmation(df_all):
        return

    if df_all.empty:
        st.info("발주 데이터가 없습니다.")
        return
    
    c1, c2, c3, c4 = st.columns(4)
    dt_from = c1.date_input("시작일", date.today() - timedelta(days=7), key="admin_mng_from")
    dt_to = c2.date_input("종료일", date.today(), key="admin_mng_to")
    stores = ["(전체)"] + sorted(df_all["지점명"].dropna().unique().tolist())
    store = c3.selectbox("지점", stores, key="admin_mng_store")
    order_id_search = c4.text_input("발주번호로 검색", key="admin_mng_order_id", placeholder="전체 또는 일부 입력")
    
    df = df_all.copy()
    if order_id_search:
        df = df[df["발주번호"].str.contains(order_id_search, na=False)]
    else:
        df['주문일시_dt'] = pd.to_datetime(df['주문일시'], errors='coerce').dt.date
        df.dropna(subset=['주문일시_dt'], inplace=True)
        df = df[(df['주문일시_dt'] >= dt_from) & (df['주문일시_dt'] <= dt_to)]
        if store != "(전체)":
            df = df[df["지점명"] == store]
    
    orders = df.groupby("발주번호").agg(
        주문일시=("주문일시", "first"), 지점명=("지점명", "first"), 건수=("품목코드", "count"), 
        합계금액=("합계금액", "sum"), 상태=("상태", "first"), 처리일시=("처리일시", "first"),
        반려사유=("반려사유", "first")
    ).reset_index().sort_values(by="주문일시", ascending=False)
    
    orders.rename(columns={"합계금액": "합계금액(원)"}, inplace=True)
    pending = orders[orders["상태"] == CONFIG['ORDER_STATUS']['PENDING']].copy()
    shipped = orders[orders["상태"].isin([CONFIG['ORDER_STATUS']['APPROVED'], CONFIG['ORDER_STATUS']['SHIPPED']])].copy()
    
    # ▼▼▼ [수정] '변동출고' 상태 필터링 추가 ▼▼▼
    modified = orders[orders["상태"] == CONFIG['ORDER_STATUS']['MODIFIED']].copy()
    rejected = orders[orders["상태"].isin([CONFIG['ORDER_STATUS']['REJECTED'], CONFIG['ORDER_STATUS']['CANCELED_STORE'], CONFIG['ORDER_STATUS']['CANCELED_ADMIN']])].copy()
    
    # ▼▼▼ [수정] 탭을 4개로 확장 ▼▼▼
    tab1, tab2, tab3, tab4 = st.tabs([
        f"📦 발주 요청 ({len(pending)}건)", 
        f"✅ 승인/출고 ({len(shipped)}건)", 
        f"🔄 변동/출고 ({len(modified)}건)", 
        f"❌ 반려/취소 ({len(rejected)}건)"
    ])
    
    with tab1:
        render_pending_orders_tab(pending, df_all, master_df)
    with tab2:
        render_shipped_orders_tab(shipped, df_all, store_info_df, master_df) # 인자 추가
    with tab3:
        # 새로운 함수 호출 (나중에 생성)
        render_modified_orders_tab(modified, df_all, store_info_df, master_df)
    with tab4:
        render_rejected_orders_tab(rejected)
    
    v_spacer(16)
    selected_ids = [k for k, v in st.session_state.admin_orders_selection.items() if v]
    render_order_details_section(selected_ids, df_all, store_info_df, master_df)
    
def page_admin_sales_inquiry(master_df: pd.DataFrame):
    st.subheader("📈 매출 조회")
    
    df_orders = get_orders_df() 
    
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
        df_sales_raw['주문일시'] = pd.to_datetime(df_sales_raw['주문일시'], errors='coerce')
    
    df_sales_raw.dropna(subset=['주문일시'], inplace=True)

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
                column_config={ "매출액(%)": st.column_config.ProgressColumn( "매출액(%)", format="%.1f%%", min_value=0, max_value=item_sales['매출액(%)'].max()) },
                use_container_width=True, hide_index=True
            )

    df_sales['연'] = df_sales['주문일시'].dt.strftime('%y')
    df_sales['월'] = df_sales['주문일시'].dt.month
    df_sales['일'] = df_sales['주문일시'].dt.day

    daily_pivot = df_sales.pivot_table(index=['연', '월', '일'], columns='지점명', values='합계금액', aggfunc='sum', fill_value=0, margins=True, margins_name='합계')
    monthly_pivot = df_sales.pivot_table(index=['연', '월'], columns='지점명', values='합계금액', aggfunc='sum', fill_value=0, margins=True, margins_name='합계')
    
    with sales_tab2:
        st.markdown("##### 📅 일별 상세")
        daily_display_df = daily_pivot.reset_index()
        numeric_cols = daily_display_df.columns.drop(['연', '월', '일'])
        st.dataframe(daily_display_df.style.format("{:,.0f}", subset=numeric_cols), use_container_width=True, hide_index=True)
        
    with sales_tab3:
        st.markdown("##### 🗓️ 월별 상세")
        monthly_display_df = monthly_pivot.reset_index()
        numeric_cols = monthly_display_df.columns.drop(['연', '월'])
        st.dataframe(monthly_display_df.style.format("{:,.0f}", subset=numeric_cols), use_container_width=True, hide_index=True)

    st.divider()
    summary_data = {
        'total_sales': total_sales, 'total_supply': total_supply,
        'total_tax': total_tax, 'total_orders': total_orders_count
    }
    filter_info = {
        'period': f"{dt_from.strftime('%Y-%m-%d')} ~ {dt_to.strftime('%Y-%m-%d')}",
        'store': store_sel
    }
    # df_orders 인자를 제거하고 함수 호출
    excel_buffer = make_sales_summary_excel(df_sales, daily_pivot, monthly_pivot, summary_data, filter_info)
    st.download_button(label="📥 매출정산표 다운로드", data=excel_buffer, file_name=f"매출정산표_{dt_from}_to_{dt_to}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)

def page_admin_documents(store_info_df: pd.DataFrame, master_df: pd.DataFrame):
    st.subheader("📑 증빙서류 다운로드")

    # 세션 상태 초기화 (안전장치)
    if 'report_df' not in st.session_state: st.session_state.report_df = pd.DataFrame()
    if 'excel_buffer' not in st.session_state: st.session_state.excel_buffer = None
    if 'report_filename' not in st.session_state: st.session_state.report_filename = ""
    if 'report_info' not in st.session_state: st.session_state.report_info = {}

    st.markdown("##### 1. 조건 설정")
    with st.container(border=True):
        c1, c2 = st.columns(2)
        with c1:
            admin_stores = store_info_df[store_info_df['역할'] == CONFIG['ROLES']['ADMIN']]["지점명"].tolist()
            regular_stores = sorted(store_info_df[store_info_df['역할'] != CONFIG['ROLES']['ADMIN']]["지점명"].dropna().unique().tolist())
            admin_selection_list = [f"{name} (관리자)" for name in admin_stores]
            selection_list = ["(선택하세요)"] + admin_selection_list + regular_stores
            selected_entity_display = st.selectbox("대상 선택", selection_list, key="admin_doc_entity_select")

        sub_doc_type = ""
        selected_entity_real_name = selected_entity_display.replace(" (관리자)", "")

        if selected_entity_display != "(선택하세요)":
            selected_entity_info = store_info_df[store_info_df['지점명'] == selected_entity_real_name].iloc[0]
            with c2:
                if selected_entity_info['역할'] == CONFIG['ROLES']['ADMIN']:
                    sub_doc_type = st.selectbox("서류 종류", ["매출정산표", "품목생산보고서", "재고변동보고서", "현재고현황보고서"], key="admin_doc_type_admin")
                else:
                    sub_doc_type = st.selectbox("서류 종류", ["금전거래내역서", "품목거래내역서"], key="admin_doc_type_store")
        
        c1, c2 = st.columns(2)
        
        is_inventory_report = sub_doc_type == "현재고현황보고서"
        
        dt_to_label = "조회 기준일" if is_inventory_report else "조회 종료일"
        dt_to = c2.date_input(dt_to_label, date.today(), key="admin_doc_to_individual")
        dt_from_value = dt_to if is_inventory_report else date.today() - timedelta(days=30)
        dt_from = c1.date_input("조회 시작일", dt_from_value, key="admin_doc_from_individual", disabled=is_inventory_report)
        
        if st.button("🔍 데이터 조회하기", key="preview_individual_doc", use_container_width=True, type="primary"):
            st.session_state.report_df = pd.DataFrame()
            st.session_state.excel_buffer = None
            st.session_state.report_filename = ""
            st.session_state.report_info = {}
            
            if selected_entity_display != "(선택하세요)":
                report_df = pd.DataFrame()
                excel_buffer = None
                file_name = ""
                selected_info = store_info_df[store_info_df['지점명'] == selected_entity_real_name].iloc[0]
                
                if selected_info['역할'] == CONFIG['ROLES']['ADMIN']:
                    log_df_raw = get_inventory_log_df()
                    orders_df = get_orders_df()
                    
                    if sub_doc_type == "매출정산표":
                        df_sales_raw = orders_df[orders_df['상태'].isin(['승인', '출고완료'])].copy()
                        if not df_sales_raw.empty:
                            df_sales_raw['주문일시_dt'] = pd.to_datetime(df_sales_raw['주문일시'], errors='coerce').dt.date
                            report_df = df_sales_raw[(df_sales_raw['주문일시_dt'] >= dt_from) & (df_sales_raw['주문일시_dt'] <= dt_to)]
                        
                        if not report_df.empty:
                            report_df['연'] = report_df['주문일시'].dt.strftime('%y')
                            report_df['월'] = report_df['주문일시'].dt.month
                            report_df['일'] = report_df['주문일시'].dt.day
                            daily_pivot = report_df.pivot_table(index=['연', '월', '일'], columns='지점명', values='합계금액', aggfunc='sum', fill_value=0, margins=True, margins_name='합계')
                            monthly_pivot = report_df.pivot_table(index=['연', '월'], columns='지점명', values='합계금액', aggfunc='sum', fill_value=0, margins=True, margins_name='합계')
                            summary_data = { 'total_sales': report_df["합계금액"].sum(), 'total_supply': report_df["공급가액"].sum(), 'total_tax': report_df["세액"].sum(), 'total_orders': report_df['발주번호'].nunique() }
                            filter_info = { 'period': f"{dt_from.strftime('%Y-%m-%d')} ~ {dt_to.strftime('%Y-%m-%d')}", 'store': "(전체 통합)" }
                            excel_buffer = make_sales_summary_excel(report_df, daily_pivot, monthly_pivot, summary_data, filter_info)
                            file_name = f"매출정산표_{dt_from}_to_{dt_to}.xlsx"

                    elif sub_doc_type == "품목생산보고서":
                        production_log = log_df_raw[log_df_raw['구분'] == CONFIG['INV_CHANGE_TYPE']['PRODUCE']].copy()
                        if not production_log.empty:
                             report_df = production_log[(pd.to_datetime(production_log['작업일자']).dt.date >= dt_from) & (pd.to_datetime(production_log['작업일자']).dt.date <= dt_to)]
                        if not report_df.empty:
                            excel_buffer = make_inventory_production_report_excel(report_df, sub_doc_type, dt_from, dt_to)
                            file_name = f"{sub_doc_type}_{dt_from}_to_{dt_to}.xlsx"
                    
                    elif sub_doc_type == "재고변동보고서":
                        if not log_df_raw.empty:
                            report_df = log_df_raw[(pd.to_datetime(log_df_raw['작업일자']).dt.date >= dt_from) & (pd.to_datetime(log_df_raw['작업일자']).dt.date <= dt_to)]
                        if not report_df.empty:
                            excel_buffer = make_inventory_change_report_excel(report_df, sub_doc_type, dt_from, dt_to)
                            file_name = f"{sub_doc_type}_{dt_from}_to_{dt_to}.xlsx"

                    elif sub_doc_type == "현재고현황보고서":
                        report_df = get_inventory_from_log(master_df, target_date=dt_to)
                        if not report_df.empty:
                            excel_buffer = make_inventory_current_report_excel(report_df, sub_doc_type, dt_from, dt_to)
                            file_name = f"{sub_doc_type}_{dt_to}.xlsx"
                
                else: # 지점별 서류
                    if sub_doc_type == "금전거래내역서":
                        transactions_all_df = get_transactions_df()
                        store_transactions = transactions_all_df[transactions_all_df['지점명'] == selected_entity_real_name]
                        if not store_transactions.empty:
                            store_transactions['일시_dt'] = pd.to_datetime(store_transactions['일시'], errors='coerce').dt.date
                            report_df = store_transactions[(store_transactions['일시_dt'] >= dt_from) & (store_transactions['일시_dt'] <= dt_to)]
                        if not report_df.empty:
                            supplier_info_df = store_info_df[store_info_df['역할'] == CONFIG['ROLES']['ADMIN']]
                            if not supplier_info_df.empty:
                                supplier_info = supplier_info_df.iloc[0]
                                excel_buffer = create_unified_financial_statement(report_df, get_transactions_df(), supplier_info, selected_info)
                                file_name = f"금전거래내역서_{selected_entity_real_name}_{dt_from}_to_{dt_to}.xlsx"

                    elif sub_doc_type == "품목거래내역서":
                        orders_df = get_orders_df()
                        store_orders = orders_df[(orders_df['지점명'] == selected_entity_real_name) & (orders_df['상태'].isin([CONFIG['ORDER_STATUS']['APPROVED'], CONFIG['ORDER_STATUS']['SHIPPED']]))]
                        if not store_orders.empty:
                            store_orders['주문일시_dt'] = pd.to_datetime(store_orders['주문일시'], errors='coerce').dt.date
                            report_df = store_orders[(store_orders['주문일시_dt'] >= dt_from) & (store_orders['주문일시_dt'] <= dt_to)]
                        if not report_df.empty:
                            supplier_info_df = store_info_df[store_info_df['역할'] == CONFIG['ROLES']['ADMIN']]
                            if not supplier_info_df.empty:
                                supplier_info = supplier_info_df.iloc[0]
                                excel_buffer = create_unified_item_statement(report_df, supplier_info, selected_info)
                                file_name = f"품목거래내역서_{selected_entity_real_name}_{dt_from}_to_{dt_to}.xlsx"
                
                if report_df.empty:
                    st.session_state.warning_message = "해당 조건으로 조회된 데이터가 없습니다."
                
                st.session_state.excel_buffer = excel_buffer
                st.session_state.report_filename = file_name
                st.session_state.report_df = report_df
                st.session_state.report_info = {'name': selected_entity_real_name, 'type': sub_doc_type, 'from': dt_from, 'to': dt_to}
                st.rerun()

    st.markdown("---")
    st.markdown("##### 2. 미리보기 및 다운로드")
    
    report_df = st.session_state.get('report_df')
    info = st.session_state.get('report_info', {})

    if info and not report_df.empty:
        st.markdown(f"**'{info['name']}'**의 **'{info['type']}'** 조회 결과입니다. (기간: {info['from']} ~ {info['to']})")

        with st.container(border=True):
            if info['type'] == '매출정산표':
                m1, m2, m3, m4 = st.columns(4)
                m1.metric("총 매출 (VAT 포함)", f"{report_df['합계금액'].sum():,.0f}원")
                m2.metric("공급가액", f"{report_df['공급가액'].sum():,.0f}원")
                m3.metric("부가세액", f"{report_df['세액'].sum():,.0f}원")
                m4.metric("총 발주 건수", f"{report_df['발주번호'].nunique()} 건")
            
            elif info['type'] == '품목생산보고서':
                price_history_df = get_price_history_df()
                
                report_df_copy = report_df.copy()
                report_df_copy['단가'] = report_df_copy.apply(
                    lambda row: get_price_at_date(row['품목코드'], pd.to_datetime(row['작업일자']).date(), price_history_df, master_df),
                    axis=1
                )
                
                total_value = (pd.to_numeric(report_df_copy['수량변경'], errors='coerce').fillna(0) * report_df_copy['단가']).sum()
                
                m1, m2, m3 = st.columns(3)
                m1.metric("총 생산 품목 수", f"{report_df_copy['품목코드'].nunique()} 종")
                m2.metric("총 생산 수량", f"{pd.to_numeric(report_df_copy['수량변경'], errors='coerce').sum():,.0f} 개")
                m3.metric("총 생산 평가금액", f"{total_value:,.0f} 원")

            elif info['type'] == '재고변동보고서':
                m1, m2, m3, m4 = st.columns(4)
                m1.metric("총 변동 품목 수", f"{report_df['품목코드'].nunique()} 종")
                m2.metric("총 입고량", f"{report_df[report_df['구분'].isin(['생산입고', '승인취소'])]['수량변경'].sum():,.0f} 개")
                m3.metric("총 출고량", f"{abs(report_df[report_df['구분'] == '발주출고']['수량변경'].sum()):,.0f} 개")
                m4.metric("총 조정량", f"{report_df[report_df['구분'] == '재고조정']['수량변경'].sum():,.0f} 개")

            elif info['type'] == '현재고현황보고서':
                price_history_df = get_price_history_df()
                report_df_copy = report_df.copy()

                report_df_copy['단가'] = report_df_copy.apply(
                    lambda row: get_price_at_date(row['품목코드'], info['to'], price_history_df, master_df),
                    axis=1
                )

                total_value = (pd.to_numeric(report_df_copy['현재고수량'], errors='coerce').fillna(0) * report_df_copy['단가']).sum()
                
                m1, m2, m3 = st.columns(3)
                m1.metric("재고 보유 품목 수", f"{len(report_df_copy[report_df_copy['현재고수량'] > 0])} / {len(report_df_copy)} 종")
                m2.metric("총 재고 수량", f"{pd.to_numeric(report_df_copy['현재고수량'], errors='coerce').sum():,.0f} 개")
                m3.metric("총 재고 평가금액", f"{total_value:,.0f} 원")
            
            elif info['type'] == '금전거래내역서':
                m1, m2, m3 = st.columns(3)
                m1.metric("기간 내 총 입금액", f"{report_df['금액'][report_df['금액'] > 0].sum():,.0f} 원")
                m2.metric("기간 내 총 출금액", f"{report_df['금액'][report_df['금액'] < 0].sum():,.0f} 원")
                m3.metric("순 변동액", f"{report_df['금액'].sum():,.0f} 원")
            
            elif info['type'] == '품목거래내역서':
                m1, m2, m3 = st.columns(3)
                m1.metric("거래 품목 수", f"{report_df['품목코드'].nunique()} 종")
                m2.metric("총 거래 수량", f"{report_df['수량'].sum():,.0f} 개")
                m3.metric("총 거래 금액 (VAT 포함)", f"{report_df['합계금액'].sum():,.0f} 원")

        st.dataframe(report_df.head(10), use_container_width=True, hide_index=True)
        
        if st.session_state.get('excel_buffer'):
            st.download_button(
                label=f"⬇️ '{st.session_state.report_filename}' 엑셀 파일 다운로드",
                data=st.session_state.excel_buffer,
                file_name=st.session_state.report_filename,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                key="download_button_final"
            )
    else:
        st.info("조회할 조건을 선택하고 '데이터 조회하기' 버튼을 눌러주세요.")
        
def page_admin_balance_management(store_info_df: pd.DataFrame):
    st.subheader("💰 결제 관리")
    
    balance_df = get_balance_df()
    charge_requests_df = get_charge_requests_df()
    pending_requests = charge_requests_df[charge_requests_df['상태'] == '요청']
    
    st.markdown("##### 💳 충전/상환 요청 처리")
    if pending_requests.empty:
        st.info("처리 대기 중인 요청이 없습니다.")
    else:
        st.dataframe(pending_requests, hide_index=True, use_container_width=True)
        
        c1, c2, c3 = st.columns(3)
        
        req_options = {
            f"{row['요청일시']} / {row['지점명']} / {int(row['입금액']):,}원": row 
            for _, row in pending_requests.iterrows()
        }
        
        if not req_options:
            st.info("처리 대기 중인 요청이 없습니다.")
            if st.button("새로고침"): st.rerun()
            return

        selected_req_str = c1.selectbox("처리할 요청 선택", list(req_options.keys()))
        action = c2.selectbox("처리 방식", ["승인", "반려"])
        reason = c3.text_input("반려 사유 (반려 시 필수)")

        if st.button("처리 실행", type="primary", use_container_width=True):
            if not selected_req_str or (action == "반려" and not reason):
                st.warning("처리할 요청을 선택하고, 반려 시 사유를 입력해야 합니다.")
                st.stop()

            selected_req_data = req_options[selected_req_str]
            
            user = st.session_state.auth
            add_audit_log(
                user_id=user['user_id'], user_name=user['name'],
                action_type=f"{selected_req_data['종류']} 요청 처리",
                target_id=selected_req_data['지점ID'], target_name=selected_req_data['지점명'],
                changed_item="상태", before_value="요청", after_value=action,
                reason=reason if action == "반려" else ""
            )
            
            selected_timestamp_str = selected_req_data['요청일시'].strftime('%Y-%m-%d %H:%M:%S')

            try:
                with st.spinner("요청 처리 중..."):
                    ws_charge_req = open_spreadsheet().worksheet(CONFIG['CHARGE_REQ']['name'])
                    all_data = ws_charge_req.get_all_values()
                    header = all_data[0]
                    
                    target_row_index = -1
                    for i, row in enumerate(all_data[1:], start=2):
                        if row[header.index('요청일시')] == selected_timestamp_str and row[header.index('지점ID')] == selected_req_data['지점ID']:
                            target_row_index = i
                            break

                    if target_row_index == -1:
                        st.error("처리할 요청을 시트에서 찾을 수 없습니다. 페이지를 새로고침하고 다시 시도하세요.")
                        st.stop()
                    
                    cells_to_update = []
                    status_col_index = header.index('상태') + 1
                    reason_col_index = header.index('처리사유') + 1

                    if action == "승인":
                        store_id = selected_req_data['지점ID']
                        current_balance_info = balance_df[balance_df['지점ID'] == store_id]
                        if current_balance_info.empty:
                            st.error(f"'{selected_req_data['지점명']}'의 잔액 정보가 없습니다.")
                            st.rerun()

                        current_balance = current_balance_info.iloc[0]
                        new_prepaid = int(current_balance['선충전잔액'])
                        new_used_credit = int(current_balance['사용여신액'])
                        amount = int(selected_req_data['입금액'])
                        trans_record = {}

                        if selected_req_data['종류'] == '선충전':
                            new_prepaid += amount
                            trans_record = {"구분": "선충전승인", "내용": f"선충전 입금 확인 ({selected_req_data['입금자명']})"}
                        else:
                            new_used_credit -= amount
                            trans_record = {"구분": "여신상환승인", "내용": f"여신 상환 입금 확인 ({selected_req_data['입금자명']})"}
                            if new_used_credit < 0:
                                new_prepaid += abs(new_used_credit)
                                new_used_credit = 0
                        
                        if update_balance_sheet(store_id, {'선충전잔액': new_prepaid, '사용여신액': new_used_credit}):
                            full_trans_record = {
                                "일시": now_kst_str(), "지점ID": store_id, "지점명": selected_req_data['지점명'],
                                "금액": amount, "처리후선충전잔액": new_prepaid,
                                "처리후사용여신액": new_used_credit, "관련발주번호": "", "처리자": st.session_state.auth["name"],
                                **trans_record
                            }
                            append_rows_to_sheet(CONFIG['TRANSACTIONS']['name'], [full_trans_record], CONFIG['TRANSACTIONS']['cols'])
                            cells_to_update.append(gspread.Cell(target_row_index, status_col_index, '승인'))
                            st.session_state.success_message = "요청이 승인 처리되고 거래내역에 기록되었습니다."
                        else:
                            st.session_state.error_message = "잔액 정보 업데이트에 실패했습니다."
                            st.rerun()
                    else: # 반려
                        cells_to_update.append(gspread.Cell(target_row_index, status_col_index, '반려'))
                        cells_to_update.append(gspread.Cell(target_row_index, reason_col_index, reason))
                        st.session_state.success_message = "요청이 반려 처리되었습니다."

                    if cells_to_update:
                        ws_charge_req.update_cells(cells_to_update, value_input_option='USER_ENTERED')

                    clear_data_cache()
                    st.rerun()
            except Exception as e:
                st.error(f"처리 중 오류가 발생했습니다: {e}")

    st.markdown("---")
    st.markdown("##### 🏢 지점별 잔액 현황")
    st.dataframe(balance_df, hide_index=True, use_container_width=True)
    
    with st.expander("✍️ 잔액/여신 수동 조정"):
        with st.form("manual_adjustment_form"):
            store_info_filtered = store_info_df[store_info_df['역할'] != CONFIG['ROLES']['ADMIN']]
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
                            st.error(f"'{selected_store}'의 잔액 정보가 '잔액마스터' 시트에 없습니다.")
                        else:
                            current_balance = current_balance_query.iloc[0]
                            user = st.session_state.auth
                            old_value = int(current_balance[adj_type])
                            new_value = old_value + adj_amount

                            # ✨ 수정된 부분: st.error 대신 st.session_state.error_message 사용
                            if new_value < 0:
                                st.session_state.error_message = f"조정 후 {adj_type}이(가) 0보다 작아질 수 없습니다. (현재값: {old_value}, 조정액: {adj_amount})"
                                st.rerun()
                            # ✨ 수정 끝

                            add_audit_log(
                                user_id=user['user_id'], user_name=user['name'],
                                action_type="잔액 수동 조정", target_id=store_id,
                                target_name=selected_store, changed_item=adj_type,
                                before_value=old_value, after_value=new_value,
                                reason=adj_reason
                            )

                            if adj_type == "여신한도":
                                update_balance_sheet(store_id, {adj_type: new_value})
                                st.session_state.success_message = f"'{selected_store}'의 여신한도가 조정되었습니다. (거래내역에 기록되지 않음)"
                            else:
                                current_prepaid = int(current_balance['선충전잔액'])
                                current_used_credit = int(current_balance['사용여신액'])
                                
                                if adj_type == "선충전잔액":
                                    update_balance_sheet(store_id, {adj_type: new_value})
                                    trans_record = {"구분": "수동조정(충전)", "처리후선충전잔액": new_value, "처리후사용여신액": current_used_credit}
                                elif adj_type == "사용여신액":
                                    update_balance_sheet(store_id, {adj_type: new_value})
                                    trans_record = {"구분": "수동조정(여신)", "처리후선충전잔액": current_prepaid, "처리후사용여신액": new_value}

                                full_trans_record = {
                                    "일시": now_kst_str(), "지점ID": store_id, "지점명": selected_store,
                                    "금액": adj_amount, "내용": adj_reason, "처리자": user['name'],
                                    **trans_record
                                }
                                append_rows_to_sheet(CONFIG['TRANSACTIONS']['name'], [full_trans_record], CONFIG['TRANSACTIONS']['cols'])
                                st.session_state.success_message = f"'{selected_store}'의 {adj_type}이(가) 조정되고 거래내역에 기록되었습니다."
                            
                            clear_data_cache()
                            st.rerun()

def render_master_settings_tab(master_df_raw: pd.DataFrame):
    st.markdown("##### 🏷️ 품목 정보 설정")
    
    # 원본 데이터의 숫자형 변환 (비교를 위해)
    master_df_raw['단가'] = pd.to_numeric(master_df_raw['단가'], errors='coerce').fillna(0)

    edited_master_df = st.data_editor(
        master_df_raw, 
        num_rows="dynamic", 
        use_container_width=True, 
        key="master_editor"
    )
    
    if st.button("품목 정보 저장", type="primary", key="save_master"):
        with st.spinner("변경 사항을 저장하고 이력을 기록하는 중입니다..."):
            edited_df = pd.DataFrame(edited_master_df)
            edited_df['단가'] = pd.to_numeric(edited_df['단가'], errors='coerce').fillna(0)

            # 1. [순서 변경] 가장 중요한 상품마스터를 먼저 저장합니다.
            if save_df_to_sheet(CONFIG['MASTER']['name'], edited_df):
                
                # 2. 저장이 성공했을 때만, 가격 변경 이력을 감지하고 기록합니다.
                comparison_df = pd.merge(
                    master_df_raw.rename(columns={'단가': '단가_old', '품목명': '품목명_old'}),
                    edited_df.rename(columns={'단가': '단가_new', '품목명': '품목명_new'}),
                    on="품목코드",
                    how='inner'
                )
                price_changes = comparison_df[comparison_df['단가_old'] != comparison_df['단가_new']]
                
                new_history_records = []
                if not price_changes.empty:
                    for _, row in price_changes.iterrows():
                        record = {
                            "변경일시": now_kst_str(),
                            "품목코드": row['품목코드'],
                            "품목명": row['품목명_new'],
                            "이전단가": row['단가_old'],
                            "새단가": row['단가_new'],
                        }
                        new_history_records.append(record)
                
                if new_history_records:
                    # 이력 기록에 실패하더라도 이미 원본 데이터는 저장되었으므로 치명적이지 않음
                    if not append_rows_to_sheet(CONFIG['PRICE_HISTORY']['name'], new_history_records, CONFIG['PRICE_HISTORY']['cols']):
                         st.session_state.warning_message = "품목 정보는 저장되었으나, 가격 변경 이력 기록에 실패했습니다. 수동 확인이 필요합니다."

                st.session_state.success_message = "품목 정보가 성공적으로 저장되었습니다."
                if new_history_records:
                    st.session_state.success_message += f" ({len(new_history_records)}건의 가격 변경 이력이 기록되었습니다.)"
                
                clear_data_cache()
                st.rerun()
            else:
                # 마스터 시트 저장 자체에 실패한 경우
                st.session_state.error_message = "품목 정보 저장에 실패했습니다. 잠시 후 다시 시도해주세요."
                st.rerun()

    st.divider()

    st.markdown("##### 🧾 품목 가격 변경 이력")
    price_history_df = get_price_history_df()

    if price_history_df.empty:
        st.info("기록된 가격 변경 이력이 없습니다.")
    else:
        search_term = st.text_input("품목명 또는 코드로 이력 검색", placeholder="예: 양배추, P001")
        
        if search_term:
            search_term_lower = search_term.lower()
            filtered_history = price_history_df[
                price_history_df['품목명'].str.lower().contains(search_term_lower, na=False) |
                price_history_df['품목코드'].str.lower().contains(search_term_lower, na=False)
            ]
        else:
            filtered_history = price_history_df

        st.dataframe(
            filtered_history,
            use_container_width=True,
            hide_index=True
        )

def render_store_settings_tab(store_info_df_raw: pd.DataFrame):
    st.markdown("##### 🏢 지점(사용자) 정보 설정")
    st.info(
        """
        이 표에서는 지점의 기본 정보(상호명, 사업자 정보, 주소 등)만 수정할 수 있습니다.
        **지점ID, 역할, 활성 상태, 비밀번호**는 이 표에서 직접 관리할 수 없습니다.
        특히, 지점 비밀번호(지점PW)는 보안을 위해 **암호화**되어 별도로 관리됩니다.
        비밀번호 초기화나 계정 활성/비활성화는 하단의 '개별 지점 관리' 메뉴를 이용해주세요.
        """
    )
    edited_store_df = st.data_editor(
        store_info_df_raw, num_rows="fixed", use_container_width=True, 
        key="store_editor", disabled=["지점ID", "지점PW", "역할", "활성"]
    )
    if st.button("기본 정보 저장", type="primary", key="save_stores"):
        try:
            store_info_df_raw_c = store_info_df_raw.astype(str)
            edited_store_df_c = pd.DataFrame(edited_store_df).astype(str)
            
            diff = store_info_df_raw_c.compare(edited_store_df_c)
            if not diff.empty:
                user = st.session_state.auth
                for idx, row in diff.iterrows():
                    store_info = store_info_df_raw.iloc[int(idx)]
                    for col_name in diff.columns.levels[0]:
                        old_val = row[(col_name, 'self')]
                        new_val = row[(col_name, 'other')]
                        if pd.notna(old_val) or pd.notna(new_val):
                            add_audit_log(
                                user_id=user['user_id'], user_name=user['name'],
                                action_type="지점 정보 수정",
                                target_id=store_info['지점ID'], target_name=store_info['지점명'],
                                changed_item=col_name,
                                before_value=old_val, after_value=new_val
                            )
        except Exception as e:
            print(f"Error during audit logging for store data: {e}")
            
        if save_df_to_sheet(CONFIG['STORES']['name'], edited_store_df):
            clear_data_cache()
            st.session_state.success_message = "지점 정보가 성공적으로 저장되었습니다."
            st.rerun()
    
    st.divider()
    with st.expander("➕ 신규 지점 생성"):
        with st.form("new_store_form"):
            st.markdown("###### 신규 지점 정보 입력")
            c1, c2, c3 = st.columns(3)
            new_id = c1.text_input("지점ID (로그인 아이디, 변경 불가)")
            new_pw = c2.text_input("초기 비밀번호", type="password")
            new_name = c3.text_input("지점명")
            if st.form_submit_button("신규 지점 생성"):
                if not (new_id and new_pw and new_name):
                    st.warning("지점ID, 초기 비밀번호, 지점명은 필수입니다.")
                elif not store_info_df_raw[store_info_df_raw['지점ID'] == new_id].empty:
                    st.error("이미 존재하는 지점ID입니다.")
                else:
                    new_store_data = {col: '' for col in CONFIG['STORES']['cols']}
                    new_store_data.update({
                        "지점ID": new_id, "지점PW": hash_password(new_pw), "지점명": new_name, 
                        "역할": CONFIG['ROLES']['STORE'], "활성": "TRUE"
                    })
                    new_balance_data = {"지점ID": new_id, "지점명": new_name, "선충전잔액": 0, "여신한도": 0, "사용여신액": 0}
                    if append_rows_to_sheet(CONFIG['STORES']['name'], [new_store_data], CONFIG['STORES']['cols']) and \
                       append_rows_to_sheet(CONFIG['BALANCE']['name'], [new_balance_data], CONFIG['BALANCE']['cols']):
                        
                        user = st.session_state.auth
                        add_audit_log(user['user_id'], user['name'], "신규 지점 생성", new_id, new_name)

                        clear_data_cache()
                        st.session_state.success_message = f"'{new_name}' 지점이 성공적으로 생성되었습니다."
                        st.rerun()
                    else:
                        st.error("지점 생성 중 오류가 발생했습니다.")
    st.divider()
    st.markdown("##### 🔧 개별 지점 관리")
    all_stores = store_info_df_raw['지점명'].tolist()
    selected_store_name = st.selectbox("관리할 지점 선택", all_stores)
    if selected_store_name:
        selected_store_info = store_info_df_raw[store_info_df_raw['지점명'] == selected_store_name].iloc[0]
        store_id = selected_store_info['지점ID']
        is_active = str(selected_store_info.get('활성', 'FALSE')).upper() == 'TRUE'
        role = selected_store_info['역할']
        c1, c2 = st.columns(2)
        with c1:
            if st.button("🔑 비밀번호 초기화", key=f"reset_pw_{store_id}", use_container_width=True):
                temp_pw = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
                hashed_pw = hash_password(temp_pw)
                ws = open_spreadsheet().worksheet(CONFIG['STORES']['name'])
                cell = ws.find(store_id, in_column=1)
                if cell:
                    pw_col_idx = ws.row_values(1).index('지점PW') + 1
                    ws.update_cell(cell.row, pw_col_idx, hashed_pw)
                    
                    user = st.session_state.auth
                    add_audit_log(user['user_id'], user['name'], "비밀번호 초기화", store_id, selected_store_name)

                    clear_data_cache()
                    st.info(f"'{selected_store_name}'의 비밀번호가 임시 비밀번호 '{temp_pw}' (으)로 초기화되었습니다.")
                else:
                    st.error("시트에서 해당 지점을 찾을 수 없습니다.")
        if role != CONFIG['ROLES']['ADMIN']:
            with c2:
                action_key = f"deactivate_{store_id}" if is_active else f"activate_{store_id}"
                button_text = "🔒 계정 비활성화" if is_active else "✅ 계정 활성화"
                if st.button(button_text, key=action_key, use_container_width=True):
                    st.session_state.confirm_action = "toggle_activation"
                    st.session_state.confirm_data = {'store_id': store_id, 'is_active': is_active, 'name': selected_store_name}
                    st.rerun()

def render_system_audit_tab(
    store_info_df_raw,
    master_df_raw,
    orders_df,
    balance_df,
    transactions_df,
    inventory_log_df,
    charge_req_df  # ✅ 추가
):
    """시스템 점검 탭 UI를 렌더링합니다."""
    st.markdown("##### 🩺 시스템 점검")
    with st.expander("도움말: 각 점검 항목은 무엇을 의미하나요?"):
        st.markdown("""
        각 점검 항목은 우리 시스템의 데이터가 서로 잘 맞물려 정확하게 돌아가고 있는지 확인하는 **'시스템 건강 검진'** 과정입니다.

        ---
        * **💰 재무 점검**
            * **무엇을?** 각 지점의 최종 잔액(선충전, 여신)과 모든 입출금 거래내역의 합산 금액이 일치하는지 검사합니다.
            * **왜?** 시스템의 장부와 실제 돈의 흐름이 정확히 일치하는지 확인하여 재무 데이터의 신뢰성을 보장합니다.

        * **🔗 거래 점검**
            * **무엇을?** 모든 결제/환불 거래 기록이 실제 '발주' 내역과 1:1로 연결되는지, 금액은 정확한지 검사합니다.
            * **왜?** 주문 없는 '유령 거래'나 계산 오류를 찾아내어 모든 거래의 투명성을 보장합니다.

        * **📦 재고 점검**
            * **무엇을?** '승인' 또는 '출고완료'된 주문 건에 대해 재고가 빠짐없이 출고 처리되었는지 검사합니다.
            * **왜?** 판매는 되었지만 재고가 차감되지 않는 실수를 막아, 시스템 재고 수량의 정확성을 유지합니다.

        * **🏛️ 무결성 점검**
            * **무엇을?** 모든 기록에 사용된 '지점 ID'나 '품목 코드'가 현재 시스템에 등록된 유효한 정보인지 검사합니다.
            * **왜?** 삭제된 지점이나 단종된 상품 데이터가 일으킬 수 있는 혼란을 막고, 모든 데이터가 깨끗하고 유효한 상태임을 보장합니다.
        """)

    if st.button("🚀 전체 시스템 점검 시작", use_container_width=True, type="primary"):
        with st.spinner("시스템 전체 데이터를 분석 중입니다..."):
            results = {}
            # ✅ charge_req_df를 함께 전달 (변경 없음)
            results['financial'] = audit_financial_data(balance_df, transactions_df, charge_req_df)
            results['links'] = audit_transaction_links(transactions_df, orders_df)
            results['inventory'] = audit_inventory_logs(inventory_log_df, orders_df)
            results['integrity'] = audit_data_integrity(orders_df, transactions_df, store_info_df_raw, master_df_raw)
            st.session_state['audit_results'] = results
            st.rerun()

    if 'audit_results' in st.session_state:
        st.markdown(f"##### ✅ 점검 결과 ({now_kst_str('%Y-%m-%d %H:%M:%S')} 기준)")
        results = st.session_state['audit_results']
        cols = st.columns(4)
        status_map = {
            "재무": results['financial'], "거래": results['links'],
            "재고": results['inventory'], "무결성": results['integrity']
        }
        
        for i, (key, (status, issues)) in enumerate(status_map.items()):
            with cols[i]:
                st.metric(
                    f"{key} 점검",
                    status,
                    f"{len(issues)}건 문제" if issues else "문제 없음",
                    delta_color=("inverse" if "오류" in status else "off") if "정상" not in status else "normal"
                )

        display_map = {
            "links": ("🔗 거래 점검", results['links']),
            "inventory": ("📦 재고 점검", results['inventory']),
            "financial": ("💰 재무 점검", results['financial']),
            "integrity": ("🏛️ 무결성 점검", results['integrity'])
        }
        for key, (title, (status, issues)) in display_map.items():
            if issues:
                with st.expander(f"{title} 상세 내역 ({len(issues)}건)", expanded=True):
                    st.markdown("\n".join(issues))

def page_admin_settings(store_info_df_raw: pd.DataFrame, master_df_raw: pd.DataFrame, orders_df: pd.DataFrame, balance_df: pd.DataFrame, transactions_df: pd.DataFrame, inventory_log_df: pd.DataFrame, charge_req_df: pd.DataFrame):
    st.subheader("🛠️ 관리 설정")
    if st.session_state.get('confirm_action') == "toggle_activation":
        data = st.session_state.confirm_data
        store_id = data['store_id']
        store_name = data['name']
        is_active = data['is_active']
        action_text = "비활성화" if is_active else "활성화"
        st.warning(f"**확인 필요**: 정말로 '{store_name}({store_id})' 계정을 **{action_text}**하시겠습니까?")
        c1, c2 = st.columns(2)
        if c1.button(f"예, {action_text}합니다.", key="confirm_yes", type="primary", use_container_width=True):
            ws_stores = open_spreadsheet().worksheet(CONFIG['STORES']['name'])
            cell_stores = ws_stores.find(store_id, in_column=1)
            if cell_stores:
                active_col_idx = ws_stores.row_values(1).index('활성') + 1
                new_status = 'FALSE' if is_active else 'TRUE'
                ws_stores.update_cell(cell_stores.row, active_col_idx, new_status)
                
                user = st.session_state.auth
                add_audit_log(
                    user['user_id'], user['name'], "계정 상태 변경", store_id, store_name,
                    "활성", str(is_active).upper(), new_status
                )

                st.session_state.success_message = f"'{store_name}' 계정이 {action_text} 처리되었습니다."
                st.session_state.confirm_action = None
                st.session_state.confirm_data = None
                clear_data_cache()
                st.rerun()
        if c2.button("아니요, 취소합니다.", key="confirm_no", use_container_width=True):
            st.session_state.confirm_action = None
            st.session_state.confirm_data = None
            st.rerun()
        return

    tabs = st.tabs(["품목 관리", "지점 관리", "시스템 점검 🩺", "📜 활동 로그"])
    with tabs[0]:
        render_master_settings_tab(master_df_raw)
    with tabs[1]:
        render_store_settings_tab(store_info_df_raw)
    with tabs[2]:
        render_system_audit_tab(store_info_df_raw, master_df_raw, orders_df, balance_df, transactions_df, inventory_log_df, charge_req_df)
    with tabs[3]:
        page_admin_audit_log()

# =============================================================================
# 8) 라우팅
# =============================================================================
if __name__ == "__main__":
    init_session_state()
    
    if require_login():
        if st.session_state.auth['role'] == CONFIG['ROLES']['ADMIN'] and 'initial_audit_done' not in st.session_state:
            perform_initial_audit()
            
        st.title("📦 식자재 발주 시스템")
        display_feedback()
        
        user = st.session_state.auth
        
        if user["role"] == CONFIG['ROLES']['ADMIN']:
            admin_tabs = ["📊 대시보드", "🏭 일일 생산 보고", "📊 생산/재고 관리", "📋 발주요청 조회", "📈 매출 조회", "💰 결제 관리", "📑 증빙서류 다운로드", "🛠️ 관리 설정"]
            tabs = st.tabs(admin_tabs)
            
            with tabs[0]: page_admin_dashboard(get_master_df())
            with tabs[1]: page_admin_daily_production(get_master_df())
            with tabs[2]: page_admin_inventory_management(get_master_df())
            with tabs[3]: page_admin_unified_management(get_orders_df(), get_stores_df(), get_master_df())
            with tabs[4]: page_admin_sales_inquiry(get_master_df())
            with tabs[5]: page_admin_balance_management(get_stores_df())
            with tabs[6]: page_admin_documents(get_stores_df(), get_master_df())
            with tabs[7]:
                page_admin_settings(
                    get_stores_df(), get_master_df(), get_orders_df(), 
                    get_balance_df(), get_transactions_df(), get_inventory_log_df(),
                    get_charge_requests_df() # ✅ 이 부분을 추가합니다.
                )

        else: # store
            tabs = st.tabs(["🛒 발주 요청", "🧾 발주 조회", "💰 결제 관리", "📑 증빙서류 다운로드", "🏷️ 품목 단가 조회", "👤 내 정보 관리"])
            
            balance_df = get_balance_df()
            my_balance_series = balance_df[balance_df['지점ID'] == user['user_id']]
            my_balance_info = my_balance_series.iloc[0] if not my_balance_series.empty else pd.Series(dtype='object')
            
            stores_df = get_stores_df()
            master_df = get_master_df()
            
            with tabs[0]: page_store_register_confirm(master_df, my_balance_info)
            with tabs[1]: page_store_orders_change(stores_df, master_df)
            with tabs[2]: page_store_balance(get_charge_requests_df(), my_balance_info)
            with tabs[3]: page_store_documents(stores_df, master_df)
            with tabs[4]: page_store_master_view(master_df)
            with tabs[5]: page_store_my_info()
