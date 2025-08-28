# -*- coding: utf-8 -*-
# =============================================================================
# 📦 Streamlit 식자재 발주 시스템 (v19.0 - UX 및 코드 품질 개선)
#
# - 주요 변경 사항 (v19.0):
#   - (UX) 관리자용 대시보드 탭을 추가하여 주요 현황(발주,충전,재고) 요약 제공
#   - (UX) 발주 목록 등 긴 데이터에 페이지네이션(쪽 나누기)을 적용하여 성능 및 가독성 향상
#   - (UX) 발주 취소, 반려 등 주요 기능에 확인 절차를 추가하여 사용자 실수 방지
#   - (품질) 역할, 주문 상태 등 고정 문자열을 CONFIG 상수로 중앙화하여 유지보수성 강화
#   - (품질) 페이지별 대규모 함수들을 기능에 따라 작은 함수로 분리(리팩토링)하여 가독성 및 구조 개선
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

# --- [개선] 설정 정보 중앙 관리 ---
CONFIG = {
    'STORES': {
        'name': "지점마스터",
        'cols': ["지점ID", "지점PW", "역할", "지점명", "사업자등록번호", "상호명", "대표자명", "사업장주소", "업태", "종목", "활성"]
    },
    'MASTER': {
        'name': "상품마스터",
        'cols': ["품목코드", "품목명", "품목규격", "분류", "단위", "단가", "과세구분", "활성"]
    },
    'ORDERS': {
        'name': "발주",
        'cols': ["주문일시", "발주번호", "지점ID", "지점명", "품목코드", "품목명", "단위", "수량", "단가", "공급가액", "세액", "합계금액", "비고", "상태", "처리일시", "처리자", "반려사유"]
    },
    'BALANCE': {
        'name': "잔액마스터",
        'cols': ["지점ID", "지점명", "선충전잔액", "여신한도", "사용여신액"]
    },
    'CHARGE_REQ': {
        'name': "충전요청",
        'cols': ["요청일시", "지점ID", "지점명", "입금자명", "입금액", "종류", "상태", "처리사유"]
    },
    'TRANSACTIONS': {
        'name': "거래내역",
        'cols': ["일시", "지점ID", "지점명", "구분", "내용", "금액", "처리후선충전잔액", "처리후사용여신액", "관련발주번호", "처리자"]
    },
    'AUDIT_LOG': {
        'name': "활동로그",
        'cols': ["로그일시", "변경자 ID", "변경자 이름", "작업 종류", "대상 ID", "대상 이름", "변경 항목", "이전 값", "새로운 값", "사유"]
    },
    'INVENTORY_LOG': {
        'name': "재고로그",
        'cols': ["로그일시", "작업일자", "품목코드", "품목명", "구분", "수량변경", "처리후재고", "관련번호", "처리자", "사유"]
    },
    'CART': {
        'cols': ["품목코드", "분류", "품목명", "단위", "단가", "단가(VAT포함)", "수량", "합계금액(VAT포함)"]
    },
    # [개선] 역할 및 상태 상수화
    'ROLES': {
        'ADMIN': 'admin',
        'STORE': 'store'
    },
    'ORDER_STATUS': {
        'PENDING': '요청',
        'APPROVED': '승인',
        'SHIPPED': '출고완료',
        'REJECTED': '반려',
        'CANCELED_STORE': '취소',
        'CANCELED_ADMIN': '승인취소'
    },
    'INV_CHANGE_TYPE': {
        'PRODUCE': '생산입고',
        'SHIPMENT': '발주출고',
        'ADJUSTMENT': '재고조정',
        'CANCEL_SHIPMENT': '승인취소'
    }
}

# =============================================================================
# 0-1) 기본 유틸리티 함수
# =============================================================================
def now_kst_str(fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    return datetime.now(KST).strftime(fmt)

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

# [개선] 페이지네이션 UI를 위한 헬퍼 함수
def render_paginated_ui(total_items, page_size, key_prefix):
    """페이지네이션 UI를 렌더링하고 현재 페이지 번호를 반환합니다."""
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

# [신규] 감사 로그 기록을 위한 중앙 함수
def add_audit_log(
    user_id: str, 
    user_name: str, 
    action_type: str, 
    target_id: str, 
    target_name: str = "", 
    changed_item: str = "", 
    before_value: Any = "", 
    after_value: Any = "", 
    reason: str = ""
):
    """모든 주요 데이터 변경 사항을 '활동로그' 시트에 기록합니다."""
    
    log_sheet_name = CONFIG['AUDIT_LOG']['name']
    log_columns = CONFIG['AUDIT_LOG']['cols']
    
    new_log_entry = {
        "로그일시": now_kst_str(),
        "변경자 ID": user_id,
        "변경자 이름": user_name,
        "작업 종류": action_type,
        "대상 ID": target_id,
        "대상 이름": target_name,
        "변경 항목": str(changed_item),
        "이전 값": str(before_value),
        "새로운 값": str(after_value),
        "사유": reason
    }
    
    try:
        ws = open_spreadsheet().worksheet(log_sheet_name)
        values_to_append = [[new_log_entry.get(col, "") for col in log_columns]]
        ws.append_rows(values_to_append, value_input_option='USER_ENTERED')
    except gspread.WorksheetNotFound:
        # 시트가 없을 경우 새로 만들고 헤더 추가 후 다시 시도
        sh = open_spreadsheet()
        ws = sh.add_worksheet(title=log_sheet_name, rows="1", cols=len(log_columns))
        ws.append_row(log_columns, value_input_option='USER_ENTERED')
        ws.append_rows(values_to_append, value_input_option='USER_ENTERED')
    except Exception as e:
        print(f"CRITICAL: 감사 로그 기록 실패! - {e}")

# =============================================================================
# 2) Google Sheets 연결 및 I/O (기존과 동일)
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

@st.cache_data(ttl=60) # 데이터 변경이 잦은 시트는 캐시 시간을 짧게 유지
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
        ws = open_spreadsheet().worksheet(CONFIG['BALANCE']['name'])
        cell = ws.find(store_id, in_column=1)
        if not cell:
            st.error(f"'{CONFIG['BALANCE']['name']}' 시트에서 지점ID '{store_id}'를 찾을 수 없습니다.")
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
        # ▼▼▼ [감사 로그] 코드 추가 ▼▼▼
        orders_df = get_orders_df()
        user = st.session_state.auth
        
        for order_id in selected_ids:
            order_info = orders_df[orders_df['발주번호'] == order_id]
            if not order_info.empty:
                old_status = order_info['상태'].iloc[0]
                add_audit_log(
                    user_id=user['user_id'], user_name=user['name'],
                    action_type="주문 상태 변경",
                    target_id=order_id,
                    target_name=order_info['지점명'].iloc[0],
                    changed_item="상태",
                    before_value=old_status,
                    after_value=new_status,
                    reason=reason
                )
        # ▲▲▲ 코드 추가 끝 ▲▲▲

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
        
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"발주 상태 업데이트 중 오류가 발생했습니다: {e}")
        return False
        
# =============================================================================
# 3) 로그인, 인증 및 데이터 로더
# =============================================================================
def hash_password(password: str) -> str:
    """비밀번호를 SHA256으로 해싱합니다."""
    return hashlib.sha256(password.encode()).hexdigest()

def authenticate_user(uid, pwd, store_master_df):
    """사용자 ID와 비밀번호를 인증합니다."""
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
    """데이터프레임의 날짜/시간 관련 열을 datetime 객체로 변환합니다."""
    for col in ['주문일시', '요청일시', '처리일시', '일시', '로그일시', '작업일자']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    return df

def clear_data_cache():
    """st.session_state에 저장된 모든 데이터프레임 캐시를 지웁니다."""
    for key in list(st.session_state.keys()):
        if key.endswith('_df'):
            del st.session_state[key]
    st.cache_data.clear()

# --- 데이터 지연 로딩(Lazy Loading)을 위한 로더 함수들 ---
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
# 4) Excel 생성 (기존과 동일)
# =============================================================================
def make_order_id(store_id: str) -> str: return f"{datetime.now(KST):%Y%m%d%H%M%S}{store_id}"

def get_vat_inclusive_price(row: pd.Series) -> int:
    price = int(row.get('단가', 0))
    tax_type = row.get('과세구분', '과세')
    return int(price * 1.1) if tax_type == '과세' else price

# [개선] 단일/기간 모든 품목거래를 처리하는 통합 엑셀 생성 함수
def make_item_transaction_report_excel(orders_df: pd.DataFrame, supplier_info: pd.Series, customer_info: pd.Series) -> BytesIO:
    output = BytesIO()
    if orders_df.empty: return output

    # 데이터 준비: 날짜 형식 변환 및 정렬
    df = orders_df.copy()
    df['거래일자'] = pd.to_datetime(df['주문일시']).dt.date
    df = df.sort_values(by=['거래일자', '품목명'])

    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        workbook = writer.book
        worksheet = workbook.add_worksheet("품목거래내역서")
        
        # --- 서식 정의 ---
        fmt_title = workbook.add_format({'bold': True, 'font_size': 20, 'align': 'center', 'valign': 'vcenter', 'border': 1})
        fmt_h2 = workbook.add_format({'bold': True, 'font_size': 11, 'bg_color': '#F2F2F2', 'align': 'center', 'valign': 'vcenter', 'border': 1})
        fmt_info = workbook.add_format({'font_size': 10, 'border': 1, 'align': 'left', 'valign': 'vcenter'})
        fmt_header = workbook.add_format({'bold': True, 'bg_color': '#DDEBF7', 'border': 1, 'align': 'center', 'valign': 'vcenter'})
        fmt_money = workbook.add_format({'num_format': '#,##0', 'border': 1})
        fmt_date = workbook.add_format({'num_format': 'yyyy-mm-dd', 'border': 1, 'align': 'center'})
        fmt_border_c = workbook.add_format({'border': 1, 'align': 'center'})
        fmt_border_l = workbook.add_format({'border': 1, 'align': 'left'})
        fmt_daily_total_label = workbook.add_format({'bold': True, 'bg_color': '#FFF2CC', 'border': 1, 'align': 'center'})
        fmt_daily_total_value = workbook.add_format({'bold': True, 'bg_color': '#FFF2CC', 'border': 1, 'num_format': '#,##0'})
        fmt_grand_total_label = workbook.add_format({'bold': True, 'font_size': 12, 'bg_color': '#DDEBF7', 'border': 1, 'align': 'center'})
        fmt_grand_total_value = workbook.add_format({'bold': True, 'font_size': 12, 'bg_color': '#DDEBF7', 'border': 1, 'num_format': '#,##0'})

        # --- 문서 기본 정보 및 헤더 ---
        worksheet.set_column('A:A', 12); worksheet.set_column('B:C', 20); worksheet.set_column('D:E', 10)
        worksheet.set_column('F:I', 14)
        worksheet.merge_range('A1:I2', '품 목 거 래 내 역 서', fmt_title)
        
        # ▼▼▼ [복구] 공급자/공급받는 자 정보 작성 ▼▼▼
        worksheet.merge_range('A4:I4', '') # 여백
        
        # 공급하는 자 정보
        worksheet.merge_range('A5:A9', '공\n급\n하\n는\n자', fmt_h2)
        worksheet.write('B5', '사업자등록번호', fmt_h2); worksheet.merge_range('C5:E5', supplier_info.get('사업자등록번호', ''), fmt_info)
        worksheet.write('B6', '상호', fmt_h2); worksheet.write('C6', supplier_info.get('상호명', ''), fmt_info)
        worksheet.write('D6', '대표', fmt_h2); worksheet.write('E6', supplier_info.get('대표자명', ''), fmt_info)
        worksheet.write('B7', '사업장 주소', fmt_h2); worksheet.merge_range('C7:E7', supplier_info.get('사업장주소', ''), fmt_info)
        worksheet.write('B8', '업태', fmt_h2); worksheet.write('C8', supplier_info.get('업태', ''), fmt_info)
        worksheet.write('D8', '종목', fmt_h2); worksheet.write('E8', supplier_info.get('종목', ''), fmt_info)
        
        # 공급받는 자 정보
        worksheet.merge_range('F5:F9', '공\n급\n받\n는\n자', fmt_h2)
        worksheet.write('G5', '사업자등록번호', fmt_h2); worksheet.merge_range('H5:I5', customer_info.get('사업자등록번호', ''), fmt_info)
        worksheet.write('G6', '상호', fmt_h2); worksheet.merge_range('H6:I6', customer_info.get('상호명', ''), fmt_info)
        worksheet.write('G7', '대표', fmt_h2); worksheet.merge_range('H7:I7', customer_info.get('대표자명', ''), fmt_info)
        worksheet.write('G8', '사업장 주소', fmt_h2); worksheet.merge_range('H8:I8', customer_info.get('사업장주소', ''), fmt_info)
        worksheet.write('G9', '업태/종목', fmt_h2); worksheet.merge_range('H9:I9', f"{customer_info.get('업태', '')}/{customer_info.get('종목', '')}", fmt_info)
        # ▲▲▲ 정보 작성 끝 ▲▲▲

        headers = ["거래일자", "품목코드", "품목명", "단위", "수량", "단가", "공급가액", "세액", "합계금액"]
        worksheet.write_row('A11', headers, fmt_header)
        
        row_num = 11
        # --- 데이터 작성 (날짜별 그룹) ---
        for trade_date, daily_group in df.groupby('거래일자'):
            for _, record in daily_group.iterrows():
                row_num += 1
                worksheet.write_datetime(row_num, 0, trade_date, fmt_date)
                worksheet.write(row_num, 1, record['품목코드'], fmt_border_c)
                worksheet.write(row_num, 2, record['품목명'], fmt_border_l)
                worksheet.write(row_num, 3, record['단위'], fmt_border_c)
                worksheet.write(row_num, 4, record['수량'], fmt_money)
                worksheet.write(row_num, 5, record['단가'], fmt_money)
                worksheet.write(row_num, 6, record['공급가액'], fmt_money)
                worksheet.write(row_num, 7, record['세액'], fmt_money)
                worksheet.write(row_num, 8, record['합계금액'], fmt_money)

            # --- 일계 행 추가 ---
            row_num += 1
            daily_supply = daily_group['공급가액'].sum()
            daily_tax = daily_group['세액'].sum()
            daily_total = daily_group['합계금액'].sum()
            related_orders = ", ".join(daily_group['발주번호'].unique())

            worksheet.merge_range(row_num, 0, row_num, 2, '일계', fmt_daily_total_label)
            worksheet.merge_range(row_num, 3, row_num, 5, f"(관련 발주번호: {related_orders})", fmt_daily_total_label)
            worksheet.write(row_num, 6, daily_supply, fmt_daily_total_value)
            worksheet.write(row_num, 7, daily_tax, fmt_daily_total_value)
            worksheet.write(row_num, 8, daily_total, fmt_daily_total_value)

        # --- 총계 행 추가 ---
        row_num += 1
        grand_supply = df['공급가액'].sum()
        grand_tax = df['세액'].sum()
        grand_total = df['합계금액'].sum()
        
        worksheet.merge_range(row_num, 0, row_num, 5, '총계', fmt_grand_total_label)
        worksheet.write(row_num, 6, grand_supply, fmt_grand_total_value)
        worksheet.write(row_num, 7, grand_tax, fmt_grand_total_value)
        worksheet.write(row_num, 8, grand_total, fmt_grand_total_value)

    output.seek(0)
    return output

def make_financial_transaction_report_excel(
    df_transactions_period: pd.DataFrame, 
    df_transactions_all: pd.DataFrame, 
    store_info: pd.Series, 
    dt_from: date
) -> BytesIO:
    output = BytesIO()
    if df_transactions_all.empty: return output

    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        workbook = writer.book
        worksheet = workbook.add_worksheet(f"{store_info['지점명']} 금전거래내역서")

        # --- 서식 정의 ---
        fmt_title = workbook.add_format({'bold': True, 'font_size': 18, 'align': 'center', 'valign': 'vcenter'})
        fmt_h2 = workbook.add_format({'bold': True, 'font_size': 11, 'bg_color': '#F2F2F2', 'align': 'center', 'valign': 'vcenter', 'border': 1})
        fmt_header = workbook.add_format({'bold': True, 'bg_color': '#DDEBF7', 'border': 1, 'align': 'center', 'valign': 'vcenter'})
        fmt_money = workbook.add_format({'num_format': '#,##0', 'border': 1})
        fmt_border_c = workbook.add_format({'border': 1, 'align': 'center', 'valign': 'vcenter'})
        fmt_border_l = workbook.add_format({'border': 1, 'align': 'left', 'valign': 'vcenter'})

        worksheet.set_column('A:A', 20); worksheet.set_column('B:B', 12); worksheet.set_column('C:C', 35)
        worksheet.set_column('D:F', 15)

        worksheet.merge_range('A1:F1', f"{store_info['지점명']} 금전 거래 내역서", fmt_title)
        
        # --- [신규] 거래 요약 섹션 ---
        all_tx = df_transactions_all[df_transactions_all['지점ID'] == store_info['지점ID']].copy()
        all_tx['일시_dt'] = pd.to_datetime(all_tx['일시']).dt.date
        
        tx_before = all_tx[all_tx['일시_dt'] < dt_from]
        opening_balance = tx_before.iloc[-1]['처리후선충전잔액'] if not tx_before.empty else 0
        
        period_income = df_transactions_period[df_transactions_period['금액'] > 0]['금액'].sum()
        period_outcome = df_transactions_period[df_transactions_period['금액'] < 0]['금액'].sum()
        closing_balance = df_transactions_period.iloc[-1]['처리후선충전잔액'] if not df_transactions_period.empty else opening_balance
        
        worksheet.merge_range('A3:B3', '거래 요약', fmt_h2)
        worksheet.write('A4', '기초 잔액'); worksheet.write('B4', opening_balance, fmt_money)
        worksheet.write('A5', '기간 내 입금(+)'); worksheet.write('B5', period_income, fmt_money)
        worksheet.write('A6', '기간 내 출금(-)'); worksheet.write('B6', period_outcome, fmt_money)
        worksheet.write('A7', '기말 잔액'); worksheet.write('B7', closing_balance, fmt_money)

        # --- 상세 거래 내역 ---
        headers = ['일시', '구분', '내용', '금액', '선충전 잔액', '사용 여신액']
        worksheet.write_row('A9', headers, fmt_header)
        
        df_sorted = df_transactions_period.sort_values(by='일시', ascending=True).reset_index(drop=True)
        
        for idx, row in df_sorted.iterrows():
            row_num = idx + 10
            worksheet.write(f'A{row_num}', row.get('일시', ''), fmt_border_c)
            worksheet.write(f'B{row_num}', row.get('구분', ''), fmt_border_c)
            worksheet.write(f'C{row_num}', row.get('내용', ''), fmt_border_l)
            worksheet.write(f'D{row_num}', row.get('금액', 0), fmt_money)
            worksheet.write(f'E{row_num}', row.get('처리후선충전잔액', 0), fmt_money)
            worksheet.write(f'F{row_num}', row.get('처리후사용여신액', 0), fmt_money)
    
    output.seek(0)
    return output

def make_multi_date_item_statement_excel(orders_df: pd.DataFrame, supplier_info: pd.Series, customer_info: pd.Series, dt_from: date, dt_to: date) -> BytesIO:
    output = BytesIO()
    if orders_df.empty: return output

    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        workbook = writer.book
        worksheet = workbook.add_worksheet("기간별_품목거래명세서")
        
        # --- 서식 정의 (위 함수와 유사하므로 생략)
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
        
        # --- (공급자/받는자 정보는 생략) ---

        headers = ["No", "품목명", "발주번호", "단위", "수량", "단가", "공급가액", "세액", "합계금액"]
        
        orders_df['주문일'] = pd.to_datetime(orders_df['주문일시']).dt.date
        
        row_num = 13
        grand_total_supply = 0
        grand_total_tax = 0
        grand_total_amount = 0

        for order_date, group in orders_df.sort_values(by=['주문일', '발주번호']).groupby('주문일'):
            worksheet.merge_range(row_num, 0, row_num, len(headers)-1, f"▶ 거래일자: {order_date.strftime('%Y-%m-%d')}", fmt_date_header)
            row_num += 1
            worksheet.write_row(row_num, 0, headers, fmt_header)
            
            start_row_daily = row_num + 1
            group = group.reset_index(drop=True)
            for i, record in group.iterrows():
                row_num += 1
                worksheet.write(row_num, 0, i + 1, fmt_border_c)
                # ... (데이터 쓰는 로직은 위 함수와 유사)
                worksheet.write(row_num, 1, record['품목명'], fmt_border)
                worksheet.write(row_num, 2, record['발주번호'], fmt_border_c)
                worksheet.write(row_num, 3, record['단위'], fmt_border_c)
                worksheet.write(row_num, 4, record['수량'], fmt_money)
                worksheet.write(row_num, 5, record['단가'], fmt_money)
                worksheet.write(row_num, 6, record['공급가액'], fmt_money)
                worksheet.write(row_num, 7, record['세액'], fmt_money)
                worksheet.write(row_num, 8, record['합계금액'], fmt_money)

            row_num += 1
            worksheet.merge_range(row_num, 0, row_num, 5, '일계', fmt_daily_total)
            worksheet.write_formula(row_num, 6, f"=SUM(G{start_row_daily}:G{row_num})", fmt_daily_total)
            worksheet.write_formula(row_num, 7, f"=SUM(H{start_row_daily}:H{row_num})", fmt_daily_total)
            worksheet.write_formula(row_num, 8, f"=SUM(I{start_row_daily}:I{row_num})", fmt_daily_total)
            row_num += 2

            grand_total_supply += group['공급가액'].sum()
            grand_total_tax += group['세액'].sum()
            grand_total_amount += group['합계금액'].sum()

        worksheet.merge_range(row_num, 0, row_num, 5, '총계', fmt_grand_total)
        worksheet.write(row_num, 6, grand_total_supply, fmt_grand_total)
        worksheet.write(row_num, 7, grand_total_tax, fmt_grand_total)
        worksheet.write(row_num, 8, grand_total_amount, fmt_grand_total)

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

# [신규] 월별/기간별 종합 정산 리포트 엑셀 생성 함수
def make_settlement_report_excel(dt_from: date, dt_to: date, orders_df: pd.DataFrame, transactions_df: pd.DataFrame) -> BytesIO:
    output = BytesIO()
    
    # 해당 기간에 승인/출고 완료된 발주 건만 필터링
    sales_df = orders_df[orders_df['상태'].isin([CONFIG['ORDER_STATUS']['APPROVED'], CONFIG['ORDER_STATUS']['SHIPPED']])].copy()
    sales_df['주문일'] = pd.to_datetime(sales_df['주문일시']).dt.date
    sales_df = sales_df[(sales_df['주문일'] >= dt_from) & (sales_df['주문일'] <= dt_to)]

    # 해당 기간의 거래 내역 필터링
    trans_df = transactions_df.copy()
    trans_df['일시'] = pd.to_datetime(trans_df['일시']).dt.date
    trans_df = trans_df[(trans_df['일시'] >= dt_from) & (trans_df['일시'] <= dt_to)]

    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        workbook = writer.book
        # --- 공통 서식 ---
        fmt_h1 = workbook.add_format({'bold': True, 'font_size': 18, 'align': 'center', 'valign': 'vcenter'})
        fmt_header = workbook.add_format({'bold': True, 'bg_color': '#F2F2F2', 'border': 1, 'align': 'center'})
        fmt_money = workbook.add_format({'num_format': '#,##0', 'border': 1})
        
        # --- 1. 종합 현황 시트 ---
        ws1 = workbook.add_worksheet('종합 현황')
        ws1.merge_range('A1:C1', f"종합 정산 리포트 ({dt_from} ~ {dt_to})", fmt_h1)
        
        total_sales = sales_df['합계금액'].sum()
        total_supply = sales_df['공급가액'].sum()
        total_tax = sales_df['세액'].sum()
        total_orders = sales_df['발주번호'].nunique()
        
        ws1.write('A3', '항목', fmt_header); ws1.write('B3', '금액', fmt_header)
        ws1.set_column('A:B', 20)
        ws1.write('A4', '총 매출 (VAT 포함)'); ws1.write('B4', total_sales, fmt_money)
        ws1.write('A5', '총 공급가액'); ws1.write('B5', total_supply, fmt_money)
        ws1.write('A6', '총 부가세액'); ws1.write('B6', total_tax, fmt_money)
        ws1.write('A7', '총 발주 건수'); ws1.write('B7', total_orders, fmt_money)

        # --- 2. 지점별 매출 현황 ---
        if not sales_df.empty:
            store_summary = sales_df.groupby('지점명').agg(
                총매출=('합계금액', 'sum'),
                공급가액=('공급가액', 'sum'),
                세액=('세액', 'sum'),
                발주건수=('발주번호', 'nunique')
            ).reset_index()
            store_summary.to_excel(writer, sheet_name='지점별 매출 현황', index=False, startrow=1)
            ws2 = writer.sheets['지점별 매출 현황']
            ws2.merge_range(0, 0, 0, len(store_summary.columns) - 1, "지점별 매출 현황", fmt_h1)
            for col_num, value in enumerate(store_summary.columns.values):
                ws2.write(1, col_num, value, fmt_header)

        # --- 3. 품목별 판매 현황 ---
        if not sales_df.empty:
            item_summary = sales_df.groupby(['품목코드', '품목명']).agg(
                총판매수량=('수량', 'sum'),
                총매출=('합계금액', 'sum')
            ).reset_index().sort_values(by='총매출', ascending=False)
            item_summary.to_excel(writer, sheet_name='품목별 판매 현황', index=False, startrow=1)
            ws3 = writer.sheets['품목별 판매 현황']
            ws3.merge_range(0, 0, 0, len(item_summary.columns) - 1, "품목별 판매 현황", fmt_h1)
            for col_num, value in enumerate(item_summary.columns.values):
                ws3.write(1, col_num, value, fmt_header)
        
        # --- 4. 상세 발주 내역 ---
        sales_df.to_excel(writer, sheet_name='상세 발주 내역', index=False)

        # --- 5. 상세 거래 내역 ---
        trans_df.to_excel(writer, sheet_name='상세 거래 내역', index=False)

    output.seek(0)
    return output

# =============================================================================
# 5) 유틸리티 함수 (기존과 동일)
# =============================================================================
def init_session_state():
    defaults = {
        "cart": pd.DataFrame(columns=CONFIG['CART']['cols']),
        "store_editor_ver": 0, 
        "production_cart": pd.DataFrame(),
        "production_date_to_log": date.today(),
        "production_change_reason": "",
        "production_editor_ver": 0,
        "success_message": "", "error_message": "", "warning_message": "",
        "store_orders_selection": {}, "admin_orders_selection": {},
        "charge_type_radio": "선충전", "charge_amount": 1000,
        "charge_type_index": 0,
        "confirm_action": None, # [개선] 확인 절차를 위한 세션 상태
        "confirm_data": None
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
        "분류": "last",
        "품목명": "last", 
        "단위": "last", 
        "단가": "last", 
        "단가(VAT포함)": "last",
        "수량": "sum"
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
            "로그일시": now_kst_str(),
            "작업일자": working_date.strftime('%Y-%m-%d'),
            "품목코드": item_code, 
            "품목명": item_name,
            "구분": change_type, 
            "수량변경": int(quantity_change), 
            "처리후재고": int(new_stock), 
            "관련번호": ref_id,
            "처리자": handler, 
            "사유": reason
        })

    if append_rows_to_sheet(CONFIG['INVENTORY_LOG']['name'], log_rows, CONFIG['INVENTORY_LOG']['cols']):
        clear_data_cache()
        return True
        
    return False
    
# =============================================================================
# 6) 지점 페이지 (기존과 거의 동일, 일부 상수화 적용)
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
                items_to_add = coerce_cart_df(pd.DataFrame(edited_disp))
                if not items_to_add[items_to_add["수량"] > 0].empty:
                    add_to_cart(items_to_add, master_df)
                    st.session_state.store_editor_ver += 1
                    st.session_state.success_message = "선택한 품목이 장바구니에 추가되었습니다."
                st.rerun()

    v_spacer(16)
    
    with st.container(border=True):
        st.markdown("##### 🧺 장바구니 및 최종 확인")
        cart_now = st.session_state.cart.copy()

        if '분류' not in cart_now.columns and not cart_now.empty:
            cart_now = pd.merge(
                cart_now.drop(columns=['분류'], errors='ignore'),
                master_df[['품목코드', '분류']],
                on='품목코드',
                how='left'
            )
            cart_now['분류'] = cart_now['분류'].fillna('미지정')
            st.session_state.cart = cart_now.copy()
        
        if cart_now.empty:
            st.info("장바구니가 비어 있습니다.")
        else:
            st.dataframe(
                cart_now[CONFIG['CART']['cols']], 
                hide_index=True, 
                use_container_width=True
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
                            rows.append({"주문일시": now_kst_str(), "발주번호": order_id, "지점ID": user["user_id"], "지점명": user["name"], "품목코드": r["품목코드"], "품목명": r["품목명"], "단위": r["단위"], "수량": r["수량"], "단가": r["단가"], "공급가액": r['공급가액'], "세액": r['세액'], "합계금액": r['합계금액_final'], "비고": memo, "상태": CONFIG['ORDER_STATUS']['PENDING'], "처리자": "", "처리일시": "", "반려사유":""})

                        original_balance = {"선충전잔액": prepaid_balance, "사용여신액": used_credit}
                        
                        if payment_method == "선충전 잔액 결제":
                            new_balance = prepaid_balance - total_final_amount_sum
                            new_used_credit = used_credit
                            trans_desc = "선충전결제"
                        else:
                            new_balance = prepaid_balance
                            new_used_credit = used_credit + total_final_amount_sum
                            trans_desc = "여신결제"

                        if update_balance_sheet(user["user_id"], {"선충전잔액": new_balance, "사용여신액": new_used_credit}):
                            try:
                                append_rows_to_sheet(CONFIG['ORDERS']['name'], rows, CONFIG['ORDERS']['cols'])
                                transaction_record = {
                                    "일시": now_kst_str(), "지점ID": user["user_id"], "지점명": user["name"],
                                    "구분": trans_desc, "내용": f"{cart_now.iloc[0]['품목명']} 등 {len(cart_now)}건 발주",
                                    "금액": -total_final_amount_sum, "처리후선충전잔액": new_balance,
                                    "처리후사용여신액": new_used_credit, "관련발주번호": order_id, "처리자": user["name"]
                                }
                                append_rows_to_sheet(CONFIG['TRANSACTIONS']['name'], [transaction_record], CONFIG['TRANSACTIONS']['cols'])
                                
                                st.session_state.success_message = "발주 및 결제가 성공적으로 완료되었습니다."
                                st.session_state.cart = pd.DataFrame(columns=CONFIG['CART']['cols'])
                                clear_data_cache()
                                st.rerun()
                            except Exception as e:
                                st.error(f"발주/거래 기록 중 오류 발생: {e}. 결제를 원상복구합니다.")
                                update_balance_sheet(user["user_id"], original_balance)
                                clear_data_cache()
                                st.rerun()
                        else:
                            st.session_state.error_message = "결제 처리 중 오류가 발생했습니다."
                            st.rerun()
                with c2:
                    if st.form_submit_button("🗑️ 장바구니 비우기", use_container_width=True):
                        st.session_state.cart = pd.DataFrame(columns=CONFIG['CART']['cols'])
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

    def on_radio_change():
        options = ["선충전", "여신상환"]
        st.session_state.charge_type_index = options.index(st.session_state.charge_type_radio)

    charge_type = st.radio(
        "종류 선택", ["선충전", "여신상환"], 
        key="charge_type_radio", 
        horizontal=True,
        index=st.session_state.charge_type_index,
        on_change=on_radio_change
    )

    if st.session_state.charge_type_radio == '여신상환':
        st.session_state.charge_amount = repayable_amount
        is_disabled = True
    else:
        is_disabled = False

    with st.form("charge_request_form", border=True):
        st.markdown(f"##### {charge_type} 알림 보내기")
        c1, c2 = st.columns(2)
        depositor_name = c1.text_input("입금자명")
        
        charge_amount = c2.number_input(
            "입금액", min_value=0, step=1000, 
            key="charge_amount", disabled=is_disabled
        )
        
        if st.form_submit_button("알림 보내기", type="primary"):
            if depositor_name and (charge_amount > 0 or (charge_type == '여신상환' and charge_amount >= 0)):
                new_request = {
                    "요청일시": now_kst_str(), "지점ID": user["user_id"], "지점명": user["name"],
                    "입금자명": depositor_name, "입금액": charge_amount, "종류": charge_type, "상태": "요청", "처리사유": ""
                }
                if append_rows_to_sheet(CONFIG['CHARGE_REQ']['name'], [new_request], CONFIG['CHARGE_REQ']['cols']):
                    st.session_state.success_message = "관리자에게 입금 완료 알림을 보냈습니다. 확인 후 처리됩니다."
                else: 
                    st.session_state.error_message = "알림 전송에 실패했습니다."
            else: 
                st.warning("입금자명과 0원 이상의 입금액을 올바르게 입력해주세요.")
            
            clear_data_cache()
            st.rerun()
            
    st.markdown("---")
    st.markdown("##### 나의 충전/상환 요청 현황")
    my_requests = charge_requests_df[charge_requests_df['지점ID'] == user['user_id']]
    st.dataframe(my_requests, use_container_width=True, hide_index=True)

def page_store_orders_change(store_info_df: pd.DataFrame, master_df: pd.DataFrame):
    st.subheader("🧾 발주 조회")
    
    df_all_orders = get_orders_df()
    df_all_transactions = get_transactions_df()
    df_balance = get_balance_df()
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
    
    # --- [수정] 여러 개 선택이 가능하도록 하고, 불필요한 st.rerun()을 제거한 최종 콜백 함수 ---
    def handle_multiselect(key, source_df):
        # data_editor에서 편집된 내용을 st.session_state에서 직접 가져옴
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
            with st.spinner("발주 취소 및 환불 처리 중..."):
                for order_id in selected_to_cancel:
                    original_transaction = df_all_transactions[df_all_transactions['관련발주번호'] == order_id]
                    if not original_transaction.empty:
                        trans_info = original_transaction.iloc[0]
                        refund_amount = abs(int(trans_info['금액']))
                        
                        balance_info_df = df_balance[df_balance['지점ID'] == user['user_id']]
                        if not balance_info_df.empty:
                            balance_info = balance_info_df.iloc[0]
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
                            append_rows_to_sheet(CONFIG['TRANSACTIONS']['name'], [refund_record], CONFIG['TRANSACTIONS']['cols'])
                
                update_order_status(selected_to_cancel, "취소", user["name"])
                st.session_state.success_message = f"{len(selected_to_cancel)}건의 발주가 취소되고 환불 처리되었습니다."
                st.session_state.store_orders_selection = {}
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
            total_amount = target_df['합계금액'].sum()
            
            st.markdown(f"**선택된 발주번호:** `{target_id}` / **총 합계금액(VAT포함):** `{total_amount:,.0f}원`")
            
            display_df = pd.merge(target_df, master_df[['품목코드', '과세구분']], on='품목코드', how='left')
            display_df['단가(VAT포함)'] = display_df.apply(get_vat_inclusive_price, axis=1)
            display_df.rename(columns={'합계금액': '합계금액(VAT포함)'}, inplace=True)
            
            st.dataframe(display_df[["품목코드", "품목명", "단위", "수량", "단가(VAT포함)", "합계금액(VAT포함)"]], hide_index=True, use_container_width=True)

            if not target_df.empty and target_df.iloc[0]['상태'] in ["승인", "출고완료"]:
                supplier_info_df = store_info_df[store_info_df['역할'] == 'admin']
                customer_info_df = store_info_df[store_info_df['지점ID'] == user['user_id']]
                if not supplier_info_df.empty and not customer_info_df.empty:
                    supplier_info = supplier_info_df.iloc[0]
                    customer_info = customer_info_df.iloc[0]
                    buf = make_item_transaction_statement_excel(target_df, supplier_info, customer_info)
                    st.download_button("📄 품목 거래명세서 다운로드", data=buf, file_name=f"품목거래명세서_{user['name']}_{target_id}.xlsx", mime="application/vnd.ms-excel", use_container_width=True, type="primary")

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
    
    doc_type = c3.selectbox("서류 종류", ["금전 거래내역서", "품목 거래명세서"])
    
    if doc_type == "금전 거래내역서":
        c4.empty()
        transactions_df = get_transactions_df()
        my_transactions = transactions_df[transactions_df['지점ID'] == user['user_id']]
        if my_transactions.empty: 
            st.info("거래 내역이 없습니다.")
            return
        
        my_transactions['일시_dt'] = pd.to_datetime(my_transactions['일시'], errors='coerce').dt.date
        my_transactions.dropna(subset=['일시_dt'], inplace=True)
        mask = (my_transactions['일시_dt'] >= dt_from) & (my_transactions['일시_dt'] <= dt_to)
        dfv = my_transactions[mask].copy()
        if dfv.empty: 
            st.warning("해당 기간의 거래 내역이 없습니다.")
            return
            
        st.dataframe(dfv.drop(columns=['일시_dt']), use_container_width=True, hide_index=True)
        
        customer_info_df = store_info_df[store_info_df['지점ID'] == user['user_id']]
        if not customer_info_df.empty:
            customer_info = customer_info_df.iloc[0]
            buf = make_full_transaction_statement_excel(dfv, customer_info)
            st.download_button("엑셀 다운로드", data=buf, file_name=f"금전거래명세서_{user['name']}_{dt_from}_to_{dt_to}.xlsx", mime="application/vnd.ms-excel", use_container_width=True, type="primary")
    
    elif doc_type == "품목 거래명세서":
        orders_df = get_orders_df()
        my_orders = orders_df[(orders_df['지점ID'] == user['user_id']) & (orders_df['상태'].isin(['승인', '출고완료']))]
        
        if my_orders.empty:
            st.warning("승인/출고된 발주 내역이 없습니다.")
            return

        my_orders['주문일시_dt'] = pd.to_datetime(my_orders['주문일시'], errors='coerce').dt.date
        my_orders.dropna(subset=['주문일시_dt'], inplace=True)
        filtered_orders = my_orders[my_orders['주문일시_dt'].between(dt_from, dt_to)]
        
        if filtered_orders.empty:
            st.warning("선택한 기간 내에 승인/출고된 발주 내역이 없습니다.")
            return

        order_options = ["(기간 전체)"] + filtered_orders['발주번호'].unique().tolist()
        selected_order_id = c4.selectbox("발주번호 선택", order_options, key="store_doc_order_select")

        supplier_info_df = store_info_df[store_info_df['역할'] == 'admin']
        customer_info_df = store_info_df[store_info_df['지점ID'] == user['user_id']]
        
        if supplier_info_df.empty or customer_info_df.empty:
            st.error("거래명세서 출력에 필요한 공급자 또는 지점 정보가 없습니다.")
            return
            
        supplier_info = supplier_info_df.iloc[0]
        customer_info = customer_info_df.iloc[0]

        if selected_order_id == "(기간 전체)":
            preview_df = filtered_orders
            st.dataframe(preview_df, use_container_width=True, hide_index=True)
            if not preview_df.empty:
                buf = make_multi_date_item_statement_excel(preview_df, supplier_info, customer_info, dt_from, dt_to)
                st.download_button("기간 전체 명세서 다운로드", data=buf, file_name=f"기간별_거래명세서_{user['name']}.xlsx", mime="application/vnd.ms-excel", use_container_width=True, type="primary")
        else:
            preview_df = filtered_orders[filtered_orders['발주번호'] == selected_order_id]
            st.dataframe(preview_df, use_container_width=True, hide_index=True)
            if not preview_df.empty:
                buf = make_item_transaction_statement_excel(preview_df, supplier_info, customer_info)
                st.download_button(f"'{selected_order_id}' 명세서 다운로드", data=buf, file_name=f"거래명세서_{user['name']}_{selected_order_id}.xlsx", mime="application/vnd.ms-excel", use_container_width=True, type="primary")

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
                # Google Sheets 직접 업데이트 로직
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

        ---
        * **로그일시**: 작업이 기록된 정확한 시간입니다.
        * **변경자 ID/이름**: 작업을 수행한 관리자(사용자)의 정보입니다.
        * **작업 종류**: '주문 상태 변경', '잔액 수동 조정' 등 수행된 작업의 유형입니다.
        * **대상 ID/이름**: 작업의 영향을 받은 대상의 정보입니다. (예: 특정 지점 ID, 발주 번호)
        * **변경 항목**: 변경된 데이터의 구체적인 항목입니다. (예: '상태', '선충전잔액')
        * **이전 값 / 새로운 값**: 해당 항목이 어떻게 변경되었는지를 보여줍니다.
        * **사유**: 관리자가 직접 입력한 작업 사유입니다. (예: 잔액 조정 사유, 주문 반려 사유)
        """)
    
    try:
        audit_log_df = load_data(CONFIG['AUDIT_LOG']['name'], CONFIG['AUDIT_LOG']['cols'])
    except gspread.WorksheetNotFound:
        st.warning("'활동로그' 시트를 찾을 수 없습니다. 로그가 기록되면 자동으로 생성됩니다.")
        return
        
    if audit_log_df.empty:
        st.info("활동 기록이 없습니다.")
        return

    # --- 필터링 UI ---
    c1, c2, c3 = st.columns(3)
    # 날짜 필터
    default_start = audit_log_df['로그일시'].min().date() if not audit_log_df.empty else date.today()
    dt_from = c1.date_input("조회 시작일", default_start, key="audit_log_from")
    dt_to = c2.date_input("조회 종료일", date.today(), key="audit_log_to")

    # 변경자 필터
    user_list = ["(전체)"] + sorted(audit_log_df["변경자 이름"].dropna().unique().tolist())
    user_filter = c3.selectbox("변경자 필터", user_list, key="audit_log_user")
    
    # 데이터 필터링
    filtered_df = audit_log_df[
        (audit_log_df['로그일시'].dt.date >= dt_from) &
        (audit_log_df['로그일시'].dt.date <= dt_to)
    ]
    if user_filter != "(전체)":
        filtered_df = filtered_df[filtered_df["변경자 이름"] == user_filter]

    st.markdown(f"총 **{len(filtered_df)}**개의 기록이 조회되었습니다.")
    
    # --- 페이지네이션 적용 및 데이터 표시 ---
    page_size = 20
    page_number = render_paginated_ui(len(filtered_df), page_size, "audit_log")
    start_idx = (page_number - 1) * page_size
    end_idx = start_idx + page_size
    
    st.dataframe(filtered_df.iloc[start_idx:end_idx], use_container_width=True, hide_index=True)

# [신규] 로그인 시 시스템 자동 점검을 위한 헬퍼 함수
def perform_initial_audit():
    """관리자 로그인 시 시스템 상태를 점검하고 결과를 세션 상태에 저장합니다."""
    with st.spinner("시스템 상태를 자동으로 점검하는 중입니다..."):
        # 점검에 필요한 모든 데이터 로드
        stores_df = get_stores_df()
        master_df = get_master_df()
        orders_df = get_orders_df()
        balance_df = get_balance_df()
        transactions_df = get_transactions_df()
        inventory_log_df = get_inventory_log_df()

        results = {}
        results['financial'] = audit_financial_data(balance_df, transactions_df)
        results['links'] = audit_transaction_links(transactions_df, orders_df)
        results['inventory'] = audit_inventory_logs(inventory_log_df, orders_df)
        results['integrity'] = audit_data_integrity(orders_df, transactions_df, stores_df, master_df)
        
        st.session_state['audit_results'] = results
        st.session_state['initial_audit_done'] = True # 점검 완료 플래그 설정

# 헬퍼 함수: 재무 데이터 감사
def audit_financial_data(balance_df, transactions_df):
    issues = []
    store_ids = balance_df[balance_df['지점ID'] != ''].dropna(subset=['지점ID'])['지점ID'].unique()
    for store_id in store_ids:
        store_balance = balance_df[balance_df['지점ID'] == store_id].iloc[0]
        store_tx = transactions_df[transactions_df['지점ID'] == store_id]
        prepaid_tx = store_tx[store_tx['구분'].str.contains('선충전|발주취소|발주반려|수동조정\(충전\)', na=False)]
        calculated_prepaid = prepaid_tx['금액'].sum()
        credit_tx = store_tx[store_tx['구분'].str.contains('여신결제|여신상환|수동조정\(여신\)', na=False)]
        calculated_credit = credit_tx[credit_tx['구분'].str.contains('여신결제', na=False)]['금액'].abs().sum() - credit_tx[credit_tx['구분'].str.contains('여신상환', na=False)]['금액'].abs().sum()
        master_prepaid = int(store_balance['선충전잔액'])
        master_credit = int(store_balance['사용여신액'])
        if master_prepaid != calculated_prepaid:
            issues.append(f"- **{store_balance['지점명']}**: 선충전 잔액 불일치 (장부: {master_prepaid: ,}원 / 계산: {calculated_prepaid: ,}원)")
        if master_credit != calculated_credit:
            issues.append(f"- **{store_balance['지점명']}**: 사용 여신액 불일치 (장부: {master_credit: ,}원 / 계산: {calculated_credit: ,}원)")
    if issues:
        return "❌ 오류", issues
    return "✅ 정상", []

# 헬퍼 함수: 거래-발주 데이터 교차 감사
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

# 헬퍼 함수: 재고 데이터 감사
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

# 헬퍼 함수: 데이터 무결성 감사
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


# [신규] 관리자 대시보드 페이지
def page_admin_dashboard(master_df: pd.DataFrame):
    st.subheader("📊 대시보드")

    # --- 1. 즉시 처리 필요 항목 ---
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

    # --- 2. 시스템 상태 요약 ---
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

    # [수정] 재고 부족 기준을 설정할 수 있도록 UI 변경
    c1, c2 = st.columns([3, 1])
    with c1:
        st.markdown("##### ⚠️ **재고 부족 경고 품목**")
    with c2:
        low_stock_threshold = st.number_input(
            "알림 기준 재고 (이하)", 
            min_value=0, 
            value=5, 
            step=1, 
            key="low_stock_threshold",
            label_visibility="collapsed"
        )
    
    current_inv_df = get_inventory_from_log(master_df)
    pending_orders = orders_df[orders_df['상태'] == CONFIG['ORDER_STATUS']['PENDING']]
    pending_qty = pending_orders.groupby('품목코드')['수량'].sum().reset_index().rename(columns={'수량': '출고 대기 수량'})
    
    display_inv = pd.merge(current_inv_df, pending_qty, on='품목코드', how='left').fillna(0)
    display_inv['실질 가용 재고'] = pd.to_numeric(display_inv['현재고수량'], errors='coerce').fillna(0) - pd.to_numeric(display_inv['출고 대기 수량'], errors='coerce').fillna(0)

    active_master_df = master_df[master_df['활성'].astype(str).str.upper() == 'TRUE']
    low_stock_df = display_inv[
        (display_inv['실질 가용 재고'] <= low_stock_threshold) & # [수정] 설정된 기준으로 필터링
        (display_inv['품목코드'].isin(active_master_df['품목코드']))
    ].copy()

    if low_stock_df.empty:
        st.info(f"가용 재고가 {low_stock_threshold}개 이하인 품목이 없습니다.")
    else:
        st.dataframe(
            low_stock_df[['품목코드', '품목명', '현재고수량', '출고 대기 수량', '실질 가용 재고']],
            use_container_width=True, hide_index=True
        )

# (기존 `page_admin_daily_production` 함수는 그대로 유지)
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
                    st.warning("생산일자를 변경한 경우, 변경 사유를 반드시 입력해야 합니다.")
                else:
                    items_to_add = pd.DataFrame(edited_production)[pd.DataFrame(edited_production)['생산수량'] > 0]
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
                
                with btn_cols[1]:
                    if st.form_submit_button("🗑️ 목록 비우기", use_container_width=True):
                        st.session_state.production_cart = pd.DataFrame()
                        st.session_state.success_message = "생산 목록을 모두 삭제했습니다."
                        st.rerun()

# [수정] 생산/재고 관리 페이지
def page_admin_inventory_management(master_df: pd.DataFrame):
    st.subheader("📊 생산/재고 관리")

    # [수정] '재고 부족 품목' 탭 삭제
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
            
    with inventory_tabs[1]: # [수정] 인덱스 변경 (2 -> 1)
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

    with inventory_tabs[2]: # [수정] 인덱스 변경 (3 -> 2)
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
    """발주 처리 관련 확인 창 로직을 중앙에서 처리하고, 확인 창이 활성화되었는지 여부를 반환합니다."""
    action = st.session_state.get('confirm_action')
    data = st.session_state.get('confirm_data', {})
    
    if not action:
        return False

    # 발주 반려 확인 창
    if action == "reject_order":
        st.warning(f"**확인 필요**: 선택한 {len(data['ids'])}건의 발주를 정말로 **반려**하시겠습니까?")
        c1, c2 = st.columns(2)
        if c1.button("예, 반려합니다.", key="confirm_yes_reject", type="primary", use_container_width=True):
            with st.spinner("발주 반려 및 환불 처리 중..."):
                balance_df = get_balance_df()
                transactions_df = get_transactions_df()
                for order_id in data['ids']:
                    order_items = df_all[df_all['발주번호'] == order_id]
                    if order_items.empty: continue
                    store_id = order_items.iloc[0]['지점ID']
                    original_tx = transactions_df[transactions_df['관련발주번호'] == order_id]
                    if not original_tx.empty:
                        tx_info = original_tx.iloc[0]
                        refund_amount = abs(int(tx_info['금액']))
                        balance_info = balance_df[balance_df['지점ID'] == store_id].iloc[0]
                        new_prepaid = int(balance_info['선충전잔액'])
                        new_used_credit = int(balance_info['사용여신액'])
                        credit_refund = min(refund_amount, new_used_credit)
                        new_used_credit -= credit_refund
                        new_prepaid += (refund_amount - credit_refund)
                        update_balance_sheet(store_id, {'선충전잔액': new_prepaid, '사용여신액': new_used_credit})
                        refund_record = {
                            "일시": now_kst_str(), "지점ID": store_id, "지점명": tx_info['지점명'],
                            "구분": "발주반려", "내용": f"발주 반려 환불 ({order_id})", "금액": refund_amount,
                            "처리후선충전잔액": new_prepaid, "처리후사용여신액": new_used_credit,
                            "관련발주번호": order_id, "처리자": st.session_state.auth["name"]
                        }
                        append_rows_to_sheet(CONFIG['TRANSACTIONS']['name'], [refund_record], CONFIG['TRANSACTIONS']['cols'])
                
                update_order_status(data['ids'], CONFIG['ORDER_STATUS']['REJECTED'], st.session_state.auth["name"], reason=data['reason'])
                st.session_state.success_message = f"{len(data['ids'])}건이 반려 처리되고 환불되었습니다."
                st.session_state.confirm_action = None
                st.session_state.confirm_data = None
                st.session_state.admin_orders_selection.clear()
                st.rerun()

        if c2.button("아니요, 취소합니다.", key="confirm_no_reject", use_container_width=True):
            st.session_state.confirm_action = None
            st.session_state.confirm_data = None
            st.rerun()
        return True

    # '요청' 상태로 되돌리기 확인 창
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
                st.rerun()

        if c2.button("아니요, 취소합니다.", key="confirm_no_revert", use_container_width=True):
            st.session_state.confirm_action = None
            st.session_state.confirm_data = None
            st.rerun()
        return True

    return False

def render_pending_orders_tab(pending_orders: pd.DataFrame, df_all: pd.DataFrame, master_df: pd.DataFrame):
    """'발주 요청' 탭의 UI와 로직을 렌더링합니다."""
    page_size = 10
    page_number = render_paginated_ui(len(pending_orders), page_size, "pending_orders")
    start_idx = (page_number - 1) * page_size
    end_idx = start_idx + page_size
    
    pending_display = pending_orders.iloc[start_idx:end_idx].copy()
    pending_display.insert(0, '선택', [st.session_state.admin_orders_selection.get(x, False) for x in pending_display['발주번호']])
    
    edited_pending = st.data_editor(pending_display, key="admin_pending_editor", hide_index=True, disabled=pending_display.columns.drop("선택"), column_order=("선택", "주문일시", "발주번호", "지점명", "건수", "합계금액(원)", "상태"))
    
    for _, row in edited_pending.iterrows():
        st.session_state.admin_orders_selection[row['발주번호']] = row['선택']
    
    selected_pending_ids = [oid for oid, selected in st.session_state.admin_orders_selection.items() if selected and oid in pending_orders['발주번호'].values]
    
    st.markdown("---")
    st.markdown("##### 📦 선택한 발주 처리")

    btn_cols = st.columns(2)
    with btn_cols[0]:
        if st.button("✅ 선택 발주 승인", disabled=not selected_pending_ids, use_container_width=True, type="primary"):
            current_inv_df = get_inventory_from_log(master_df)
            all_pending_orders = get_orders_df().query(f"상태 == '{CONFIG['ORDER_STATUS']['PENDING']}'")
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
                    item_name_series = master_df.loc[master_df['품목코드'] == item_code, '품목명']
                    item_name = item_name_series.iloc[0] if not item_name_series.empty else item_code
                    shortfall = needed_qty - available_stock
                    lacking_items_details.append(f"- **{item_name}** (부족: **{shortfall}**개 / 필요: {needed_qty}개 / 가용: {available_stock}개)")
            if lacking_items_details:
                details_str = "\n".join(lacking_items_details)
                st.error(f"🚨 재고 부족으로 승인할 수 없습니다:\n{details_str}")
            else:
                with st.spinner("발주 승인 및 재고 차감 처리 중..."):
                    items_to_deduct = orders_to_approve_df.groupby(['품목코드', '품목명'])['수량'].sum().reset_index()
                    items_to_deduct['수량변경'] = -items_to_deduct['수량']
                    ref_id = ", ".join(selected_pending_ids)
                    if update_inventory(items_to_deduct, CONFIG['INV_CHANGE_TYPE']['SHIPMENT'], "system_auto", date.today(), ref_id=ref_id):
                        if update_order_status(selected_pending_ids, CONFIG['ORDER_STATUS']['APPROVED'], st.session_state.auth["name"]):
                            st.session_state.success_message = f"{len(selected_pending_ids)}건이 승인 처리되고 재고가 차감되었습니다."
                            st.session_state.admin_orders_selection.clear()
                            st.rerun()
                        else:
                            st.session_state.error_message = "치명적 오류: 재고는 차감되었으나 발주 상태 변경에 실패했습니다."
                    else:
                        st.session_state.error_message = "발주 승인 중 재고 차감 단계에서 오류가 발생했습니다."
                    st.rerun()

    with btn_cols[1]:
        if st.button("❌ 선택 발주 반려", disabled=not selected_pending_ids, key="admin_reject_btn", use_container_width=True):
            rejection_reason = st.session_state.get("rejection_reason_input", "")
            if not rejection_reason:
                st.warning("반려 사유를 반드시 입력해야 합니다.")
            else:
                st.session_state.confirm_action = "reject_order"
                st.session_state.confirm_data = {'ids': selected_pending_ids, 'reason': rejection_reason}
                st.rerun()

    st.text_input("반려 사유 (반려 시 필수)", key="rejection_reason_input", placeholder="예: 재고 부족")

def render_shipped_orders_tab(shipped_orders: pd.DataFrame, df_all: pd.DataFrame):
    """'승인/출고' 탭의 UI와 로직을 렌더링합니다."""
    page_size = 10
    page_number = render_paginated_ui(len(shipped_orders), page_size, "shipped_orders")
    start_idx = (page_number - 1) * page_size
    end_idx = start_idx + page_size
    shipped_display = shipped_orders.iloc[start_idx:end_idx].copy()

    shipped_display.insert(0, '선택', [st.session_state.admin_orders_selection.get(x, False) for x in shipped_display['발주번호']])
    edited_shipped = st.data_editor(shipped_display[['선택', '주문일시', '발주번호', '지점명', '건수', '합계금액(원)', '상태', '처리일시']], key="admin_shipped_editor", hide_index=True, disabled=shipped_orders.columns)
    
    for _, row in edited_shipped.iterrows():
        st.session_state.admin_orders_selection[row['발주번호']] = row['선택']
        
    selected_shipped_ids = [oid for oid, selected in st.session_state.admin_orders_selection.items() if selected and oid in shipped_orders['발주번호'].values]
    
    if st.button("↩️ 선택 건 요청 상태로 되돌리기", key="revert_shipped", disabled=not selected_shipped_ids, use_container_width=True):
        st.session_state.confirm_action = "revert_to_pending"
        st.session_state.confirm_data = {'ids': selected_shipped_ids}
        st.rerun()

def render_rejected_orders_tab(rejected_orders: pd.DataFrame):
    """'반려' 탭의 UI와 로직을 렌더링합니다."""
    page_size = 10
    page_number = render_paginated_ui(len(rejected_orders), page_size, "rejected_orders")
    start_idx = (page_number - 1) * page_size
    end_idx = start_idx + page_size
    rejected_display = rejected_orders.iloc[start_idx:end_idx].copy()

    rejected_display.insert(0, '선택', [st.session_state.admin_orders_selection.get(x, False) for x in rejected_display['발주번호']])
    edited_rejected = st.data_editor(rejected_display[['선택', '주문일시', '발주번호', '지점명', '건수', '합계금액(원)', '상태', '반려사유']], key="admin_rejected_editor", hide_index=True, disabled=rejected_orders.columns)

    for _, row in edited_rejected.iterrows():
        st.session_state.admin_orders_selection[row['발주번호']] = row['선택']
            
    selected_rejected_ids = [oid for oid, selected in st.session_state.admin_orders_selection.items() if selected and oid in rejected_orders['발주번호'].values]

    if st.button("↩️ 선택 건 요청 상태로 되돌리기", key="revert_rejected", disabled=not selected_rejected_ids, use_container_width=True):
        update_order_status(selected_rejected_ids, CONFIG['ORDER_STATUS']['PENDING'], "")
        st.session_state.success_message = f"{len(selected_rejected_ids)}건이 '요청' 상태로 변경되었습니다."
        st.session_state.admin_orders_selection.clear()
        st.rerun()

def render_order_details_section(selected_ids: List[str], df_all: pd.DataFrame, store_info_df: pd.DataFrame, master_df: pd.DataFrame):
    """선택된 발주의 상세 내역을 표시하는 섹션을 렌더링합니다."""
    with st.container(border=True):
        st.markdown("##### 📄 발주 품목 상세 조회")
        if len(selected_ids) == 1:
            target_id = selected_ids[0]
            target_df = df_all[df_all["발주번호"] == target_id]
            if not target_df.empty:
                total_amount = target_df['합계금액'].sum()
                st.markdown(f"**선택된 발주번호:** `{target_id}` / **총 합계금액(VAT포함):** `{total_amount:,.0f}원`")
                display_df = pd.merge(target_df, master_df[['품목코드', '과세구분']], on='품목코드', how='left')
                display_df['단가(VAT포함)'] = display_df.apply(get_vat_inclusive_price, axis=1)
                display_df.rename(columns={'합계금액': '합계금액(VAT포함)'}, inplace=True)
                st.dataframe(display_df[["품목코드", "품목명", "단위", "수량", "단가(VAT포함)", "합계금액(VAT포함)"]], hide_index=True, use_container_width=True)
                if target_df.iloc[0]['상태'] in [CONFIG['ORDER_STATUS']['APPROVED'], CONFIG['ORDER_STATUS']['SHIPPED']]:
                    supplier_info_df = store_info_df[store_info_df['역할'] == CONFIG['ROLES']['ADMIN']]
                    store_name = target_df.iloc[0]['지점명']
                    customer_info_df = store_info_df[store_info_df['지점명'] == store_name]
                    if not supplier_info_df.empty and not customer_info_df.empty:
                        supplier_info = supplier_info_df.iloc[0]
                        customer_info = customer_info_df.iloc[0]
                        buf = make_item_transaction_statement_excel(target_df, supplier_info, customer_info)
                        st.download_button("📄 품목 거래명세서 다운로드", data=buf, file_name=f"품목거래명세서_{store_name}_{target_id}.xlsx", mime="application/vnd.ms-excel", use_container_width=True, type="primary")
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
    rejected = orders[orders["상태"] == CONFIG['ORDER_STATUS']['REJECTED']].copy()
    
    tab1, tab2, tab3 = st.tabs([f"📦 발주 요청 ({len(pending)}건)", f"✅ 승인/출고 ({len(shipped)}건)", f"❌ 반려 ({len(rejected)}건)"])
    
    with tab1:
        render_pending_orders_tab(pending, df_all, master_df)
    with tab2:
        render_shipped_orders_tab(shipped, df_all)
    with tab3:
        render_rejected_orders_tab(rejected)
    
    v_spacer(16)
    selected_ids = [k for k, v in st.session_state.admin_orders_selection.items() if v]
    render_order_details_section(selected_ids, df_all, store_info_df, master_df)
    
def page_admin_sales_inquiry(master_df: pd.DataFrame):
    st.subheader("📈 매출 조회")
    
    # [수정] 데이터 로더 함수 사용
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
    
    # '주문일시'가 datetime 객체가 아닐 경우 변환
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
    excel_buffer = make_sales_summary_excel(daily_pivot, monthly_pivot, summary_data, filter_info)
    st.download_button(label="📥 매출 정산표 다운로드", data=excel_buffer, file_name=f"매출정산표_{dt_from}_to_{dt_to}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)

def page_admin_documents(store_info_df: pd.DataFrame, master_df: pd.DataFrame):
    st.subheader("📑 증빙서류 다운로드")

    # --- 1. 보고서 종류 선택 ---
    doc_type = st.radio(
        "원하는 보고서 종류를 선택하세요.",
        ["지점별 서류 (거래명세서 등)", "기간별 종합 리포트 (정산용)"],
        horizontal=True, key="admin_doc_main_type", label_visibility="collapsed"
    )
    st.divider()

    # --- A. 지점별 서류 ---
    if doc_type == "지점별 서류 (거래명세서 등)":
        st.markdown("##### 1. 조건 설정")
        with st.container(border=True):
            # --- 조건 설정 UI ---
            c1, c2 = st.columns(2)
            with c1:
                admin_stores = store_info_df[store_info_df['역할'] == CONFIG['ROLES']['ADMIN']]["지점명"].tolist()
                regular_stores = sorted(store_info_df[store_info_df['역할'] != CONFIG['ROLES']['ADMIN']]["지점명"].dropna().unique().tolist())
                admin_selection_list = [f"{name} (Admin)" for name in admin_stores]
                selection_list = ["(지점/관리를 선택하세요)"] + admin_selection_list + regular_stores
                selected_entity_display = st.selectbox("대상 선택", selection_list, key="admin_doc_entity_select")

            sub_doc_type = ""
            if selected_entity_display != "(지점/관리를 선택하세요)":
                selected_entity_real_name = selected_entity_display.replace(" (Admin)", "")
                selected_entity_info = store_info_df[store_info_df['지점명'] == selected_entity_real_name].iloc[0]
                
                with c2:
                    if selected_entity_info['역할'] == CONFIG['ROLES']['ADMIN']:
                        sub_doc_type = st.selectbox("서류 종류", ["품목 생산 보고서", "품목 재고 변동 보고서", "현재고 현황 보고서"], key="admin_doc_type_admin")
                    else:
                        sub_doc_type = st.selectbox("서류 종류", ["금전 거래내역서", "품목 거래명세서"], key="admin_doc_type_store")
            
            c1, c2 = st.columns(2)
            is_inventory_report = sub_doc_type == "현재고 현황 보고서"
            dt_to_label = "조회 기준일" if is_inventory_report else "조회 종료일"
            dt_to = c2.date_input(dt_to_label, date.today(), key="admin_doc_to_individual")
            dt_from_value = dt_to if is_inventory_report else date.today() - timedelta(days=30)
            dt_from = c1.date_input("조회 시작일", dt_from_value, key="admin_doc_from_individual", disabled=is_inventory_report)

            if st.button("🔍 데이터 조회하기", key="preview_individual_doc", use_container_width=True, type="primary"):
                if selected_entity_display != "(지점/관리를 선택하세요)":
                    report_df = pd.DataFrame()
                    if selected_entity_info['역할'] == CONFIG['ROLES']['ADMIN']:
                        log_df_raw = get_inventory_log_df()
                        if not log_df_raw.empty:
                            if sub_doc_type == "품목 생산 보고서":
                                production_log = log_df_raw[log_df_raw['구분'] == CONFIG['INV_CHANGE_TYPE']['PRODUCE']].copy()
                                report_df = production_log[(pd.to_datetime(production_log['작업일자']).dt.date >= dt_from) & (pd.to_datetime(production_log['작업일자']).dt.date <= dt_to)]
                            elif sub_doc_type == "품목 재고 변동 보고서":
                                report_df = log_df_raw[(pd.to_datetime(log_df_raw['작업일자']).dt.date >= dt_from) & (pd.to_datetime(log_df_raw['작업일자']).dt.date <= dt_to)]
                        if sub_doc_type == "현재고 현황 보고서":
                            report_df = get_inventory_from_log(master_df, target_date=dt_to)
                    else: # store 역할
                        if sub_doc_type == "금전 거래내역서":
                            transactions_df = get_transactions_df()
                            store_transactions = transactions_df[transactions_df['지점명'] == selected_entity_real_name]
                            if not store_transactions.empty:
                                store_transactions['일시_dt'] = pd.to_datetime(store_transactions['일시'], errors='coerce').dt.date
                                report_df = store_transactions[(store_transactions['일시_dt'] >= dt_from) & (store_transactions['일시_dt'] <= dt_to)]
                        elif sub_doc_type == "품목 거래명세서":
                            orders_df = get_orders_df()
                            store_orders = orders_df[(orders_df['지점명'] == selected_entity_real_name) & (orders_df['상태'].isin([CONFIG['ORDER_STATUS']['APPROVED'], CONFIG['ORDER_STATUS']['SHIPPED']]))]
                            if not store_orders.empty:
                                store_orders['주문일시_dt'] = pd.to_datetime(store_orders['주문일시'], errors='coerce').dt.date
                                report_df = store_orders[(store_orders['주문일시_dt'] >= dt_from) & (store_orders['주문일시_dt'] <= dt_to)]
                    
                    st.session_state['preview_df'] = report_df
                    st.session_state['preview_info'] = {'type': sub_doc_type, 'entity': selected_entity_info, 'from': dt_from, 'to': dt_to}
                    if 'report_buffer' in st.session_state: del st.session_state['report_buffer']

    # --- B. 기간별 종합 리포트 ---
    elif doc_type == "기간별 종합 리포트 (정산용)":
        with st.container(border=True):
            st.markdown("###### 📅 기간별 종합 리포트")
            st.info("아래에서 설정된 조회 기간의 전체 데이터를 종합하여 정산용 엑셀 파일을 생성합니다.")
            
            c1, c2 = st.columns(2)
            dt_from_report = c1.date_input("조회 시작일", date.today().replace(day=1), key="report_from")
            dt_to_report = c2.date_input("조회 종료일", date.today(), key="report_to")
            
            if st.button("🚀 리포트 생성", use_container_width=True, type="primary"):
                with st.spinner("종합 리포트를 생성하는 중입니다..."):
                    all_orders_df = get_orders_df()
                    all_transactions_df = get_transactions_df()
                    excel_buffer = make_settlement_report_excel(dt_from_report, dt_to_report, all_orders_df, all_transactions_df)
                    st.session_state['report_buffer'] = excel_buffer
                    st.session_state['report_filename'] = f"종합정산리포트_{dt_from_report}_to_{dt_to_report}.xlsx"
                    if 'preview_df' in st.session_state: del st.session_state['preview_df']
    
    # --- 3. 미리보기 및 다운로드 섹션 ---
    st.divider()
    st.markdown("##### 2. 미리보기 및 다운로드")
    
    if 'preview_df' in st.session_state and st.session_state['preview_df'] is not None:
        preview_df = st.session_state['preview_df']
        info = st.session_state['preview_info']
        
        st.markdown(f"**- 서류 종류:** {info['type']} | **- 대상:** {info['entity']['지점명']} | **- 기간:** {info['from']} ~ {info['to']}")
        
        if preview_df.empty:
            st.warning("선택하신 조건에 해당하는 데이터가 없습니다.")
        else:
            st.dataframe(preview_df.head(10), use_container_width=True, hide_index=True)
            if len(preview_df) > 10:
                st.info(f"총 {len(preview_df)}건 중 상위 10건만 표시됩니다. 전체 내용은 엑셀 파일로 확인하세요.")
            
            excel_buffer = None
            file_name = "report.xlsx"
            entity_info = info['entity']

            if entity_info['역할'] == CONFIG['ROLES']['ADMIN']:
                excel_buffer = make_inventory_report_excel(preview_df, info['type'], info['from'], info['to'])
                file_name = f"{info['type'].replace(' ', '_')}_{info['from']}.xlsx"
            else:
                if info['type'] == "금전 거래내역서":
                    excel_buffer = make_full_transaction_statement_excel(preview_df, entity_info)
                    file_name = f"금전거래명세서_{entity_info['지점명']}_{info['from']}_to_{info['to']}.xlsx"
                elif info['type'] == "품목 거래명세서":
                    supplier_info_df = store_info_df[store_info_df['역할'] == CONFIG['ROLES']['ADMIN']]
                    if not supplier_info_df.empty:
                        supplier_info = supplier_info_df.iloc[0]
                        excel_buffer = make_multi_date_item_statement_excel(preview_df, supplier_info, entity_info, info['from'], info['to'])
                        file_name = f"기간별_거래명세서_{entity_info['지점명']}.xlsx"
            
            if excel_buffer:
                st.download_button(
                    label="⬇️ 엑셀 파일 다운로드", data=excel_buffer, file_name=file_name,
                    mime="application/vnd.ms-excel", use_container_width=True, type="primary"
                )

    elif 'report_buffer' in st.session_state and st.session_state['report_buffer']:
        st.download_button(
            label="✅ 종합 리포트 다운로드 준비 완료! (클릭)",
            data=st.session_state['report_buffer'],
            file_name=st.session_state['report_filename'],
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            on_click=lambda: st.session_state.update({'report_buffer': None, 'report_filename': None})
        )
    else:
        st.info("보고서 종류를 선택하고 버튼을 눌러주세요.")

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
            
            # ▼▼▼ [감사 로그] 코드 추가 ▼▼▼
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
                    else:  # 반려
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

                            # ▼▼▼ [감사 로그] 코드 추가 ▼▼▼
                            add_audit_log(
                                user_id=user['user_id'],
                                user_name=user['name'],
                                action_type="잔액 수동 조정",
                                target_id=store_id,
                                target_name=selected_store,
                                changed_item=adj_type,
                                before_value=old_value,
                                after_value=new_value,
                                reason=adj_reason
                            )
                            # ▲▲▲ 코드 추가 끝 ▲▲▲

                            if adj_type == "여신한도":
                                update_balance_sheet(store_id, {adj_type: new_value})
                                st.session_state.success_message = f"'{selected_store}'의 여신한도가 조정되었습니다. (거래내역에 기록되지 않음)"
                            else:
                                # (기존 선충전잔액, 사용여신액 처리 로직)
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
    """품목 관리 탭 UI를 렌더링합니다."""
    st.markdown("##### 🏷️ 품목 정보 설정")
    edited_master_df = st.data_editor(master_df_raw, num_rows="dynamic", use_container_width=True, key="master_editor")
    
    if st.button("품목 정보 저장", type="primary", key="save_master"):
        # [감사 로그] 변경된 내용 추적 및 기록
        try:
            # 원본과 수정본의 데이터 타입을 일관성 있게 맞춤
            master_df_raw_c = master_df_raw.astype(str)
            edited_master_df_c = pd.DataFrame(edited_master_df).astype(str)
            
            # DataFrame 비교를 통해 변경점 찾기
            diff = master_df_raw_c.compare(edited_master_df_c)
            if not diff.empty:
                user = st.session_state.auth
                for idx, row in diff.iterrows():
                    item_info = master_df_raw.iloc[int(idx)]
                    for col_name in diff.columns.levels[0]:
                        old_val = row[(col_name, 'self')]
                        new_val = row[(col_name, 'other')]
                        if pd.notna(old_val) or pd.notna(new_val):
                            add_audit_log(
                                user_id=user['user_id'], user_name=user['name'],
                                action_type="품목 정보 수정",
                                target_id=item_info['품목코드'], target_name=item_info['품목명'],
                                changed_item=col_name,
                                before_value=old_val, after_value=new_val
                            )
        except Exception as e:
            print(f"Error during audit logging for master data: {e}")

        # 시트 저장
        if save_df_to_sheet(CONFIG['MASTER']['name'], edited_master_df):
            st.session_state.success_message = "품목 정보가 성공적으로 저장되었습니다."
            clear_data_cache()
            st.rerun()

def render_store_settings_tab(store_info_df_raw: pd.DataFrame):
    """지점 관리 탭 UI를 렌더링합니다."""
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
        # [감사 로그] 변경된 내용 추적 및 기록
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
            
        # 시트 저장
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

def render_system_audit_tab(store_info_df_raw, master_df_raw, orders_df, balance_df, transactions_df, inventory_log_df):
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
            results['financial'] = audit_financial_data(balance_df, transactions_df)
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
                    f"{key} 점검", status, f"{len(issues)}건 문제" if issues else "문제 없음", 
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

def page_admin_settings(store_info_df_raw: pd.DataFrame, master_df_raw: pd.DataFrame, orders_df: pd.DataFrame, balance_df: pd.DataFrame, transactions_df: pd.DataFrame, inventory_log_df: pd.DataFrame):
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
        render_system_audit_tab(store_info_df_raw, master_df_raw, orders_df, balance_df, transactions_df, inventory_log_df)
    with tabs[3]:
        # [추가] 활동 로그 탭 내용 렌더링
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
            # ▼▼▼ [수정] '활동 로그' 탭을 메인 탭 목록에서 제거 ▼▼▼
            admin_tabs = ["📊 대시보드", "🏭 일일 생산 보고", "📊 생산/재고 관리", "📋 발주요청 조회", "📈 매출 조회", "💰 결제 관리", "📑 증빙서류 다운로드", "🛠️ 관리 설정"]
            tabs = st.tabs(admin_tabs)
            
            with tabs[0]: page_admin_dashboard(get_master_df())
            with tabs[1]: page_admin_daily_production(get_master_df())
            with tabs[2]: page_admin_inventory_management(get_master_df())
            with tabs[3]: page_admin_unified_management(get_orders_df(), get_stores_df(), get_master_df())
            with tabs[4]: page_admin_sales_inquiry(get_orders_df())
            with tabs[5]: page_admin_balance_management(get_stores_df())
            with tabs[6]: page_admin_documents(get_stores_df(), get_master_df())
            # [수정] 탭 인덱스 변경 (8 -> 7)
            with tabs[7]:
                page_admin_settings(
                    get_stores_df(), get_master_df(), get_orders_df(), 
                    get_balance_df(), get_transactions_df(), get_inventory_log_df()
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
