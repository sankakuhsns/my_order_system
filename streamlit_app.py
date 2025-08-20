# -*- coding: utf-8 -*-
# =============================================================================
# 📦 Streamlit 식자재 발주 시스템 (v10.0 - 충전/결제 시스템 최종본)
#
# - 주요 기능:
#   - 지점별 잔액 충전 및 차감(결제) 시스템 구현
#   - 관리자의 충전 요청 승인 및 잔액 관리 기능
#   - 누적액이 포함된 신규 거래명세서 생성 기능
#   - 기존 v9.7의 모든 개선사항 포함
# =============================================================================

from io import BytesIO
from datetime import datetime, date, timedelta
from typing import Dict, Any, List, Optional
from collections.abc import Mapping
from zoneinfo import ZoneInfo
import math
import hashlib
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

def now_kst_str(fmt: str = "%Y-%m-%d %H:%M:%S") -> str: return datetime.now(KST).strftime(fmt)

def display_feedback():
    if "success_message" in st.session_state and st.session_state.success_message:
        st.success(st.session_state.success_message)
        st.session_state.success_message = ""

def v_spacer(height: int):
    st.markdown(f"<div style='height:{height}px'></div>", unsafe_allow_html=True)

def _normalize_store_info(store_info: pd.Series) -> dict:
    s = {k: ("" if pd.isna(v) else v) for k, v in store_info.to_dict().items()}
    return {
        "사업자등록번호": s.get("사업자등록번호") or s.get("사업자번호") or s.get("등록번호") or "",
        "상호명":         s.get("상호명") or s.get("지점명") or s.get("상호") or "",
        "사업장주소":     s.get("사업장주소") or s.get("주소") or "",
        "업태":           s.get("업태") or s.get("업종") or "",
    }

# =============================================================================
# 1) 시트/스키마 정의
# =============================================================================
SHEET_NAME_STORES = "지점마스터"
SHEET_NAME_MASTER = "상품마스터"
SHEET_NAME_ORDERS = "발주"
SHEET_NAME_BALANCE = "잔액마스터"
SHEET_NAME_CHARGE_REQ = "충전요청"
SHEET_NAME_TRANSACTIONS = "거래내역"

MASTER_COLUMNS = ["품목코드", "품목명", "품목규격", "분류", "단위", "단가", "과세구분", "활성"]
ORDERS_COLUMNS = ["주문일시", "발주번호", "지점ID", "지점명", "품목코드", "품목명", "단위", "수량", "단가", "공급가액", "세액", "합계금액", "비고", "상태", "처리일시", "처리자"]
CART_COLUMNS = ["품목코드", "품목명", "단위", "단가", "수량", "합계금액"]
BALANCE_COLUMNS = ["지점ID", "지점명", "현재잔액"]
CHARGE_REQ_COLUMNS = ["요청일시", "지점ID", "지점명", "입금자명", "입금액", "상태"]
TRANSACTIONS_COLUMNS = ["일시", "지점ID", "지점명", "구분", "내용", "금액", "처리후잔액", "관련발주번호", "처리자"]

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
    try: return get_gs_client().open_by_key(key)
    except Exception as e: st.error(f"스프레드시트 열기 실패: {e}"); st.stop()

@st.cache_data(ttl=60)
def load_data(sheet_name: str, columns: List[str] = None) -> pd.DataFrame:
    try:
        ws = open_spreadsheet().worksheet(sheet_name)
        df = pd.DataFrame(ws.get_all_records(empty2zero=False))
        
        numeric_cols = {
            SHEET_NAME_BALANCE: ['현재잔액'],
            SHEET_NAME_CHARGE_REQ: ['입금액'],
            SHEET_NAME_TRANSACTIONS: ['금액', '처리후잔액'],
            SHEET_NAME_ORDERS: ["수량", "단가", "공급가액", "세액", "합계금액"],
            SHEET_NAME_MASTER: ["단가"]
        }
        if sheet_name in numeric_cols:
            for col in numeric_cols[sheet_name]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        if columns:
            for col in columns:
                if col not in df.columns: df[col] = ''
            df = df[columns]
            
        if '주문일시' in df.columns: df = df.sort_values(by="주문일시", ascending=False)
            
        return df
    except gspread.WorksheetNotFound:
        st.warning(f"'{sheet_name}' 시트를 찾을 수 없습니다. 시트를 먼저 생성해주세요.")
        return pd.DataFrame(columns=columns) if columns else pd.DataFrame()

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

def update_balance(store_id: str, new_balance: int, store_name: str):
    try:
        ws = open_spreadsheet().worksheet(SHEET_NAME_BALANCE)
        cell = ws.find(store_id, in_column=1)
        if cell:
            ws.update_cell(cell.row, 3, new_balance)
        else:
            ws.append_row([store_id, store_name, new_balance])
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"잔액 업데이트 중 오류 발생: {e}")
        return False

def update_charge_request_status(request_timestamp: str, new_status: str):
    try:
        ws = open_spreadsheet().worksheet(SHEET_NAME_CHARGE_REQ)
        cell = ws.find(request_timestamp, in_column=1)
        if cell:
            ws.update_cell(cell.row, 6, new_status)
            st.cache_data.clear()
            return True
        return False
    except Exception as e:
        st.error(f"충전 요청 상태 업데이트 중 오류 발생: {e}")
        return False

def update_order_status(selected_ids: List[str], new_status: str, handler: str) -> bool:
    if not selected_ids: return True
    try:
        ws = open_spreadsheet().worksheet(SHEET_NAME_ORDERS)
        all_data = ws.get_all_values()
        header = all_data[0]
        id_col_idx = header.index("발주번호")
        status_col_idx = header.index("상태")
        handler_col_idx = header.index("처리자")
        timestamp_col_idx = header.index("처리일시")
        
        cells_to_update = []
        now_str = now_kst_str()
        for i, row in enumerate(all_data[1:], start=2):
            if row[id_col_idx] in selected_ids:
                cells_to_update.append(gspread.Cell(i, status_col_idx + 1, new_status))
                cells_to_update.append(gspread.Cell(i, handler_col_idx + 1, handler))
                cells_to_update.append(gspread.Cell(i, timestamp_col_idx + 1, now_str))
        if cells_to_update: ws.update_cells(cells_to_update, value_input_option='USER_ENTERED')
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"발주 상태 업데이트 중 오류가 발생했습니다: {e}")
        return False

# =============================================================================
# 3) 로그인 및 인증
# =============================================================================
@st.cache_data
def load_users_from_secrets() -> Dict[str, Dict[str, Any]]:
    cleaned: Dict[str, Dict[str, str]] = {}
    users_root = st.secrets.get("users", None)
    if isinstance(users_root, Mapping) and len(users_root) > 0:
        for uid, payload in users_root.items():
            if isinstance(payload, Mapping): cleaned[str(uid)] = _normalize_account(str(uid), payload)
    if not cleaned: st.error("로그인 계정을 찾을 수 없습니다. Secrets 의 [users] 구조를 확인하세요."); st.stop()
    return cleaned

def _normalize_account(uid: str, payload: Mapping) -> dict:
    pwd_plain, pwd_hash = payload.get("password"), payload.get("password_hash")
    name = str(payload.get("name", uid)).strip()
    role = str(payload.get("role", "store")).strip().lower()
    if not (pwd_plain or pwd_hash): st.error(f"[users.{uid}]에 password 또는 password_hash가 필요합니다."); st.stop()
    if role not in {"store", "admin"}: st.error(f"[users.{uid}].role 은 'store' 또는 'admin' 이어야 합니다. (현재: {role})"); st.stop()
    return {"password": str(pwd_plain) if pwd_plain else None, "password_hash": str(pwd_hash).lower() if pwd_hash else None, "name": name, "role": role}

USERS = load_users_from_secrets()

def require_login():
    if st.session_state.get("auth", {}).get("login"): return True
    st.markdown('<div style="text-align:center; font-size:42px; font-weight:800; margin:16px 0 12px;">식자재 발주 시스템</div>', unsafe_allow_html=True)
    _, mid, _ = st.columns([3, 2, 3])
    with mid.form("login_form"):
        uid = st.text_input("아이디 또는 지점명", key="login_uid", placeholder="예: jeondae / 전대점")
        pwd = st.text_input("비밀번호", type="password", key="login_pw")
        if st.form_submit_button("로그인", use_container_width=True):
            real_uid, acct = _find_account(uid)
            if not (real_uid and acct and verify_password(pwd, acct.get("password_hash"), acct.get("password"))):
                st.error("아이디(또는 지점명) 또는 비밀번호가 올바르지 않습니다.")
            else:
                st.session_state["auth"] = {"login": True, "user_id": real_uid, "name": acct["name"], "role": acct["role"]}; st.rerun()
    return False

def verify_password(input_pw: str, stored_hash: Optional[str], fallback_plain: Optional[str]) -> bool:
    if stored_hash: return hashlib.sha256(input_pw.encode()).hexdigest() == stored_hash.strip().lower()
    return str(input_pw) == str(fallback_plain) if fallback_plain is not None else False

def _find_account(uid_or_name: str):
    s_lower = str(uid_or_name or "").strip().lower()
    if not s_lower: return None, None
    for uid, acct in USERS.items():
        if uid.lower() == s_lower or acct.get("name", "").lower() == s_lower: return uid, acct
    return None, None
    
# =============================================================================
# 4) Excel 생성 (코드로 직접 그리기)
# =============================================================================
def make_order_id(store_id: str) -> str: return f"{datetime.now(KST):%Y%m%d%H%M%S}{store_id}"

def make_full_transaction_statement_excel(df_transactions: pd.DataFrame, store_info: pd.Series) -> BytesIO:
    output = BytesIO()
    if df_transactions.empty: return output

    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        workbook = writer.book
        worksheet = workbook.add_worksheet(f"{store_info['상호명']} 거래명세서")

        fmt_title = workbook.add_format({'bold': True, 'font_size': 18, 'align': 'center', 'valign': 'vcenter'})
        fmt_header = workbook.add_format({'bold': True, 'bg_color': '#F2F2F2', 'border': 1, 'align': 'center', 'valign': 'vcenter'})
        fmt_money = workbook.add_format({'num_format': '#,##0', 'border': 1})
        fmt_border_c = workbook.add_format({'border': 1, 'align': 'center', 'valign': 'vcenter'})
        fmt_border_l = workbook.add_format({'border': 1, 'align': 'left', 'valign': 'vcenter'})

        worksheet.set_paper(9); worksheet.set_landscape(); worksheet.fit_to_pages(1, 1)
        worksheet.set_margins(left=0.5, right=0.5, top=0.5, bottom=0.5)
        col_widths = {'A': 20, 'B': 10, 'C': 30, 'D': 15, 'E': 15}
        for col, width in col_widths.items(): worksheet.set_column(f'{col}:{col}', width)

        worksheet.merge_range('A1:E1', f"{store_info['상호명']} 거래 상세 명세서", fmt_title)
        headers = ['일시', '구분', '내용', '금액', '누적 잔액']
        worksheet.write_row('A3', headers, fmt_header)

        df_transactions['금액'] = pd.to_numeric(df_transactions['금액'], errors='coerce').fillna(0)
        df_transactions_sorted = df_transactions.sort_values(by="일시").reset_index(drop=True)
        df_transactions_sorted['누적액'] = df_transactions_sorted['금액'].cumsum()
        
        row_num = 3
        for _, row in df_transactions_sorted.iterrows():
            row_num += 1
            worksheet.write(f'A{row_num}', str(row['일시']), fmt_border_c)
            worksheet.write(f'B{row_num}', row['구분'], fmt_border_c)
            worksheet.write(f'C{row_num}', row['내용'], fmt_border_l)
            worksheet.write(f'D{row_num}', row['금액'], fmt_money)
            worksheet.write(f'E{row_num}', row['누적액'], fmt_money)
            
    return output

def make_sales_summary_excel(daily_pivot: pd.DataFrame, monthly_pivot: pd.DataFrame) -> BytesIO:
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        daily_pivot.reset_index().to_excel(writer, sheet_name='일별매출현황', index=False)
        monthly_pivot.reset_index().to_excel(writer, sheet_name='월별매출현황', index=False)
        workbook = writer.book
        h_format = workbook.add_format({'bold': True, 'font_size': 18, 'align': 'center', 'valign': 'vcenter'})
        header_format = workbook.add_format({'bold': True, 'bg_color': '#F2F2F2', 'border': 1, 'align': 'center'})
        money_format = workbook.add_format({'num_format': '#,##0', 'border': 1})
        for name, pivot_df in [('일별매출현황', daily_pivot), ('월별매출현황', monthly_pivot)]:
            worksheet = writer.sheets[name]
            worksheet.set_zoom(90)
            df_for_format = pivot_df.reset_index()
            worksheet.merge_range(0, 0, 0, len(df_for_format.columns) - 1, f"거래처별 {name}", h_format)
            for col_num, value in enumerate(df_for_format.columns.values):
                worksheet.write(2, col_num, value, header_format)
            worksheet.set_column(0, len(df_for_format.columns), 14)
            worksheet.conditional_format(3, 1, len(df_for_format) + 2, len(df_for_format.columns), {'type': 'no_blanks', 'format': money_format})
    return output

# =============================================================================
# 5) 장바구니 유틸
# =============================================================================
def init_session_state():
    defaults = {"cart": pd.DataFrame(columns=CART_COLUMNS), "store_editor_ver": 0, "success_message": ""}
    for key, value in defaults.items():
        if key not in st.session_state: st.session_state[key] = value

def coerce_cart_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in CART_COLUMNS:
        if col not in out.columns: out[col] = 0 if col in ["단가", "수량", "합계금액"] else ""
    out["수량"] = pd.to_numeric(out["수량"], errors="coerce").fillna(0).astype(int)
    out["단가"] = pd.to_numeric(out["단가"], errors="coerce").fillna(0).astype(int)
    out["합계금액"] = out["단가"] * out["수량"]
    return out

def add_to_cart(rows_df: pd.DataFrame):
    add = rows_df[rows_df["수량"] > 0].copy()
    if add.empty: return
    add["합계금액"] = add["단가"] * add["수량"]
    cart = st.session_state.cart.copy()
    merged = pd.concat([cart, add]).groupby("품목코드", as_index=False).agg({"품목명": "last", "단위": "last", "단가": "last", "수량": "sum"})
    merged["합계금액"] = merged["단가"] * merged["수량"]
    st.session_state.cart = merged[CART_COLUMNS]

# =============================================================================
# 6) 지점 페이지
# =============================================================================
def page_store_register_confirm(master_df: pd.DataFrame, current_balance: int):
    st.subheader("🛒 발주 요청")
    user = st.session_state.auth
    st.info(f"**현재 잔액: {current_balance:,.0f}원**")
    v_spacer(10)
    with st.container(border=True):
        st.markdown("##### 🗓️ 요청사항")
        memo = st.text_area("요청 사항(선택)", height=80, placeholder="예: 2025-12-25 에 출고 부탁드립니다", key="store_reg_memo")
    v_spacer(16)
    with st.container(border=True):
        st.markdown("##### 🧾 발주 수량 입력")
        l, r = st.columns([2, 1])
        keyword = l.text_input("품목 검색(이름/코드)", placeholder="오이, P001 등", key="store_reg_keyword")
        cat_opt = ["(전체)"] + sorted(master_df["분류"].dropna().unique().tolist())
        cat_sel = r.selectbox("분류(선택)", cat_opt, key="store_reg_category")
        df_view = master_df.copy()
        if keyword: df_view = df_view[df_view.apply(lambda row: keyword.strip().lower() in str(row["품목명"]).lower() or keyword.strip().lower() in str(row["품목코드"]).lower(), axis=1)]
        if cat_sel != "(전체)": df_view = df_view[df_view["분류"] == cat_sel]
        with st.form(key="add_to_cart_form"):
            df_edit = df_view[["품목코드", "품목명", "단위", "단가", "과세구분"]].copy()
            df_edit["단가(VAT포함)"] = df_edit.apply(lambda row: row['단가'] * 1.1 if row['과세구분'] == '과세' else row['단가'], axis=1).astype(int)
            df_edit["수량"] = 0
            df_edit.rename(columns={"단가": "단가(원)"}, inplace=True)
            edited_disp = st.data_editor(df_edit[["품목코드", "품목명", "단위", "단가(원)", "단가(VAT포함)", "수량"]], key=f"editor_v{st.session_state.store_editor_ver}", hide_index=True, disabled=["품목코드", "품목명", "단위", "단가(원)", "단가(VAT포함)"], use_container_width=True, column_config={"단가(원)": st.column_config.NumberColumn(), "단가(VAT포함)": st.column_config.NumberColumn(), "수량": st.column_config.NumberColumn(min_value=0)})
            if st.form_submit_button("장바구니 추가", use_container_width=True, type="primary"):
                edited_disp.rename(columns={"단가(원)": "단가"}, inplace=True)
                items_to_add = coerce_cart_df(edited_disp)
                if not items_to_add[items_to_add["수량"] > 0].empty:
                    add_to_cart(items_to_add); st.session_state.store_editor_ver += 1
                st.rerun()
    v_spacer(16)
    with st.container(border=True):
        st.markdown("##### 🧺 장바구니")
        cart = st.session_state.cart
        if not cart.empty:
            cart_display = pd.merge(cart, master_df[['품목코드', '과세구분']], on='품목코드', how='left')
            cart_display.rename(columns={"합계금액": "공급가액"}, inplace=True)
            cart_display['합계금액(VAT포함)'] = cart_display.apply(lambda row: row['공급가액'] + math.ceil(row['공급가액'] * 0.1) if row.get('과세구분') == '과세' else row['공급가액'], axis=1).astype(int)
            cart_display.rename(columns={"단가": "단가(원)", "공급가액": "공급가액(원)"}, inplace=True)
            edited_cart = st.data_editor(cart_display[["품목코드", "품목명", "단위", "단가(원)", "수량", "공급가액(원)", "합계금액(VAT포함)"]], key="cart_editor", hide_index=True, disabled=["품목코드", "품목명", "단위", "단가(원)", "공급가액(원)", "합계금액(VAT포함)"], column_config={"단가(원)": st.column_config.NumberColumn(), "수량": st.column_config.NumberColumn(min_value=0), "공급가액(원)": st.column_config.NumberColumn(), "합계금액(VAT포함)": st.column_config.NumberColumn()})
            edited_cart.rename(columns={"단가(원)": "단가", "공급가액(원)": "합계금액"}, inplace=True)
            st.session_state.cart = coerce_cart_df(edited_cart)
            if st.button("장바구니 비우기", use_container_width=True): st.session_state.cart = pd.DataFrame(columns=CART_COLUMNS); st.rerun()
        else: st.info("장바구니가 비어 있습니다.")
    v_spacer(16)
    with st.form("submit_form"):
        cart_now = st.session_state.cart
        total_final_amount_sum = 0
        cart_with_master = pd.DataFrame()
        if not cart_now.empty:
            cart_with_master = pd.merge(cart_now, master_df[['품목코드', '과세구분']], on='품목코드', how='left')
            cart_with_master['공급가액'] = cart_with_master['단가'] * cart_with_master['수량']
            cart_with_master['최종합계'] = cart_with_master.apply(lambda r: r['공급가액'] + math.ceil(r['공급가액'] * 0.1) if r['과세구분'] == '과세' else r['공급가액'], axis=1)
            total_final_amount_sum = int(cart_with_master['최종합계'].sum())

        st.markdown(f"**최종 확인:** 총 {len(cart_now)}개 품목, 최종 합계금액(VAT포함) **{total_final_amount_sum:,.0f}원**")
        confirm = st.checkbox("위 내용으로 발주를 제출합니다.")
        can_order = current_balance >= total_final_amount_sum
        if not can_order and not cart_now.empty:
            st.error(f"잔액이 부족합니다. (부족 금액: {total_final_amount_sum - current_balance:,.0f}원)")

        if st.form_submit_button("📦 발주 제출 및 결제", type="primary", use_container_width=True, disabled=cart_now.empty or not can_order):
            if not confirm: st.warning("제출 확인 체크박스를 선택해주세요."); st.stop()
            order_id = make_order_id(user["user_id"])
            rows = []
            for _, r in cart_with_master.iterrows():
                supply_price = r['공급가액']
                tax = math.ceil(supply_price * 0.1) if r.get('과세구분', '과세') == '과세' else 0
                rows.append({"주문일시": now_kst_str(), "발주번호": order_id, "지점ID": user["user_id"], "지점명": user["name"], "품목코드": r["품목코드"], "품목명": r["품목명"], "단위": r["단위"], "수량": r["수량"], "단가": r["단가"], "공급가액": supply_price, "세액": tax, "합계금액": supply_price + tax, "비고": memo, "상태": "접수"})
            
            if append_rows_to_sheet(SHEET_NAME_ORDERS, rows, ORDERS_COLUMNS):
                new_balance = current_balance - total_final_amount_sum
                transaction_record = {
                    "일시": now_kst_str(), "지점ID": user["user_id"], "지점명": user["name"],
                    "구분": "차감", "내용": f"{cart_now.iloc[0]['품목명']} 등 {len(cart_now)}건 발주",
                    "금액": -total_final_amount_sum, "처리후잔액": new_balance,
                    "관련발주번호": order_id, "처리자": user["name"]
                }
                append_rows_to_sheet(SHEET_NAME_TRANSACTIONS, [transaction_record], TRANSACTIONS_COLUMNS)
                update_balance(user["user_id"], new_balance, user["name"])
                st.session_state.success_message = "발주 및 결제가 성공적으로 완료되었습니다."
                st.session_state.cart = pd.DataFrame(columns=CART_COLUMNS)
                st.rerun()
            else:
                st.error("발주 제출 중 오류가 발생했습니다.")

def page_store_balance(charge_requests_df: pd.DataFrame):
    st.subheader("💰 금액 충전")
    user = st.session_state.auth
    st.info("**입금 계좌: OOO은행 123-456-789 (주)산카쿠**\n\n위 계좌로 입금하신 후, 아래 양식을 작성하여 '알림 보내기' 버튼을 눌러주세요.")
    with st.form("charge_request_form", border=True):
        st.markdown("##### 입금 완료 알림 보내기")
        depositor_name = st.text_input("입금자명")
        charge_amount = st.number_input("입금액", min_value=1000, step=1000, format="%d")
        if st.form_submit_button("알림 보내기", type="primary"):
            if depositor_name and charge_amount > 0:
                new_request = {
                    "요청일시": now_kst_str(), "지점ID": user["user_id"], "지점명": user["name"],
                    "입금자명": depositor_name, "입금액": charge_amount, "상태": "확인대기"
                }
                if append_rows_to_sheet(SHEET_NAME_CHARGE_REQ, [new_request], CHARGE_REQ_COLUMNS):
                    st.success("관리자에게 입금 완료 알림을 보냈습니다. 확인 후 잔액이 충전됩니다.")
                else: st.error("알림 전송에 실패했습니다.")
            else: st.warning("입금자명과 입금액을 모두 입력해주세요.")
    st.markdown("---")
    st.markdown("##### 나의 충전 요청 현황")
    my_requests = charge_requests_df[charge_requests_df['지점ID'] == user['user_id']].sort_values(by="요청일시", ascending=False)
    st.dataframe(my_requests, use_container_width=True, hide_index=True)

def page_store_orders_change(store_info_df: pd.DataFrame, master_df: pd.DataFrame):
    st.subheader("🧾 발주 조회·수정")
    display_feedback()
    df_all, user = load_data(SHEET_NAME_ORDERS, ORDERS_COLUMNS), st.session_state.auth
    df_user = df_all[df_all["지점ID"] == user["user_id"]]
    if df_user.empty: st.info("발주 데이터가 없습니다."); return
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
    
    orders = df_filtered.groupby("발주번호").agg(주문일시=("주문일시", "first"), 건수=("품목코드", "count"), 합계금액=("합계금액", "sum"), 상태=("상태", "first"), 처리일시=("처리일시", "first")).reset_index().sort_values("주문일시", ascending=False)
    orders.rename(columns={"합계금액": "합계금액(원)"}, inplace=True)
    pending = orders[orders["상태"] == "접수"].copy()
    shipped = orders[orders["상태"] == "출고완료"].copy()
    shipped.rename(columns={"처리일시": "출고일시"}, inplace=True)

    if 'store_pending_selection' not in st.session_state: st.session_state.store_pending_selection = {}
    if 'store_shipped_selection' not in st.session_state: st.session_state.store_shipped_selection = {}
    tab1, tab2 = st.tabs([f"접수 ({len(pending)}건)", f"출고완료 ({len(shipped)}건)"])
    with tab1:
        pending.insert(0, "선택", pending['발주번호'].apply(lambda x: st.session_state.store_pending_selection.get(x, False)))
        edited_pending = st.data_editor(pending, key="store_pending_editor", hide_index=True, disabled=["주문일시", "발주번호", "건수", "합계금액(원)", "상태"], column_order=("선택", "주문일시", "발주번호", "건수", "합계금액(원)", "상태"), column_config={"합계금액(원)": st.column_config.NumberColumn(), "선택": st.column_config.CheckboxColumn(width="small")})
        st.session_state.store_pending_selection = dict(zip(edited_pending['발주번호'], edited_pending['선택']))
        selected_pending_ids = [k for k, v in st.session_state.store_pending_selection.items() if v]
        if st.button("선택 발주 삭제", disabled=not selected_pending_ids, key="delete_pending_btn"):
            if update_order_status(selected_pending_ids, "삭제", user["name"]):
                st.session_state.success_message = f"{len(selected_pending_ids)}건의 발주가 삭제되었습니다."; st.rerun()
    with tab2:
        shipped.insert(0, "선택", shipped['발주번호'].apply(lambda x: st.session_state.store_shipped_selection.get(x, False)))
        edited_shipped = st.data_editor(shipped, key="store_shipped_editor", hide_index=True, disabled=["주문일시", "발주번호", "건수", "합계금액(원)", "상태", "출고일시"], column_order=("선택", "주문일시", "발주번호", "건수", "합계금액(원)", "상태", "출고일시"), column_config={"합계금액(원)": st.column_config.NumberColumn(), "선택": st.column_config.CheckboxColumn(width="small")})
        st.session_state.store_shipped_selection = dict(zip(edited_shipped['발주번호'], edited_shipped['선택']))
    v_spacer(16)
    with st.container(border=True):
        st.markdown("##### 📄 발주 품목 상세 조회")
        selected_ids = [k for k,v in {**st.session_state.store_pending_selection, **st.session_state.store_shipped_selection}.items() if v]
        if len(selected_ids) == 1:
            target_id = selected_ids[0]
            target_df = df_user[df_user["발주번호"] == target_id]
            df_display = target_df.copy().rename(columns={"단가": "단가(원)", "공급가액": "공급가액(원)", "세액": "세액(원)", "합계금액": "합계금액(원)"})
            display_cols = ["품목코드", "품목명", "단위", "수량", "단가(원)", "공급가액(원)", "세액(원)", "합계금액(원)"]
            st.dataframe(df_display[display_cols], hide_index=True, use_container_width=True, column_config={"단가(원)": st.column_config.NumberColumn(), "공급가액(원)": st.column_config.NumberColumn(), "세액(원)": st.column_config.NumberColumn(), "합계금액(원)": st.column_config.NumberColumn()})
        else:
            st.info("상세 내용을 보려면 위 목록에서 발주를 **하나만** 선택하세요.")

def page_store_documents(store_info_df: pd.DataFrame):
    st.subheader("📑 증빙서류 다운로드")
    user = st.session_state.auth
    transactions_df = load_data(SHEET_NAME_TRANSACTIONS)
    my_transactions = transactions_df[transactions_df['지점ID'] == user['user_id']]
    if my_transactions.empty: st.info("거래 내역이 없습니다."); return
    c1, c2 = st.columns(2)
    dt_from = c1.date_input("조회 시작일", date.today() - timedelta(days=30), key="store_doc_from")
    dt_to = c2.date_input("조회 종료일", date.today(), key="store_doc_to")
    my_transactions['일시_dt'] = pd.to_datetime(my_transactions['일시']).dt.date
    mask = (my_transactions['일시_dt'] >= dt_from) & (my_transactions['일시_dt'] <= dt_to)
    dfv = my_transactions[mask].copy()
    if dfv.empty: st.warning("해당 기간의 거래 내역이 없습니다."); return
    st.dataframe(dfv.drop(columns=['일시_dt']), use_container_width=True, hide_index=True)
    my_store_info = store_info_df[store_info_df['지점ID'] == user['user_id']].iloc[0]
    buf = make_full_transaction_statement_excel(dfv, my_store_info)
    st.download_button("상세 거래명세서 다운로드", data=buf, file_name=f"상세거래명세서_{user['name']}_{dt_from}_to_{dt_to}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True, type="primary")

def page_store_master_view(master_df: pd.DataFrame):
    st.subheader("🏷️ 품목 단가 조회")
    l, r = st.columns([2, 1])
    keyword = l.text_input("품목 검색(이름/코드)", placeholder="오이, P001 등", key="store_master_keyword")
    cat_opt = ["(전체)"] + sorted(master_df["분류"].dropna().unique().tolist())
    cat_sel = r.selectbox("분류(선택)", cat_opt, key="store_master_category")
    df_view = master_df.copy()
    if keyword: df_view = df_view[df_view.apply(lambda row: keyword.strip().lower() in str(row["품목명"]).lower() or keyword.strip().lower() in str(row["품목코드"]).lower(), axis=1)]
    if cat_sel != "(전체)": df_view = df_view[df_view["분류"] == cat_sel]
    df_view['단가(VAT포함)'] = df_view.apply(lambda row: row['단가'] * 1.1 if row['과세구분'] == '과세' else row['단가'], axis=1).astype(int)
    df_view = df_view.rename(columns={"단가": "단가(원)"})
    st.dataframe(df_view[["품목코드", "품목명", "품목규격", "분류", "단위", "단가(원)", "단가(VAT포함)"]], use_container_width=True, hide_index=True, column_config={"단가(원)": st.column_config.NumberColumn(), "단가(VAT포함)": st.column_config.NumberColumn()})

# =============================================================================
# 7) 관리자 페이지
# =============================================================================
def page_admin_unified_management(df_all: pd.DataFrame, store_info_df: pd.DataFrame, master_df: pd.DataFrame):
    st.subheader("📋 발주요청 조회·수정")
    display_feedback()
    if df_all.empty: st.info("발주 데이터가 없습니다."); return
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
        df['주문일시_dt'] = pd.to_datetime(df['주문일시']).dt.date
        df = df[(df['주문일시_dt'] >= dt_from) & (df['주문일시_dt'] <= dt_to)]
        if store != "(전체)": df = df[df["지점명"] == store]
    
    # [수정] 출고일시(처리일시) 추가
    orders = df.groupby("발주번호").agg(주문일시=("주문일시", "first"), 지점명=("지점명", "first"), 건수=("품목코드", "count"), 합계금액=("합계금액", "sum"), 상태=("상태", "first"), 처리일시=("처리일시", "first")).reset_index().sort_values("주문일시", ascending=False)
    orders.rename(columns={"합계금액": "합계금액(원)"}, inplace=True)
    pending = orders[orders["상태"] == "접수"].copy()
    shipped = orders[orders["상태"] == "출고완료"].copy()
    shipped.rename(columns={"처리일시": "출고일시"}, inplace=True)

    if 'admin_pending_selection' not in st.session_state: st.session_state.admin_pending_selection = {}
    if 'admin_shipped_selection' not in st.session_state: st.session_state.admin_shipped_selection = {}
    tab1, tab2 = st.tabs([f"📦 발주 요청 접수 ({len(pending)}건)", f"✅ 출고 완료 ({len(shipped)}건)"])
    with tab1:
        pending.insert(0, '선택', pending['발주번호'].apply(lambda x: st.session_state.admin_pending_selection.get(x, False)))
        # [수정] 컬럼 순서 변경
        edited_pending = st.data_editor(pending, key="admin_pending_editor", hide_index=True, disabled=pending.columns.drop("선택"), column_order=("선택", "주문일시", "발주번호", "지점명", "건수", "합계금액(원)", "상태"), column_config={"합계금액(원)": st.column_config.NumberColumn()})
        st.session_state.admin_pending_selection = dict(zip(edited_pending['발주번호'], edited_pending['선택']))
        selected_pending_ids = [k for k, v in st.session_state.admin_pending_selection.items() if v]
        if st.button("✅ 선택 발주 출고", disabled=not selected_pending_ids, key="admin_ship_btn"):
            if update_order_status(selected_pending_ids, "출고완료", st.session_state.auth["name"]):
                st.session_state.success_message = f"{len(selected_pending_ids)}건이 출고 처리되었습니다."; st.rerun()
    with tab2:
        shipped.insert(0, '선택', shipped['발주번호'].apply(lambda x: st.session_state.admin_shipped_selection.get(x, False)))
        # [수정] 컬럼 순서 및 출고일시 추가
        edited_shipped = st.data_editor(shipped, key="admin_shipped_editor", hide_index=True, disabled=shipped.columns.drop("선택"), column_order=("선택", "주문일시", "발주번호", "지점명", "건수", "합계금액(원)", "상태", "출고일시"), column_config={"합계금액(원)": st.column_config.NumberColumn()})
        st.session_state.admin_shipped_selection = dict(zip(edited_shipped['발주번호'], edited_shipped['선택']))
        selected_shipped_ids = [k for k, v in st.session_state.admin_shipped_selection.items() if v]
        if st.button("↩️ 접수 상태로 변경", disabled=not selected_shipped_ids, key="admin_revert_btn"):
            if update_order_status(selected_shipped_ids, "접수", st.session_state.auth["name"]):
                st.session_state.success_message = f"{len(selected_shipped_ids)}건이 접수 상태로 변경되었습니다."; st.rerun()
    v_spacer(16)
    with st.container(border=True):
        st.markdown("##### 📄 발주 품목 상세 조회")
        selected_ids = [k for k,v in {**st.session_state.admin_pending_selection, **st.session_state.admin_shipped_selection}.items() if v]
        if len(selected_ids) == 1:
            target_id = selected_ids[0]
            st.markdown(f"**선택된 발주번호:** `{target_id}`")
            target_df = df_all[df_all["발주번호"] == target_id]
            target_status = target_df.iloc[0]["상태"]
            df_display = target_df.copy().rename(columns={"단가": "단가(원)", "공급가액": "공급가액(원)", "세액": "세액(원)", "합계금액": "합계금액(원)"})
            display_cols = ["품목코드", "품목명", "단위", "수량", "단가(원)", "공급가액(원)", "세액(원)", "합계금액(원)"]
            st.dataframe(df_display[display_cols], hide_index=True, use_container_width=True, column_config={"단가(원)": st.column_config.NumberColumn(), "공급가액(원)": st.column_config.NumberColumn(), "세액(원)": st.column_config.NumberColumn(), "합계금액(원)": st.column_config.NumberColumn()})
            if target_status == '출고완료':
                v_spacer(10)
                store_id = target_df.iloc[0]["지점ID"]
                store_info_series = store_info_df[store_info_df["지점ID"] == store_id]
                if not store_info_series.empty:
                    store_info = store_info_series.iloc[0]
                    buf = make_trading_statement_excel(target_df, store_info, master_df)
                    st.download_button(f"'{target_id}' 거래명세서 다운로드", data=buf, file_name=f"거래명세서_{store_info.get('상호명')}_{target_id}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
        else:
            st.info("상세 내용을 보려면 위 목록에서 발주를 **하나만** 선택하세요.")

def page_admin_documents(store_info_df: pd.DataFrame, master_df: pd.DataFrame):
    st.subheader("📑 증빙서류 다운로드")
    df = load_orders_df()
    df_completed = df[df["상태"] == "출고완료"]
    if df_completed.empty: st.info("'출고완료' 상태의 발주 데이터가 없습니다."); return
    search_mode = st.radio("조회 방식", ["기간으로 조회", "발주번호로 조회"], key="admin_doc_search_mode", horizontal=True)
    dfv = pd.DataFrame(); doc_type = "거래명세서"
    if search_mode == "기간으로 조회":
        c1, c2, c3, c4 = st.columns(4)
        dt_from = c1.date_input("조회 시작일", date.today() - timedelta(days=30), key="admin_doc_from")
        dt_to = c2.date_input("조회 종료일", date.today(), key="admin_doc_to")
        stores = sorted(df_completed["지점명"].dropna().unique().tolist())
        store_sel = c3.selectbox("지점 선택", stores, key="admin_doc_store")
        doc_type = c4.selectbox("문서 종류", ["거래명세서", "세금계산서"], key="admin_doc_type")
        df_completed['주문일시_dt'] = pd.to_datetime(df_completed['주문일시']).dt.date
        mask = (df_completed['주문일시_dt'] >= dt_from) & (df_completed['주문일시_dt'] <= dt_to) & (df_completed["지점명"] == store_sel)
        dfv = df_completed[mask].copy()
    else:
        c1, c2 = st.columns([1, 1])
        order_ids = sorted(df_completed["발주번호"].dropna().unique().tolist(), reverse=True)
        order_id_sel = c1.selectbox("발주번호 선택", [""] + order_ids, key="admin_doc_order_id")
        doc_type = c2.selectbox("문서 종류", ["거래명세서", "세금계산서"], key="admin_doc_type_by_id")
        if order_id_sel: dfv = df_completed[df_completed["발주번호"] == order_id_sel].copy()
    if dfv.empty: st.warning("해당 조건으로 조회된 데이터가 없습니다."); return
    st.dataframe(dfv.drop(columns=['주문일시_dt'], errors='ignore'), use_container_width=True, hide_index=True)
    if not dfv.empty:
        store_id = dfv.iloc[0]["지점ID"]; store_name = dfv.iloc[0]["지점명"]
        store_info_series = store_info_df[store_info_df["지점ID"] == store_id]
        if not store_info_series.empty:
            store_info = store_info_series.iloc[0]
            buf = make_trading_statement_excel(dfv, store_info, master_df) if doc_type == "거래명세서" else make_tax_invoice_excel(dfv, store_info, master_df)
            st.download_button(f"'{store_name}' {doc_type} 다운로드", data=buf, file_name=f"{doc_type}_{store_name}_{now_kst_str('%Y%m%d')}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True, type="primary")
        else: st.error("지점 정보를 찾을 수 없어 서류를 생성할 수 없습니다.")

def page_admin_items_price(master_df: pd.DataFrame):
    st.subheader("🏷️ 품목 단가 설정")
    st.caption("단가(VAT 제외)를 수정하거나 품목을 추가/삭제한 후 '변경사항 저장' 버튼을 누르세요. 모든 변경 내역은 로그에 기록됩니다.")
    original_df = master_df.copy()
    with st.form("master_edit_form"):
        df_display = master_df.rename(columns={"단가": "단가(원)"})
        edited = st.data_editor(df_display.assign(삭제=False), hide_index=True, num_rows="dynamic", use_container_width=True, column_config={"단가(원)": st.column_config.NumberColumn()})
        if st.form_submit_button("변경사항 저장", type="primary", use_container_width=True):
            edited.rename(columns={"단가(원)": "단가"}, inplace=True)
            edited['삭제'] = edited['삭제'].fillna(False).astype(bool)
            final_df = edited[~edited["삭제"]].drop(columns=["삭제"])
            if write_master_df(final_df, original_df):
                st.session_state.success_message = "상품마스터가 저장되었습니다."; st.rerun()

def page_admin_sales_inquiry(master_df: pd.DataFrame):
    st.subheader("📈 매출 조회")
    df_orders = load_orders_df()
    df_sales_raw = df_orders[df_orders['상태'] == '출고완료'].copy()
    if df_sales_raw.empty: st.info("'출고완료'된 매출 데이터가 없습니다."); return

    c1, c2, c3 = st.columns(3)
    dt_from = c1.date_input("조회 시작일", date.today().replace(day=1), key="admin_sales_from")
    dt_to = c2.date_input("조회 종료일", date.today(), key="admin_sales_to")
    stores = ["(전체 통합)"] + sorted(df_sales_raw["지점명"].dropna().unique().tolist())
    store_sel = c3.selectbox("조회 지점", stores, key="admin_sales_store")

    df_sales_raw['주문일시_dt'] = pd.to_datetime(df_sales_raw['주문일시']).dt.date
    mask = (df_sales_raw['주문일시_dt'] >= dt_from) & (df_sales_raw['주문일시_dt'] <= dt_to)
    if store_sel != "(전체 통합)": mask &= (df_sales_raw["지점명"] == store_sel)
    df_sales = df_sales_raw[mask].copy()

    if df_sales.empty: st.warning("해당 조건의 매출 데이터가 없습니다."); st.stop()

    # [수정] 메트릭 UI 개선
    total_sales = df_sales["합계금액"].sum()
    total_supply = df_sales["공급가액"].sum()
    total_tax = df_sales["세액"].sum()
    with st.container(border=True):
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("총 매출 (VAT 포함)", f"{total_sales:,}원")
        m2.metric("공급가액", f"{total_supply:,}원")
        m3.metric("부가세액", f"{total_tax:,}원")
        m4.metric("총 발주 건수", f"{df_sales['발주번호'].nunique()} 건")
    
    st.divider()

    sales_tab1, sales_tab2, sales_tab3 = st.tabs(["📊 종합 분석", "📅 일별 상세", "🗓️ 월별 상세"])
    with sales_tab1:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("##### 🏢 **지점별 매출 순위**")
            store_sales = df_sales.groupby("지점명")["합계금액"].sum().nlargest(10).reset_index()
            store_sales.rename(columns={"합계금액": "매출액(원)"}, inplace=True)
            st.dataframe(store_sales, use_container_width=True, hide_index=True, column_config={"지점명": "지점", "매출액(원)": st.column_config.NumberColumn()})
        with col2:
            st.markdown("##### 🍔 **품목별 판매 순위 (Top 10)**")
            item_sales = df_sales.groupby("품목명").agg(수량=('수량', 'sum'), 매출액=('합계금액', 'sum')).nlargest(10, '매출액').reset_index()
            total_item_sales = item_sales['매출액'].sum()
            if total_item_sales > 0:
                item_sales['매출비중(%)'] = (item_sales['매출액'] / total_item_sales * 100).round(1)
            else:
                item_sales['매출비중(%)'] = 0
            st.dataframe(item_sales, use_container_width=True, hide_index=True, column_config={"매출액": st.column_config.NumberColumn(format="%d원"), "매출비중(%)": st.column_config.ProgressColumn(format="%.1f%%", min_value=0, max_value=item_sales['매출비중(%)'].max() if not item_sales.empty else 1)})
    
    # [수정] 일별/월별 상세 로직 개편
    df_sales['주문일시'] = pd.to_datetime(df_sales['주문일시'])
    df_sales['연'] = df_sales['주문일시'].dt.year
    df_sales['월'] = df_sales['주문일시'].dt.month
    df_sales['일'] = df_sales['주문일시'].dt.day

    daily_pivot = df_sales.groupby(['연', '월', '일', '지점명'])['합계금액'].sum().unstack(fill_value=0)
    if not daily_pivot.empty: daily_pivot['총 합계'] = daily_pivot.sum(axis=1)
    
    monthly_pivot = df_sales.groupby(['연', '월', '지점명'])['합계금액'].sum().unstack(fill_value=0)
    if not monthly_pivot.empty: monthly_pivot['총 합계'] = monthly_pivot.sum(axis=1)

    with sales_tab2:
        st.markdown("##### 📅 일별 매출 상세")
        if not daily_pivot.empty: 
            st.dataframe(daily_pivot.style.format("{:,.0f}"))
            st.markdown("---")
            st.markdown("##### 📈 일별 매출 추이")
            daily_summary = df_sales.groupby(df_sales['주문일시'].dt.date)['합계금액'].sum()
            st.line_chart(daily_summary)

    with sales_tab3:
        st.markdown("##### 🗓️ 월별 매출 상세")
        if not monthly_pivot.empty: 
            st.dataframe(monthly_pivot.style.format("{:,.0f}"))
            st.markdown("---")
            st.markdown("##### 📊 월별 매출 비교")
            monthly_summary = df_sales.set_index('주문일시').resample('M')['합계금액'].sum()
            monthly_summary.index = monthly_summary.index.strftime('%Y-%m')
            st.bar_chart(monthly_summary)
        
    st.divider()
    excel_buffer = make_sales_summary_excel(daily_pivot, monthly_pivot, f"매출정산표_{dt_from}~{dt_to}")
    st.download_button(label="📥 매출 정산표 다운로드", data=excel_buffer, file_name=f"매출정산표_{dt_from}_to_{dt_to}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)
    
def page_admin_balance_management(store_info_df: pd.DataFrame):
    st.subheader("💰 잔액 관리")
    charge_requests_df = load_data(SHEET_NAME_CHARGE_REQ, CHARGE_REQ_COLUMNS)
    balance_df = load_data(SHEET_NAME_BALANCE, BALANCE_COLUMNS)
    st.markdown("##### 📥 충전 요청 확인 (처리 대기)")
    pending_requests = charge_requests_df[charge_requests_df['상태'] == '확인대기'].sort_values(by="요청일시", ascending=False)
    if not pending_requests.empty:
        for index, req in pending_requests.iterrows():
            with st.container(border=True):
                c1, c2, c4 = st.columns([2, 1, 1])
                c1.text(f"요청: {req['요청일시']} / {req['지점명']} ({req['입금자명']})")
                c2.text(f"금액: {req['입금액']:,}원")
                if c4.button("✅ 충전 처리", key=f"charge_{req['요청일시']}", type="primary"):
                    with st.spinner("충전 처리 중..."):
                        current_balance_series = balance_df[balance_df['지점ID'] == req['지점ID']]
                        current_balance = int(current_balance_series['현재잔액'].iloc[0]) if not current_balance_series.empty else 0
                        new_balance = current_balance + int(req['입금액'])
                        transaction_record = {
                            "일시": now_kst_str(), "지점ID": req['지점ID'], "지점명": req['지점명'],
                            "구분": "충전", "내용": f"관리자 충전 ({req['입금자명']})",
                            "금액": int(req['입금액']), "처리후잔액": new_balance, "처리자": st.session_state.auth['name']
                        }
                        if append_rows_to_sheet(SHEET_NAME_TRANSACTIONS, [transaction_record], TRANSACTIONS_COLUMNS):
                            if update_balance(req['지점ID'], new_balance, req['지점명']):
                                if update_charge_request_status(req['요청일시'], '처리완료'):
                                    st.success(f"{req['지점명']} 충전 완료!"); st.rerun()
    else:
        st.info("처리 대기 중인 충전 요청이 없습니다.")
    st.markdown("---")
    st.markdown("##### 📋 전체 지점 잔액 현황")
    st.dataframe(balance_df, use_container_width=True, hide_index=True)

# =============================================================================
# 8) 라우팅
# =============================================================================
if __name__ == "__main__":
    if not require_login(): st.stop()
    init_session_state()
    st.title("📦 식자재 발주 시스템")
    display_feedback()
    user = st.session_state.auth
    
    master_df = load_data(SHEET_NAME_MASTER)
    store_info_df = load_data(SHEET_NAME_STORES)
    orders_df = load_data(SHEET_NAME_ORDERS, ORDERS_COLUMNS)
    balance_df = load_data(SHEET_NAME_BALANCE, BALANCE_COLUMNS)
    charge_requests_df = load_data(SHEET_NAME_CHARGE_REQ, CHARGE_REQ_COLUMNS)

    if user["role"] == "admin":
        tabs = st.tabs(["📋 발주요청 조회", "📈 매출 조회", "💰 잔액 관리", "📑 증빙서류 다운로드", "🏷️ 품목 단가 설정"])
        with tabs[0]: page_admin_unified_management(orders_df, store_info_df, master_df)
        with tabs[1]: page_admin_sales_inquiry(master_df)
        with tabs[2]: page_admin_balance_management(store_info_df)
        with tabs[3]: page_admin_documents(store_info_df, master_df)
        with tabs[4]: page_admin_items_price(master_df)
    else: # store
        tabs = st.tabs(["🛒 발주 요청", "🧾 발주 조회", "💰 금액 충전", "📑 증빙서류 다운로드", "🏷️ 품목 단가 조회"])
        
        my_balance_series = balance_df[balance_df['지점ID'] == user['user_id']]
        my_balance = my_balance_series['현재잔액'].iloc[0] if not my_balance_series.empty else 0

        with tabs[0]: page_store_register_confirm(master_df, my_balance)
        with tabs[1]: page_store_orders_change(store_info_df, master_df)
        with tabs[2]: page_store_balance(charge_requests_df)
        with tabs[3]: page_store_documents(store_info_df)
        with tabs[4]: page_store_master_view(master_df)
