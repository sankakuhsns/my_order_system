# -*- coding: utf-8 -*-
# =============================================================================
# 📦 Streamlit 식자재 발주 시스템 (v13.0 - 통합 인증 및 관리 최종본)
#
# - 주요 변경 사항:
#   - 통합 인증 시스템: 모든 사용자는 '지점마스터' 시트 기준으로 로그인
#   - 관리자 기능 강화: '관리 설정' 탭에서 품목 및 지점(사용자) 정보 직접 편집
#   - 편의 기능 추가: 사이드바에 로그인 정보 및 로그아웃 버튼 표시
#   - 요청된 모든 UI/UX 개선 및 오류 수정 완료 (전체 코드)
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

# =============================================================================
# 1) 시트/스키마 정의
# =============================================================================
SHEET_NAME_STORES = "지점마스터"
SHEET_NAME_MASTER = "상품마스터"
SHEET_NAME_ORDERS = "발주"
SHEET_NAME_BALANCE = "잔액마스터"
SHEET_NAME_CHARGE_REQ = "충전요청"
SHEET_NAME_TRANSACTIONS = "거래내역"

STORES_COLUMNS = ["지점ID", "지점PW", "역할", "지점명", "사업자등록번호", "상호명", "대표자명", "사업장주소", "업태", "종목"]
MASTER_COLUMNS = ["품목코드", "품목명", "품목규격", "분류", "단위", "단가", "과세구분", "활성"]
ORDERS_COLUMNS = ["주문일시", "발주번호", "지점ID", "지점명", "품목코드", "품목명", "단위", "수량", "단가", "공급가액", "세액", "합계금액", "비고", "상태", "처리일시", "처리자", "반려사유"]
CART_COLUMNS = ["품목코드", "품목명", "단위", "단가", "수량", "합계금액"]
BALANCE_COLUMNS = ["지점ID", "지점명", "선충전잔액", "여신한도", "사용여신액"]
CHARGE_REQ_COLUMNS = ["요청일시", "지점ID", "지점명", "입금자명", "입금액", "종류", "상태", "처리사유"]
TRANSACTIONS_COLUMNS = ["일시", "지점ID", "지점명", "구분", "내용", "금액", "처리후선충전잔액", "처리후사용여신액", "관련발주번호", "처리자"]

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
            SHEET_NAME_MASTER: ["단가"]
        }
        if sheet_name in numeric_cols:
            for col in numeric_cols[sheet_name]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)

        if columns:
            for col in columns:
                if col not in df.columns: df[col] = ''
            df = df[columns]
            
        sort_key_map = {'주문일시': "주문일시", '요청일시': "요청일시", '일시': "일시"}
        for col, key in sort_key_map.items():
            if col in df.columns:
                try:
                    pd.to_datetime(df[key])
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
                ws.update_cell(cell.row, col_idx, value)
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
        now_str = now_kst_str()
        for i, row in enumerate(all_data[1:], start=2):
            if row[id_col_idx] in selected_ids:
                cells_to_update.append(gspread.Cell(i, status_col_idx + 1, new_status))
                cells_to_update.append(gspread.Cell(i, handler_col_idx + 1, handler))
                cells_to_update.append(gspread.Cell(i, timestamp_col_idx + 1, now_str))
                if new_status == "반려" and reason_col_idx != -1:
                    cells_to_update.append(gspread.Cell(i, reason_col_idx + 1, reason))

        if cells_to_update: ws.update_cells(cells_to_update, value_input_option='USER_ENTERED')
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"발주 상태 업데이트 중 오류가 발생했습니다: {e}")
        return False

# =============================================================================
# 3) 로그인 및 인증
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
# 4) Excel 생성
# =============================================================================
def make_order_id(store_id: str) -> str: return f"{datetime.now(KST):%Y%m%d%H%M%S}{store_id}"

def make_item_transaction_statement_excel(order_df: pd.DataFrame, store_info: pd.Series) -> BytesIO:
    output = BytesIO()
    if order_df.empty: return output

    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        workbook = writer.book
        worksheet = workbook.add_worksheet("품목거래명세서")
        
        fmt_h1 = workbook.add_format({'bold': True, 'font_size': 20, 'align': 'center', 'valign': 'vcenter'})
        fmt_h2 = workbook.add_format({'bold': True, 'font_size': 11})
        fmt_header = workbook.add_format({'bold': True, 'bg_color': '#F2F2F2', 'border': 1, 'align': 'center', 'valign': 'vcenter'})
        fmt_money = workbook.add_format({'num_format': '#,##0', 'border': 1})
        fmt_border = workbook.add_format({'border': 1})
        fmt_border_c = workbook.add_format({'border': 1, 'align': 'center'})
        fmt_total = workbook.add_format({'bold': True, 'bg_color': '#F2F2F2', 'border': 1, 'num_format': '#,##0'})

        worksheet.set_column('A:A', 5); worksheet.set_column('B:B', 25)
        worksheet.set_column('C:C', 10); worksheet.set_column('D:D', 10)
        worksheet.set_column('E:F', 15); worksheet.set_column('G:G', 12)
        worksheet.set_column('H:H', 16)

        worksheet.merge_range('A1:H1', '품목 거래명세서', fmt_h1)
        
        order_info = order_df.iloc[0]
        worksheet.write('A3', f"발주번호: {order_info['발주번호']}", fmt_h2)
        worksheet.write('A4', f"발주일시: {order_info['주문일시']}", fmt_h2)

        worksheet.write('E3', "공급받는자", fmt_h2)
        worksheet.write('E4', f"상호: {store_info['지점명']}")
        worksheet.write('E5', f"주소: {store_info['사업장주소']}")

        headers = ["No", "품목명", "단위", "수량", "단가", "공급가액", "세액", "합계금액"]
        worksheet.write_row('A8', headers, fmt_header)
        
        row_num = 8
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

        total_row = row_num + 1
        worksheet.merge_range(f'A{total_row}:D{total_row}', '합계', fmt_total)
        worksheet.write_formula(f'E{total_row}', f"=SUM(E9:E{row_num})", fmt_total)
        worksheet.write_formula(f'F{total_row}', f"=SUM(F9:F{row_num})", fmt_total)
        worksheet.write_formula(f'G{total_row}', f"=SUM(G9:G{row_num})", fmt_total)
        worksheet.write_formula(f'H{total_row}', f"=SUM(H9:H{row_num})", fmt_total)

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

def make_sales_summary_excel(daily_pivot: pd.DataFrame, monthly_pivot: pd.DataFrame) -> BytesIO:
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        daily_pivot.reset_index().to_excel(writer, sheet_name='일별매출현황', index=False, startrow=2)
        monthly_pivot.reset_index().to_excel(writer, sheet_name='월별매출현황', index=False, startrow=2)
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
    output.seek(0)
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
def page_store_register_confirm(master_df: pd.DataFrame, balance_info: pd.Series):
    user = st.session_state.auth
    st.subheader("🛒 발주 요청")
    st.caption("발주할 품목의 수량을 입력하고 '장바구니 담기' 버튼을 클릭하세요.")

    active_items = master_df[master_df['활성'].astype(str).str.lower() == 'true'].copy()
    if '수량' not in active_items.columns:
        active_items['수량'] = 0

    edited_df = st.data_editor(
        active_items[['품목코드', '품목명', '품목규격', '단위', '단가', '수량']],
        key="item_selector",
        num_rows="dynamic",
        use_container_width=True,
        column_config={
            "품목코드": st.column_config.TextColumn(label="품목코드", disabled=True),
            "품목명": st.column_config.TextColumn(label="품목명", disabled=True),
            "품목규격": st.column_config.TextColumn(label="규격", disabled=True),
            "단위": st.column_config.TextColumn(label="단위", disabled=True),
            "단가": st.column_config.NumberColumn(label="단가", format="%d", disabled=True),
            "수량": st.column_config.NumberColumn(label="발주수량", min_value=0, step=1),
        },
        hide_index=True
    )
    if st.button("🛒 장바구니 담기", use_container_width=True):
        add_to_cart(edited_df)
        st.rerun()

    st.markdown("---")
    st.subheader("🛍️ 장바구니")
    cart = coerce_cart_df(st.session_state.cart)
    if cart.empty:
        st.info("장바구니가 비어있습니다. 위 목록에서 품목을 추가해주세요.")
        return

    st.dataframe(cart, use_container_width=True, hide_index=True)
    total_price = cart['합계금액'].sum()
    st.markdown(f"<h4 style='text-align: right; color: {THEME['PRIMARY']};'>총 합계: {total_price:,.0f}원</h4>", unsafe_allow_html=True)
    
    col1, col2 = st.columns([1, 1])
    with col1:
        if st.button("🗑️ 장바구니 비우기", use_container_width=True):
            st.session_state.cart = pd.DataFrame(columns=CART_COLUMNS)
            st.rerun()

    with col2:
        if st.button("🚀 발주 요청하기", type="primary", use_container_width=True):
            if balance_info.empty:
                st.error("잔액 정보를 불러올 수 없습니다. 관리자에게 문의하세요.")
                return

            available_credit = balance_info.get('선충전잔액', 0) + (balance_info.get('여신한도', 0) - balance_info.get('사용여신액', 0))
            if total_price > available_credit:
                st.error(f"주문 금액({total_price:,.0f}원)이 결제 가능 금액({available_credit:,.0f}원)을 초과합니다.")
                return

            new_order_id = make_order_id(user['user_id'])
            order_time = now_kst_str()
            new_orders = []
            for _, row in cart.iterrows():
                unit_price = int(row['단가'])
                quantity = int(row['수량'])
                total_amount = unit_price * quantity
                supply_price = round(total_amount / 1.1)
                tax_amount = total_amount - supply_price
                new_orders.append({
                    "주문일시": order_time, "발주번호": new_order_id, "지점ID": user['user_id'], "지점명": user['name'],
                    "품목코드": row['품목코드'], "품목명": row['품목명'], "단위": row['단위'], "수량": quantity,
                    "단가": unit_price, "공급가액": supply_price, "세액": tax_amount, "합계금액": total_amount,
                    "비고": "", "상태": "요청", "처리일시": "", "처리자": "", "반려사유": ""
                })

            if append_rows_to_sheet(SHEET_NAME_ORDERS, new_orders, ORDERS_COLUMNS):
                st.session_state.success_message = f"발주 요청이 완료되었습니다. (발주번호: {new_order_id})"
                st.session_state.cart = pd.DataFrame(columns=CART_COLUMNS)
                st.rerun()
            else:
                st.error("발주 요청 중 오류가 발생했습니다.")

def page_store_balance(charge_requests_df: pd.DataFrame, balance_info: pd.Series):
    user = st.session_state.auth
    st.subheader("💰 결제 관리")
    
    if balance_info.empty:
        st.warning("결제 정보를 조회할 수 없습니다. 관리자에게 문의하세요.")
        return

    prepaid = int(balance_info.get('선충전잔액', 0))
    credit_limit = int(balance_info.get('여신한도', 0))
    credit_used = int(balance_info.get('사용여신액', 0))
    credit_available = credit_limit - credit_used
    total_available = prepaid + credit_available

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("선충전 잔액", f"{prepaid:,.0f}원")
    col2.metric("사용 가능 여신", f"{credit_available:,.0f}원", f"총 {credit_limit:,.0f}원")
    col3.metric("총 결제 가능 금액", f"{total_available:,.0f}원")
    
    st.markdown("---")
    st.markdown("##### 💳 충전 요청하기")
    with st.form("charge_request_form", clear_on_submit=True):
        c1, c2, c3 = st.columns([1,1,2])
        depositor_name = c1.text_input("입금자명")
        charge_amount = c2.number_input("입금액", min_value=0, step=10000)
        charge_type = c3.radio("종류", ["선충전금 충전", "여신 상환"], horizontal=True)
        
        if st.form_submit_button("충전 요청", use_container_width=True, type="primary"):
            if depositor_name and charge_amount > 0:
                new_request = {
                    "요청일시": now_kst_str(), "지점ID": user['user_id'], "지점명": user['name'],
                    "입금자명": depositor_name, "입금액": charge_amount, "종류": charge_type,
                    "상태": "요청", "처리사유": ""
                }
                if append_rows_to_sheet(SHEET_NAME_CHARGE_REQ, [new_request], CHARGE_REQ_COLUMNS):
                    st.session_state.success_message = "충전 요청이 성공적으로 접수되었습니다."
                    st.rerun()
                else:
                    st.error("충전 요청 중 오류가 발생했습니다.")
            else:
                st.warning("입금자명과 입금액을 올바르게 입력해주세요.")

    st.markdown("---")
    st.markdown("##### 📜 충전 요청 내역")
    my_requests = charge_requests_df[charge_requests_df['지점ID'] == user['user_id']]
    st.dataframe(my_requests, use_container_width=True, hide_index=True)


def page_store_orders_change(store_info_df: pd.DataFrame, master_df: pd.DataFrame):
    user = st.session_state.auth
    st.subheader("🧾 발주 조회")
    
    orders_df = load_data(SHEET_NAME_ORDERS, ORDERS_COLUMNS)
    my_orders = orders_df[orders_df['지점ID'] == user['user_id']].copy()
    
    if my_orders.empty:
        st.info("아직 발주 내역이 없습니다.")
        return

    # 날짜 필터
    today = datetime.now(KST).date()
    c1, c2, _ = st.columns([1, 1, 3])
    start_date = c1.date_input("조회 시작일", today - timedelta(days=30))
    end_date = c2.date_input("조회 종료일", today)
    
    my_orders['주문일'] = pd.to_datetime(my_orders['주문일시']).dt.date
    filtered_orders = my_orders[(my_orders['주문일'] >= start_date) & (my_orders['주문일'] <= end_date)]

    unique_order_ids = filtered_orders['발주번호'].unique()

    for order_id in unique_order_ids:
        order_items = filtered_orders[filtered_orders['발주번호'] == order_id]
        order_info = order_items.iloc[0]
        total_amount = order_items['합계금액'].sum()
        status = order_info['상태']
        
        status_color = {"요청": "blue", "승인": "green", "반려": "red"}.get(status, "gray")
        
        with st.expander(f"**{order_info['주문일시']}** | 발주번호: {order_id} | 총 {total_amount:,.0f}원 | 상태: <span style='color:{status_color};'>{status}</span>", expanded=False):
            st.dataframe(order_items[['품목명', '단위', '수량', '단가', '합계금액']], use_container_width=True, hide_index=True)
            if status == "반려":
                st.warning(f"반려 사유: {order_info.get('반려사유', '기재 없음')}")
            
            # 발주 요청 상태일 때만 취소 버튼 표시
            if status == "요청":
                if st.button("이 발주 요청 취소하기", key=f"cancel_{order_id}", type="secondary"):
                    update_order_status([order_id], "취소", user['name'])
                    st.success(f"발주번호 {order_id}가 취소되었습니다.")
                    st.rerun()

def page_store_documents(store_info_df: pd.DataFrame):
    user = st.session_state.auth
    st.subheader("📑 증빙서류 다운로드")

    doc_type = st.radio("다운로드할 서류 종류를 선택하세요.", ["품목 거래명세서", "금전 거래 상세 명세서"], horizontal=True, key="store_doc_type")
    
    my_info = store_info_df[store_info_df['지점ID'] == user['user_id']].iloc[0]
    
    if doc_type == "품목 거래명세서":
        orders_df = load_data(SHEET_NAME_ORDERS, ORDERS_COLUMNS)
        my_orders = orders_df[(orders_df['지점ID'] == user['user_id']) & (orders_df['상태'] == '승인')]
        
        if my_orders.empty:
            st.warning("다운로드할 승인된 발주 내역이 없습니다.")
            return
            
        order_options = my_orders.groupby('발주번호')['주문일시'].first().sort_index(ascending=False).apply(lambda x: f"{x} - {my_orders[my_orders['발주번호'] == my_orders.loc[my_orders['주문일시']==x].iloc[0]['발주번호']]['합계금액'].sum():,}원")
        selected_order_id = st.selectbox("거래명세서를 출력할 발주번호를 선택하세요.", order_options.index, format_func=lambda x: order_options[x])
        
        if st.button("엑셀 다운로드", key="download_order_statement"):
            order_to_print = my_orders[my_orders['발주번호'] == selected_order_id]
            excel_data = make_item_transaction_statement_excel(order_to_print, my_info)
            st.download_button(
                label="✅ 다운로드 준비 완료",
                data=excel_data,
                file_name=f"거래명세서_{my_info['지점명']}_{selected_order_id}.xlsx",
                mime="application/vnd.ms-excel"
            )

    elif doc_type == "금전 거래 상세 명세서":
        transactions_df = load_data(SHEET_NAME_TRANSACTIONS, TRANSACTIONS_COLUMNS)
        my_transactions = transactions_df[transactions_df['지점ID'] == user['user_id']]
        
        if my_transactions.empty:
            st.warning("거래 내역이 없습니다.")
            return

        today = datetime.now(KST).date()
        c1, c2, _ = st.columns([1, 1, 3])
        start_date = c1.date_input("조회 시작일", today - timedelta(days=365))
        end_date = c2.date_input("조회 종료일", today)

        my_transactions['거래일'] = pd.to_datetime(my_transactions['일시']).dt.date
        filtered_transactions = my_transactions[(my_transactions['거래일'] >= start_date) & (my_transactions['거래일'] <= end_date)]

        if st.button("엑셀 다운로드", key="download_full_statement"):
            excel_data = make_full_transaction_statement_excel(filtered_transactions, my_info)
            st.download_button(
                label="✅ 다운로드 준비 완료",
                data=excel_data,
                file_name=f"금전거래명세서_{my_info['지점명']}_{start_date}~{end_date}.xlsx",
                mime="application/vnd.ms-excel"
            )

def page_store_master_view(master_df: pd.DataFrame):
    st.subheader("🏷️ 품목 단가 조회")
    st.caption("현재 발주 가능한 전체 품목 및 단가 정보입니다.")
    active_items = master_df[master_df['활성'].astype(str).str.lower() == 'true']
    st.dataframe(active_items[['품목코드', '분류', '품목명', '품목규격', '단위', '단가']], use_container_width=True, hide_index=True)

# =============================================================================
# 7) 관리자 페이지
# =============================================================================
def page_admin_unified_management(df_all: pd.DataFrame, store_info_df: pd.DataFrame, master_df: pd.DataFrame):
    st.subheader("📋 발주요청 통합 관리")
    
    if df_all.empty:
        st.info("현재 접수된 발주 요청이 없습니다.")
        return

    # 필터링 UI
    c1, c2, c3 = st.columns(3)
    status_filter = c1.multiselect("상태", options=df_all['상태'].unique(), default=["요청"])
    store_filter = c2.multiselect("지점명", options=store_info_df['지점명'].unique())
    
    # 날짜 필터
    today = datetime.now(KST).date()
    start_date = c3.date_input("조회 시작일", today - timedelta(days=7), key="admin_order_start")
    
    filtered_df = df_all.copy()
    if status_filter:
        filtered_df = filtered_df[filtered_df['상태'].isin(status_filter)]
    if store_filter:
        filtered_df = filtered_df[filtered_df['지점명'].isin(store_filter)]
    
    filtered_df['주문일'] = pd.to_datetime(filtered_df['주문일시']).dt.date
    filtered_df = filtered_df[filtered_df['주문일'] >= start_date]
    
    # 발주 번호별로 그룹화하여 표시
    order_groups = filtered_df.groupby('발주번호')
    
    if order_groups.ngroups == 0:
        st.warning("선택한 조건에 맞는 발주 내역이 없습니다.")
        return

    selected_order_ids = []
    
    # 전체 선택 체크박스
    select_all = st.checkbox("전체 선택", key="select_all_orders")
    
    for order_id, group in order_groups:
        order_info = group.iloc[0]
        total_amount = group['합계금액'].sum()
        
        expander_cols = st.columns([0.05, 0.95])
        with expander_cols[1]:
            with st.expander(f"**{order_info['지점명']}** - {order_info['주문일시']} (총 {total_amount:,.0f}원)"):
                st.dataframe(group[['품목명', '단위', '수량', '단가', '합계금액']], hide_index=True, use_container_width=True)

        checkbox_checked = expander_cols[0].checkbox("", key=f"select_{order_id}", value=select_all)
        if checkbox_checked:
            selected_order_ids.append(order_id)
            
    st.markdown("---")
    st.markdown("##### 📦 선택한 발주 처리")
    
    if not selected_order_ids:
        st.caption("처리할 발주를 위에서 선택하세요.")
        return
        
    st.write(f"**선택된 발주 {len(selected_order_ids)}건**")
    
    action_cols = st.columns(2)
    
    with action_cols[0]:
        if st.button("✅ 일괄 승인", use_container_width=True, type="primary"):
            balance_df = load_data(SHEET_NAME_BALANCE, BALANCE_COLUMNS)
            
            # 발주 승인 및 잔액 차감 로직
            with st.spinner("발주를 승인하고 잔액을 처리 중입니다..."):
                all_succeeded = True
                transactions_to_add = []

                for order_id in selected_order_ids:
                    order_data = df_all[df_all['발주번호'] == order_id]
                    store_id = order_data.iloc[0]['지점ID']
                    store_name = order_data.iloc[0]['지점명']
                    order_total = order_data['합계금액'].sum()
                    
                    current_balance_info = balance_df[balance_df['지점ID'] == store_id]
                    if current_balance_info.empty:
                        st.error(f"{store_name}의 잔액 정보를 찾을 수 없어 처리를 중단합니다.")
                        all_succeeded = False
                        break
                    
                    current_balance = current_balance_info.iloc[0]
                    prepaid = current_balance.get('선충전잔액', 0)
                    credit_used = current_balance.get('사용여신액', 0)
                    
                    # 선충전금에서 먼저 차감
                    new_prepaid = prepaid - order_total
                    new_credit_used = credit_used
                    
                    if new_prepaid < 0:
                        # 부족분은 여신에서 차감
                        new_credit_used += abs(new_prepaid)
                        new_prepaid = 0

                    updates = {'선충전잔액': new_prepaid, '사용여신액': new_credit_used}
                    if update_balance_sheet(store_id, updates):
                        update_order_status([order_id], "승인", st.session_state.auth['name'])
                        transactions_to_add.append({
                            "일시": now_kst_str(), "지점ID": store_id, "지점명": store_name,
                            "구분": "발주승인", "내용": f"발주승인 (주문번호:{order_id})", "금액": -order_total,
                            "처리후선충전잔액": new_prepaid, "처리후사용여신액": new_credit_used,
                            "관련발주번호": order_id, "처리자": st.session_state.auth['name']
                        })
                    else:
                        st.error(f"{store_name}의 발주(번호:{order_id}) 처리 중 오류가 발생했습니다.")
                        all_succeeded = False
                        break
                
                if all_succeeded and transactions_to_add:
                    append_rows_to_sheet(SHEET_NAME_TRANSACTIONS, transactions_to_add, TRANSACTIONS_COLUMNS)
                    st.success(f"{len(selected_order_ids)}건의 발주가 성공적으로 승인 처리되었습니다.")
                    st.rerun()

    with action_cols[1]:
        rejection_reason = st.text_input("반려 사유 (일괄 적용)", placeholder="예: 재고 부족")
        if st.button("❌ 일괄 반려", use_container_width=True):
            if not rejection_reason:
                st.warning("반려 사유를 입력해주세요.")
            else:
                if update_order_status(selected_order_ids, "반려", st.session_state.auth['name'], rejection_reason):
                    st.success(f"{len(selected_order_ids)}건의 발주가 반려 처리되었습니다.")
                    st.rerun()

def page_admin_documents(store_info_df: pd.DataFrame):
    st.subheader("📑 증빙서류 다운로드 (관리자)")

    selected_store_name = st.selectbox("서류를 다운로드할 지점을 선택하세요.", options=store_info_df['지점명'].unique())
    
    if not selected_store_name:
        st.info("지점을 먼저 선택해주세요.")
        return
        
    selected_store_info = store_info_df[store_info_df['지점명'] == selected_store_name].iloc[0]
    selected_store_id = selected_store_info['지점ID']

    doc_type = st.radio("다운로드할 서류 종류를 선택하세요.", ["품목 거래명세서", "금전 거래 상세 명세서"], horizontal=True, key="admin_doc_type")
    
    if doc_type == "품목 거래명세서":
        orders_df = load_data(SHEET_NAME_ORDERS, ORDERS_COLUMNS)
        store_orders = orders_df[(orders_df['지점ID'] == selected_store_id) & (orders_df['상태'] == '승인')]
        
        if store_orders.empty:
            st.warning(f"{selected_store_name} 지점의 승인된 발주 내역이 없습니다.")
            return
            
        order_options = store_orders.groupby('발주번호')['주문일시'].first().sort_index(ascending=False).apply(lambda x: f"{x} - {store_orders[store_orders['발주번호'] == store_orders.loc[store_orders['주문일시']==x].iloc[0]['발주번호']]['합계금액'].sum():,}원")
        selected_order_id = st.selectbox("거래명세서를 출력할 발주번호를 선택하세요.", order_options.index, format_func=lambda x: order_options[x], key=f"order_select_{selected_store_id}")
        
        if st.button("엑셀 다운로드", key="admin_download_order_statement"):
            order_to_print = store_orders[store_orders['발주번호'] == selected_order_id]
            excel_data = make_item_transaction_statement_excel(order_to_print, selected_store_info)
            st.download_button(
                label="✅ 다운로드 준비 완료",
                data=excel_data,
                file_name=f"거래명세서_{selected_store_name}_{selected_order_id}.xlsx",
                mime="application/vnd.ms-excel"
            )

    elif doc_type == "금전 거래 상세 명세서":
        transactions_df = load_data(SHEET_NAME_TRANSACTIONS, TRANSACTIONS_COLUMNS)
        store_transactions = transactions_df[transactions_df['지점ID'] == selected_store_id]
        
        if store_transactions.empty:
            st.warning(f"{selected_store_name} 지점의 거래 내역이 없습니다.")
            return

        today = datetime.now(KST).date()
        c1, c2, _ = st.columns([1, 1, 3])
        start_date = c1.date_input("조회 시작일", today - timedelta(days=365), key=f"trans_start_{selected_store_id}")
        end_date = c2.date_input("조회 종료일", today, key=f"trans_end_{selected_store_id}")

        store_transactions['거래일'] = pd.to_datetime(store_transactions['일시']).dt.date
        filtered_transactions = store_transactions[(store_transactions['거래일'] >= start_date) & (store_transactions['거래일'] <= end_date)]

        if st.button("엑셀 다운로드", key="admin_download_full_statement"):
            excel_data = make_full_transaction_statement_excel(filtered_transactions, selected_store_info)
            st.download_button(
                label="✅ 다운로드 준비 완료",
                data=excel_data,
                file_name=f"금전거래명세서_{selected_store_name}_{start_date}~{end_date}.xlsx",
                mime="application/vnd.ms-excel"
            )


def page_admin_sales_inquiry(master_df: pd.DataFrame):
    st.subheader("📈 매출 조회")
    
    orders_df = load_data(SHEET_NAME_ORDERS, ORDERS_COLUMNS)
    approved_orders = orders_df[orders_df['상태'] == '승인'].copy()
    
    if approved_orders.empty:
        st.info("매출 데이터가 없습니다.")
        return

    approved_orders['주문일시'] = pd.to_datetime(approved_orders['주문일시'])
    
    # 날짜 범위 선택
    today = datetime.now(KST).date()
    c1, c2, _ = st.columns([1,1,3])
    start_date = c1.date_input("조회 시작일", today - timedelta(days=30))
    end_date = c2.date_input("조회 종료일", today)
    
    start_dt = pd.to_datetime(start_date)
    end_dt = pd.to_datetime(end_date)
    
    mask = (approved_orders['주문일시'].dt.date >= start_date) & (approved_orders['주문일시'].dt.date <= end_date)
    filtered_sales = approved_orders.loc[mask]

    if filtered_sales.empty:
        st.warning("선택한 기간에 해당하는 매출 데이터가 없습니다.")
        return

    # 일별 매출 현황
    st.markdown("##### 📅 일별 매출 현황")
    daily_pivot = filtered_sales.pivot_table(index='지점명', columns=filtered_sales['주문일시'].dt.strftime('%Y-%m-%d'), values='합계금액', aggfunc='sum', fill_value=0)
    st.dataframe(daily_pivot.style.format("{:,.0f}"))

    # 월별 매출 현황
    st.markdown("##### 🗓️ 월별 매출 현황")
    monthly_pivot = filtered_sales.pivot_table(index='지점명', columns=filtered_sales['주문일시'].dt.strftime('%Y-%m'), values='합계금액', aggfunc='sum', fill_value=0)
    st.dataframe(monthly_pivot.style.format("{:,.0f}"))
    
    # 엑셀 다운로드
    excel_data = make_sales_summary_excel(daily_pivot, monthly_pivot)
    st.download_button(
        label=" 매출 현황 엑셀 다운로드",
        data=excel_data,
        file_name=f"매출현황_{start_date}~{end_date}.xlsx",
        mime="application/vnd.ms-excel"
    )

def page_admin_balance_management(store_info_df: pd.DataFrame):
    st.subheader("💰 결제 관리 (충전/여신)")
    
    tab1, tab2 = st.tabs(["충전 요청 처리", "지점별 잔액 현황"])
    
    with tab1:
        st.markdown("##### 💳 충전 요청 처리")
        charge_req_df = load_data(SHEET_NAME_CHARGE_REQ, CHARGE_REQ_COLUMNS)
        pending_requests = charge_req_df[charge_req_df['상태'] == '요청'].copy()
        
        if pending_requests.empty:
            st.info("현재 처리 대기 중인 충전 요청이 없습니다.")
        else:
            edited_requests = st.data_editor(
                pending_requests,
                column_config={"선택": st.column_config.CheckboxColumn(default=False)},
                disabled=CHARGE_REQ_COLUMNS,
                hide_index=True,
                key="charge_req_editor"
            )
            
            selected_requests = edited_requests[edited_requests['선택']]
            
            if not selected_requests.empty:
                st.write(f"**선택된 요청 {len(selected_requests)}건**")
                c1, c2 = st.columns(2)
                
                with c1:
                    if st.button("✅ 일괄 승인", use_container_width=True, type="primary"):
                        balance_df = load_data(SHEET_NAME_BALANCE, BALANCE_COLUMNS)
                        all_req = load_data(SHEET_NAME_CHARGE_REQ, CHARGE_REQ_COLUMNS)
                        transactions = []
                        
                        with st.spinner("충전 요청을 처리중입니다..."):
                            for idx, req in selected_requests.iterrows():
                                store_id = req['지점ID']
                                current_balance = balance_df.loc[balance_df['지점ID'] == store_id].iloc[0]
                                new_prepaid, new_credit_used = current_balance['선충전잔액'], current_balance['사용여신액']

                                if req['종류'] == '선충전금 충전':
                                    new_prepaid += req['입금액']
                                    desc = f"선충전금 충전 ({req['입금자명']})"
                                else: # 여신 상환
                                    new_credit_used -= req['입금액']
                                    if new_credit_used < 0: # 초과 상환 시 선충전금으로
                                        new_prepaid += abs(new_credit_used)
                                        new_credit_used = 0
                                    desc = f"여신 상환 ({req['입금자명']})"
                                
                                update_balance_sheet(store_id, {'선충전잔액': new_prepaid, '사용여신액': new_credit_used})
                                transactions.append({
                                    "일시": now_kst_str(), "지점ID": store_id, "지점명": req['지점명'], "구분": "입금",
                                    "내용": desc, "금액": req['입금액'], "처리후선충전잔액": new_prepaid, "처리후사용여신액": new_credit_used,
                                    "관련발주번호": "", "처리자": st.session_state.auth['name']
                                })
                                # 원본 데이터프레임 상태 변경
                                all_req.loc[(all_req['요청일시'] == req['요청일시']) & (all_req['지점ID'] == req['지점ID']), '상태'] = '승인'
                            
                            append_rows_to_sheet(SHEET_NAME_TRANSACTIONS, transactions, TRANSACTIONS_COLUMNS)
                            save_df_to_sheet(SHEET_NAME_CHARGE_REQ, all_req)
                        
                        st.success(f"{len(selected_requests)}건의 충전 요청이 성공적으로 처리되었습니다.")
                        st.rerun()

                with c2:
                    reason = st.text_input("반려 사유 (일괄 적용)")
                    if st.button("❌ 일괄 반려", use_container_width=True):
                        if not reason:
                            st.warning("반려 사유를 입력해야 합니다.")
                        else:
                            all_req = load_data(SHEET_NAME_CHARGE_REQ, CHARGE_REQ_COLUMNS)
                            for idx, req in selected_requests.iterrows():
                                all_req.loc[(all_req['요청일시'] == req['요청일시']) & (all_req['지점ID'] == req['지점ID']), ['상태', '처리사유']] = ['반려', reason]
                            save_df_to_sheet(SHEET_NAME_CHARGE_REQ, all_req)
                            st.success(f"{len(selected_requests)}건의 요청이 반려 처리되었습니다.")
                            st.rerun()

    with tab2:
        st.markdown("##### 🏢 지점별 잔액/여신 현황")
        balance_df = load_data(SHEET_NAME_BALANCE, BALANCE_COLUMNS)
        st.dataframe(balance_df, hide_index=True, use_container_width=True)
        
def page_admin_settings(store_info_df_raw: pd.DataFrame, master_df_raw: pd.DataFrame):
    st.subheader("🛠️ 관리 설정")
    
    tab1, tab2 = st.tabs(["품목 관리", "지점 관리"])

    with tab1:
        st.markdown("##### 🏷️ 품목 정보 설정")
        st.caption("품목을 추가/수정/삭제한 후 '품목 정보 저장' 버튼을 누르세요.")
        
        edited_master_df = st.data_editor(
            master_df_raw, 
            num_rows="dynamic", 
            use_container_width=True,
            key="master_editor"
        )
        
        if st.button("품목 정보 저장", type="primary", key="save_master"):
            if save_df_to_sheet(SHEET_NAME_MASTER, edited_master_df):
                st.success("상품 마스터가 성공적으로 저장되었습니다. 데이터가 즉시 반영됩니다.")
                st.rerun()

    with tab2:
        st.markdown("##### 🏢 지점(사용자) 정보 설정")
        st.caption("지점(사용자)을 추가/수정/삭제한 후 '지점 정보 저장' 버튼을 누르세요. 역할은 'admin' 또는 'store'만 가능합니다.")
        
        edited_store_df = st.data_editor(
            store_info_df_raw, 
            num_rows="dynamic", 
            use_container_width=True,
            key="store_editor"
        )

        if st.button("지점 정보 저장", type="primary", key="save_stores"):
            # TODO: 지점 추가/삭제 시 잔액 마스터 시트와 동기화하는 로직 추가 필요
            if save_df_to_sheet(SHEET_NAME_STORES, edited_store_df):
                st.success("지점 마스터가 성공적으로 저장되었습니다. 변경사항은 다음 로그인부터 적용됩니다.")
                st.rerun()

# =============================================================================
# 8) 라우팅
# =============================================================================
if __name__ == "__main__":
    if not require_login():
        st.stop()
        
    init_session_state()
    st.title("📦 식자재 발주 시스템")
    display_feedback()
    user = st.session_state.auth
    
    # 데이터 로딩
    master_df = load_data(SHEET_NAME_MASTER, MASTER_COLUMNS)
    store_info_df_raw = load_data(SHEET_NAME_STORES, STORES_COLUMNS)
    orders_df = load_data(SHEET_NAME_ORDERS, ORDERS_COLUMNS)
    balance_df = load_data(SHEET_NAME_BALANCE, BALANCE_COLUMNS)
    charge_requests_df = load_data(SHEET_NAME_CHARGE_REQ, CHARGE_REQ_COLUMNS)

    if user["role"] == "admin":
        store_info_for_display = store_info_df_raw[store_info_df_raw['역할'] == 'store'].copy()
        
        tabs = st.tabs(["📋 발주요청 조회", "📈 매출 조회", "💰 결제 관리", "📑 증빙서류 다운로드", "🛠️ 관리 설정"])
        with tabs[0]: page_admin_unified_management(orders_df, store_info_for_display, master_df)
        with tabs[1]: page_admin_sales_inquiry(master_df)
        with tabs[2]: page_admin_balance_management(store_info_for_display)
        with tabs[3]: page_admin_documents(store_info_for_display)
        with tabs[4]: page_admin_settings(store_info_df_raw, master_df)
    
    else: # store
        my_balance_series = balance_df[balance_df['지점ID'] == user['user_id']]
        my_balance_info = my_balance_series.iloc[0] if not my_balance_series.empty else pd.Series(dtype=object)
        
        my_store_info = store_info_df_raw[store_info_df_raw['지점ID'] == user['user_id']]

        tabs = st.tabs(["🛒 발주 요청", "🧾 발주 조회", "💰 결제 관리", "📑 증빙서류 다운로드", "🏷️ 품목 단가 조회"])
        with tabs[0]: page_store_register_confirm(master_df, my_balance_info)
        with tabs[1]: page_store_orders_change(my_store_info, master_df)
        with tabs[2]: page_store_balance(charge_requests_df, my_balance_info)
        with tabs[3]: page_store_documents(my_store_info)
        with tabs[4]: page_store_master_view(master_df)
