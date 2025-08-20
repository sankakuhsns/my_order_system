# -*- coding: utf-8 -*-
# =============================================================================
# 📦 Streamlit 식자재 발주 시스템 (v12.0 - UI 복원 및 지점마스터 로그인 통합)
#
# - 주요 변경 사항:
#   - UI 복원: 모든 탭의 UI를 v11.6 버전 기준으로 완벽히 복원
#   - 로그인 시스템 교체: Google Sheets '지점마스터' 시트 기반의 통합 인증 시스템 적용
#   - 편의성 개선: 사이드바에 로그인 정보 및 로그아웃 버튼을 항상 표시
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

# =============================================================================
# 1) 시트/스키마 정의
# =============================================================================
SHEET_NAME_STORES = "지점마스터"
SHEET_NAME_MASTER = "상품마스터"
SHEET_NAME_ORDERS = "발주"
SHEET_NAME_BALANCE = "잔액마스터"
SHEET_NAME_CHARGE_REQ = "충전요청"
SHEET_NAME_TRANSACTIONS = "거래내역"

# 지점마스터 시트에 PW와 역할을 추가해야 합니다.
STORES_COLUMNS = ["지점ID", "지점PW", "역할", "지점명", "사업자등록번호", "상호명", "사업장주소", "업태"]
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

@st.cache_data(ttl=60)
def load_data(sheet_name: str, columns: List[str] = None) -> pd.DataFrame:
    try:
        ws = open_spreadsheet().worksheet(sheet_name)
        records = ws.get_all_records(empty2zero=False, head=1)
        if not records:
            return pd.DataFrame(columns=columns) if columns else pd.DataFrame()
        
        df = pd.DataFrame(records)
        df = df.astype(str) # 먼저 모든 데이터를 문자열로 변환
        
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
            
        if '주문일시' in df.columns: df = df.sort_values(by="주문일시", ascending=False)
        if '요청일시' in df.columns: df = df.sort_values(by="요청일시", ascending=False)
        if '일시' in df.columns: df = df.sort_values(by="일시", ascending=False)
            
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

def update_charge_request(timestamp: str, new_status: str, reason: str = ""):
    try:
        ws = open_spreadsheet().worksheet(SHEET_NAME_CHARGE_REQ)
        cell = ws.find(timestamp, in_column=1)
        if cell:
            header = ws.row_values(1)
            status_col_idx = header.index("상태") + 1
            reason_col_idx = header.index("처리사유") + 1
            ws.update_cell(cell.row, status_col_idx, new_status)
            ws.update_cell(cell.row, reason_col_idx, reason)
            st.cache_data.clear()
            return True
        return False
    except Exception as e:
        st.error(f"충전 요청 상태 업데이트 중 오류 발생: {e}")
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
                # 지점 ID도 반환하도록 수정
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
        worksheet.write(f'E{total_row}', order_df['단가'].sum(), fmt_total)
        worksheet.write(f'F{total_row}', order_df['공급가액'].sum(), fmt_total)
        worksheet.write(f'G{total_row}', order_df['세액'].sum(), fmt_total)
        worksheet.write(f'H{total_row}', order_df['합계금액'].sum(), fmt_total)
        
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
        col_widths = {'A': 20, 'B': 10, 'C': 30, 'D': 15, 'E': 15, 'F':15, 'G':15}
        for col, width in col_widths.items(): worksheet.set_column(f'{col}:{col}', width)

        worksheet.merge_range('A1:G1', f"{store_info['지점명']} 금전 거래 상세 명세서", fmt_title)
        headers = ['일시', '구분', '내용', '금액', '처리후 선충전잔액', '처리후 사용여신액', '관련발주번호']
        worksheet.write_row('A3', headers, fmt_header)

        df_transactions_sorted = df_transactions.sort_values(by="일시").reset_index(drop=True)
        
        row_num = 3
        for _, row in df_transactions_sorted.iterrows():
            row_num += 1
            worksheet.write(f'A{row_num}', str(row['일시']), fmt_border_c)
            worksheet.write(f'B{row_num}', row['구분'], fmt_border_c)
            worksheet.write(f'C{row_num}', row['내용'], fmt_border_l)
            worksheet.write(f'D{row_num}', row['금액'], fmt_money)
            worksheet.write(f'E{row_num}', row['처리후선충전잔액'], fmt_money)
            worksheet.write(f'F{row_num}', row['처리후사용여신액'], fmt_money)
            worksheet.write(f'G{row_num}', row.get('관련발주번호', ''), fmt_border_c)
            
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
# 6) 지점 페이지 (UI 복원 버전)
# =============================================================================
def page_store_register_confirm(master_df: pd.DataFrame, balance_info: pd.Series):
    st.subheader("🛒 발주 요청")
    user = st.session_state.auth
    
    prepaid_balance = int(balance_info.get('선충전잔액', 0))
    credit_limit = int(balance_info.get('여신한도', 0))
    used_credit = int(balance_info.get('사용여신액', 0))
    available_credit = credit_limit - used_credit
    total_available = prepaid_balance + available_credit
    
    with st.container(border=True):
        c1, c2, c3 = st.columns(3)
        c1.metric("선충전 잔액", f"{prepaid_balance:,.0f}원")
        c2.metric("사용 가능 여신", f"{available_credit:,.0f}원", delta=f"한도: {credit_limit:,.0f}원", delta_color="off")
        c3.metric("총 결제 가능액", f"{total_available:,.0f}원")

    if credit_limit > 0 and used_credit > 0 and (available_credit / credit_limit) < 0.2 :
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
        
        with st.form(key="add_to_cart_form"):
            df_edit = df_view.copy()
            df_edit["수량"] = 0
            edited_disp = st.data_editor(df_edit[["품목코드", "품목명", "단위", "단가", "수량"]], key=f"editor_v{st.session_state.store_editor_ver}", hide_index=True, disabled=["품목코드", "품목명", "단위", "단가"], use_container_width=True, column_config={"단가": st.column_config.NumberColumn(format="%d원"), "수량": st.column_config.NumberColumn(min_value=0)})
            
            if st.form_submit_button("장바구니 추가", use_container_width=True, type="primary"):
                items_to_add = coerce_cart_df(edited_disp)
                if not items_to_add[items_to_add["수량"] > 0].empty:
                    add_to_cart(items_to_add); st.session_state.store_editor_ver += 1
                st.rerun()
    
    v_spacer(16)
    with st.container(border=True):
        st.markdown("##### 🧺 장바구니")
        cart = coerce_cart_df(st.session_state.cart)
        if not cart.empty:
            st.dataframe(cart, hide_index=True, use_container_width=True)
            total_price = cart['합계금액'].sum()
            st.markdown(f"<h4 style='text-align:right;'>합계: {total_price:,.0f}원</h4>", unsafe_allow_html=True)
            
            c1, c2 = st.columns(2)
            memo = c1.text_input("요청사항 (선택)", placeholder="예: 25일 출고 요청")
            
            if c2.button("🗑️ 장바구니 비우기", use_container_width=True): 
                st.session_state.cart = pd.DataFrame(columns=CART_COLUMNS)
                st.rerun()

            if st.button("📦 발주 제출", type="primary", use_container_width=True):
                if total_price > total_available:
                    st.error(f"결제 가능 금액({total_available:,.0f}원)이 부족합니다.")
                else:
                    order_id = make_order_id(user["user_id"])
                    rows, new_balance, new_used_credit = [], prepaid_balance, used_credit
                    
                    # 결제 처리
                    if prepaid_balance >= total_price:
                        new_balance -= total_price
                    else:
                        remaining_cost = total_price - prepaid_balance
                        new_balance = 0
                        new_used_credit += remaining_cost

                    for _, r in cart.iterrows():
                        supply_price = r['합계금액']
                        # 과세/면세 구분하여 세액 계산
                        item_info = master_df[master_df['품목코드'] == r['품목코드']].iloc[0]
                        tax = math.ceil(supply_price * 0.1) if item_info['과세구분'] == '과세' else 0
                        rows.append({"주문일시": now_kst_str(), "발주번호": order_id, "지점ID": user["user_id"], "지점명": user["name"], "품목코드": r["품목코드"], "품목명": r["품목명"], "단위": r["단위"], "수량": r["수량"], "단가": r["단가"], "공급가액": supply_price, "세액": tax, "합계금액": supply_price + tax, "비고": memo, "상태": "접수", "처리자": "", "처리일시": "", "반려사유":""})
                    
                    if append_rows_to_sheet(SHEET_NAME_ORDERS, rows, ORDERS_COLUMNS):
                        update_balance_sheet(user["user_id"], {"선충전잔액": new_balance, "사용여신액": new_used_credit})
                        
                        transaction_record = {
                            "일시": now_kst_str(), "지점ID": user["user_id"], "지점명": user["name"],
                            "구분": "발주결제", "내용": f"{cart.iloc[0]['품목명']} 등 {len(cart)}건 발주",
                            "금액": -total_price, "처리후선충전잔액": new_balance,
                            "처리후사용여신액": new_used_credit, "관련발주번호": order_id, "처리자": user["name"]
                        }
                        append_rows_to_sheet(SHEET_NAME_TRANSACTIONS, [transaction_record], TRANSACTIONS_COLUMNS)
                        
                        st.session_state.success_message = "발주 및 결제가 성공적으로 완료되었습니다."
                        st.session_state.cart = pd.DataFrame(columns=CART_COLUMNS)
                        st.rerun()
                    else:
                        st.error("발주 제출 중 오류가 발생했습니다.")
        else:
            st.info("장바구니가 비어 있습니다.")

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
        if credit_limit > 0 and (available_credit / credit_limit) < 0.2:
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
                    "입금자명": depositor_name, "입금액": charge_amount, "종류": charge_type, "상태": "확인대기", "처리사유": ""
                }
                if append_rows_to_sheet(SHEET_NAME_CHARGE_REQ, [new_request], CHARGE_REQ_COLUMNS):
                    st.success("관리자에게 입금 완료 알림을 보냈습니다. 확인 후 처리됩니다.")
                else: st.error("알림 전송에 실패했습니다.")
            else: st.warning("입금자명과 입금액을 모두 입력해주세요.")
            
    st.markdown("---")
    st.markdown("##### 나의 충전/상환 요청 현황")
    my_requests = charge_requests_df[charge_requests_df['지점ID'] == user['user_id']]
    st.dataframe(my_requests, use_container_width=True, hide_index=True)


def page_store_orders_change(store_info_df: pd.DataFrame, master_df: pd.DataFrame):
    st.subheader("🧾 발주 조회")
    display_feedback()
    df_all_orders = load_data(SHEET_NAME_ORDERS, ORDERS_COLUMNS)
    user = st.session_state.auth
    
    df_user = df_all_orders[df_all_orders["지점ID"] == user["user_id"]]
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
    
    orders = df_filtered.groupby("발주번호").agg(
        주문일시=("주문일시", "first"), 
        건수=("품목코드", "count"), 
        합계금액=("합계금액", "sum"), 
        상태=("상태", "first"), 
        처리일시=("처리일시", "first"),
        반려사유=("반려사유", "first")
    ).reset_index().sort_values("주문일시", ascending=False)
    
    pending = orders[orders["상태"] == "접수"].copy()
    shipped = orders[orders["상태"] == "출고완료"].copy()
    rejected = orders[orders["상태"] == "반려"].copy()

    tab1, tab2, tab3 = st.tabs([f"접수 ({len(pending)}건)", f"출고완료 ({len(shipped)}건)", f"반려 ({len(rejected)}건)"])
    
    with tab1:
        st.dataframe(pending, hide_index=True, use_container_width=True)
    
    with tab2:
        st.dataframe(shipped, hide_index=True, use_container_width=True)
        
    with tab3:
        st.dataframe(rejected[['주문일시', '발주번호', '건수', '합계금액', '상태', '반려사유']], hide_index=True, use_container_width=True)

def page_store_documents(store_info_df: pd.DataFrame):
    st.subheader("📑 증빙서류 다운로드")
    user = st.session_state.auth
    
    c1, c2, c3 = st.columns(3)
    dt_from = c1.date_input("조회 시작일", date.today() - timedelta(days=30), key="store_doc_from")
    dt_to = c2.date_input("조회 종료일", date.today(), key="store_doc_to")
    doc_type = c3.selectbox("서류 종류", ["금전 거래내역서", "품목 거래내역서"])

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

    elif doc_type == "품목 거래내역서":
        orders_df = load_data(SHEET_NAME_ORDERS, ORDERS_COLUMNS)
        my_orders = orders_df[(orders_df['지점ID'] == user['user_id']) & (orders_df['상태'] == '출고완료')]
        if my_orders.empty: st.info("출고 완료된 발주 내역이 없습니다."); return

        my_orders['주문일시_dt'] = pd.to_datetime(my_orders['주문일시']).dt.date
        mask = (my_orders['주문일시_dt'] >= dt_from) & (my_orders['주문일시_dt'] <= dt_to)
        dfv = my_orders[mask].copy()
        if dfv.empty: st.warning("해당 기간의 출고 완료된 발주 내역이 없습니다."); return
        st.dataframe(dfv, use_container_width=True, hide_index=True)
        
        # 다운로드는 단일 발주건만 가능하므로, Selectbox로 선택하게 함
        selected_order_id = st.selectbox("다운로드할 발주번호 선택", dfv['발주번호'].unique())
        if selected_order_id:
            order_to_print = dfv[dfv['발주번호'] == selected_order_id]
            buf = make_item_transaction_statement_excel(order_to_print, my_store_info)
            st.download_button(f"'{selected_order_id}' 품목거래명세서 다운로드", data=buf, file_name=f"품목거래명세서_{user['name']}_{selected_order_id}.xlsx", mime="application/vnd.ms-excel", use_container_width=True, type="primary")

def page_store_master_view(master_df: pd.DataFrame):
    st.subheader("🏷️ 품목 단가 조회")
    l, r = st.columns([2, 1])
    keyword = l.text_input("품목 검색(이름/코드)", placeholder="오이, P001 등", key="store_master_keyword")
    cat_opt = ["(전체)"] + sorted(master_df["분류"].dropna().unique().tolist())
    cat_sel = r.selectbox("분류(선택)", cat_opt, key="store_master_category")
    
    df_view = master_df[master_df['활성'].astype(str).str.lower() == 'true'].copy()
    if keyword: df_view = df_view[df_view.apply(lambda row: keyword.strip().lower() in str(row["품목명"]).lower() or keyword.strip().lower() in str(row["품목코드"]).lower(), axis=1)]
    if cat_sel != "(전체)": df_view = df_view[df_view["분류"] == cat_sel]
    
    st.dataframe(df_view[["품목코드", "품목명", "품목규격", "분류", "단위", "단가"]], use_container_width=True, hide_index=True, column_config={"단가": st.column_config.NumberColumn(format="%d원")})


# =============================================================================
# 7) 관리자 페이지 (UI 복원 버전)
# =============================================================================
def page_admin_unified_management(df_all: pd.DataFrame, store_info_df: pd.DataFrame, master_df: pd.DataFrame):
    st.subheader("📋 발주요청 통합 관리")
    display_feedback()
    
    if df_all.empty: st.info("발주 데이터가 없습니다."); return
    
    c1, c2, c3 = st.columns(3)
    status_filter = c1.selectbox("상태", ["전체", "접수", "출고완료", "반려"], key="admin_status_filter")
    store_filter = c2.selectbox("지점", ["전체"] + store_info_df['지점명'].tolist(), key="admin_store_filter")
    order_id_search = c3.text_input("발주번호 검색", key="admin_order_search")

    df_filtered = df_all.copy()
    if status_filter != "전체": df_filtered = df_filtered[df_filtered['상태'] == status_filter]
    if store_filter != "전체": df_filtered = df_filtered[df_filtered['지점명'] == store_filter]
    if order_id_search: df_filtered = df_filtered[df_filtered['발주번호'].str.contains(order_id_search, na=False)]
    
    order_groups = df_filtered.groupby("발주번호")
    
    selected_ids = []
    for order_id, group in order_groups:
        with st.expander(f"{group.iloc[0]['지점명']} - {group.iloc[0]['주문일시']} (발주번호: {order_id}, 상태: {group.iloc[0]['상태']})"):
            st.dataframe(group[['품목명', '수량', '합계금액']], hide_index=True)
            if group.iloc[0]['상태'] == '접수':
                selected = st.checkbox("선택", key=f"select_{order_id}")
                if selected:
                    selected_ids.append(order_id)
    
    if not selected_ids: 
        if status_filter == '접수' or status_filter == '전체':
            st.info("처리할 '접수' 상태의 발주를 선택하세요.")
        return
    
    st.markdown("---")
    st.write(f"**선택된 발주 {len(selected_ids)}건 처리**")
    
    c1, c2 = st.columns(2)
    with c1:
        if st.button("✅ 일괄 출고 완료 처리", use_container_width=True, type="primary"):
            update_order_status(selected_ids, "출고완료", st.session_state.auth["name"])
            st.session_state.success_message = f"{len(selected_ids)}건의 발주가 '출고완료' 처리되었습니다."
            st.rerun()

    with c2:
        rejection_reason = st.text_input("반려 사유", placeholder="예: 재고 부족")
        if st.button("❌ 일괄 반려 처리", use_container_width=True):
            if not rejection_reason:
                st.warning("반려 사유를 입력해야 합니다.")
            else:
                with st.spinner("발주 반려 및 환불 처리 중..."):
                    df_balance = load_data(SHEET_NAME_BALANCE, BALANCE_COLUMNS)
                    df_transactions = load_data(SHEET_NAME_TRANSACTIONS, TRANSACTIONS_COLUMNS)
                    
                    for order_id in selected_ids:
                        order_items = df_all[df_all['발주번호'] == order_id]
                        store_id = order_items.iloc[0]['지점ID']
                        
                        original_tx = df_transactions[df_transactions['관련발주번호'] == order_id]
                        if not original_tx.empty:
                            tx = original_tx.iloc[0]
                            refund_amount = abs(tx['금액'])
                            
                            balance_info = df_balance[df_balance['지점ID'] == store_id].iloc[0]
                            
                            new_prepaid = balance_info['선충전잔액']
                            new_used_credit = balance_info['사용여신액']

                            # 환불 로직: 사용된 여신부터 복구하고, 나머지를 선충전액으로
                            used_credit_on_tx = tx['처리후사용여신액']
                            prev_used_credit = used_credit_on_tx - (tx['처리후선충전잔액'] - balance_info['선충전잔액']) if tx['처리후선충전잔액'] < balance_info['선충전잔액'] else used_credit_on_tx

                            credit_refund = min(refund_amount, balance_info['사용여신액'])
                            prepaid_refund = refund_amount - credit_refund
                            
                            new_used_credit -= credit_refund
                            new_prepaid += prepaid_refund
                            
                            update_balance_sheet(store_id, {'선충전잔액': new_prepaid, '사용여신액': new_used_credit})
                            
                            refund_tx = {
                                "일시": now_kst_str(), "지점ID": store_id, "지점명": tx['지점명'],
                                "구분": "발주반려", "내용": f"발주 반려 환불 ({order_id})", "금액": refund_amount,
                                "처리후선충전잔액": new_prepaid, "처리후사용여신액": new_used_credit,
                                "관련발주번호": order_id, "처리자": st.session_state.auth["name"]
                            }
                            append_rows_to_sheet(SHEET_NAME_TRANSACTIONS, [refund_tx], TRANSACTIONS_COLUMNS)
                    
                    update_order_status(selected_ids, "반려", st.session_state.auth["name"], reason=rejection_reason)
                    st.session_state.success_message = f"{len(selected_ids)}건이 반려 처리되고 환불되었습니다."
                    st.rerun()

def page_admin_sales_inquiry(master_df: pd.DataFrame):
    st.subheader("📈 매출 조회")
    orders_df = load_data(SHEET_NAME_ORDERS, ORDERS_COLUMNS)
    approved_orders = orders_df[orders_df['상태'] == '출고완료'].copy()
    
    if approved_orders.empty:
        st.info("매출 데이터가 없습니다.")
        return

    approved_orders['주문일시'] = pd.to_datetime(approved_orders['주문일시'])
    
    c1, c2, _ = st.columns([1,1,3])
    start_date = c1.date_input("조회 시작일", date.today() - timedelta(days=30))
    end_date = c2.date_input("조회 종료일", date.today())
    
    mask = (approved_orders['주문일시'].dt.date >= start_date) & (approved_orders['주문일시'].dt.date <= end_date)
    filtered_sales = approved_orders.loc[mask]

    if filtered_sales.empty:
        st.warning("선택한 기간에 해당하는 매출 데이터가 없습니다.")
        return

    st.markdown("##### 📅 일별 매출 현황")
    daily_pivot = filtered_sales.pivot_table(index='지점명', columns=filtered_sales['주문일시'].dt.strftime('%Y-%m-%d'), values='합계금액', aggfunc='sum', fill_value=0)
    st.dataframe(daily_pivot.style.format("{:,.0f}"))

    st.markdown("##### 🗓️ 월별 매출 현황")
    monthly_pivot = filtered_sales.pivot_table(index='지점명', columns=filtered_sales['주문일시'].dt.strftime('%Y-%m'), values='합계금액', aggfunc='sum', fill_value=0)
    st.dataframe(monthly_pivot.style.format("{:,.0f}"))
    
    excel_data = make_sales_summary_excel(daily_pivot, monthly_pivot)
    st.download_button("엑셀 다운로드", data=excel_data, file_name=f"매출현황_{start_date}_to_{end_date}.xlsx", mime="application/vnd.ms-excel")

def page_admin_balance_management(store_info_df: pd.DataFrame):
    st.subheader("💰 결제 관리")
    charge_req_df = load_data(SHEET_NAME_CHARGE_REQ, CHARGE_REQ_COLUMNS)
    balance_df = load_data(SHEET_NAME_BALANCE, BALANCE_COLUMNS)
    pending_requests = charge_req_df[charge_req_df['상태'] == '확인대기']
    
    st.markdown("##### 💳 충전/상환 요청 처리")
    if pending_requests.empty:
        st.info("처리 대기 중인 요청이 없습니다.")
    else:
        st.dataframe(pending_requests, hide_index=True)
        
        c1, c2, c3 = st.columns(3)
        req_options = {f"{row['요청일시']} / {row['지점명']} / {row['입금액']:,}원": row for _, row in pending_requests.iterrows()}
        selected_req_str = c1.selectbox("처리할 요청 선택", req_options.keys())
        action = c2.selectbox("처리 방식", ["승인", "반려"])
        reason = c3.text_input("반려 사유 (반려 시 필수)")

        if st.button("처리 실행", type="primary"):
            selected_req = req_options[selected_req_str]
            if action == "반려" and not reason:
                st.warning("반려 시 사유를 입력해야 합니다.")
            else:
                if action == "승인":
                    store_id = selected_req['지점ID']
                    current_balance = balance_df[balance_df['지점ID'] == store_id].iloc[0]
                    amount = selected_req['입금액']
                    new_prepaid, new_used_credit = current_balance['선충전잔액'], current_balance['사용여신액']

                    if selected_req['종류'] == '선충전':
                        new_prepaid += amount
                    else: # 여신상환
                        new_used_credit -= amount
                        if new_used_credit < 0: # 초과상환
                            new_prepaid += abs(new_used_credit)
                            new_used_credit = 0

                    update_balance_sheet(store_id, {'선충전잔액': new_prepaid, '사용여신액': new_used_credit})
                    update_charge_request(selected_req['요청일시'], "승인")
                    
                    transaction = {
                        "일시": now_kst_str(), "지점ID": store_id, "지점명": selected_req['지점명'], "구분": "입금",
                        "내용": f"{selected_req['종류']} ({selected_req['입금자명']})", "금액": amount,
                        "처리후선충전잔액": new_prepaid, "처리후사용여신액": new_used_credit, "관련발주번호": "", "처리자": st.session_state.auth["name"]
                    }
                    append_rows_to_sheet(SHEET_NAME_TRANSACTIONS, [transaction], TRANSACTIONS_COLUMNS)
                    st.success("요청이 승인 처리되었습니다.")
                else: # 반려
                    update_charge_request(selected_req['요청일시'], "반려", reason)
                    st.success("요청이 반려 처리되었습니다.")
                st.rerun()

    st.markdown("---")
    st.markdown("##### 🏢 지점별 잔액 현황")
    st.dataframe(balance_df, hide_index=True)

def page_admin_documents(store_info_df: pd.DataFrame):
    st.subheader("📑 증빙서류 다운로드")
    
    c1, c2, c3, c4 = st.columns(4)
    dt_from = c1.date_input("조회 시작일", date.today() - timedelta(days=30), key="admin_doc_from")
    dt_to = c2.date_input("조회 종료일", date.today(), key="admin_doc_to")
    
    stores = ["(모든 지점)"] + sorted(store_info_df["지점명"].dropna().unique().tolist())
    store_sel = c3.selectbox("지점 선택", stores, key="admin_doc_store")
    doc_type = c4.selectbox("서류 종류", ["금전 거래내역서", "품목 거래내역서"])
    
    if store_sel == "(모든 지점)":
        st.info("지점을 선택하면 내역 조회 및 다운로드가 가능합니다.")
        return
        
    selected_store_info = store_info_df[store_info_df['지점명'] == store_sel].iloc[0]

    if doc_type == "금전 거래내역서":
        transactions_df = load_data(SHEET_NAME_TRANSACTIONS, TRANSACTIONS_COLUMNS)
        store_transactions = transactions_df[transactions_df['지점명'] == store_sel]
        
        store_transactions['일시_dt'] = pd.to_datetime(store_transactions['일시']).dt.date
        mask = (store_transactions['일시_dt'] >= dt_from) & (store_transactions['일시_dt'] <= dt_to)
        dfv = store_transactions[mask].copy()

        st.dataframe(dfv.drop(columns=['일시_dt']), use_container_width=True, hide_index=True)
        if not dfv.empty:
            buf = make_full_transaction_statement_excel(dfv, selected_store_info)
            st.download_button("엑셀 다운로드", data=buf, file_name=f"금전거래명세서_{store_sel}_{dt_from}_to_{dt_to}.xlsx", mime="application/vnd.ms-excel", use_container_width=True, type="primary")

    elif doc_type == "품목 거래내역서":
        orders_df = load_data(SHEET_NAME_ORDERS, ORDERS_COLUMNS)
        store_orders = orders_df[(orders_df['지점명'] == store_sel) & (orders_df['상태'] == '출고완료')]
        
        store_orders['주문일시_dt'] = pd.to_datetime(store_orders['주문일시']).dt.date
        mask = (store_orders['주문일시_dt'] >= dt_from) & (store_orders['주문일시_dt'] <= dt_to)
        dfv = store_orders[mask].copy()

        st.dataframe(dfv, use_container_width=True, hide_index=True)
        if not dfv.empty:
            selected_order_id = st.selectbox("다운로드할 발주번호 선택", dfv['발주번호'].unique())
            if selected_order_id:
                order_to_print = dfv[dfv['발주번호'] == selected_order_id]
                buf = make_item_transaction_statement_excel(order_to_print, selected_store_info)
                st.download_button(f"'{selected_order_id}' 품목거래명세서 다운로드", data=buf, file_name=f"품목거래명세서_{store_sel}_{selected_order_id}.xlsx", mime="application/vnd.ms-excel", use_container_width=True, type="primary")

def page_admin_items_price(master_df: pd.DataFrame):
    st.subheader("🏷️ 품목 단가 설정")
    st.caption("품목 정보를 수정하고 '저장하기' 버튼을 누르면 시트에 반영됩니다.")

    edited_df = st.data_editor(master_df, num_rows="dynamic", use_container_width=True)
    
    if st.button("변경된 품목 정보 저장하기", type="primary"):
        ws = open_spreadsheet().worksheet(SHEET_NAME_MASTER)
        ws.clear()
        ws.update([edited_df.columns.values.tolist()] + edited_df.fillna('').values.tolist(), value_input_option='USER_ENTERED')
        st.cache_data.clear()
        st.success("품목 정보가 성공적으로 업데이트되었습니다.")
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
    
    master_df = load_data(SHEET_NAME_MASTER, MASTER_COLUMNS)
    store_info_df_raw = load_data(SHEET_NAME_STORES, STORES_COLUMNS)
    
    if user["role"] == "admin":
        store_info_df = store_info_df_raw[store_info_df_raw['지점명'] != '대전 가공장'].copy()
    else:
        store_info_df = store_info_df_raw

    orders_df = load_data(SHEET_NAME_ORDERS, ORDERS_COLUMNS)
    balance_df = load_data(SHEET_NAME_BALANCE, BALANCE_COLUMNS)
    charge_requests_df = load_data(SHEET_NAME_CHARGE_REQ, CHARGE_REQ_COLUMNS)

    if user["role"] == "admin":
        tabs = st.tabs(["📋 발주요청 조회", "📈 매출 조회", "💰 결제 관리", "📑 증빙서류 다운로드", "🏷️ 품목 단가 설정"])
        with tabs[0]: page_admin_unified_management(orders_df, store_info_df, master_df)
        with tabs[1]: page_admin_sales_inquiry(master_df)
        with tabs[2]: page_admin_balance_management(store_info_df)
        with tabs[3]: page_admin_documents(store_info_df)
        with tabs[4]: page_admin_items_price(master_df)
    else: # store
        tabs = st.tabs(["🛒 발주 요청", "🧾 발주 조회", "💰 결제 관리", "📑 증빙서류 다운로드", "🏷️ 품목 단가 조회"])
        
        my_balance_series = balance_df[balance_df['지점ID'] == user['user_id']]
        my_balance_info = my_balance_series.iloc[0] if not my_balance_series.empty else pd.Series(dtype='object')
        
        my_store_info = store_info_df_raw[store_info_df_raw['지점ID'] == user['user_id']]

        with tabs[0]: page_store_register_confirm(master_df, my_balance_info)
        with tabs[1]: page_store_orders_change(my_store_info, master_df)
        with tabs[2]: page_store_balance(charge_requests_df, my_balance_info)
        with tabs[3]: page_store_documents(my_store_info)
        with tabs[4]: page_store_master_view(master_df)
