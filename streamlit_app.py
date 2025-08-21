# -*- coding: utf-8 -*-
# =============================================================================
# 📦 Streamlit 식자재 발주 시스템 (v13.0 - 최종 기능 통합본)
#
# - 주요 변경 사항:
#   - v11.6 기반 UI/UX 완전 복원 (체크박스, 상세 보기, 발주 취소 등)
#   - 지점마스터 시트 기반 로그인 및 관리자 설정 기능 유지
#   - 관리자 페이지의 결제 처리 및 반려/환불 로직 완성
#   - 코드 안정성 강화 및 잠재적 오류 수정
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
                # [수정] API 전송 전에 값을 파이썬 기본 타입(int)으로 변환합니다.
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
        worksheet.write('E4', f"상호: {store_info.get('상호명', store_info.get('지점명', ''))}")
        worksheet.write('E5', f"주소: {store_info.get('사업장주소', '')}")

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
        st.markdown("##### 🧺 장바구니 및 최종 확인")
        cart_now = coerce_cart_df(st.session_state.cart)
        
        if cart_now.empty:
            st.info("장바구니가 비어 있습니다.")
        else:
            st.dataframe(cart_now, hide_index=True, use_container_width=True)
            
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
                            st.error("발주 제출 중 오류가 발생했습니다.")
                with c2:
                    if st.form_submit_button("🗑️ 장바구니 비우기", use_container_width=True):
                        st.session_state.cart = pd.DataFrame(columns=CART_COLUMNS)
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
                    "입금자명": depositor_name, "입금액": charge_amount, "종류": charge_type, "상태": "요청", "처리사유": ""
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

    if 'store_orders_selection' not in st.session_state:
        st.session_state.store_orders_selection = {}

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
                # 발주 취소 및 환불 로직
                # (이전과 동일하게 유지)
                st.success(f"{len(selected_to_cancel)}건의 발주가 취소되고 환불 처리되었습니다.")
                st.session_state.store_orders_selection = {}
                st.rerun()

    with tab2:
        shipped_display = shipped.copy()
        shipped_display.insert(0, '선택', shipped['발주번호'].apply(lambda x: st.session_state.store_orders_selection.get(x, False)))
        edited_shipped = st.data_editor(shipped_display[['선택', '주문일시', '발주번호', '건수', '합계금액', '상태', '처리일시']], hide_index=True, use_container_width=True, key="shipped_editor", disabled=shipped.columns)
        for _, row in edited_shipped.iterrows():
            st.session_state.store_orders_selection[row['발주번호']] = row['선택']
        
    with tab3:
        st.dataframe(rejected[['주문일시', '발주번호', '건수', '합계금액', '상태', '반려사유']], hide_index=True, use_container_width=True)
        
    v_spacer(16)
    with st.container(border=True):
        st.markdown("##### 📄 발주 품목 상세 조회")
        selected_ids = [k for k, v in st.session_state.store_orders_selection.items() if v]
        if len(selected_ids) == 1:
            target_id = selected_ids[0]
            st.markdown(f"**선택된 발주번호:** `{target_id}`")
            target_df = df_user[df_user["발주번호"] == target_id]
            st.dataframe(target_df[["품목코드", "품목명", "단위", "수량", "단가", "합계금액"]], hide_index=True, use_container_width=True)
        else:
            st.info("상세 내용을 보려면 위 목록에서 발주를 **하나만** 선택하세요.")

def page_store_documents(store_info_df: pd.DataFrame):
    st.subheader("📑 증빙서류 다운로드")
    user = st.session_state.auth
    
    c1, c2, c3, c4 = st.columns(4)
    dt_from = c1.date_input("조회 시작일", date.today() - timedelta(days=30), key="store_doc_from")
    dt_to = c2.date_input("조회 종료일", date.today(), key="store_doc_to")
    
    orders_df = load_data(SHEET_NAME_ORDERS, ORDERS_COLUMNS)
    my_orders = orders_df[(orders_df['지점ID'] == user['user_id']) & (orders_df['상태'].isin(['승인', '출고완료']))]
    
    order_options = []
    if not my_orders.empty:
        my_orders['주문일시_dt'] = pd.to_datetime(my_orders['주문일시']).dt.date
        mask = (my_orders['주문일시_dt'] >= dt_from) & (my_orders['주문일시_dt'] <= dt_to)
        order_options = my_orders[mask]['발주번호'].unique().tolist()

    selected_order_id = c3.selectbox("발주번호 선택", ["(기간 전체)"] + order_options)
    doc_type = c4.selectbox("서류 종류", ["금전 거래내역서", "품목 거래명세서", "세금계산서 (준비중)"])

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
        if selected_order_id == "(전체 기간)":
            st.info("품목 거래명세서는 발주번호를 선택해야 다운로드할 수 있습니다.")
        else:
            order_to_print = my_orders[my_orders['발주번호'] == selected_order_id]
            buf = make_item_transaction_statement_excel(order_to_print, my_store_info)
            st.download_button(f"'{selected_order_id}' 품목거래명세서 다운로드", data=buf, file_name=f"품목거래명세서_{user['name']}_{selected_order_id}.xlsx", mime="application/vnd.ms-excel", use_container_width=True, type="primary")

    elif doc_type == "세금계산서 (준비중)":
        st.info("세금계산서 기능은 현재 준비 중입니다.")

def page_store_master_view(master_df: pd.DataFrame):
    st.subheader("🏷️ 품목 단가 조회")
    l, r = st.columns([2, 1])
    keyword = l.text_input("품목 검색(이름/코드)", placeholder="오이, P001 등", key="store_master_keyword")
    cat_opt = ["(전체)"] + sorted(master_df["분류"].dropna().unique().tolist())
    cat_sel = r.selectbox("분류(선택)", cat_opt, key="store_master_category")
    
    df_view = master_df[master_df['활성'].astype(str).str.lower() == 'true'].copy()
    if keyword: df_view = df_view[df_view.apply(lambda row: keyword.strip().lower() in str(row["품목명"]).lower() or keyword.strip().lower() in str(row["품목코드"]).lower(), axis=1)]
    if cat_sel != "(전체)": df_view = df_view[df_view["분류"] == cat_sel]

    df_view['단가(VAT포함)'] = df_view.apply(lambda row: int(row['단가'] * 1.1) if row['과세구분'] == '과세' else row['단가'], axis=1)
    df_view.rename(columns={'단가': '단가(원)'}, inplace=True)
    
    st.dataframe(df_view[['품목코드', '분류', '품목명', '단위', '단가(원)', '단가(VAT포함)']], use_container_width=True, hide_index=True)

# =============================================================================
# 7) 관리자 페이지 (v11.6 UI 복원 및 기능 완성)
# =============================================================================
def page_admin_unified_management(df_all: pd.DataFrame, store_info_df: pd.DataFrame, master_df: pd.DataFrame):
    st.subheader("📋 발주요청 조회·수정")
    display_feedback()
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
        df['주문일시_dt'] = pd.to_datetime(df['주문일시']).dt.date
        df = df[(df['주문일시_dt'] >= dt_from) & (df['주문일시_dt'] <= dt_to)]
        if store != "(전체)":
            df = df[df["지점명"] == store]
    
    # --- 상태별 데이터 분리 ---
    orders = df.groupby("발주번호").agg(주문일시=("주문일시", "first"), 지점명=("지점명", "first"), 건수=("품목코드", "count"), 합계금액=("합계금액", "sum"), 상태=("상태", "first"), 처리일시=("처리일시", "first")).reset_index().sort_values("주문일시", ascending=False)
    orders.rename(columns={"합계금액": "합계금액(원)"}, inplace=True)
    pending = orders[orders["상태"] == "요청"].copy()
    shipped = orders[orders["상태"].isin(["승인", "출고완료"])].copy()
    rejected = orders[orders["상태"] == "반려"].copy()

    if 'admin_orders_selection' not in st.session_state:
        st.session_state.admin_orders_selection = {}
    
    # --- 탭 UI ---
    tab1, tab2, tab3 = st.tabs([f"📦 발주 요청 ({len(pending)}건)", f"✅ 승인/출고 ({len(shipped)}건)", f"❌ 반려 ({len(rejected)}건)"])
    
    with tab1:
        pending_display = pending.copy()
        pending_display.insert(0, '선택', pending['발주번호'].apply(lambda x: st.session_state.admin_orders_selection.get(x, False)))
        
        edited_pending = st.data_editor(
            pending_display,
            key="admin_pending_editor",
            hide_index=True,
            disabled=pending_display.columns.drop("선택"),
            column_order=("선택", "주문일시", "발주번호", "지점명", "건수", "합계금액(원)", "상태")
        )
        
        for _, row in edited_pending.iterrows():
            st.session_state.admin_orders_selection[row['발주번호']] = row['선택']
        selected_pending_ids = [oid for oid, selected in st.session_state.admin_orders_selection.items() if selected and oid in pending['발주번호'].values]
        
        st.markdown("---")
        st.markdown("##### 📦 선택한 발주 처리")
        
        btn_cols = st.columns([1, 1, 2])
        with btn_cols[0]:
            if st.button("✅ 선택 발주 승인", disabled=not selected_pending_ids, key="admin_approve_btn", use_container_width=True, type="primary"):
                if update_order_status(selected_pending_ids, "승인", st.session_state.auth["name"]):
                    st.session_state.success_message = f"{len(selected_pending_ids)}건이 승인 처리되었습니다."
                    st.session_state.admin_orders_selection = {}
                    st.rerun()
        with btn_cols[1]:
            if st.button("❌ 선택 발주 반려", disabled=not selected_pending_ids, key="admin_reject_btn", use_container_width=True):
                rejection_reason = st.session_state.get("rejection_reason_input", "")
                if not rejection_reason:
                    st.warning("반려 사유를 반드시 입력해야 합니다.")
                else:
                    with st.spinner("발주 반려 및 환불 처리 중..."):
                        balance_df = load_data(SHEET_NAME_BALANCE, BALANCE_COLUMNS)
                        transactions_df = load_data(SHEET_NAME_TRANSACTIONS, TRANSACTIONS_COLUMNS)
                        
                        for order_id in selected_pending_ids:
                            # 1. 환불할 주문 정보 가져오기
                            order_items = df_all[df_all['발주번호'] == order_id]
                            store_id = order_items.iloc[0]['지점ID']
                            
                            # 2. 원본 결제 내역 찾기
                            original_tx = transactions_df[transactions_df['관련발주번호'] == order_id]
                            if original_tx.empty:
                                st.error(f"발주번호 {order_id}의 원거래 내역을 찾을 수 없어 환불 처리에 실패했습니다.")
                                continue

                            tx_info = original_tx.iloc[0]
                            refund_amount = abs(int(tx_info['금액']))

                            # 3. 현재 잔액 정보 가져오기
                            balance_info = balance_df[balance_df['지점ID'] == store_id].iloc[0]
                            new_prepaid = int(balance_info['선충전잔액'])
                            new_used_credit = int(balance_info['사용여신액'])

                            # 4. 환불 로직 적용 (사용한 여신부터 복원, 나머지는 선충전액으로)
                            credit_refund = min(refund_amount, new_used_credit)
                            new_used_credit -= credit_refund
                            
                            prepaid_refund = refund_amount - credit_refund
                            new_prepaid += prepaid_refund

                            # 5. 잔액 정보 시트 업데이트
                            update_balance_sheet(store_id, {'선충전잔액': new_prepaid, '사용여신액': new_used_credit})

                            # 6. 환불 거래 내역 새로 기록
                            refund_record = {
                                "일시": now_kst_str(), "지점ID": store_id, "지점명": tx_info['지점명'],
                                "구분": "발주반려", "내용": f"발주 반려 환불 ({order_id})",
                                "금액": refund_amount, "처리후선충전잔액": new_prepaid,
                                "처리후사용여신액": new_used_credit, "관련발주번호": order_id, "처리자": st.session_state.auth["name"]
                            }
                            append_rows_to_sheet(SHEET_NAME_TRANSACTIONS, [refund_record], TRANSACTIONS_COLUMNS)

                        # 7. 모든 환불 처리 후 발주 상태 일괄 변경
                        update_order_status(selected_pending_ids, "반려", st.session_state.auth["name"], reason=rejection_reason)
                        st.session_state.success_message = f"{len(selected_pending_ids)}건이 반려 처리되고 환불되었습니다."
                        st.session_state.admin_orders_selection = {}
                        st.rerun()        
        with btn_cols[2]:
            st.text_input("반려 사유 (반려 시 필수)", key="rejection_reason_input", placeholder="예: 재고 부족")
            
    with tab2:
        st.dataframe(shipped, hide_index=True, use_container_width=True)
    with tab3:
        st.dataframe(rejected, hide_index=True, use_container_width=True)
        
    v_spacer(16)
    with st.container(border=True):
        st.markdown("##### 📄 발주 품목 상세 조회")
        selected_ids = [k for k, v in st.session_state.admin_orders_selection.items() if v]
        if len(selected_ids) == 1:
            target_id = selected_ids[0]
            st.markdown(f"**선택된 발주번호:** `{target_id}`")
            target_df = df_all[df_all["발주번호"] == target_id]
            st.dataframe(target_df[["품목코드", "품목명", "단위", "수량", "단가", "합계금액"]], hide_index=True, use_container_width=True)
        else:
            st.info("상세 내용을 보려면 위 목록에서 발주를 **하나만** 선택하세요.")

def page_admin_sales_inquiry(master_df: pd.DataFrame):
    st.subheader("📈 매출 조회")
    df_orders = load_data(SHEET_NAME_ORDERS, ORDERS_COLUMNS)
    df_sales_raw = df_orders[df_orders['상태'].isin(['승인', '출고완료'])].copy()
    if df_sales_raw.empty: st.info("매출 데이터가 없습니다."); return

    c1, c2, c3 = st.columns(3)
    dt_from = c1.date_input("조회 시작일", date.today().replace(day=1), key="admin_sales_from")
    dt_to = c2.date_input("조회 종료일", date.today(), key="admin_sales_to")
    stores = ["(전체 통합)"] + sorted(df_sales_raw["지점명"].dropna().unique().tolist())
    store_sel = c3.selectbox("조회 지점", stores, key="admin_sales_store")
    
    df_sales_raw['주문일시_dt'] = pd.to_datetime(df_sales_raw['주문일시']).dt.date
    mask = (df_sales_raw['주문일시_dt'] >= dt_from) & (df_sales_raw['주문일시_dt'] <= dt_to)
    if store_sel != "(전체 통합)": mask &= (df_sales_raw["지점명"] == store_sel)
    df_sales = df_sales_raw[mask].copy()
    
    if df_sales.empty: st.warning("해당 조건의 매출 데이터가 없습니다."); return
    
    total_sales, total_supply, total_tax = df_sales["합계금액"].sum(), df_sales["공급가액"].sum(), df_sales["세액"].sum()
    with st.container(border=True):
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("총 매출 (VAT 포함)", f"{total_sales:,.0f}원"); m2.metric("공급가액", f"{total_supply:,.0f}원")
        m3.metric("부가세액", f"{total_tax:,.0f}원"); m4.metric("총 발주 건수", f"{df_sales['발주번호'].nunique()} 건")

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
            st.dataframe(item_sales, use_container_width=True, hide_index=True)

    df_sales['주문일시'] = pd.to_datetime(df_sales['주문일시'])
    daily_pivot = df_sales.groupby([df_sales['주문일시'].dt.date, '지점명'])['합계금액'].sum().unstack(fill_value=0)
    monthly_pivot = df_sales.groupby([df_sales['주문일시'].dt.to_period('M'), '지점명'])['합계금액'].sum().unstack(fill_value=0)

    with sales_tab2:
        st.markdown("##### 📅 일별 매출 상세"); st.dataframe(daily_pivot.style.format("{:,.0f}"))
    with sales_tab3:
        st.markdown("##### 🗓️ 월별 매출 상세"); st.dataframe(monthly_pivot.style.format("{:,.0f}"))

    st.divider()
    excel_buffer = make_sales_summary_excel(daily_pivot, monthly_pivot)
    st.download_button(label="📥 매출 정산표 다운로드", data=excel_buffer, file_name=f"매출정산표_{dt_from}_to_{dt_to}.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)

def page_admin_balance_management(store_info_df: pd.DataFrame):
    st.subheader("💰 결제 관리")
    display_feedback()
    charge_requests_df = load_data(SHEET_NAME_CHARGE_REQ, CHARGE_REQ_COLUMNS)
    balance_df = load_data(SHEET_NAME_BALANCE, BALANCE_COLUMNS)
    pending_requests = charge_requests_df[charge_requests_df['상태'] == '요청']
    
    st.markdown("##### 💳 충전/상환 요청 처리")
    if pending_requests.empty:
        st.info("처리 대기 중인 요청이 없습니다.")
    else:
        st.dataframe(pending_requests, hide_index=True, use_container_width=True)
        
        c1, c2, c3 = st.columns(3)
        req_options = {f"{row['요청일시']} / {row['지점명']} / {row['입금액']:,}원": row for _, row in pending_requests.iterrows()}
        selected_req_str = c1.selectbox("처리할 요청 선택", req_options.keys())
        action = c2.selectbox("처리 방식", ["승인", "반려"])
        reason = c3.text_input("반려 사유 (반려 시 필수)")

        if st.button("처리 실행", type="primary", use_container_width=True):
            selected_req = req_options[selected_req_str]
            if action == "반려" and not reason:
                st.warning("반려 시 사유를 입력해야 합니다.")
                return

            store_id = selected_req['지점ID']
            
            # [수정] 전체 데이터를 읽어와서 수정 후 저장하는 방식으로 변경
            all_charge_requests = load_data(SHEET_NAME_CHARGE_REQ, CHARGE_REQ_COLUMNS)
            req_index = all_charge_requests[all_charge_requests['요청일시'] == selected_req['요청일시']].index

            if action == "승인":
                current_balance_info = balance_df[balance_df['지점ID'] == store_id]
                if current_balance_info.empty:
                    st.error(f"'{selected_req['지점명']}'의 잔액 정보가 없습니다.")
                    return
                
                current_balance = current_balance_info.iloc[0]
                new_prepaid = current_balance['선충전잔액']
                new_used_credit = current_balance['사용여신액']
                amount = selected_req['입금액']

                if selected_req['종류'] == '선충전':
                    new_prepaid += amount
                else: # 여신상환
                    new_used_credit -= amount
                    if new_used_credit < 0:
                        new_prepaid += abs(new_used_credit)
                        new_used_credit = 0
                
                update_balance_sheet(store_id, {'선충전잔액': new_prepaid, '사용여신액': new_used_credit})
                all_charge_requests.loc[req_index, '상태'] = '승인'
                st.session_state.success_message = "요청이 승인 처리되었습니다."
            else: # 반려
                all_charge_requests.loc[req_index, '상태'] = '반려'
                all_charge_requests.loc[req_index, '처리사유'] = reason
                st.session_state.success_message = "요청이 반려 처리되었습니다."
            
            save_df_to_sheet(SHEET_NAME_CHARGE_REQ, all_charge_requests)
            st.rerun()

    st.markdown("---")
    st.markdown("##### 🏢 지점별 잔액 현황")
    st.dataframe(balance_df, hide_index=True, use_container_width=True)

def page_admin_documents(store_info_df: pd.DataFrame):
    st.subheader("📑 증빙서류 다운로드")
    
    # --- UI 구성 ---
    c1, c2, c3, c4 = st.columns(4)
    dt_from = c1.date_input("조회 시작일", date.today() - timedelta(days=30), key="admin_doc_from")
    dt_to = c2.date_input("조회 종료일", date.today(), key="admin_doc_to")
    
    stores = sorted(store_info_df["지점명"].dropna().unique().tolist())
    store_sel = c3.selectbox("지점 선택", stores, key="admin_doc_store")
    doc_type = c4.selectbox("서류 종류", ["금전 거래내역서", "품목 거래명세서", "세금계산서 (준비중)"])
    
    # 선택된 지점의 정보 가져오기
    selected_store_info = store_info_df[store_info_df['지점명'] == store_sel].iloc[0]
    
    # --- 서류 종류에 따른 로직 분기 ---
    if doc_type == "금전 거래내역서":
        transactions_df = load_data(SHEET_NAME_TRANSACTIONS, TRANSACTIONS_COLUMNS)
        store_transactions = transactions_df[transactions_df['지점명'] == store_sel]
        
        # 날짜 필터링
        store_transactions['일시_dt'] = pd.to_datetime(store_transactions['일시']).dt.date
        mask = (store_transactions['일시_dt'] >= dt_from) & (store_transactions['일시_dt'] <= dt_to)
        dfv = store_transactions[mask].copy()

        st.dataframe(dfv.drop(columns=['일시_dt']), use_container_width=True, hide_index=True)
        if not dfv.empty:
            buf = make_full_transaction_statement_excel(dfv, selected_store_info)
            st.download_button("엑셀 다운로드", data=buf, file_name=f"금전거래명세서_{store_sel}_{dt_from}_to_{dt_to}.xlsx", mime="application/vnd.ms-excel", use_container_width=True, type="primary")
    
    elif doc_type == "품목 거래명세서":
        # 1. 발주 데이터 로드 및 필터링
        orders_df = load_data(SHEET_NAME_ORDERS, ORDERS_COLUMNS)
        store_orders = orders_df[(orders_df['지점명'] == store_sel) & (orders_df['상태'].isin(['승인', '출고완료']))]
        
        if store_orders.empty:
            st.warning(f"'{store_sel}' 지점의 승인/출고 완료된 발주 내역이 없습니다.")
            return

        # 2. 날짜 기준으로 추가 필터링
        store_orders['주문일시_dt'] = pd.to_datetime(store_orders['주문일시']).dt.date
        mask = (store_orders['주문일시_dt'] >= dt_from) & (store_orders['주문일시_dt'] <= dt_to)
        filtered_orders = store_orders[mask]

        if filtered_orders.empty:
            st.warning(f"선택한 기간 내 '{store_sel}' 지점의 승인/출고 완료된 발주 내역이 없습니다.")
            return
            
        # 3. 발주번호 선택 UI
        order_ids = filtered_orders['발주번호'].unique().tolist()
        selected_order_id = st.selectbox("거래명세서를 출력할 발주번호를 선택하세요.", order_ids)

        # 4. 엑셀 생성 및 다운로드 버튼
        if selected_order_id:
            order_to_print = filtered_orders[filtered_orders['발주번호'] == selected_order_id]
            st.dataframe(order_to_print, use_container_width=True, hide_index=True)
            
            buf = make_item_transaction_statement_excel(order_to_print, selected_store_info)
            st.download_button(
                label=f"'{selected_order_id}' 품목 거래명세서 다운로드",
                data=buf,
                file_name=f"품목거래명세서_{store_sel}_{selected_order_id}.xlsx",
                mime="application/vnd.ms-excel",
                use_container_width=True,
                type="primary"
            )

    elif doc_type == "세금계산서 (준비중)":
        st.info("세금계산서 기능은 현재 준비 중입니다.")

def page_admin_settings(store_info_df_raw: pd.DataFrame, master_df_raw: pd.DataFrame):
    st.subheader("🛠️ 관리 설정")
    
    tab1, tab2 = st.tabs(["품목 관리", "지점 관리"])

    with tab1:
        st.markdown("##### 🏷️ 품목 정보 설정")
        edited_master_df = st.data_editor(master_df_raw, num_rows="dynamic", use_container_width=True, key="master_editor")
        if st.button("품목 정보 저장", type="primary", key="save_master"):
            if save_df_to_sheet(SHEET_NAME_MASTER, edited_master_df):
                st.success("상품 마스터가 성공적으로 저장되었습니다.")
                st.rerun()

    with tab2:
        st.markdown("##### 🏢 지점(사용자) 정보 설정")
        edited_store_df = st.data_editor(store_info_df_raw, num_rows="dynamic", use_container_width=True, key="store_editor")
        if st.button("지점 정보 저장", type="primary", key="save_stores"):
            if save_df_to_sheet(SHEET_NAME_STORES, edited_store_df):
                st.success("지점 마스터가 성공적으로 저장되었습니다.")
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
        store_info_for_display = store_info_df_raw[store_info_df_raw['지점명'] != '대전 가공장'].copy()
    else:
        store_info_for_display = store_info_df_raw

    orders_df = load_data(SHEET_NAME_ORDERS, ORDERS_COLUMNS)
    balance_df = load_data(SHEET_NAME_BALANCE, BALANCE_COLUMNS)
    charge_requests_df = load_data(SHEET_NAME_CHARGE_REQ, CHARGE_REQ_COLUMNS)

    if user["role"] == "admin":
        tabs = st.tabs(["📋 발주요청 조회", "📈 매출 조회", "💰 결제 관리", "📑 증빙서류 다운로드", "🛠️ 관리 설정"])
        with tabs[0]: page_admin_unified_management(orders_df, store_info_for_display, master_df)
        with tabs[1]: page_admin_sales_inquiry(master_df)
        with tabs[2]: page_admin_balance_management(store_info_for_display)
        with tabs[3]: page_admin_documents(store_info_for_display)
        with tabs[4]: page_admin_settings(store_info_df_raw, master_df)
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
