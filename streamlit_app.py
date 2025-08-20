# -*- coding: utf-8 -*-
# =============================================================================
# 📦 Streamlit 식자재 발주 시스템 (v13.0 - 통합 인증 및 관리 최종본)
#
# - 주요 변경 사항:
#   - 통합 인증 시스템: 모든 사용자는 '지점마스터' 시트 기준으로 로그인
#   - 관리자 기능 강화: '관리 설정' 탭에서 품목 및 지점(사용자) 정보 직접 편집
#   - 편의 기능 추가: 사이드바에 로그인 정보 및 로그아웃 버튼 표시
#   - 요청된 모든 UI/UX 개선 및 오류 수정 완료
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

def init_session_state():
    if "auth" not in st.session_state:
        st.session_state.auth = {"login": False}
    if "cart" not in st.session_state:
        st.session_state.cart = []
    if "success_message" not in st.session_state:
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
# 4) 공통 유틸리티 (엑셀 다운로드 등)
# =============================================================================
def to_excel(df: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Sheet1')
        writer.close()
    processed_data = output.getvalue()
    return processed_data

# =============================================================================
# 5) 지점(Store) 페이지 함수
# =============================================================================
def page_store_register_confirm(master_df: pd.DataFrame, my_balance_info: pd.Series):
    st.subheader("🛒 발주 요청")
    
    # 잔액 정보 표시
    if my_balance_info.empty:
        st.error("잔액 정보를 불러올 수 없습니다. 관리자에게 문의하세요.")
        return
        
    prepaid_balance = my_balance_info.get('선충전잔액', 0)
    credit_limit = my_balance_info.get('여신한도', 0)
    credit_used = my_balance_info.get('사용여신액', 0)
    available_credit = credit_limit - credit_used
    total_available = prepaid_balance + available_credit
    
    c1, c2, c3 = st.columns(3)
    c1.metric("✅ 선충전 잔액", f"{prepaid_balance:,.0f}원")
    c2.metric("CHỈNH SỬAクレジット限度額", f"{credit_limit:,.0f}원")
    c3.metric("💳 사용 가능 금액", f"{total_available:,.0f}원")
    st.divider()

    # 품목 선택
    active_items = master_df[master_df['활성'].astype(str).str.upper() == 'Y'].copy()
    active_items['수량'] = 0
    
    st.markdown("##### 📦 발주할 품목을 선택하고 수량을 입력하세요")
    edited_items = st.data_editor(
        active_items[['품목코드', '품목명', '품목규격', '단위', '단가', '수량']],
        num_rows="dynamic",
        use_container_width=True,
        key="item_selector"
    )
    
    selected_items = edited_items[edited_items['수량'] > 0]
    
    st.divider()

    # 장바구니 및 발주 확정
    st.markdown("##### 🛒 장바구니")
    if not selected_items.empty:
        cart_df = selected_items.copy()
        cart_df['단가'] = pd.to_numeric(cart_df['단가'], errors='coerce').fillna(0)
        cart_df['수량'] = pd.to_numeric(cart_df['수량'], errors='coerce').fillna(0)
        cart_df['합계금액'] = cart_df['단가'] * cart_df['수량']
        st.dataframe(cart_df[['품목명', '단위', '단가', '수량', '합계금액']], use_container_width=True)

        total_price = cart_df['합계금액'].sum()
        
        st.markdown(f"<h4 style='text-align: right; color: {THEME['PRIMARY']};'>총 합계 금액: {total_price:,.0f}원</h4>", unsafe_allow_html=True)
        
        if total_price > total_available:
            st.error(f"주문 금액({total_price:,.0f}원)이 사용 가능 금액({total_available:,.0f}원)을 초과합니다.")
        else:
            if st.button("최종 발주 확정", type="primary", use_container_width=True):
                user = st.session_state.auth
                order_time = now_kst_str()
                order_id = f"ORD-{user['user_id']}-{datetime.now(KST).strftime('%y%m%d%H%M%S')}"

                new_orders = []
                for _, row in cart_df.iterrows():
                    price = row['단가']
                    tax_type = master_df[master_df['품목코드'] == row['품목코드']].iloc[0]['과세구분']
                    supply_price = price / 1.1 if tax_type == '과세' else price
                    tax = price - supply_price if tax_type == '과세' else 0
                    
                    new_order = {
                        "주문일시": order_time, "발주번호": order_id, "지점ID": user['user_id'], "지점명": user['name'],
                        "품목코드": row['품목코드'], "품목명": row['품목명'], "단위": row['단위'], "수량": row['수량'],
                        "단가": price, "공급가액": supply_price * row['수량'], "세액": tax * row['수량'], "합계금액": row['합계금액'],
                        "비고": "", "상태": "요청", "처리일시": "", "처리자": "", "반려사유": ""
                    }
                    new_orders.append(new_order)
                
                # 1. 발주 시트 추가
                append_rows_to_sheet(SHEET_NAME_ORDERS, new_orders, ORDERS_COLUMNS)

                # 2. 잔액 업데이트 및 거래내역 기록
                new_prepaid = prepaid_balance
                new_used_credit = credit_used
                if prepaid_balance >= total_price:
                    new_prepaid -= total_price
                else:
                    new_prepaid = 0
                    new_used_credit += (total_price - prepaid_balance)

                update_balance_sheet(user['user_id'], {'선충전잔액': new_prepaid, '사용여신액': new_used_credit})

                transaction = {
                    "일시": order_time, "지점ID": user['user_id'], "지점명": user['name'], "구분": "발주",
                    "내용": f"{cart_df.iloc[0]['품목명']} 등 {len(cart_df)}건", "금액": -total_price,
                    "처리후선충전잔액": new_prepaid, "처리후사용여신액": new_used_credit,
                    "관련발주번호": order_id, "처리자": "시스템"
                }
                append_rows_to_sheet(SHEET_NAME_TRANSACTIONS, [transaction], TRANSACTIONS_COLUMNS)

                st.session_state.success_message = f"발주번호 '{order_id}'로 총 {total_price:,.0f}원의 발주가 성공적으로 완료되었습니다."
                st.rerun()
    else:
        st.info("발주할 품목의 수량을 입력해주세요.")

def page_store_orders_change(my_store_info: pd.DataFrame, master_df: pd.DataFrame):
    st.subheader("🧾 발주 조회")
    user_id = st.session_state.auth['user_id']
    orders_df = load_data(SHEET_NAME_ORDERS, ORDERS_COLUMNS)
    my_orders = orders_df[orders_df['지점ID'] == user_id]
    
    if my_orders.empty:
        st.info("아직 발주 내역이 없습니다.")
        return

    st.dataframe(my_orders, use_container_width=True)

def page_store_balance(charge_requests_df: pd.DataFrame, my_balance_info: pd.Series):
    st.subheader("💰 결제 관리")
    user = st.session_state.auth
    
    # 잔액 현황
    if not my_balance_info.empty:
        prepaid = my_balance_info.get('선충전잔액', 0)
        limit = my_balance_info.get('여신한도', 0)
        used = my_balance_info.get('사용여신액', 0)
        c1, c2, c3 = st.columns(3)
        c1.metric("선충전 잔액", f"{prepaid:,.0f}원")
        c2.metric("여신 한도", f"{limit:,.0f}원")
        c3.metric("사용 여신액", f"{used:,.0f}원")
    else:
        st.warning("잔액 정보를 표시할 수 없습니다.")
        
    st.divider()

    # 충전 요청
    with st.expander("➕ 선충전금 충전 요청하기"):
        with st.form("charge_request_form"):
            depositor_name = st.text_input("입금자명")
            charge_amount = st.number_input("입금액", min_value=0, step=10000)
            submitted = st.form_submit_button("충전 요청", use_container_width=True)
            
            if submitted:
                if not depositor_name or charge_amount <= 0:
                    st.error("입금자명과 입금액을 정확히 입력해주세요.")
                else:
                    new_req = {
                        "요청일시": now_kst_str(), "지점ID": user['user_id'], "지점명": user['name'],
                        "입금자명": depositor_name, "입금액": charge_amount, "종류": "충전",
                        "상태": "요청", "처리사유": ""
                    }
                    if append_rows_to_sheet(SHEET_NAME_CHARGE_REQ, [new_req], CHARGE_REQ_COLUMNS):
                        st.success(f"{charge_amount:,.0f}원의 충전 요청이 완료되었습니다. 관리자 확인 후 잔액에 반영됩니다.")

    # 충전 요청 내역
    st.markdown("##### 🧾 충전 요청 내역")
    my_reqs = charge_requests_df[charge_requests_df['지점ID'] == user['user_id']]
    st.dataframe(my_reqs, use_container_width=True)

    st.divider()

    # 거래 내역
    st.markdown("##### 📑 전체 거래 내역")
    transactions_df = load_data(SHEET_NAME_TRANSACTIONS, TRANSACTIONS_COLUMNS)
    my_trans = transactions_df[transactions_df['지점ID'] == user['user_id']]
    st.dataframe(my_trans, use_container_width=True)

def page_store_documents(my_store_info: pd.DataFrame):
    st.subheader("📑 증빙서류 다운로드")
    
    st.info("기간을 선택하여 해당 기간의 발주 내역을 엑셀 파일로 다운로드할 수 있습니다.")
    
    today = date.today()
    c1, c2 = st.columns(2)
    start_date = c1.date_input("조회 시작일", today.replace(day=1))
    end_date = c2.date_input("조회 종료일", today)
    
    if start_date > end_date:
        st.error("시작일은 종료일보다 이전이어야 합니다.")
        return

    if st.button("발주 내역 조회 및 다운로드", type="primary"):
        orders_df = load_data(SHEET_NAME_ORDERS, ORDERS_COLUMNS)
        user_id = st.session_state.auth['user_id']
        my_orders = orders_df[orders_df['지점ID'] == user_id]
        
        # 날짜 필터링
        my_orders['주문일시_dt'] = pd.to_datetime(my_orders['주문일시']).dt.date
        mask = (my_orders['주문일시_dt'] >= start_date) & (my_orders['주문일시_dt'] <= end_date)
        filtered_orders = my_orders.loc[mask].drop(columns=['주문일시_dt'])
        
        if filtered_orders.empty:
            st.warning("선택하신 기간에 해당하는 발주 내역이 없습니다.")
        else:
            st.dataframe(filtered_orders, use_container_width=True)
            
            excel_data = to_excel(filtered_orders)
            st.download_button(
                label="📁 엑셀 파일 다운로드",
                data=excel_data,
                file_name=f"{st.session_state.auth['name']}_발주내역_{start_date}_to_{end_date}.xlsx",
                mime="application/vnd.ms-excel"
            )

def page_store_master_view(master_df: pd.DataFrame):
    st.subheader("🏷️ 품목 단가 조회")
    active_items = master_df[master_df['활성'].astype(str).str.upper() == 'Y'].copy()
    
    search_term = st.text_input("품목명으로 검색", placeholder="예: 삼겹살")
    
    if search_term:
        display_df = active_items[active_items['품목명'].str.contains(search_term, na=False)]
    else:
        display_df = active_items
        
    st.dataframe(display_df[['품목명', '품목규격', '단위', '단가']], use_container_width=True)


# =============================================================================
# 6) 관리자(Admin) 페이지 함수
# =============================================================================
def page_admin_unified_management(orders_df: pd.DataFrame, store_info_df: pd.DataFrame, master_df: pd.DataFrame):
    st.subheader("📋 발주요청 조회 및 처리")
    
    # 필터링 옵션
    store_list = ['전체'] + store_info_df['지점명'].tolist()
    status_list = ['전체', '요청', '완료', '반려']
    
    c1, c2, c3 = st.columns(3)
    selected_store = c1.selectbox("지점 선택", store_list)
    selected_status = c2.selectbox("상태 선택", status_list)
    selected_date = c3.date_input("날짜 선택", date.today())
    
    # 데이터 필터링
    filtered_df = orders_df.copy()
    filtered_df['주문일시_date'] = pd.to_datetime(filtered_df['주문일시']).dt.date
    
    if selected_store != '전체':
        filtered_df = filtered_df[filtered_df['지점명'] == selected_store]
    if selected_status != '전체':
        filtered_df = filtered_df[filtered_df['상태'] == selected_status]
    filtered_df = filtered_df[filtered_df['주문일시_date'] == selected_date]
    
    st.markdown("##### 📝 발주 내역")
    st.caption("'상태' 및 '반려사유'를 수정한 후 하단의 '변경사항 저장' 버튼을 누르세요.")
    
    edited_df = st.data_editor(
        filtered_df,
        use_container_width=True,
        disabled=["주문일시", "발주번호", "지점ID", "지점명", "품목코드", "품목명", "단위", "수량", "단가", "공급가액", "세액", "합계금액", "비고", "처리일시", "처리자"],
        key="order_editor"
    )
    
    if st.button("변경사항 저장", type="primary"):
        # 원본 orders_df와 edited_df 비교
        changes = []
        original_indexed = orders_df.set_index('발주번호')
        edited_indexed = edited_df.set_index('발주번호')

        for order_id, row in edited_indexed.iterrows():
            original_row = original_indexed.loc[order_id].iloc[0] # 중복 발주번호 처리
            if original_row['상태'] != row['상태'] or original_row['반려사유'] != row['반려사유']:
                changes.append({'발주번호': order_id, '새 상태': row['상태'], '반려사유': row['반려사유'], '기존 상태': original_row['상태']})

        if not changes:
            st.warning("변경된 내용이 없습니다.")
        else:
            all_orders_df = load_data(SHEET_NAME_ORDERS, ORDERS_COLUMNS)
            balance_df = load_data(SHEET_NAME_BALANCE, BALANCE_COLUMNS)
            
            for change in changes:
                # 1. 발주 시트 업데이트
                mask = all_orders_df['발주번호'] == change['발주번호']
                all_orders_df.loc[mask, '상태'] = change['새 상태']
                all_orders_df.loc[mask, '반려사유'] = change['반려사유']
                all_orders_df.loc[mask, '처리일시'] = now_kst_str()
                all_orders_df.loc[mask, '처리자'] = st.session_state.auth['name']

                # 2. '요청' -> '반려' 시 잔액 복원
                if change['기존 상태'] == '요청' and change['새 상태'] == '반려':
                    rejected_orders = all_orders_df[mask]
                    store_id = rejected_orders.iloc[0]['지점ID']
                    store_name = rejected_orders.iloc[0]['지점명']
                    refund_amount = rejected_orders['합계금액'].sum()
                    
                    store_balance = balance_df[balance_df['지점ID'] == store_id].iloc[0]
                    
                    # 사용여신액에서 먼저 차감, 나머지는 선충전잔액으로
                    new_used_credit = store_balance['사용여신액']
                    new_prepaid = store_balance['선충전잔액']
                    
                    if new_used_credit >= refund_amount:
                        new_used_credit -= refund_amount
                    else:
                        new_prepaid += (refund_amount - new_used_credit)
                        new_used_credit = 0
                        
                    update_balance_sheet(store_id, {'선충전잔액': new_prepaid, '사용여신액': new_used_credit})
                    
                    # 거래내역 기록
                    transaction = {
                        "일시": now_kst_str(), "지점ID": store_id, "지점명": store_name, "구분": "발주반려",
                        "내용": f"발주번호 {change['발주번호']} 반려", "금액": refund_amount,
                        "처리후선충전잔액": new_prepaid, "처리후사용여신액": new_used_credit,
                        "관련발주번호": change['발주번호'], "처리자": st.session_state.auth['name']
                    }
                    append_rows_to_sheet(SHEET_NAME_TRANSACTIONS, [transaction], TRANSACTIONS_COLUMNS)

            save_df_to_sheet(SHEET_NAME_ORDERS, all_orders_df)
            st.session_state.success_message = "발주 상태 변경사항이 성공적으로 저장되었습니다."
            st.rerun()

def page_admin_sales_inquiry(master_df: pd.DataFrame):
    st.subheader("📈 매출 조회")
    orders_df = load_data(SHEET_NAME_ORDERS, ORDERS_COLUMNS)
    completed_orders = orders_df[orders_df['상태'] == '완료'].copy()

    if completed_orders.empty:
        st.info("매출 데이터가 없습니다.")
        return

    # 날짜 범위 선택
    today = datetime.now(KST).date()
    c1, c2 = st.columns(2)
    start_date = c1.date_input("조회 시작일", today - timedelta(days=30))
    end_date = c2.date_input("조회 종료일", today)

    # 데이터 필터링
    completed_orders['주문일시_dt'] = pd.to_datetime(completed_orders['주문일시']).dt.date
    mask = (completed_orders['주문일시_dt'] >= start_date) & (completed_orders['주문일시_dt'] <= end_date)
    filtered_sales = completed_orders.loc[mask]

    if filtered_sales.empty:
        st.warning("선택된 기간에 해당하는 매출 데이터가 없습니다.")
        return

    # 분석
    total_sales = filtered_sales['합계금액'].sum()
    st.metric("총 매출액 (선택 기간)", f"{total_sales:,.0f}원")

    st.markdown("##### 📊 품목별 매출 현황")
    sales_by_item = filtered_sales.groupby('품목명')['합계금액'].sum().sort_values(ascending=False)
    st.dataframe(sales_by_item)
    st.bar_chart(sales_by_item)

    st.markdown("##### 🏢 지점별 매출 현황")
    sales_by_store = filtered_sales.groupby('지점명')['합계금액'].sum().sort_values(ascending=False)
    st.dataframe(sales_by_store)
    st.bar_chart(sales_by_store)


def page_admin_balance_management(store_info_df: pd.DataFrame):
    st.subheader("💰 결제 관리")
    
    # 1. 충전 요청 처리
    st.markdown("##### 📨 충전 요청 승인/반려")
    charge_requests_df = load_data(SHEET_NAME_CHARGE_REQ, CHARGE_REQ_COLUMNS)
    pending_reqs = charge_requests_df[charge_requests_df['상태'] == '요청'].copy()
    
    if pending_reqs.empty:
        st.info("처리할 충전 요청이 없습니다.")
    else:
        edited_reqs = st.data_editor(
            pending_reqs,
            column_config={
                "상태": st.column_config.SelectboxColumn("상태", options=["요청", "완료", "반려"]),
            },
            disabled=[col for col in CHARGE_REQ_COLUMNS if col not in ['상태', '처리사유']],
            use_container_width=True,
            key="charge_req_editor"
        )
        
        if st.button("충전 요청 처리 저장", type="primary"):
            full_req_df = load_data(SHEET_NAME_CHARGE_REQ, CHARGE_REQ_COLUMNS)
            
            for index, row in edited_reqs.iterrows():
                original_row = pending_reqs.loc[index]
                if row['상태'] != original_row['상태']:
                    # Update the request status in the full dataframe
                    req_time = original_row['요청일시']
                    store_id_val = original_row['지점ID']
                    
                    mask = (full_req_df['요청일시'] == req_time) & (full_req_df['지점ID'] == store_id_val)
                    full_req_df.loc[mask, '상태'] = row['상태']
                    full_req_df.loc[mask, '처리사유'] = row['처리사유']

                    # If approved, update balance and add transaction
                    if row['상태'] == '완료':
                        balance_df = load_data(SHEET_NAME_BALANCE, BALANCE_COLUMNS)
                        store_balance = balance_df[balance_df['지점ID'] == store_id_val].iloc[0]
                        
                        new_prepaid = store_balance['선충전잔액'] + row['입금액']
                        update_balance_sheet(store_id_val, {'선충전잔액': new_prepaid})
                        
                        transaction = {
                            "일시": now_kst_str(), "지점ID": store_id_val, "지점명": row['지점명'], "구분": "충전",
                            "내용": f"{row['입금자명']} 충전 승인", "금액": row['입금액'],
                            "처리후선충전잔액": new_prepaid, "처리후사용여신액": store_balance['사용여신액'],
                            "관련발주번호": "", "처리자": st.session_state.auth['name']
                        }
                        append_rows_to_sheet(SHEET_NAME_TRANSACTIONS, [transaction], TRANSACTIONS_COLUMNS)

            save_df_to_sheet(SHEET_NAME_CHARGE_REQ, full_req_df)
            st.success("충전 요청 처리가 완료되었습니다.")
            st.rerun()

    st.divider()
    
    # 2. 지점별 잔액/여신 현황
    st.markdown("##### 🏦 지점별 잔액 및 여신한도 관리")
    balance_df = load_data(SHEET_NAME_BALANCE, BALANCE_COLUMNS)
    edited_balance_df = st.data_editor(
        balance_df,
        disabled=['지점ID', '지점명', '선충전잔액', '사용여신액'],
        use_container_width=True,
        key="balance_editor"
    )
    
    if st.button("여신한도 변경사항 저장", type="primary"):
        if save_df_to_sheet(SHEET_NAME_BALANCE, edited_balance_df):
            st.success("지점별 여신한도가 성공적으로 업데이트되었습니다.")
            st.rerun()

def page_admin_documents(store_info_df: pd.DataFrame):
    st.subheader("📑 증빙서류 다운로드")
    
    store_list = store_info_df['지점명'].tolist()
    selected_store = st.selectbox("지점 선택", store_list)
    
    today = date.today()
    c1, c2 = st.columns(2)
    start_date = c1.date_input("조회 시작일", today.replace(day=1))
    end_date = c2.date_input("조회 종료일", today)

    if st.button("거래명세서 다운로드", type="primary"):
        store_id = store_info_df[store_info_df['지점명'] == selected_store].iloc[0]['지점ID']
        orders_df = load_data(SHEET_NAME_ORDERS, ORDERS_COLUMNS)
        store_orders = orders_df[orders_df['지점ID'] == store_id]
        
        # 날짜 필터링
        store_orders['주문일시_dt'] = pd.to_datetime(store_orders['주문일시']).dt.date
        mask = (store_orders['주문일시_dt'] >= start_date) & (store_orders['주문일시_dt'] <= end_date)
        filtered_orders = store_orders.loc[mask].drop(columns=['주문일시_dt'])
        
        if filtered_orders.empty:
            st.warning("선택된 지점의 해당 기간 발주 내역이 없습니다.")
        else:
            excel_data = to_excel(filtered_orders)
            st.download_button(
                label="📁 엑셀 파일 다운로드",
                data=excel_data,
                file_name=f"{selected_store}_거래명세서_{start_date}_to_{end_date}.xlsx",
                mime="application/vnd.ms-excel"
            )

# =============================================================================
# 7) 관리자 페이지 - 설정
# =============================================================================
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
            if save_df_to_sheet(SHEET_NAME_STORES, edited_store_df):
                st.success("지점 마스터가 성공적으로 저장되었습니다. 변경사항은 다음 로그인부터 적용됩니다.")
                st.rerun()

# =============================================================================
# 8) 라우팅
# =============================================================================
if __name__ == "__main__":
    init_session_state()
    if not require_login():
        st.stop()
        
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
        store_info_for_display = store_info_df_raw[store_info_df_raw['지점명'] != '대전 가공장'].copy()
        
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
