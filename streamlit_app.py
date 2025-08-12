# -*- coding: utf-8 -*-
# =============================================================================
# Streamlit 식자재 발주 시스템 (무료 지향 · Google Sheets 연동 샘플)
# - 사용자 역할: 지점(발주), 본사/공장(조회·출고처리)
# - 데이터 저장: Google Sheets (대체: 로컬 CSV 백업)
# - 인증: st.secrets["users"]에 계정/권한/비번 저장 (예시는 아래 주석 참고)
# - 제품 마스터: Google Sheets의 "상품마스터" 시트에서 관리
# - 발주 기록: Google Sheets의 "발주" 시트로 적재
# =============================================================================
# 요구 패키지 (requirements.txt 예시)
# streamlit
# pandas
# gspread
# google-auth
# gspread_dataframe
# =============================================================================

import os
from pathlib import Path
import uuid
from datetime import datetime, date, timedelta
from typing import Dict, Any, List

import pandas as pd
import streamlit as st

# === (선택) Google Sheets 연동 ===
try:
    import gspread
    from google.oauth2 import service_account
    from gspread_dataframe import set_with_dataframe, get_as_dataframe
    GDRIVE_AVAILABLE = True
except Exception:
    GDRIVE_AVAILABLE = False

# =============================================================================
# 0) 페이지 설정 & 공통 스타일
# =============================================================================
st.set_page_config(
    page_title="발주 시스템",
    page_icon="📦",
    layout="wide"
)

COMMON_CSS = """
<style>
    .small {font-size: 12px; color: #777;}
    .tag {display:inline-block; padding:2px 8px; border-radius:999px; border:1px solid #ddd; margin-right:6px}
    .ok {background:#ecfff3; border-color:#cde9d7}
    .warn {background:#fff7e6; border-color:#ffe1a8}
    .danger {background:#fff0f0; border-color:#ffd6d6}
</style>
"""
st.markdown(COMMON_CSS, unsafe_allow_html=True)

# =============================================================================
# 1) 설정 (시트 이름 등)
# =============================================================================
SHEET_NAME_MASTER = "상품마스터"
SHEET_NAME_ORDERS = "발주"
# 로컬 백업 경로 (Streamlit Cloud 호환). 컨테이너가 재시작되면 삭제될 수 있으므로 임시 용도입니다.
LOCAL_BACKUP_DIR = Path("local_backup")
LOCAL_BACKUP_DIR.mkdir(exist_ok=True)
LOCAL_BACKUP_ORDERS = str(LOCAL_BACKUP_DIR / "orders_backup.csv")

# =============================================================================
# 2) 인증/권한
# - st.secrets에 다음과 같이 저장해서 사용합니다.
# [users]
# # 지점 예시
# jeondae.password = "store_pw"
# jeondae.name = "전대점"
# jeondae.role = "store"
# chungdae.password = "store_pw2"
# chungdae.name = "충대점"
# chungdae.role = "store"
# # 본사/공장 계정
# hq.password = "admin_pw"
# hq.name = "본사(공장)"
# hq.role = "admin"
# 
# [google]
# type="service_account"
# project_id="..."
# private_key_id="..."
# private_key="-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
# client_email="...@...gserviceaccount.com"
# client_id="..."
# token_uri="https://oauth2.googleapis.com/token"
# =============================================================================

def load_users_from_secrets() -> pd.DataFrame:
    rows = []
    users = st.secrets.get("users", {})
    # st.secrets["users"]는 section-like 접근이 어려울 수 있으므로 keys를 탐색
    # Streamlit의 TOML 구조상 users.jeondae.password 형식일 수 있음 -> st.secrets["users"]["jeondae"]["password"] 형태 지원
    try:
        for uid, payload in users.items():
            if isinstance(payload, dict):
                rows.append({
                    "user_id": uid,
                    "password": payload.get("password", ""),
                    "name": payload.get("name", uid),
                    "role": payload.get("role", "store"),
                })
    except Exception:
        pass
    return pd.DataFrame(rows)

USERS_DF = load_users_from_secrets()

# =============================================================================
# 3) Google Sheets 클라이언트
# =============================================================================
@st.cache_resource(show_spinner=False)
def get_gs_client():
    if not GDRIVE_AVAILABLE:
        return None
    try:
        creds_dict = st.secrets.get("google", None)
        if not creds_dict:
            return None
        creds = service_account.Credentials.from_service_account_info(
            creds_dict,
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ],
        )
        return gspread.authorize(creds)
    except Exception as e:
        st.warning(f"Google 인증 실패: {e}")
        return None

# 스프레드시트 열기 (문서 키 또는 URL 필요)
SPREADSHEET_KEY = st.secrets.get("SPREADSHEET_KEY", "")  # st.secrets에 키 저장

@st.cache_resource(show_spinner=False)
def open_spreadsheet():
    gc = get_gs_client()
    if not gc or not SPREADSHEET_KEY:
        return None
    try:
        sh = gc.open_by_key(SPREADSHEET_KEY)
        return sh
    except Exception as e:
        st.error(f"스프레드시트 열기 실패: {e}")
        return None

# =============================================================================
# 4) 데이터 I/O
# =============================================================================
@st.cache_data(ttl=60)
def load_master_df() -> pd.DataFrame:
    """상품 마스터 로드. 컬럼 예시: [품목코드, 품목명, 단위, 활성, 기본리드타임, 안전재고]"""
    sh = open_spreadsheet()
    if sh:
        try:
            ws = sh.worksheet(SHEET_NAME_MASTER)
            df = pd.DataFrame(ws.get_all_records())
            if not df.empty:
                # 활성 필터 (있으면)
                if "활성" in df.columns:
                    df = df[df["활성"].astype(str).str.lower().isin(["1", "true", "y", "yes"]) | (df["활성"].isna())]
            return df
        except Exception as e:
            st.warning(f"상품마스터 로딩 실패(시트): {e}")
    # 시트가 없을 때 샘플 제공
    sample = pd.DataFrame([
        {"품목코드": "P001", "품목명": "오이", "단위": "EA", "기본리드타임": 1, "안전재고": 10},
        {"품목코드": "P002", "품목명": "대파", "단위": "KG", "기본리드타임": 1, "안전재고": 5},
        {"품목코드": "P003", "품목명": "간장", "단위": "L", "기본리드타임": 2, "안전재고": 2},
    ])
    return sample

@st.cache_data(ttl=30)
def load_orders_df() -> pd.DataFrame:
    """발주 데이터 로드"""
    sh = open_spreadsheet()
    if sh:
        try:
            ws = sh.worksheet(SHEET_NAME_ORDERS)
            df = pd.DataFrame(ws.get_all_records())
            return df
        except Exception as e:
            st.warning(f"발주 로딩 실패(시트): {e}")
    # 로컬 백업
    if os.path.exists(LOCAL_BACKUP_ORDERS):
        try:
            return pd.read_csv(LOCAL_BACKUP_ORDERS, encoding="utf-8-sig")
        except Exception:
            pass
    # 빈 스키마 반환
    return pd.DataFrame(columns=[
        "주문일시","발주번호","지점ID","지점명","품목코드","품목명","단위","수량","비고","상태","처리일시","처리자"
    ])


def _ensure_orders_sheet_columns(ws):
    """시트가 비어 있을 때 헤더 생성"""
    records = ws.get_all_values()
    if len(records) == 0:
        header = [
            "주문일시","발주번호","지점ID","지점명","품목코드","품목명","단위","수량","비고","상태","처리일시","처리자"
        ]
        ws.append_row(header)


def append_orders(rows: List[Dict[str, Any]]):
    """발주 데이터 append. 시트 실패시 CSV 백업"""
    sh = open_spreadsheet()
    if sh:
        try:
            ws = sh.worksheet(SHEET_NAME_ORDERS)
        except Exception:
            # 시트 없으면 생성
            try:
                ws = sh.add_worksheet(title=SHEET_NAME_ORDERS, rows=1000, cols=20)
            except Exception as e:
                st.error(f"발주 시트 생성 실패: {e}")
                ws = None
        if ws is not None:
            try:
                _ensure_orders_sheet_columns(ws)
                # append_row 반복보다 batch update가 빠르지만 간단하게 처리
                for r in rows:
                    ws.append_row([
                        r.get("주문일시",""), r.get("발주번호",""), r.get("지점ID",""), r.get("지점명",""),
                        r.get("품목코드",""), r.get("품목명",""), r.get("단위",""), r.get("수량",0),
                        r.get("비고",""), r.get("상태","접수"), r.get("처리일시",""), r.get("처리자","")
                    ])
                load_orders_df.clear()  # 캐시 무효화
                return True
            except Exception as e:
                st.warning(f"시트 기록 실패: {e}")
    # 로컬 백업
    df_old = pd.DataFrame()
    if os.path.exists(LOCAL_BACKUP_ORDERS):
        try:
            df_old = pd.read_csv(LOCAL_BACKUP_ORDERS, encoding="utf-8-sig")
        except Exception:
            df_old = pd.DataFrame()
    df_new = pd.DataFrame(rows)
    df_all = pd.concat([df_old, df_new], ignore_index=True)
    # 백업 디렉토리 보장 후 저장
    parent = os.path.dirname(LOCAL_BACKUP_ORDERS)
    if parent and not os.path.exists(parent):
        os.makedirs(parent, exist_ok=True)
    df_all.to_csv(LOCAL_BACKUP_ORDERS, index=False, encoding="utf-8-sig")
    load_orders_df.clear()
    return True


def update_order_status(selected_ids: List[str], new_status: str, handler: str):
    """선택된 발주번호들의 상태를 변경"""
    sh = open_spreadsheet()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if sh:
        try:
            ws = sh.worksheet(SHEET_NAME_ORDERS)
            data = ws.get_all_records()
            df = pd.DataFrame(data)
            if df.empty:
                st.warning("변경할 데이터가 없습니다.")
                return False
            mask = df["발주번호"].astype(str).isin([str(x) for x in selected_ids])
            df.loc[mask, "상태"] = new_status
            df.loc[mask, "처리일시"] = now
            df.loc[mask, "처리자"] = handler
            # 덮어쓰기
            ws.clear()
            header = df.columns.tolist()
            ws.append_row(header)
            for _, row in df.iterrows():
                ws.append_row(list(row.values))
            load_orders_df.clear()
            return True
        except Exception as e:
            st.warning(f"상태 변경 실패(시트): {e}")
    # 로컬 백업에서 변경
    if os.path.exists(LOCAL_BACKUP_ORDERS):
        try:
            df = pd.read_csv(LOCAL_BACKUP_ORDERS, encoding="utf-8-sig")
            mask = df["발주번호"].astype(str).isin([str(x) for x in selected_ids])
            df.loc[mask, "상태"] = new_status
            df.loc[mask, "처리일시"] = now
            df.loc[mask, "처리자"] = handler
            df.to_csv(LOCAL_BACKUP_ORDERS, index=False, encoding="utf-8-sig")
            load_orders_df.clear()
            return True
        except Exception as e:
            st.error(f"상태 변경 실패(백업): {e}")
    return False

# =============================================================================
# 5) 유틸
# =============================================================================

def gen_order_id() -> str:
    # 날짜 + 6자리 난수 기반 (중복 방지)
    return datetime.now().strftime("%Y%m%d-") + uuid.uuid4().hex[:6].upper()


def require_login():
    st.session_state.setdefault("auth", {})
    if st.session_state["auth"].get("login", False):
        return True

    st.header("🔐 로그인")
    if USERS_DF.empty:
        st.info("secrets에 사용자 계정을 등록하면 로그인 기능이 활성화됩니다. (지금은 게스트 접근)")
        if st.button("게스트로 계속"):
            st.session_state["auth"] = {"login": True, "user_id": "guest", "name": "게스트", "role": "admin"}
            st.rerun()
        return False

    user_ids = USERS_DF["user_id"].tolist()
    col1, col2 = st.columns([2, 1])
    with col1:
        uid = st.selectbox("아이디", user_ids)
    with col2:
        pwd = st.text_input("비밀번호", type="password")
    if st.button("로그인"):
        row = USERS_DF[USERS_DF["user_id"] == uid].iloc[0]
        if str(pwd) == str(row["password"]):
            st.session_state["auth"] = {
                "login": True,
                "user_id": uid,
                "name": row["name"],
                "role": row["role"],
            }
            st.success(f"{row['name']}님 환영합니다!")
            st.rerun()
        else:
            st.error("아이디 또는 비밀번호가 올바르지 않습니다.")
    return False


# =============================================================================
# 6) 지점(발주) 화면
# =============================================================================

def page_store(master_df: pd.DataFrame):
    st.subheader("🛒 발주 등록")

    # 제품 검색/선택
    left, right = st.columns([2, 1])
    with left:
        keyword = st.text_input("품목 검색(이름/코드)")
    with right:
        st.write("")
    df_view = master_df.copy()
    if keyword:
        kw = keyword.strip().lower()
        df_view = df_view[df_view.apply(lambda r: kw in str(r.get("품목명","")) .lower() or kw in str(r.get("품목코드","")) .lower(), axis=1)]

    st.dataframe(
        df_view[[c for c in ["품목코드","품목명","단위","기본리드타임","안전재고"] if c in df_view.columns]].reset_index(drop=True),
        use_container_width=True,
        height=240
    )

    st.markdown("---")
    st.markdown("**발주 품목 추가**")
    c1, c2, c3, c4 = st.columns([2, 1, 1, 2])
    with c1:
        sel = st.selectbox("품목 선택", [f"{row['품목명']} ({row['품목코드']})" for _, row in master_df.iterrows()])
    with c2:
        qty = st.number_input("수량", min_value=0.0, step=1.0, value=0.0)
    with c3:
        unit = None
        # 선택된 품목의 기본 단위 자동 표시
        try:
            code = sel.split("(")[-1].strip(")")
            unit = master_df.loc[master_df["품목코드"] == code, "단위"].iloc[0]
        except Exception:
            unit = "EA"
        st.text_input("단위", value=str(unit), disabled=True)
    with c4:
        memo = st.text_input("비고", value="")

    st.session_state.setdefault("cart", [])
    if st.button("장바구니 추가"):
        if qty and qty > 0:
            code = sel.split("(")[-1].strip(")")
            name = sel.split("(")[0].strip()
            st.session_state["cart"].append({
                "품목코드": code,
                "품목명": name,
                "단위": unit,
                "수량": qty,
                "비고": memo
            })
            st.success(f"[ {name} ] {qty} {unit} 추가")
        else:
            st.warning("수량을 입력해 주세요.")

    if st.session_state["cart"]:
        st.markdown("---")
        st.markdown("**장바구니**")
        cart_df = pd.DataFrame(st.session_state["cart"]).reset_index().rename(columns={"index":"#"})
        edited = st.data_editor(cart_df, num_rows="dynamic", use_container_width=True)
        # 편집 반영
        st.session_state["cart"] = edited.drop(columns=["#"], errors="ignore").to_dict(orient="records")

        cols = st.columns([1, 1, 2])
        with cols[0]:
            if st.button("전체 비우기", type="secondary"):
                st.session_state["cart"] = []
                st.rerun()
        with cols[1]:
            if st.button("선택 삭제"):
                # data_editor에서 행 선택 기능이 없어 임시로 수량0을 삭제 규칙으로 사용
                st.session_state["cart"] = [r for r in st.session_state["cart"] if float(r.get("수량",0)) > 0]
                st.rerun()
        with cols[2]:
            pass

        st.markdown("---")
        if st.button("📦 발주 제출", type="primary"):
            user = st.session_state["auth"]
            order_id = gen_order_id()
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            rows = []
            for r in st.session_state["cart"]:
                rows.append({
                    "주문일시": now,
                    "발주번호": order_id,
                    "지점ID": user.get("user_id"),
                    "지점명": user.get("name"),
                    "품목코드": r.get("품목코드"),
                    "품목명": r.get("품목명"),
                    "단위": r.get("단위"),
                    "수량": r.get("수량"),
                    "비고": r.get("비고",""),
                    "상태": "접수",
                    "처리일시": "",
                    "처리자": ""
                })
            ok = append_orders(rows)
            if ok:
                st.success(f"발주가 접수되었습니다. 발주번호: {order_id}")
                st.session_state["cart"] = []
            else:
                st.error("발주 저장에 실패했습니다.")


# =============================================================================
# 7) 본사/공장(관리) 화면
# =============================================================================

def page_admin(master_df: pd.DataFrame):
    st.subheader("📋 발주 조회/처리")
    df = load_orders_df()

    c1, c2, c3, c4 = st.columns([1,1,1,2])
    with c1:
        dt_from = st.date_input("시작일", value=date.today() - timedelta(days=3))
    with c2:
        dt_to = st.date_input("종료일", value=date.today())
    with c3:
        stores = ["(전체)"] + sorted(df["지점명"].dropna().unique().tolist()) if not df.empty else ["(전체)"]
        store = st.selectbox("지점", stores)
    with c4:
        status = st.multiselect("상태", ["접수","출고완료"], default=["접수","출고완료"]) if not df.empty else []

    if not df.empty:
        # 필터링
        def _to_dt(s):
            try:
                return pd.to_datetime(s)
            except Exception:
                return pd.NaT
        df["주문일시_dt"] = df["주문일시"].apply(_to_dt)
        mask = (df["주문일시_dt"].dt.date >= dt_from) & (df["주문일시_dt"].dt.date <= dt_to)
        if store != "(전체)":
            mask &= (df["지점명"] == store)
        if status:
            mask &= df["상태"].isin(status)
        dfv = df[mask].copy().sort_values(["주문일시","발주번호"])  # 보기 정렬
    else:
        dfv = df.copy()

    st.caption(f"조회 건수: {len(dfv):,}건")
    st.dataframe(dfv, use_container_width=True, height=420)

    csv = dfv.to_csv(index=False).encode("utf-8-sig")
    st.download_button("CSV 다운로드", data=csv, file_name="orders.csv", mime="text/csv")

    st.markdown("---")
    st.markdown("**출고 처리**")
    if not dfv.empty:
        # 같은 발주번호 단위로 처리하도록 선택 박스 제공
        order_ids = sorted(dfv["발주번호"].unique().tolist())
        sel_ids = st.multiselect("발주번호 선택", order_ids)
        if st.button("선택 발주 출고완료 처리"):
            if sel_ids:
                ok = update_order_status(sel_ids, new_status="출고완료", handler=st.session_state["auth"].get("name","관리자"))
                if ok:
                    st.success("출고완료 처리되었습니다.")
                    st.rerun()
                else:
                    st.error("상태 변경 실패")
            else:
                st.warning("발주번호를 선택하세요.")

    st.markdown("---")
    with st.expander("설정/운영 팁"):
        st.markdown(
            "- 상품마스터 시트에서 품목을 추가/비활성화하면 즉시 반영됩니다.\n"
            "- 발주 시트 컬럼은 [주문일시, 발주번호, 지점ID, 지점명, 품목코드, 품목명, 단위, 수량, 비고, 상태, 처리일시, 처리자] 고정입니다.\n"
            "- 수량=0인 행은 장바구니에서 자동 무시하거나 삭제하세요.\n"
        )


# =============================================================================
# 8) 라우팅
# =============================================================================

if __name__ == "__main__":
    st.title("📦 식자재 발주 시스템")
    st.caption("무료 지향 · Google Sheets 연동 샘플")

    if not require_login():
        st.stop()

    user = st.session_state["auth"]
    role = user.get("role", "store")

    master = load_master_df()

    st.markdown("""
    <div class="small">
    ※ 본 샘플은 <span class="tag ok">Google Sheets</span> 중심으로 동작합니다. 연결이 없으면 로컬 CSV 백업만 저장됩니다.<br/>
    ※ 운영 이전에 <b>SPREADSHEET_KEY</b>와 <b>secrets.users</b>, <b>secrets.google</b>을 반드시 설정하세요.
    </div>
    """, unsafe_allow_html=True)

    tabs = st.tabs(["발주", "관리자"])

    with tabs[0]:
        page_store(master)
    with tabs[1]:
        if role == "admin":
            page_admin(master)
        else:
            st.info("관리자 권한이 필요합니다.")
