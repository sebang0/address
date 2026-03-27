import streamlit as st
import pandas as pd
import requests
import urllib.parse
from io import BytesIO
import datetime

# --- API 이력 기록용 함수 ---
def log_api(api_type, query, status_result):
    if 'api_log' not in st.session_state:
        st.session_state.api_log = []
    
    st.session_state.api_log.append({
        "호출 시간": datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3],
        "API 종류": api_type,
        "요청 파라미터(검색어/좌표)": str(query),
        "응답 결과": status_result
    })

# --- 1. 카카오 API 통신 함수 ---
def get_address_info(address, api_key):
    if pd.isna(address) or str(address).strip() == "":
        return "입력값 없음", "", None, None, []

    headers = {"Authorization": f"KakaoAK {api_key}"}
    addr_str = str(address).strip()
    
    url = "https://dapi.kakao.com/v2/local/search/address.json"
    res = requests.get(url, headers=headers, params={"query": addr_str})
    
    if res.status_code == 200:
        docs = res.json().get('documents', [])
        if len(docs) == 1:
            log_api("주소 검색", addr_str, "성공(1건)")
            return "정상", docs[0]['address_name'], docs[0]['x'], docs[0]['y'], []
        elif len(docs) > 1:
            log_api("주소 검색", addr_str, f"다중검색({len(docs)}건)")
            cands = [doc['address_name'] for doc in docs][:3]
            return "모호함(다중검색)", docs[0]['address_name'], docs[0]['x'], docs[0]['y'], cands
    
    log_api("주소 검색", addr_str, "실패(결과없음)")
            
    parts = addr_str.split()
    if len(parts) >= 2:
        fallback_query = " ".join(parts[:3])
        kw_url = "https://dapi.kakao.com/v2/local/search/keyword.json"
        fb_res = requests.get(kw_url, headers=headers, params={"query": fallback_query})
        
        if fb_res.status_code == 200 and fb_res.json().get('documents'):
            log_api("유사 검색(폴백)", fallback_query, "성공")
            fb_docs = fb_res.json()['documents']
            cands = []
            for doc in fb_docs:
                if 'address_name' in doc: cands.append(doc['address_name'])
            cands = list(set(filter(None, cands)))[:3]
            if cands:
                return "검색 불가(유사추천)", "", None, None, cands
        else:
            log_api("유사 검색(폴백)", fallback_query, "실패")
                
    return "검색 불가", "", None, None, []

def get_coords_only(address, api_key):
    if not address or address in ["확인 불가", "보정 제외"]: return None, None
    headers = {"Authorization": f"KakaoAK {api_key}"}
    
    res = requests.get("https://dapi.kakao.com/v2/local/search/address.json", headers=headers, params={"query": address})
    if res.status_code == 200 and res.json().get('documents'):
        log_api("재조회(주소)", address, "성공")
        return res.json()['documents'][0]['x'], res.json()['documents'][0]['y']
        
    kw_res = requests.get("https://dapi.kakao.com/v2/local/search/keyword.json", headers=headers, params={"query": address})
    if kw_res.status_code == 200 and kw_res.json().get('documents'):
        log_api("재조회(키워드)", address, "성공")
        return kw_res.json()['documents'][0]['x'], kw_res.json()['documents'][0]['y']
    
    log_api("재조회(전체)", address, "완전 실패")
    return None, None

def get_driving_distance(start_coord, end_coord, api_key):
    url = "https://apis-navi.kakaomobility.com/v1/directions"
    headers = {"Authorization": f"KakaoAK {api_key}", "Content-Type": "application/json"}
    query_str = f"출발({start_coord[0]},{start_coord[1]}) -> 도착({end_coord[0]},{end_coord[1]})"
    params = {
        "origin": f"{start_coord[0]},{start_coord[1]}",
        "destination": f"{end_coord[0]},{end_coord[1]}",
        "priority": "RECOMMEND"
    }
    try:
        res = requests.get(url, headers=headers, params=params)
        if res.status_code == 200 and res.json().get('routes'):
            dist = round(res.json()['routes'][0]['summary']['distance'] / 1000, 2)
            log_api("길찾기(내비)", query_str, f"성공({dist}km)")
            return dist
        else:
            log_api("길찾기(내비)", query_str, "실패(경로없음)")
    except Exception as e:
        log_api("길찾기(내비)", query_str, f"에러: {e}")
        pass
    return 0.0

# --- 2. 웹 화면 UI ---
st.set_page_config(page_title="물류 구간 거리 산출기", layout="wide")
st.title("🚚 구간 주소 정정 및 주행 거리 산출")

if 'step' not in st.session_state: st.session_state.step = 0
if 'api_log' not in st.session_state: st.session_state.api_log = []

def reset_step():
    st.session_state.step = 0
    st.session_state.api_log = [] 

st.sidebar.header("1. 기본 설정")
api_key = st.sidebar.text_input("카카오 REST API 키", type="password", on_change=reset_step)
uploaded_file = st.sidebar.file_uploader("구간 엑셀 파일 업로드", type=["xlsx"], on_change=reset_step)

if uploaded_file and api_key:
    df_raw = pd.read_excel(uploaded_file)
    cols = df_raw.columns.tolist()
    
    col1, col2 = st.columns(2)
    with col1: col_start = st.selectbox("출발지 주소가 있는 열:", cols, on_change=reset_step)
    with col2: col_end = st.selectbox("도착지 주소가 있는 열:", cols, on_change=reset_step)

    st.markdown("---")

    # [Step 1] 주소 정정 실행
    if st.session_state.step == 0:
        st.info("💡 위에서 열을 선택한 뒤, 아래 **[실행]** 버튼을 눌러야 검증이 시작됩니다.")
        if st.button("🚀 1단계: 고유 주소 검증 및 매핑 시작", type="primary"):
            st.session_state.api_log = [] 
            unique_addrs = pd.unique(df_raw[[col_start, col_end]].astype(str).values.ravel('K'))
            unique_addrs = [addr for addr in unique_addrs if addr.strip() and addr.lower() != 'nan']
            
            prog = st.progress(0)
            status_text = st.empty()
            
            mapping_data = []
            candidates_dict = {}
            status_map = {}
            
            for i, raw_addr in enumerate(unique_addrs):
                status_text.text(f"고유 주소 검증 중... ({i+1}/{len(unique_addrs)})")
                status, correct_addr, x, y, cands = get_address_info(raw_addr, api_key)
                
                mapping_data.append({
                    "원본 주소": raw_addr,
                    "정정된 주소": correct_addr if correct_addr else "",
                    "상태": status,
                    "제외여부": False, 
                    "X": x, "Y": y
                })
                candidates_dict[i] = cands
                status_map[raw_addr] = status
                prog.progress((i + 1) / len(unique_addrs))
            
            st.session_state.mapping_df = pd.DataFrame(mapping_data)
            st.session_state.candidates = candidates_dict
            st.session_state.status_map = status_map
            st.session_state.df_raw = df_raw
            st.session_state.step = 1
            
            prog.empty()
            status_text.empty()
            st.rerun()

    # [Step 1.5] 정정 결과 요약표 및 수동 보정 UI
    if st.session_state.step >= 1:
        st.subheader("📋 1단계 완료: 전체 구간 주소 검증 결과")
        
        total_unique = len(st.session_state.mapping_df)
        normal_cnt = len(st.session_state.mapping_df[st.session_state.mapping_df['상태'] == '정상'])
        error_cnt = total_unique - normal_cnt
        
        m1, m2, m3 = st.columns(3)
        m1.metric("📌 총 고유 주소", f"{total_unique}개")
        m2.metric("✅ 정상 인식 완료", f"{normal_cnt}개")
        m3.metric("⚠️ 정정 필요", f"{error_cnt}개")
        
        st.write("▼ 전체 데이터 검증 결과표")
        df_summary = st.session_state.df_raw.copy()
        df_summary['출발지 검증'] = df_summary[col_start].astype(str).map(lambda x: st.session_state.status_map.get(x, "확인 불가"))
        df_summary['도착지 검증'] = df_summary[col_end].astype(str).map(lambda x: st.session_state.status_map.get(x, "확인 불가"))
        
        def highlight_verification(row):
            styles = [''] * len(row)
            for i, col_name in enumerate(row.index):
                if col_name in ['출발지 검증', '도착지 검증']:
                    val = str(row[col_name])
                    if '검색 불가' in val: styles[i] = 'background-color: #ffe6e6; color: #cc0000; font-weight: bold'
                    elif '추천' in val or '모호함' in val: styles[i] = 'background-color: #fff0b3; color: #996600; font-weight: bold'
                    elif '정상' in val: styles[i] = 'background-color: #e6ffe6; color: #006600'
            return styles

        st.dataframe(df_summary[[col_start, col_end, '출발지 검증', '도착지 검증']].style.apply(highlight_verification, axis=1), use_container_width=True, height=250)
        
        st.markdown("---")
        st.subheader("🛠️ 수정이 필요한 주소 보정")
        
        df_edit = st.session_state.mapping_df.copy()
        error_indices = df_edit[df_edit['상태'] != '정상'].index
        
        if len(error_indices) == 0:
            st.success("🎉 모든 주소가 정상적으로 검색됩니다!")
        else:
            st.write("위 표에서 '정상'이 아닌 주소들을 올바른 후보군으로 변경하거나 직접 입력해 주세요. (거리를 산출할 필요가 없는 주소는 '제외'를 체크하세요.)")
            for idx in error_indices:
                orig_addr = df_edit.loc[idx, '원본 주소']
                status = df_edit.loc[idx, '상태']
                
                color = "red" if "검색 불가" in status else "orange" if "모호함" in status else "green"
                st.markdown(f"**행 {idx+2}** ➡️ 상태: :{color}[**{status}**]")
                
                col_addr, col_btn, col_chk = st.columns([6, 2, 2])
                with col_addr:
                    st.code(orig_addr, language="plaintext")
                with col_btn:
                    search_url = f"https://www.google.com/search?q={urllib.parse.quote(orig_addr)}"
                    st.link_button("🔍 구글 검색", search_url, use_container_width=True)
                with col_chk:
                    is_excluded = st.checkbox("🚫 변환 제외", key=f"exclude_{idx}")
                    df_edit.loc[idx, '제외여부'] = is_excluded
                
                if not is_excluded:
                    cands = st.session_state.candidates.get(idx, [])
                    if cands:
                        options = cands + ["직접 입력"]
                        choice = st.radio("변경할 주소 선택", options, key=f"radio_{idx}", horizontal=True, label_visibility="collapsed")
                        
                        if choice == "직접 입력":
                            new_addr = st.text_input("새 주소 입력", key=f"text_{idx}", label_visibility="collapsed")
                            if new_addr: df_edit.loc[idx, '정정된 주소'] = new_addr
                        else:
                            df_edit.loc[idx, '정정된 주소'] = choice
                    else:
                        new_addr = st.text_input("새 주소 직접 입력", key=f"text_{idx}", placeholder="추천 후보가 없습니다. 올바른 주소를 직접 입력해주세요.")
                        if new_addr: df_edit.loc[idx, '정정된 주소'] = new_addr
                st.markdown("---")
        
        if st.session_state.step == 1:
            col_btn1, col_btn2 = st.columns([3, 1])
            with col_btn1:
                if st.button("✅ 2단계: 보정 완료 및 구간 거리 산출 시작", type="primary", use_container_width=True):
                    st.session_state.mapping_df = df_edit 
                    st.session_state.step = 2
                    st.rerun()
            with col_btn2:
                if st.button("🔄 초기화", use_container_width=True):
                    reset_step()
                    st.rerun()

    # [Step 2] 최종 거리 산출 및 통계
    if st.session_state.step == 2:
        st.subheader("📊 최종 구간 거리 산출 결과")
        
        if 'final_df' not in st.session_state:
            prog = st.progress(0)
            status_text = st.empty()
            
            addr_dict = {}
            for _, row in st.session_state.mapping_df.iterrows():
                # 제외 체크된 항목은 명시적으로 상태 기록
                if row.get('제외여부', False):
                    addr_dict[row['원본 주소']] = ("보정 제외", None, None)
                    continue
                    
                final_addr = row['정정된 주소']
                x, y = row['X'], row['Y']
                
                if final_addr and final_addr != "확인 불가" and pd.isna(x):
                    x, y = get_coords_only(final_addr, api_key)
                addr_dict[row['원본 주소']] = (final_addr, x, y)
            
            results = []
            links = []
            df_target = st.session_state.df_raw
            total_rows = len(df_target)
            
            for i, row in df_target.iterrows():
                status_text.text(f"차량 주행 거리 산출 중... ({i+1}/{total_rows})")
                s_raw, e_raw = str(row[col_start]), str(row[col_end])
                
                s_info = addr_dict.get(s_raw, ("확인 불가", None, None))
                e_info = addr_dict.get(e_raw, ("확인 불가", None, None))
                
                dist = 0.0
                note = ""
                
                # [신규 추가] 산출 비고(Note) 결정 로직
                if s_info[0] == "보정 제외" or e_info[0] == "보정 제외":
                    note = "🚫 사용자 제외"
                elif s_info[1] and e_info[1]: 
                    dist = get_driving_distance((s_info[1], s_info[2]), (e_info[1], e_info[2]), api_key)
                    note = "✅ 정상 산출" if dist > 0 else "⚠️ 경로 탐색 불가"
                else:
                    note = "⚠️ 좌표 확인 불가"
                
                s_final = s_info[0] if s_info[0] else "확인 불가"
                e_final = e_info[0] if e_info[0] else "확인 불가"
                
                # 원본 열(s_raw, e_raw)은 df_target에 그대로 유지되고, 새로운 열만 우측에 추가됩니다.
                results.append({
                    "정정_출발지": s_final,
                    "정정_도착지": e_final,
                    "주행거리(km)": dist,
                    "산출 비고": note
                })
                
                if dist > 0:
                    links.append(f"https://m.map.kakao.com/scheme/route?sp={s_info[2]},{s_info[1]}&ep={e_info[2]},{e_info[1]}&by=car")
                else:
                    links.append(None)
                    
                prog.progress((i + 1) / total_rows)
            
            df_res = pd.concat([df_target.reset_index(drop=True), pd.DataFrame(results)], axis=1)
            df_res['카카오맵 자동길찾기'] = links
            st.session_state.final_df = df_res
            
            prog.empty()
            status_text.empty()

        final_df = st.session_state.final_df
        total_cnt = len(final_df)
        success_cnt = len(final_df[final_df['산출 비고'] == '✅ 정상 산출'])
        fail_cnt = total_cnt - success_cnt
        
        st.success(f"📌 **산출 완료!** 총 **{total_cnt}건** 중 정상 산출 **{success_cnt}건** / 실패(또는 제외) **{fail_cnt}건**")

        # 결과표 스타일링 (제외된 항목은 회색으로 흐리게, 에러는 빨간색)
        def highlight_result(row):
            if '사용자 제외' in row['산출 비고']:
                return ['background-color: #f2f2f2; color: #a6a6a6'] * len(row)
            elif row['주행거리(km)'] == 0.0:
                return ['background-color: #ffe6e6; color: #cc0000'] * len(row)
            return [''] * len(row)
            
        st.dataframe(
            final_df.style.apply(highlight_result, axis=1),
            column_config={
                "카카오맵 자동길찾기": st.column_config.LinkColumn("카카오맵 자동길찾기", display_text="🚀 즉시 경로 확인")
            },
            use_container_width=True
        )

        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            final_df.to_excel(writer, index=False, sheet_name='거리산출결과')
        
        st.download_button(
            label="💾 최종 엑셀 파일 다운로드 (.xlsx)",
            data=output.getvalue(),
            file_name="route_distances.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            type="primary"
        )
        
    if st.session_state.api_log:
        st.markdown("---")
        with st.expander("🛠️ 시스템 디버그: API 호출 이력 보기", expanded=False):
            st.caption(f"이번 세션에서 총 **{len(st.session_state.api_log)}번**의 API 호출이 발생했습니다.")
            st.dataframe(pd.DataFrame(st.session_state.api_log), use_container_width=True)