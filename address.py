import streamlit as st
import pandas as pd
import requests
import urllib.parse
from io import BytesIO
import datetime
import threading
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import plotly.express as px

log_lock = threading.Lock()

# --- 톤급-차종 마스터 데이터 정의 ---
MASTER_DATA = {
    "톤급": [1, 1.4, 2, 2.5, 3, 3.5, 4, 4.5, 5, 6, 7, 7.5, 8, 9, 9.5, 10, 
           11, 12, 13, 14, 14.5, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27],
    "차종": [1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 
           4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 5, 5, 5]
}
DF_MASTER = pd.DataFrame(MASTER_DATA)
TONNAGE_MAP = dict(zip(DF_MASTER['톤급'], DF_MASTER['차종']))

def parse_tonnage(val):
    cleaned = re.sub(r'[^0-9.]', '', str(val))
    if not cleaned: return None
    try: return float(cleaned)
    except ValueError: return None

# --- 1. 카카오 API 통신 함수 ---
def get_address_info(address, api_key, session):
    logs = []
    def add_log(phase, api_type, query, status_result):
        logs.append({"단계": phase, "호출 시간": datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3], "API 종류": api_type, "요청 파라미터": str(query), "응답 결과": status_result})

    if pd.isna(address) or str(address).strip() == "":
        return "입력값 없음", "", None, None, [], logs

    headers = {"Authorization": f"KakaoAK {api_key}"}
    addr_str = str(address).strip()
    
    url = "https://dapi.kakao.com/v2/local/search/address.json"
    res = session.get(url, headers=headers, params={"query": addr_str}) 
    
    if res.status_code == 200:
        docs = res.json().get('documents', [])
        if len(docs) == 1:
            add_log("1단계", "주소 검색", addr_str, "성공(1건)")
            return "정상", docs[0]['address_name'], docs[0]['x'], docs[0]['y'], [], logs
        elif len(docs) > 1:
            add_log("1단계", "주소 검색", addr_str, f"다중검색({len(docs)}건)")
            cands = [doc['address_name'] for doc in docs][:3]
            return "모호함(다중검색)", docs[0]['address_name'], docs[0]['x'], docs[0]['y'], cands, logs
    
    add_log("1단계", "주소 검색", addr_str, "실패(결과없음)")
            
    parts = addr_str.split()
    if len(parts) >= 2:
        fallback_query = " ".join(parts[:3])
        kw_url = "https://dapi.kakao.com/v2/local/search/keyword.json"
        fb_res = session.get(kw_url, headers=headers, params={"query": fallback_query})
        
        if fb_res.status_code == 200 and fb_res.json().get('documents'):
            add_log("1단계", "유사 검색(폴백)", fallback_query, "성공")
            fb_docs = fb_res.json()['documents']
            cands = []
            for doc in fb_docs:
                if 'address_name' in doc: cands.append(doc['address_name'])
            cands = list(set(filter(None, cands)))[:3]
            if cands:
                return "검색 불가(유사추천)", "", None, None, cands, logs
        else:
            add_log("1단계", "유사 검색(폴백)", fallback_query, "실패")
                
    return "검색 불가", "", None, None, [], logs

def get_coords_only(address, api_key, session):
    logs = []
    def add_log(api_type, query, status_result):
        logs.append({"단계": "2단계", "호출 시간": datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3], "API 종류": api_type, "요청 파라미터": str(query), "응답 결과": status_result})

    if not address or address in ["확인 불가", "보정 제외"]: return None, None, logs
    headers = {"Authorization": f"KakaoAK {api_key}"}
    
    res = session.get("https://dapi.kakao.com/v2/local/search/address.json", headers=headers, params={"query": address})
    if res.status_code == 200 and res.json().get('documents'):
        add_log("재조회(주소)", address, "성공")
        return res.json()['documents'][0]['x'], res.json()['documents'][0]['y'], logs
        
    kw_res = session.get("https://dapi.kakao.com/v2/local/search/keyword.json", headers=headers, params={"query": address})
    if kw_res.status_code == 200 and kw_res.json().get('documents'):
        add_log("재조회(키워드)", address, "성공")
        return kw_res.json()['documents'][0]['x'], kw_res.json()['documents'][0]['y'], logs
    
    add_log("재조회(전체)", address, "완전 실패")
    return None, None, logs

def get_driving_distance(start_coord, end_coord, api_key, session, car_type):
    logs = []
    query_str = f"출발({start_coord[0]},{start_coord[1]}) -> 도착({end_coord[0]},{end_coord[1]}), {car_type}종"
    def add_log(api_type, status_result):
        logs.append({"단계": "2단계", "호출 시간": datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3], "API 종류": api_type, "요청 파라미터": query_str, "응답 결과": status_result})

    url = "https://apis-navi.kakaomobility.com/v1/directions"
    headers = {"Authorization": f"KakaoAK {api_key}", "Content-Type": "application/json"}
    params = {
        "origin": f"{start_coord[0]},{start_coord[1]}",
        "destination": f"{end_coord[0]},{end_coord[1]}",
        "priority": "RECOMMEND",
        "car_type": car_type
    }
    try:
        res = session.get(url, headers=headers, params=params)
        if res.status_code == 200 and res.json().get('routes'):
            dist = round(res.json()['routes'][0]['summary']['distance'] / 1000, 2)
            add_log("길찾기", f"성공({dist}km)")
            return dist, logs
        else:
            add_log("길찾기", "실패(경로없음)")
    except Exception as e:
        add_log("길찾기", f"에러: {e}")
    return 0.0, logs


# --- 2. 웹 화면 UI ---
st.set_page_config(page_title="구간 주소 정정 및 주행 거리 산출", layout="wide")
st.title("🚚 구간 주소 정정 및 톤급별 주행 거리 산출")

if 'step' not in st.session_state: st.session_state.step = 0
if 'api_log' not in st.session_state: st.session_state.api_log = []

def reset_step():
    st.session_state.step = 0
    st.session_state.api_log = [] 

# [수정] 여백 축소를 위해 header 대신 markdown과 간소화된 텍스트 사용
st.sidebar.markdown("**1. 기본 설정**")
api_key = st.sidebar.text_input("카카오 REST API 키", type="password", on_change=reset_step)
uploaded_file = st.sidebar.file_uploader("구간 엑셀 파일 업로드", type=["xlsx"], on_change=reset_step)

with st.sidebar.expander("📊 톤급-차종 마스터표 보기", expanded=False):
    st.dataframe(DF_MASTER, hide_index=True, use_container_width=True)

MAX_WORKERS = 5 
df_raw = None
col_start = None
col_end = None
col_ton = None

if uploaded_file and api_key:
    df_raw = pd.read_excel(uploaded_file)
    cols = df_raw.columns.tolist()
    
    st.sidebar.markdown("<br>**2. 열(Column) 매핑**", unsafe_allow_html=True)
    # [수정] 수직 공간 절약을 위해 열 선택 위젯을 가로로 압축 배치
    c1, c2 = st.sidebar.columns(2)
    col_ton = c1.selectbox("톤급", cols, on_change=reset_step)
    col_start = c2.selectbox("출발지", cols, on_change=reset_step)
    col_end = st.sidebar.selectbox("도착지", cols, on_change=reset_step)
    
    st.sidebar.markdown("<br>**3. 실행 및 진행 상황**", unsafe_allow_html=True)

if df_raw is not None and st.session_state.step == 0:
    st.info("👈 좌측 사이드바에서 설정을 확인한 후 **[🚀 1단계: 검증 시작]** 버튼을 눌러주세요.")
    
    unmapped_tons = set()
    for val in df_raw[col_ton].dropna():
        parsed_val = parse_tonnage(val)
        if parsed_val is None or parsed_val not in TONNAGE_MAP:
            unmapped_tons.add(val)
            
    if unmapped_tons:
        st.warning(f"⚠️ **업데이트되지 않은 신규 톤급(또는 문자) {len(unmapped_tons)}건의 차종은 1종으로 반영됩니다.** \n(대상: {', '.join(map(str, list(unmapped_tons)[:10]))} 등)")
    else:
        st.success("✅ **모든 톤급 데이터가 마스터표에 정상적으로 매핑되었습니다.**")

if df_raw is not None:
    # [Step 1] 주소 정정 실행
    if st.session_state.step == 0:
        if st.sidebar.button("🚀 1단계: 검증 시작", type="primary", use_container_width=True):
            st.session_state.api_log = [] 
            unique_addrs = pd.unique(df_raw[[col_start, col_end]].astype(str).values.ravel('K'))
            unique_addrs = [addr for addr in unique_addrs if addr.strip() and addr.lower() != 'nan']
            
            prog = st.progress(0); status_text = st.empty()
            mapping_data = []; candidates_dict = {}; status_map = {}
            http_session = requests.Session()
            
            status_text.text(f"병렬 처리 중... (0/{len(unique_addrs)})")
            
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_addr = {executor.submit(get_address_info, addr, api_key, http_session): addr for addr in unique_addrs}
                completed_count = 0
                for future in as_completed(future_to_addr):
                    raw_addr = future_to_addr[future]
                    status, correct_addr, x, y, cands, logs = future.result()
                    st.session_state.api_log.extend(logs)
                    
                    mapping_data.append({"원본 주소": raw_addr, "정정된 주소": correct_addr if correct_addr else "", "상태": status, "제외여부": False, "X": x, "Y": y})
                    
                    idx = unique_addrs.index(raw_addr)
                    candidates_dict[idx] = cands
                    status_map[raw_addr] = status
                    
                    completed_count += 1
                    prog.progress(completed_count / len(unique_addrs))
                    status_text.text(f"병렬 처리 완료... ({completed_count}/{len(unique_addrs)})")
            
            mapping_df = pd.DataFrame(mapping_data)
            mapping_df['원본 주소'] = pd.Categorical(mapping_df['원본 주소'], categories=unique_addrs, ordered=True)
            st.session_state.mapping_df = mapping_df.sort_values('원본 주소').reset_index(drop=True)
            st.session_state.candidates = candidates_dict
            st.session_state.status_map = status_map
            st.session_state.df_raw = df_raw
            st.session_state.step = 1
            prog.empty(); status_text.empty()
            st.rerun()

    # [Step 1.5] 정정 결과 요약표 & 수동 보정 UI
    if st.session_state.step >= 1:
        st.subheader("📋 1단계 완료: 전체 구간 주소 검증 결과")
        total_unique = len(st.session_state.mapping_df)
        normal_cnt = len(st.session_state.mapping_df[st.session_state.mapping_df['상태'] == '정상'])
        error_cnt = total_unique - normal_cnt
        
        dash_col, chart_col = st.columns([4, 1.2]) 
        with dash_col:
            m1, m2, m3 = st.columns(3)
            m1.metric("📌 총 고유 주소", f"{total_unique}개")
            m2.metric("✅ 정상 인식 완료", f"{normal_cnt}개")
            m3.metric("⚠️ 정정 필요", f"{error_cnt}개")
            
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
            
            st.dataframe(
                df_summary[[col_start, col_end, '출발지 검증', '도착지 검증']].style.apply(highlight_verification, axis=1), 
                column_config={col_start: st.column_config.TextColumn(width=150), col_end: st.column_config.TextColumn(width=150)},
                use_container_width=True, height=200
            )

        with chart_col:
            fig1 = px.pie(values=[normal_cnt, error_cnt], names=['정상', '정정 필요'], color=['정상', '정정 필요'], color_discrete_map={'정상': '#28a745', '정정 필요': '#ffc107'}, hole=0.4)
            fig1.update_traces(textposition='inside', textinfo='percent+label')
            fig1.update_layout(margin=dict(t=10, b=0, l=0, r=0), height=200, showlegend=False)
            st.plotly_chart(fig1, use_container_width=True)
            
        st.markdown("---")
        st.subheader("🛠️ 수정이 필요한 주소 보정")
        df_edit = st.session_state.mapping_df.copy()
        error_indices = df_edit[df_edit['상태'] != '정상'].index
        completed_tasks = 0
        completion_tracker = {}
        
        if len(error_indices) == 0:
            st.success("🎉 모든 주소가 정상적으로 검색됩니다!")
        else:
            if st.session_state.step == 1: st.info("💡 선택 또는 직접 입력이 모두 완료되어야만 최종 구간 거리를 산출할 수 있습니다.")
            else: st.info("💡 1단계에서 진행한 주소 보정 내역입니다.")

            for idx in error_indices:
                title_ph = st.empty()
                orig_addr = df_edit.loc[idx, '원본 주소']
                status = df_edit.loc[idx, '상태']
                cands = st.session_state.candidates.get(idx, [])
                
                col_addr, col_btn, col_chk = st.columns([6, 2, 2])
                with col_addr: st.code(orig_addr, language="plaintext")
                with col_btn:
                    st.link_button("🔍 구글 검색", f"https://www.google.com/search?q={urllib.parse.quote(orig_addr)}", use_container_width=True)
                with col_chk:
                    is_excluded = st.checkbox("🚫 변환 제외", key=f"exclude_{idx}", value=df_edit.loc[idx, '제외여부'])
                    df_edit.loc[idx, '제외여부'] = is_excluded
                
                is_completed = False
                if is_excluded:
                    is_completed = True
                else:
                    options = ["선택 안 함"] + cands + ["직접 입력"] if cands else ["선택 안 함", "직접 입력"]
                    current_val = df_edit.loc[idx, '정정된 주소']
                    default_idx = options.index(current_val) if current_val and current_val in options else (len(options)-1 if current_val and current_val != "선택 안 함" else 0)
                        
                    choice = st.radio("변경할 주소 선택", options, key=f"radio_{idx}", index=default_idx, horizontal=True, label_visibility="collapsed")
                    
                    if choice == "직접 입력":
                        new_addr = st.text_input("새 주소 입력", value=current_val if default_idx == len(options)-1 else "", key=f"text_{idx}", label_visibility="collapsed")
                        if new_addr.strip(): df_edit.loc[idx, '정정된 주소'] = new_addr; is_completed = True
                    elif choice != "선택 안 함":
                        df_edit.loc[idx, '정정된 주소'] = choice; is_completed = True
                
                if is_completed: completed_tasks += 1
                completion_tracker[idx] = is_completed
                
                color = "red" if "검색 불가" in status else "orange" if "모호함" in status else "green"
                mark = "✅ [완료]" if is_completed else "⏳ [대기]"
                title_ph.markdown(f"**행 {idx+2}** {mark} ➡️ 상태: :{color}[**{status}**]")
                st.markdown("---")
        
        # [수정] 사이드바 2단계 실행 영역 간소화
        if st.session_state.step == 1:
            if len(error_indices) == 0:
                st.sidebar.success("🎉 모든 주소 정상")
                if st.sidebar.button("✅ 2단계: 거리 산출 시작", type="primary", use_container_width=True):
                    st.session_state.mapping_df = df_edit 
                    st.session_state.step = 2
                    st.rerun()
            else:
                progress_val = completed_tasks / len(error_indices)
                st.sidebar.progress(progress_val)
                st.sidebar.caption(f"**진행률:** {completed_tasks} / {len(error_indices)}")
                
                uncompleted_rows = [str(idx+2) for idx, comp in completion_tracker.items() if not comp]
                if uncompleted_rows:
                    st.sidebar.caption("미완료 행: " + ", ".join(uncompleted_rows[:8]) + ("..." if len(uncompleted_rows)>8 else ""))
                
                if completed_tasks == len(error_indices):
                    st.sidebar.success("🎉 보정 완료")
                    if st.sidebar.button("✅ 2단계: 거리 산출 시작", type="primary", use_container_width=True):
                        st.session_state.mapping_df = df_edit 
                        st.session_state.step = 2
                        st.rerun()
                else:
                    st.sidebar.button("✅ 산출 대기중", disabled=True, use_container_width=True)

    # [Step 2] 최종 거리 산출
    if st.session_state.step == 2:
        st.markdown("---")
        st.subheader("📊 2단계 완료: 차량 톤급 기반 주행 거리 산출 결과")
        
        # 버튼을 상단에 콤팩트하게 배치
        if st.sidebar.button("🔄 새로 시작", use_container_width=True):
            reset_step()
            st.rerun()
            
        if 'final_df' not in st.session_state:
            prog = st.progress(0); status_text = st.empty()
            http_session = requests.Session()
            
            addr_dict = {}
            for _, row in st.session_state.mapping_df.iterrows():
                if row.get('제외여부', False):
                    addr_dict[row['원본 주소']] = ("보정 제외", None, None)
                    continue
                final_addr = row['정정된 주소']
                x, y = row['X'], row['Y']
                if final_addr and final_addr != "확인 불가" and pd.isna(x):
                    x, y, logs = get_coords_only(final_addr, api_key, http_session)
                    st.session_state.api_log.extend(logs)
                addr_dict[row['원본 주소']] = (final_addr, x, y)
            
            df_target = st.session_state.df_raw.copy()
            out_cols = ["적용 차종", "정정_출발지", "정정_도착지", "주행거리(km)", "산출 비고", "카카오맵 자동길찾기"]
            df_target_clean = df_target.drop(columns=[c for c in out_cols if c in df_target.columns]).reset_index(drop=True)
            total_rows = len(df_target_clean)
            
            routes_to_fetch = set()
            for i, row in df_target_clean.iterrows():
                s_raw, e_raw = str(row[col_start]), str(row[col_end])
                ton_val = row[col_ton]
                parsed_ton = parse_tonnage(ton_val)
                car_type = TONNAGE_MAP.get(parsed_ton, 1) if parsed_ton is not None else 1
                
                s_info = addr_dict.get(s_raw, ("확인 불가", None, None))
                e_info = addr_dict.get(e_raw, ("확인 불가", None, None))
                
                if s_info[1] and e_info[1] and s_info[0] != "보정 제외" and e_info[0] != "보정 제외":
                    start_coord = (s_info[1], s_info[2])
                    end_coord = (e_info[1], e_info[2])
                    if start_coord != end_coord:
                        routes_to_fetch.add((start_coord, end_coord, car_type))

            route_cache = {}
            if routes_to_fetch:
                status_text.text(f"필요한 {len(routes_to_fetch)}개 고유 경로(차종별) 동시 산출 중...")
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    future_to_route = {executor.submit(get_driving_distance, s, e, api_key, http_session, c_type): (s, e, c_type) for s, e, c_type in routes_to_fetch}
                    for future in as_completed(future_to_route):
                        route_tuple = future_to_route[future]
                        dist, logs = future.result()
                        st.session_state.api_log.extend(logs) 
                        route_cache[route_tuple] = dist
            
            status_text.text("최종 데이터 병합 중...")
            results = []
            links = []
            
            for i, row in df_target_clean.iterrows():
                s_raw, e_raw = str(row[col_start]), str(row[col_end])
                ton_val = row[col_ton]
                parsed_ton = parse_tonnage(ton_val)
                car_type = TONNAGE_MAP.get(parsed_ton, 1) if parsed_ton is not None else 1
                    
                s_info = addr_dict.get(s_raw, ("확인 불가", None, None))
                e_info = addr_dict.get(e_raw, ("확인 불가", None, None))
                
                dist = 0.0
                note = ""
                
                if s_info[0] == "보정 제외" or e_info[0] == "보정 제외":
                    note = "🚫 사용자 제외"
                elif s_info[1] and e_info[1]: 
                    start_coord = (s_info[1], s_info[2])
                    end_coord = (e_info[1], e_info[2])
                    
                    if start_coord == end_coord:
                        note = "✅ 정상 산출(동일 위치)"
                    else:
                        dist = route_cache.get((start_coord, end_coord, car_type), 0.0)
                        note = "✅ 정상 산출" if dist > 0 else "⚠️ 경로 탐색 불가"
                else:
                    note = "⚠️ 좌표 확인 불가"
                
                s_final = s_info[0] if s_info[0] else "확인 불가"
                e_final = e_info[0] if e_info[0] else "확인 불가"
                
                # [확인사항] 적용 차종 열 명시 (웹 UI와 엑셀 모두 동일 반영)
                results.append({
                    "적용 차종": f"{car_type}종",
                    "정정_출발지": s_final, 
                    "정정_도착지": e_final, 
                    "주행거리(km)": dist, 
                    "산출 비고": note
                })
                
                if s_info[1] and e_info[1] and "보정 제외" not in [s_info[0], e_info[0]]:
                    kakao_cartype = 1 if car_type == 1 else (4 if car_type in [4,5] else 2) 
                    links.append(f"https://m.map.kakao.com/scheme/route?sp={s_info[2]},{s_info[1]}&ep={e_info[2]},{e_info[1]}&by=car&carType={kakao_cartype}")
                else:
                    links.append(None)
                    
                prog.progress((i + 1) / total_rows)
            
            df_res = pd.concat([df_target_clean, pd.DataFrame(results)], axis=1)
            df_res['카카오맵 자동길찾기'] = links
            st.session_state.final_df = df_res
            
            prog.empty(); status_text.empty()

        final_df = st.session_state.final_df
        total_cnt = len(final_df)
        success_cnt = len(final_df[final_df['산출 비고'].str.contains('✅ 정상 산출', na=False)])
        fail_cnt = total_cnt - success_cnt
        
        msg_col, pie_col = st.columns([4, 1.2])
        with msg_col:
            st.success(f"📌 **산출 완료!** 전체 **{total_cnt}건** 중 정상 산출 **{success_cnt}건** / 실패(제외 포함) **{fail_cnt}건**")
            
        with pie_col:
            fig2 = px.pie(values=[success_cnt, fail_cnt], names=['정상 산출', '실패/제외'], color=['정상 산출', '실패/제외'], color_discrete_map={'정상 산출': '#0d6efd', '실패/제외': '#dc3545'}, hole=0.4)
            fig2.update_traces(textposition='inside', textinfo='percent+label')
            fig2.update_layout(margin=dict(t=10, b=10, l=0, r=0), height=150, showlegend=False)
            st.plotly_chart(fig2, use_container_width=True)

        def highlight_result(row):
            note = str(row.get('산출 비고', ''))
            if '사용자 제외' in note: return ['background-color: #f2f2f2; color: #a6a6a6'] * len(row)
            elif '⚠️' in note: return ['background-color: #ffe6e6; color: #cc0000'] * len(row)
            return [''] * len(row)
            
        # 웹 UI 결과표에서 '적용 차종' 열이 렌더링됩니다.
        st.dataframe(
            final_df.style.apply(highlight_result, axis=1), 
            column_config={
                col_start: st.column_config.TextColumn(width=120),
                col_end: st.column_config.TextColumn(width=120),
                "적용 차종": st.column_config.TextColumn(width=80), 
                "정정_출발지": st.column_config.TextColumn(width=120),
                "정정_도착지": st.column_config.TextColumn(width=120),
                "카카오맵 자동길찾기": st.column_config.LinkColumn("카카오맵 자동길찾기", display_text="🚀 즉시 경로 확인")
            }, 
            use_container_width=True
        )

        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            final_df.to_excel(writer, index=False, sheet_name='거리산출결과')
        st.download_button(label="💾 최종 엑셀 파일 다운로드 (.xlsx)", data=output.getvalue(), file_name="route_distances_final.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", type="primary")
        
    # --- 시스템 디버그 (로그 합계 표출) ---
    if st.session_state.api_log:
        st.markdown("---")
        df_log = pd.DataFrame(st.session_state.api_log)
        
        log_step1 = df_log[df_log['단계'] == '1단계']
        log_step2 = df_log[df_log['단계'] == '2단계']
        
        cnt_all = len(df_log)
        cnt_1 = len(log_step1)
        cnt_2 = len(log_step2)
        
        st.subheader(f"🛠️ 시스템 디버그: API 호출 이력 (총 {cnt_all}건 : 1단계 {cnt_1}건 + 2단계 {cnt_2}건)")
        col_log1, col_log2 = st.columns(2)
        
        with col_log1:
            with st.expander(f"📌 1단계 주소 검증 API 이력 ({cnt_1}건)", expanded=False):
                if not log_step1.empty: st.dataframe(log_step1.drop(columns=['단계']), use_container_width=True)
                else: st.info("기록된 이력이 없습니다.")
                    
        with col_log2:
            with st.expander(f"📌 2단계 거리 산출 API 이력 ({cnt_2}건)", expanded=False):
                if not log_step2.empty: st.dataframe(log_step2.drop(columns=['단계']), use_container_width=True)
                else: st.info("기록된 이력이 없습니다.")
