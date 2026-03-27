import streamlit as st
import pandas as pd
import requests
import urllib.parse
from io import BytesIO
import datetime
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import plotly.express as px  # 원형 차트를 위한 라이브러리 추가

# --- 멀티스레딩 환경에서 안전한 로그 기록을 위한 Lock ---
log_lock = threading.Lock()

def log_api(api_type, query, status_result):
    with log_lock:
        if 'api_log' not in st.session_state:
            st.session_state.api_log = []
        st.session_state.api_log.append({
            "호출 시간": datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3],
            "API 종류": api_type,
            "요청 파라미터": str(query),
            "응답 결과": status_result
        })

# --- 1. 카카오 API 통신 함수 ---
def get_address_info(address, api_key, session):
    if pd.isna(address) or str(address).strip() == "":
        return "입력값 없음", "", None, None, []

    headers = {"Authorization": f"KakaoAK {api_key}"}
    addr_str = str(address).strip()
    
    url = "https://dapi.kakao.com/v2/local/search/address.json"
    res = session.get(url, headers=headers, params={"query": addr_str}) 
    
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
        fb_res = session.get(kw_url, headers=headers, params={"query": fallback_query})
        
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

def get_coords_only(address, api_key, session):
    if not address or address in ["확인 불가", "보정 제외"]: return None, None
    headers = {"Authorization": f"KakaoAK {api_key}"}
    
    res = session.get("https://dapi.kakao.com/v2/local/search/address.json", headers=headers, params={"query": address})
    if res.status_code == 200 and res.json().get('documents'):
        log_api("재조회(주소)", address, "성공")
        return res.json()['documents'][0]['x'], res.json()['documents'][0]['y']
        
    kw_res = session.get("https://dapi.kakao.com/v2/local/search/keyword.json", headers=headers, params={"query": address})
    if kw_res.status_code == 200 and kw_res.json().get('documents'):
        log_api("재조회(키워드)", address, "성공")
        return kw_res.json()['documents'][0]['x'], kw_res.json()['documents'][0]['y']
    
    log_api("재조회(전체)", address, "완전 실패")
    return None, None

def get_driving_distance(start_coord, end_coord, api_key, session):
    url = "https://apis-navi.kakaomobility.com/v1/directions"
    headers = {"Authorization": f"KakaoAK {api_key}", "Content-Type": "application/json"}
    query_str = f"출발({start_coord[0]},{start_coord[1]}) -> 도착({end_coord[0]},{end_coord[1]})"
    params = {
        "origin": f"{start_coord[0]},{start_coord[1]}",
        "destination": f"{end_coord[0]},{end_coord[1]}",
        "priority": "RECOMMEND"
    }
    try:
        res = session.get(url, headers=headers, params=params)
        if res.status_code == 200 and res.json().get('routes'):
            dist = round(res.json()['routes'][0]['summary']['distance'] / 1000, 2)
            log_api("길찾기", query_str, f"성공({dist}km)")
            return dist
        else:
            log_api("길찾기", query_str, "실패(경로없음)")
    except Exception as e:
        log_api("길찾기", query_str, f"에러: {e}")
    return 0.0

# --- 2. 웹 화면 UI ---
# 타이틀 요청사항 반영
st.set_page_config(page_title="구간 주소 정정 및 주행 거리 산출", layout="wide")
st.title("🚚 구간 주소 정정 및 주행 거리 산출")

if 'step' not in st.session_state: st.session_state.step = 0
if 'api_log' not in st.session_state: st.session_state.api_log = []

def reset_step():
    st.session_state.step = 0
    st.session_state.api_log = [] 

st.sidebar.header("1. 기본 설정")
api_key = st.sidebar.text_input("카카오 REST API 키", type="password", on_change=reset_step)
uploaded_file = st.sidebar.file_uploader("구간 엑셀 파일 업로드", type=["xlsx"], on_change=reset_step)

MAX_WORKERS = 5 

if uploaded_file and api_key:
    df_raw = pd.read_excel(uploaded_file)
    cols = df_raw.columns.tolist()
    
    col1, col2 = st.columns(2)
    with col1: col_start = st.selectbox("출발지 주소가 있는 열:", cols, on_change=reset_step)
    with col2: col_end = st.selectbox("도착지 주소가 있는 열:", cols, on_change=reset_step)

    st.markdown("---")

    # [Step 1] 주소 정정 실행
    if st.session_state.step == 0:
        st.info("💡 아래 **[실행]** 버튼을 눌러 고유 주소 검증을 시작하세요.")
        if st.button("🚀 1단계: 고유 주소 검증 및 매핑 시작", type="primary"):
            st.session_state.api_log = [] 
            unique_addrs = pd.unique(df_raw[[col_start, col_end]].astype(str).values.ravel('K'))
            unique_addrs = [addr for addr in unique_addrs if addr.strip() and addr.lower() != 'nan']
            
            prog = st.progress(0)
            status_text = st.empty()
            
            mapping_data = []
            candidates_dict = {}
            status_map = {}
            http_session = requests.Session()
            
            status_text.text(f"병렬 처리 중... (0/{len(unique_addrs)})")
            
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_addr = {executor.submit(get_address_info, addr, api_key, http_session): addr for addr in unique_addrs}
                completed_count = 0
                for future in as_completed(future_to_addr):
                    raw_addr = future_to_addr[future]
                    status, correct_addr, x, y, cands = future.result()
                    
                    mapping_data.append({
                        "원본 주소": raw_addr,
                        "정정된 주소": correct_addr if correct_addr else "",
                        "상태": status,
                        "제외여부": False, 
                        "X": x, "Y": y
                    })
                    
                    idx = unique_addrs.index(raw_addr)
                    candidates_dict[idx] = cands
                    status_map[raw_addr] = status
                    
                    completed_count += 1
                    prog.progress(completed_count / len(unique_addrs))
                    status_text.text(f"병렬 처리 완료... ({completed_count}/{len(unique_addrs)})")
            
            mapping_df = pd.DataFrame(mapping_data)
            mapping_df['원본 주소'] = pd.Categorical(mapping_df['원본 주소'], categories=unique_addrs, ordered=True)
            mapping_df = mapping_df.sort_values('원본 주소').reset_index(drop=True)
            
            st.session_state.mapping_df = mapping_df
            st.session_state.candidates = candidates_dict
            st.session_state.status_map = status_map
            st.session_state.df_raw = df_raw
            st.session_state.step = 1
            
            prog.empty()
            status_text.empty()
            st.rerun()

    # [Step 1.5] 정정 결과 요약표 & 수동 보정 UI
    if st.session_state.step >= 1:
        st.subheader("📋 1단계 완료: 전체 구간 주소 검증 결과")
        
        total_unique = len(st.session_state.mapping_df)
        normal_cnt = len(st.session_state.mapping_df[st.session_state.mapping_df['상태'] == '정상'])
        error_cnt = total_unique - normal_cnt
        
        # [신규] 대시보드와 원형 차트 좌우 분리 배치
        dash_col, chart_col = st.columns([1.5, 1])
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
            st.dataframe(df_summary[[col_start, col_end, '출발지 검증', '도착지 검증']].style.apply(highlight_verification, axis=1), use_container_width=True, height=200)

        with chart_col:
            # 상태 요약 원형 차트
            fig1 = px.pie(
                values=[normal_cnt, error_cnt], 
                names=['정상', '정정 필요'],
                color=['정상', '정정 필요'],
                color_discrete_map={'정상': '#28a745', '정정 필요': '#ffc107'},
                hole=0.4,
                title="고유 주소 검증 비율"
            )
            fig1.update_traces(textposition='inside', textinfo='percent+label')
            fig1.update_layout(margin=dict(t=40, b=0, l=0, r=0), height=250, showlegend=False)
            st.plotly_chart(fig1, use_container_width=True)
            
        st.markdown("---")
        st.subheader("🛠️ 수정이 필요한 주소 보정")
        
        df_edit = st.session_state.mapping_df.copy()
        error_indices = df_edit[df_edit['상태'] != '정상'].index
        
        if len(error_indices) == 0:
            st.success("🎉 모든 주소가 정상적으로 검색됩니다! 2단계로 넘어가세요.")
            if st.session_state.step == 1:
                if st.button("✅ 2단계: 구간 거리 초고속 산출 시작", type="primary"):
                    st.session_state.mapping_df = df_edit 
                    st.session_state.step = 2
                    st.rerun()
        else:
            st.info("💡 선택 또는 직접 입력이 모두 완료되어야만 최종 구간 거리를 산출할 수 있습니다.")
            
            completed_tasks = 0
            completion_tracker = {}
            
            # 사용자 보정 UI 렌더링 및 완료 체크
            for idx in error_indices:
                title_ph = st.empty() # 완료/대기 상태 표시를 위한 빈 공간
                orig_addr = df_edit.loc[idx, '원본 주소']
                status = df_edit.loc[idx, '상태']
                cands = st.session_state.candidates.get(idx, [])
                
                # 입력 위젯 배치
                col_addr, col_btn, col_chk = st.columns([6, 2, 2])
                with col_addr: st.code(orig_addr, language="plaintext")
                with col_btn:
                    search_url = f"https://www.google.com/search?q={urllib.parse.quote(orig_addr)}"
                    st.link_button("🔍 구글 검색", search_url, use_container_width=True)
                with col_chk:
                    is_excluded = st.checkbox("🚫 변환 제외", key=f"exclude_{idx}")
                    df_edit.loc[idx, '제외여부'] = is_excluded
                
                is_completed = False
                
                if is_excluded:
                    is_completed = True
                else:
                    options = ["선택 안 함"] + cands + ["직접 입력"] if cands else ["선택 안 함", "직접 입력"]
                    choice = st.radio("변경할 주소 선택", options, key=f"radio_{idx}", horizontal=True, label_visibility="collapsed")
                    
                    if choice == "직접 입력":
                        new_addr = st.text_input("새 주소 입력", key=f"text_{idx}", label_visibility="collapsed")
                        if new_addr.strip():
                            df_edit.loc[idx, '정정된 주소'] = new_addr
                            is_completed = True
                    elif choice != "선택 안 함":
                        df_edit.loc[idx, '정정된 주소'] = choice
                        is_completed = True
                
                if is_completed: completed_tasks += 1
                completion_tracker[idx] = is_completed
                
                # 상태 텍스트 업데이트 (완료 시 체크 표시)
                color = "red" if "검색 불가" in status else "orange" if "모호함" in status else "green"
                mark = "✅ [완료]" if is_completed else "⏳ [대기]"
                title_ph.markdown(f"**행 {idx+2}** {mark} ➡️ 상태: :{color}[**{status}**]")
                st.markdown("---")
            
            # [신규] 사이드바 플로팅 진행 상황판
            if st.session_state.step == 1:
                st.sidebar.markdown("---")
                st.sidebar.subheader("📍 보정 진행 상황")
                
                progress_val = completed_tasks / len(error_indices)
                st.sidebar.progress(progress_val)
                st.sidebar.write(f"**진행률:** {completed_tasks} / {len(error_indices)} 완료")
                
                uncompleted_rows = [str(idx+2) for idx, comp in completion_tracker.items() if not comp]
                if uncompleted_rows:
                    st.sidebar.caption("⏳ 미완료 행 번호:\n" + ", ".join(uncompleted_rows[:15]) + ("..." if len(uncompleted_rows)>15 else ""))
                
                # 모든 항목이 완료되어야만 산출 버튼 활성화
                st.sidebar.markdown("---")
                if completed_tasks == len(error_indices):
                    st.sidebar.success("🎉 모든 보정이 완료되었습니다!")
                    if st.sidebar.button("✅ 2단계: 거리 산출 시작", type="primary", use_container_width=True):
                        st.session_state.mapping_df = df_edit 
                        st.session_state.step = 2
                        st.rerun()
                else:
                    st.sidebar.warning("⚠️ 모든 항목의 보정을 완료해 주세요.")
                    st.sidebar.button("✅ 2단계: 거리 산출 시작", disabled=True, use_container_width=True)

    # [Step 2] 최종 거리 산출 (멀티스레딩 최적화 적용)
    if st.session_state.step == 2:
        st.subheader("📊 최종 구간 거리 산출 결과")
        
        if 'final_df' not in st.session_state:
            prog = st.progress(0)
            status_text = st.empty()
            http_session = requests.Session()
            
            addr_dict = {}
            for _, row in st.session_state.mapping_df.iterrows():
                if row.get('제외여부', False):
                    addr_dict[row['원본 주소']] = ("보정 제외", None, None)
                    continue
                final_addr = row['정정된 주소']
                x, y = row['X'], row['Y']
                if final_addr and final_addr != "확인 불가" and pd.isna(x):
                    x, y = get_coords_only(final_addr, api_key, http_session)
                addr_dict[row['원본 주소']] = (final_addr, x, y)
            
            df_target = st.session_state.df_raw
            total_rows = len(df_target)
            
            routes_to_fetch = set()
            for i, row in df_target.iterrows():
                s_raw, e_raw = str(row[col_start]), str(row[col_end])
                s_info = addr_dict.get(s_raw, ("확인 불가", None, None))
                e_info = addr_dict.get(e_raw, ("확인 불가", None, None))
                
                if s_info[1] and e_info[1] and s_info[0] != "보정 제외" and e_info[0] != "보정 제외":
                    start_coord = (s_info[1], s_info[2])
                    end_coord = (e_info[1], e_info[2])
                    if start_coord != end_coord:
                        routes_to_fetch.add((start_coord, end_coord))

            route_cache = {}
            if routes_to_fetch:
                status_text.text(f"필요한 {len(routes_to_fetch)}개 고유 구간 동시 산출 중...")
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    future_to_route = {executor.submit(get_driving_distance, s, e, api_key, http_session): (s, e) for s, e in routes_to_fetch}
                    for future in as_completed(future_to_route):
                        route = future_to_route[future]
                        route_cache[route] = future.result()
            
            status_text.text("최종 데이터 병합 중...")
            results = []
            links = []
            
            for i, row in df_target.iterrows():
                s_raw, e_raw = str(row[col_start]), str(row[col_end])
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
                        dist = 0.0
                        note = "✅ 정상 산출(동일 위치)"
                    else:
                        dist = route_cache.get((start_coord, end_coord), 0.0)
                        note = "✅ 정상 산출" if dist > 0 else "⚠️ 경로 탐색 불가"
                else:
                    note = "⚠️ 좌표 확인 불가"
                
                s_final = s_info[0] if s_info[0] else "확인 불가"
                e_final = e_info[0] if e_info[0] else "확인 불가"
                
                results.append({"정정_출발지": s_final, "정정_도착지": e_final, "주행거리(km)": dist, "산출 비고": note})
                
                if s_info[1] and e_info[1] and "보정 제외" not in [s_info[0], e_info[0]]:
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
        success_cnt = len(final_df[final_df['산출 비고'].str.contains('✅ 정상 산출')])
        fail_cnt = total_cnt - success_cnt
        
        # [신규] 최종 결과 요약 대시보드 및 원형 차트
        st.success(f"📌 **산출 완료!** 전체 **{total_cnt}건** 중 정상 산출 **{success_cnt}건** / 실패(제외 포함) **{fail_cnt}건**")
        
        res_col, res_chart = st.columns([3, 1])
        with res_chart:
            fig2 = px.pie(
                values=[success_cnt, fail_cnt], 
                names=['정상 산출', '실패/제외'],
                color=['정상 산출', '실패/제외'],
                color_discrete_map={'정상 산출': '#0d6efd', '실패/제외': '#dc3545'},
                hole=0.4,
                title="최종 거리 산출 비율"
            )
            fig2.update_traces(textposition='inside', textinfo='percent+label')
            fig2.update_layout(margin=dict(t=40, b=0, l=0, r=0), height=250, showlegend=False)
            st.plotly_chart(fig2, use_container_width=True)
            
        with res_col:
            def highlight_result(row):
                if '사용자 제외' in row['산출 비고']: return ['background-color: #f2f2f2; color: #a6a6a6'] * len(row)
                elif '⚠️' in row['산출 비고']: return ['background-color: #ffe6e6; color: #cc0000'] * len(row)
                return [''] * len(row)
                
            st.dataframe(final_df.style.apply(highlight_result, axis=1), column_config={"카카오맵 자동길찾기": st.column_config.LinkColumn("카카오맵 자동길찾기", display_text="🚀 즉시 경로 확인")}, use_container_width=True, height=250)

        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            final_df.to_excel(writer, index=False, sheet_name='거리산출결과')
        st.download_button(label="💾 최종 엑셀 파일 다운로드 (.xlsx)", data=output.getvalue(), file_name="route_distances.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", type="primary")
        
        if st.sidebar.button("🔄 새로운 파일 작업하기", use_container_width=True):
            reset_step()
            st.rerun()
            
    if st.session_state.api_log:
        st.markdown("---")
        with st.expander("🛠️ 시스템 디버그: API 호출 이력 보기", expanded=False):
            st.caption(f"이번 세션에서 총 **{len(st.session_state.api_log)}번**의 API 호출이 발생했습니다.")
            st.dataframe(pd.DataFrame(st.session_state.api_log), use_container_width=True)
