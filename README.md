# RECOMMEND_CBS

```aiignore
🗓️ 스택: Snowflake, Stremlit, Python
🍀 성능 개선: 엑셀 1000행 기준 9분 → 1분 30초
⚙️ 처리 방식: ThreadPoolExecutor × 15 workers
```


## CBS 코드 추천이 필요한 이유

건설 현장에서 공사비를 관리할 때는 모든 원가 항목에 CBS(Cost Breakdown Structure) 코드를 붙여야 한다. 문제는 현장마다 수천 개의 항목이 있고, 담당자가 직접 수십만 건의 코드 체계에서 적절한 코드를 찾아 매핑하는 데 엄청난 시간이 소요된다는 것이었다.

기존에는 담당자가 엑셀을 보면서 수작업으로 코드를 입력하거나, 유사한 공종을 복사·붙여넣기 하는 방식이었다. 오류율이 높고, 신규 현장 셋업 시마다 반복 작업이 발생하는 구조적 문제였다.

> 💡 핵심 목표 <BR>
 엑셀 파일을 업로드하면 각 항목에 가장 적합한 CBS 코드를 자동으로 추천하고, 담당자가 불일치 항목만 검토·승인하면 되는 시스템을 만드는 것.

<br><br>

## 3단계 추천 플로우
- 단순히 LLM에 던지는 게 아니라, 비용 효율적이고 정확도 높은 순서로 단계를 거치도록 설계했다.
- 빠르고 확실한 규칙 기반 로직을 먼저 실행하고, 판단이 어려운 케이스만 LLM으로 넘기는 구조이다.

<img width="65%" alt="IQMS Invoice Processing Flow-2026-03-06-070347" src="https://github.com/user-attachments/assets/5f1ab7d6-5231-4ebd-ba75-4ce780cf8fa4" />

### Primary vs Fallback 검색
- Primary (현장 실적)는 같은 공종에서 실제로 사용된 CBS 코드를 찾는다. 현장마다 공종 체계가 다르기 때문에 DS_TREE2 필터를 걸어 상위 공종이 일치하는 결과만 가져온다.
- Primary에서 유효한 결과가 없을 때는 Fallback (전사 ERP)으로 넘어간다. ERP의 표준 코드 체계를 기반으로 유사도 검색을 하고, 그 결과를 LLM의 컨텍스트로 활용한다.


> 💡 설계 포인트 <BR>
 LLM은 마지막 수단으로 Cortex Search → 기준정보 조회 → 규칙 기반 매칭까지 처리 가능한 케이스를 먼저 걸러내야 비용과 지연 시간을 줄일 수 있다.

<br><br>

## 처음 구조
- Streamlit에서 1,000번 순차 호출
- 처음에는 Streamlit 앱에서 엑셀 행 하나마다 CALL RECOMMEND_CBS_3()를 호출하는 구조였다. 프로시저 자체는 잘 동작했지만 문제는 규모였다.

<img width="65%" alt="IQMS Invoice Processing Flow-2026-03-06-070913" src="https://github.com/user-attachments/assets/99b29023-9f63-4fd2-8a1b-98031afe83f7" />


<br>

|항목|	값 |
|---|---|
|처리 방식|	Python for문 순차 호출
|네트워크 왕복|	1,000회
|1,000행| 소요 시간	약 9분 (M Warehouse 기준)
|문제점|	사용자가 9분 대기, 세션 타임아웃 위험

<br><br>

## 한계점
### 시도 1: UDF + SELECT 절 호출
SQL의 INSERT INTO result SELECT UDF(...) FROM input 패턴을 쓰면 Snowflake가 각 행을 내부적으로 병렬 처리해줄 것이라 기대했다.

> 실패 원인<BR>
 Snowflake `UDF`는 SELECT 절에서 호출될 때 `get_active_session()`에 접근할 수 없다. Cortex Search, 테이블 조회 등 모든 DB 접근이 세션을 필요로 하는 이 로직에서는 구조적으로 불가능했다.

<BR>

### 시도 2: Streamlit ThreadPoolExecutor (max_workers=200)
병렬 처리는 되지만 Snowflake Cortex Search의 Rate Limit에 걸렸다.

> 실패 원인<BR>
 Cortex Search는 단일 서비스 기준 `20 QPS` 한도가 있다. max_workers=200이면 `동시에 200개 요청`이 날아가 즉시 `Service rate limit exceeded` 에러가 발생한다. Rate Limit은 순차/병렬 여부와 무관하게 "동시에 몇 개가 날아오냐"가 기준


<br><br>

## 최종 구조
- 프로시저 내부 ThreadPoolExecutor
- 핵심 아이디어는 병렬 처리를 Snowflake 프로시저 안으로 옮기는 것이었다.
- Streamlit은 session.call() 한 번만 호출하고, 프로시저 내부에서 ThreadPoolExecutor로 15개씩 병렬 처리한 뒤 결과를 한 번에 저장한다.

<img width="65%" alt="IQMS Invoice Processing Flow-2026-03-06-070849" src="https://github.com/user-attachments/assets/d3133ca4-a114-4950-b02d-be52c5d9f154" />

<BR>

### 왜 max_workers=15인가?
- Cortex Search의 공식 Rate Limit은 단일 서비스 기준 20 QPS다.
- 각 호출의 평균 응답 시간이 약 300ms이므로, 1초 안에 처리 가능한 최대 요청 수는 약 20개다.
- 여기에 약간의 여유를 두어 15개를 선택했다.

```python
def main(session, INPUT_TABLE, RESULT_TABLE, TOP_K, MODEL):
    # 1. 입력 테이블 전체 읽기
    df   = session.sql(f"SELECT * FROM {INPUT_TABLE}").to_pandas()
    rows = df.to_dict("records")

    # 2. 15개씩 병렬 처리 (Rate Limit 20 QPS 고려)
    results = []
    with ThreadPoolExecutor(max_workers=15) as executor:
        futures = {
            executor.submit(_run_one_row, row, int(TOP_K), MODEL): row
            for row in rows
        }
        for future in as_completed(futures):
            results.append(future.result())

    # 3. 결과 한 번에 저장
    session.write_pandas(pd.DataFrame(results), RESULT_TABLE, overwrite=True)
    return {"status": "success", "processed": len(results)}
```

<br><br>

## 성능 비교
|항목|	V1 (순차)|	V2 (병렬 프로시저)|
|---|---|---|
|네트워크 왕복|	1,000회|	1회
|병렬 처리 위치|	없음|	프로시저 내부 15 workers
|Rate Limit 위험|	없음 (순차라 느릴 뿐)|	15 QPS ← 한도 20 QPS 안전
|1,000행 처리 시간|	약 9분|	대폭 단축
|세션 문제|	없음|	프로시저 세션 공유로 해결


