## 2023-2 KHU Graduate Project


## 💡 About
###  Apache Calcite  <br>
- 하나의 통일된 쿼리 언어(SQL)로 <br>
여러 유형의 데이터베이스에 접근하여 조작할 수 있는 연합 쿼리(Query Federation) 기능을 제공해주는 프레임워크이다.

### 목표 <br>
Apache Calcite MongoDB Adapter 개선 <br>

### 필요성 <br>
- Calcite에서 결정된 실행 계획을 바탕으로 질의를 수행할 때, Calcite는 Adapter를 통해 연동된 데이터베이스의 실행 엔진을 활용한다.
- Adapter에서 SQL 연산자와 해당 DBMS 연산자 간 매핑을 수행하고, <br>
쿼리를 해당 데이터베이스 시스템에서 실행 가능한 형태로 변환함. <br>
- 따라서, Adapter의 연산자 매핑은 질의 수행 및 성능에 영향을 미침.
<br>

## 🔧 How
1. 연사자 연결 추가 
2. 해당 데이터베이스의 특화된 연산자 활용


### Detail
1. SQL: LIKE <-> MongoDB: $regex
- 사용자가 LIKE 연산자를 활용하여 Calcite를 통해 질의할 수 있도록 함.
- 과정:
 - 연산자 등록: MongoRules의 MONGO_OPERATORS에 연산자 추가.
 - 연산자 인식: translateMatch2
 - 피연산자 추출: transalteLike
 - 연산자 변환: translateLike2 (WildCard 매칭 과정 포함.)
 - MongoDB에 알맞은 쿼리 생성: translateOPt2

2. SQL: WHERE + AND/OR <-> MongoDB: $elemMatch
- 구현중.

## 👍 Result
1. 연산자 연결
- 새로운 연산자 지원.
- LIKE 연산자를 통한 질의 가능.
