## 2023-2 KHU Graduate Project

---

### 💡 About
[ Apache Calcite ] <br>
 하나의 통일된 쿼리 언어(SQL)로 <br>
여러 유형의 데이터베이스에 접근하여 조작할 수 있는 연합 쿼리(Query Federation) 기능을 제공해주는 프레임워크.


<br>
[ 목표 ] <br>
Apache Calcite MongoDB Adapter 개선 <br>

<br>
[ 필요성 ] <br>
- Adapter에서 SQL 연산자와 해당 DBMS 연산자 간 매핑을 수행하고, <br>
쿼리를 해당 데이터베이스 시스템에서 실행 가능한 형태로 변환함. <br>
- 따라서, Adapter의 연산자 매핑은 질의 수행 및 성능에 영향을 미침.

---
### 🔧 How
1. 연사자 연결 추가 
2. 해당 데이터베이스의 특화된 연산자 활용
