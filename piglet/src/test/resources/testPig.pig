---------------------------------------------------------------------
-- Top N Queries.
--   Counting how many times a query happened
---------------------------------------------------------------------
%DECLARE n 5
data =
    LOAD '$input'
    USING PigStorage(' ')
    AS (query:CHARARRAY, count:INT);

queries_group =
    GROUP data
    BY query;

queries_sum =
     FOREACH queries_group
     GENERATE
         group AS query,
         SUM(data.count) AS count;

queries_ordered =
    ORDER queries_sum
    BY count DESC;

queries_limit = LIMIT queries_ordered $n;

STORE queries_limit INTO '$output';