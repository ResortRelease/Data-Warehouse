//lastTouch
MATCH (:contacts)-[m1:mrk_message]-(:marketing)
WITH count(m1) as touches, COLLECT(m1.date_sent) AS mkColl
CALL apoc.coll.sort(mkColl) YIELD value AS touchSeq
MATCH (c:contacts)-[mk:mrk_message]-(m:marketing)
WHERE mk.date_sent = touchSeq[touches-1]
MERGE (m)-[at:ATTRIBUTED_TO {attributionModel:'lastTouch', attributionTouchTime: touchSeq[touches-1], attributionTouchSeq: touches, attributionTimeSeq: 1, attributionWeight: 1.0, attributionTouches: touches}]->(c)

//firstTouch
MATCH (c:contacts)-[mk:mrk_message]-(m:marketing)
WITH c, count(mk) as touches, COLLECT(mk.date_sent) AS mkColl
CALL apoc.coll.sort(mkColl) YIELD value AS touchSeq
MATCH (c:contacts)-[mk:mrk_message]-(m:marketing)
WHERE mk.date_sent = touchSeq[0]
MERGE (l)-[m:ATTRIBUTED_TO {attributionModel:'firstTouch', attributionTouchTime: touchSeq[0], attributionTouchSeq: 1, attributionTimeSeq: touches, attributionWeight: 1.0, attributionTouches: touches}]->(a)

//linearTouch
MATCH (c:contacts)-[mk:mrk_message]-(m:marketing)
WITH c, count(mk) as touches, COLLECT(mk.date_sent) AS mkColl, RANGE(count(mk), 1, -1) AS sequence
CALL apoc.coll.sort(mkColl) YIELD value AS touchSeq
UNWIND sequence AS seq
WITH i, touches, touchSeq[touches-seq] AS ts, seq, 1/toFloat(touches) AS linear_touch_wt
MATCH (c:contacts)-[mk:mrk_message]-(m:marketing)
WHERE mk.date_sent = ts
MERGE (l)-[m:ATTRIBUTED_TO {attributionModel:'linearTouch', attributionTouchTime: ts, attributionTouchSeq: (touches-seq+1), attributionTimeSeq: seq, attributionWeight: linear_touch_wt, attributionTouches: touches}]->(a)

//expDecay
MATCH (c:contacts)-[mk:mrk_message]-(m:marketing)
WITH c, count(mk) as touches, COLLECT(mk.date_sent) AS mkColl,  RANGE(count(mk), 1, -1) AS sequence
CALL apoc.coll.sort(mkColl) YIELD value AS touchSeq
UNWIND sequence AS seq
WITH i, touches, touchSeq[touches-seq] AS ts, seq,
CASE touches WHEN 1 THEN 1 ELSE EXP(seq*-0.7) END AS exp_decay_wt
MATCH (c:contacts)-[mk:mrk_message]-(m:marketing)
WHERE mk.date_sent = ts
MERGE (l)-[m:ATTRIBUTED_TO {attributionModel:'expDecay', attributionTouchTime: ts, attributionTouchSeq: (touches-seq+1),  attributionTimeSeq: seq, attributionWeight: exp_decay_wt, attributionTouches: touches}]->(a)
