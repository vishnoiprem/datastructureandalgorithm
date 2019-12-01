

--
--Given the following sample table ingredients that records how individual records change over time:
--
--------------------------------------------------------------------
--| id_ingredient | ingredient_name | price |  timestamp | deleted |
--------------------------------------------------------------------
--|        1      | potatoes        | 10.00 | 1445899655 |    0    |
--|        2      | tomatoes        | 20.00 | 1445836421 |    0    |
--|        1      | sweet potatoes  | 10.00 | 1445899132 |    0    |
--|        1      | sweet potatoes  | 15.00 | 1445959231 |    0    |
--|        2      | tomatoes        | 30.00 | 1445894337 |    1    |
--|        3      | chicken         | 50.00 | 1445899655 |    0    |
--							....
--							....
--|       999     | garlic          | 17.00 | 1445897351 |    0    |
--------------------------------------------------------------------


--Write an Impala SQL query that creates a view of that ingredients table, that shows only the most recent state of each ingredient that has not been yet deleted.
--
--Save your answer to a .sql file in the root directory of this repository.


CREATE VIEW OR REPLACE view  dwi_ingredient_latest_view as
SELECT
final.id_ingredient,
final.ingredient_name,
final.price,
final.ingredient_timestamp,
final.`timestamp`,
final.deleted
(
SELECT
id_ingredient,
ingredient_name,
price,
from_unixtime(cast(timestamp as bigint),"yyyy-MM-dd HH:mm:ss") as ingredient_timestamp,
deleted,
row_number() over (partition by ingredient_name order by `timestamp` desc  ) as row_num

WHERE ingredients
where deleted=0

)final where final.row_num=1
;
