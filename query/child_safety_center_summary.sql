--select * from hhee2864.child_safety_center where sido like '주소불명';

CREATE VIEW hhee2864.child_safety_center_iso AS 
(
  SELECT
    A.*, B.iso_code
  FROM
    hhee2864.child_safety_center AS A
    LEFT JOIN
      localname_iso_list AS B
    ON
      A.sido = B.sido
);

DROP TABLE IF EXISTS summary_child_safety_center;
CREATE TABLE hhee2864.summary_child_safety_center AS
(
  SELECT 
    csci.sido, COUNT(csci.sido) AS count,
    latest_pop.iso_code, latest_pop.population,
    (COUNT(csci.sido)::float / latest_pop.population) AS count_ratio
  FROM 
    hhee2864.child_safety_center_iso AS csci
  INNER JOIN 
    (
      SELECT 
        sido, iso_code, population
      FROM 
        population_by_region
      WHERE 
        occured_date = (SELECT MAX(occured_date) FROM hhee2864.population_by_region)
    ) AS latest_pop
  ON 
    csci.sido = latest_pop.sido
  GROUP BY 
    csci.sido, latest_pop.iso_code, latest_pop.population
  ORDER BY 
    count DESC
);

commit;

