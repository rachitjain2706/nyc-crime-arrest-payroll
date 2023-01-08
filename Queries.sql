-- Queries to create tables

create external table Complaint(borough string,complaint_year int,complaint_time int, ofnc_desc string, law_cat_cd string, susp_age_group string,vic_age_group string)
row format delimited fields terminated by ','
location '/user/ja4158_nyu_edu/Final_Project/output3';

Queries

---- Complaints felony,misdemeanor,violation count-----

 SELECT borough, law_cat_cd, count(*) AS num_complaints
 FROM Complaint
 Group by borough, law_cat_cd;

 SELECT borough, complaint_year, law_cat_cd, count(*) AS num_complaints
 FROM Complaint
 Group by borough, complaint_year, law_cat_cd;

 SELECT ofnc_desc, count(*) AS num_complaints
 FROM Complaint
 WHERE borough = "BRONX"
 Group by ofnc_desc
 ORDER BY num_complaints desc
 limit 5;

 SELECT ofnc_desc, law_cat_cd,count(*) AS num_complaints
 FROM Complaint
 WHERE borough = "QUEENS"
 Group by ofnc_desc
 ORDER BY num_complaints desc
 limit 5;
 
 SELECT ofnc_desc, law_cat_cd,count(*) AS num_complaints
 FROM Complaint
 WHERE borough = "BROOKLYN"
 Group by ofnc_desc
 ORDER BY num_complaints desc
 limit 5;

 SELECT ofnc_desc, law_cat_cd,count(*) AS num_complaints
 FROM Complaint
 WHERE borough = "MANHATTAN"
 Group by ofnc_desc
 ORDER BY num_complaints desc
 limit 5;

 SELECT ofnc_desc, law_cat_cd,count(*) AS num_complaints
 FROM Complaint
 WHERE borough = "STATEN ISLAND"
 Group by ofnc_desc
 ORDER BY num_complaints desc
 limit 5;

 SELECT ofnc_desc, law_cat_cd,count(*) AS num_complaints
 FROM Complaint
 WHERE borough = ""
 Group by ofnc_desc
 ORDER BY num_complaints desc
 limit 5;
----Day hour count-----

 SELECT borough, law_cat_cd, count(*) AS num_complaints
 FROM Complaint
 WHERE complaint_time BETWEEN 5 AND 17
 Group by borough, law_cat_cd;

 ----Night hour count-----

 SELECT borough, law_cat_cd, count(*) AS num_complaints
 FROM Complaint
 WHERE complaint_time < 5 OR complaint_time > 17
 Group by borough, law_cat_cd;

 ----Night hour count-----

SELECT borough, count(*), law_cat_cd FROM Complaint Where complaint_time < 5 OR complaint_time > 17 Group by borough, law_cat_cd;




----------------Payroll---------


create external table payroll(borough string, payroll_year int, department string, overtime_hours double, overtime_pay double, total_pay double)
row format delimited fields terminated by ','
location '/user/rj2219_nyu_edu/project/output10';

SELECT borough, payroll_year, avg(total_pay) AS avg_total_pay_by_year
FROM payroll
Group by borough, payroll_year;

SELECT borough, avg(total_pay) AS avg_total_pay
FROM payroll
Group by borough;

SELECT borough, avg(overtime_hours) AS avg_overtime_hours
FROM payroll
Group by borough;

SELECT borough, avg(overtime_pay) AS avg_overtime_pay
FROM payroll
Group by borough;

SELECT borough, avg(overtime_pay/overtime_hours) AS overtime_hourly_pay, avg(total_pay/(7*5*49)) AS total_hourly_pay
FROM payroll
Group by borough;

SELECT borough, payroll_year, avg(overtime_pay/overtime_hours) AS overtime_hourly_pay, avg(total_pay/(7*5*49)) AS total_hourly_pay_by_year
FROM payroll
Group by borough, payroll_year;

select percentile(cast(total_pay as BIGINT), 0.5) from payroll where borough='MANHATTAN';

select borough, percentile(cast(total_pay as BIGINT), 0.5) AS med_total_pay_by_year
FROM payroll
Group by borough, payroll_year;

SELECT borough, payroll_year, avg(overtime_hours) AS avg_overtime_hours
FROM payroll
Group by borough, payroll_year;

------------


-----Arrests----------
create external table arrests(borough string,arrest_year int,ofns_desc string,law_cat_cd string,age_group string)
row format delimited fields terminated by ','
location '/user/ss13881_nyu_edu/project/out5';


SELECT borough, law_cat_cd, count(*) AS num_arrests 
 FROM arrests
 Group by borough, law_cat_cd;


 SELECT ofns_desc, count(*) AS num_arrests 
 FROM arrests
 Where borough = "BRONX"
 Group by ofns_desc
 ORDER BY num_arrests DESC limit 5;

 SELECT ofns_desc, count(*) AS num_arrests 
 FROM arrests
 Where borough = "BROOKLYN"
 Group by ofns_desc
 ORDER BY num_arrests DESC limit 5;


 SELECT ofns_desc, count(*) AS num_arrests 
 FROM arrests
 Where borough = "MANHATTAN"
 Group by ofns_desc
 ORDER BY num_arrests DESC limit 5;


 SELECT ofns_desc, count(*) AS num_arrests 
 FROM arrests
 Where borough = "QUEENS"
 Group by ofns_desc
 ORDER BY num_arrests DESC limit 5;


 SELECT ofns_desc, count(*) AS num_arrests 
 FROM arrests
 Where borough = "STATEN ISLAND"
 Group by ofns_desc
 ORDER BY num_arrests DESC limit 5;


 SELECT borough, count(*) AS num_arrests 
 FROM arrests
 Group by borough;


 SELECT borough, arrest_year, count(*) AS num_arrests 
 FROM arrests
 Group by borough, arrest_year;

 SELECT borough, MIN(arrest_year), MAX(arrest_year)
 FROM arrests
 Group by borough;


-----------------Recommendation System---------------


SELECT safety_pay.borough, (safety_pay.safety_ratio + 1 - age_time_table.age_ratio + 1 - age_time_table.time_ratio + safety_pay.avg_pay_score) / 4 as safety_index
 FROM
 (SELECT a.borough, safety.safety_ratio, a.avg_pay_score
 FROM
 (SELECT borough, CASE 
   WHEN 70000 > pay_table.avg_pay THEN 1
   WHEN 70000-10000 > pay_table.avg_pay THEN 0.75
   ELSE 0.5 
   END as avg_pay_score 
   FROM (SELECT borough, avg(total_pay) AS avg_pay FROM payroll Group by borough UNION SELECT  'STATEN ISLAND' AS borough, 75000 AS avg_pay) pay_table) a
 JOIN
 (SELECT t1.borough, t1.num_arrests / t2.num_complaints as safety_ratio
 FROM
 (SELECT borough, COUNT(*) AS num_arrests FROM arrests GROUP BY borough) t1
 JOIN
 (SELECT borough, COUNT(*) AS num_complaints FROM complaint GROUP BY borough) t2
 ON (t1.borough = t2.borough) ) safety
 ON (safety.borough = a.borough)
 ) safety_pay
 JOIN
 (SELECT age_table.borough, age_table.age_ratio, time_table.time_ratio
 FROM
 (SELECT t3.borough, t3.age_complaints  / t4.num_complaints as age_ratio
 FROM
 (SELECT borough, COUNT(*) AS age_complaints FROM complaint WHERE vic_age_group = "18-24" GROUP BY borough) t3
 JOIN
 (SELECT borough, COUNT(*) AS num_complaints FROM complaint GROUP BY borough) t4
 ON (t3.borough = t4.borough)) age_table
 JOIN
 (SELECT t5.borough, t5.time_complaints  / t6.num_complaints as time_ratio
 FROM
 (SELECT borough, COUNT(*) AS time_complaints FROM complaint WHERE complaint_time BETWEEN 8 AND 18 GROUP BY borough) t5
 JOIN
 (SELECT borough, COUNT(*) AS num_complaints FROM complaint GROUP BY borough) t6
 ON (t5.borough = t6.borough)) time_table
 ON (age_table.borough = time_table.borough)) age_time_table
 ON (safety_pay.borough = age_time_table.borough);


 -----------------





