 --Ans.1b) Find top 5 job titles who are having highest avg growth in applications.

REGISTER '/home/hduser/ExternalJars/piggybank-0.13.0.jar'; --Register external jar 'Piggy Bank.jar'

DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage;  -- within the jar define a function CSVExcelStorage()  

data = LOAD '/home/hduser/h1b.csv' USING CSVExcelStorage() as 
(s_no:int,
case_status:chararray,
employer_name:chararray,
soc_name:chararray,
job_title:chararray,
full_time_position:chararray,
prevailing_wage:int,
year:chararray,
worksite:chararray,
longitute:double,
latitute:double);			               --Load data

noheader= filter data by $0>=1;		               --Remove header 


cleansed= filter noheader by $7 matches '2011'; --filtering dataset by year
a= group cleansed by $4;                      --grouping by job 
year_2011 = foreach a generate group,COUNT($1);	--generate year,job,count
--dump year_2011;


cleansed= filter noheader  by $7 matches '2012'; --filtering dataset by year
a= group cleansed by $4;                         --grouping by job
year_2012= foreach a generate group,COUNT($1);	 --generate year,job,count
--dump year_2012;

cleansed= filter noheader  by $7 matches '2013'; --filtering dataset by year
a= group cleansed by $4; 			 --grouping by job
year_2013= foreach a generate group,COUNT($1);	 --generate year,job,count
--dump year_2013;

cleansed= filter noheader  by $7 matches '2014'; --filtering dataset by year
a= group cleansed by $4;		         --grouping by job
year_2014= foreach a generate group,COUNT($1);	 --generate year,job,count
--dump year_2014;

cleansed= filter noheader  by $7 matches '2015'; --filtering dataset by year
a= group cleansed by $4; 			 --grouping by job
year_2015= foreach a generate group,COUNT($1);	 --generate year,job,count
--dump year_2015;

cleansed= filter noheader  by $7 matches '2016'; --filtering dataset by year
a= group cleansed by $4; 			 --grouping by job
year_2016= foreach a generate group,COUNT($1);	 --generate year,job,count
--dump year_2016;

joined= join year_2011 by $0, year_2012 by $0, year_2013 by $0, year_2014 by $0, year_2015 by $0, year_2016 by $0;
--dump joined;

year_wise_applications= foreach joined generate $0,$1,$3,$5,$7,$9,$11;
--dump year_wise_applications;

--generate progressive growth
progressive_growth= foreach year_wise_applications generate $0,
(float)($6-$5)*100/$5,(float)($5-$4)*100/$4,
(float)($4-$3)*100/$3,(float)($3-$2)*100/$2,
(float)($2-$1)*100/$1;
--dump progressive_growth;

--average progressive growth
avg_progressive_growth= foreach progressive_growth generate $0,($1+$2+$3+$4+$5)/5;
dump avg_progressive_growth;

---ordered progressive growth
ordered_avg_growth= order avg_progressive_growth by $1 desc;
--dump ordered_avg_growth;

--display top5 only
ans = limit ordered_avg_growth 5;
dump ans;

STORE ans INTO '/home/hduser/Project/Pig/question1b' USING PigStorage(',');



