register /usr/local/hive/lib/hive-exec-1.2.1.jar
register /usr/local/hive/lib/hive-common-1.2.1.jar
data1 = LOAD 'hdfs://localhost:54310/user/hive/warehouse/h1b_final' USING PigStorage('\t') as (s_no:double,case_status:chararray,employer_name:chararray,soc_name:chararray,job_title:chararray,full_time_position:chararray,prevailing_wage:double,year:chararray,worksite:chararray,longitude,latitude);
cleansed= filter data1 by $1 is not null and $1!='NA';
temp= group cleansed by $7;
total= foreach temp generate group,COUNT(cleansed.$0);
cleansed1= filter data1 by $7 is not null and $7!='NA';
temp1= group cleansed1 by ($7,$1);
yearsoccount= foreach temp1 generate group,group.$0,COUNT($1);
joined= join yearsoccount by $1,total by $0;
ans= foreach joined generate FLATTEN($0),ROUND_TO(((long)$2*100)/(long)$4,2),$2;
dump ans;
