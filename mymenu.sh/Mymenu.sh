#!/bin/bash 
show_menu()
{
    NORMAL=`echo "\033[m"`
    MENU=`echo "\033[36m"` #Blue
    NUMBER=`echo "\033[33m"` #yellow
    FGRED=`echo "\033[41m"`
    RED_TEXT=`echo "\033[31m"`
    ENTER_LINE=`echo "\033[33m"`
    echo -e "${MENU}**********************H1B APPLICATIONS***********************${NORMAL}"
    echo -e "${MENU}**${NUMBER} 1) ${MENU} Is the number of petitions with Data Engineer job title increasing over time?${NORMAL}"
    echo -e "${MENU}**${NUMBER} 2) ${MENU} Find top 5 job titles who are having highest avg growth in applications.[ALL]. ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 3) ${MENU} Which part of the US has the most Data Engineer jobs for each year? ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 4) ${MENU} find top 5 locations in the US who have got certified visa for each year.[certified]${NORMAL}"
    echo -e "${MENU}**${NUMBER} 5) ${MENU} Which industry(SOC_NAME) has the most number of Data Scientist positions? [certified]?${NORMAL}"
    echo -e "${MENU}**${NUMBER} 6) ${MENU} Which top 5 employers file the most petitions each year? - Case Status - ALL ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 7) ${MENU} Find the most popular top 10 job positions for H1B visa applications for each year for all the applications?${NORMAL}"
    echo -e "${MENU}**${NUMBER} 8) ${MENU} Find the most popular top 10 job positions for H1B visa applications for each year for only certified applications?${NORMAL}"
    echo -e "${MENU}**${NUMBER} 9) ${MENU} Find the percentage and the count of each case status on total applications for each year. Create a line graph depicting the pattern of All the cases over the period of time.${NORMAL}"
    
    echo -e "${MENU}**${NUMBER} 10) ${MENU} Create a bar graph to depict the number of applications for each year [All]${NORMAL}"
    echo -e "${MENU}**${NUMBER} 11) ${MENU} Find the average Prevailing Wage for each Job for each Year (take part time and full time separate). Arrange the output in descending order - [Certified and Certified Withdrawn.]${NORMAL}"
    echo -e "${MENU}**${NUMBER} 12) ${MENU} Which are the employers along with the number of petitions who have the success rate more than 70%  in petitions. (total petitions filed 1000 OR more than 1000) ?${NORMAL}"
    echo -e "${MENU}**${NUMBER} 13) ${MENU} Which are the  job positions along with the number of petitions which have the success rate more than 70%  in petitions (total petitions filed 1000 OR more than 1000)? ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 14) ${MENU}Export result for option no 13 to MySQL database.${NORMAL}"
    echo -e "${MENU}*********************************************${NORMAL}"
    echo -e "${ENTER_LINE}Please enter a menu option and enter or ${RED_TEXT}enter to exit. ${NORMAL}"
    read opt
}
function option_picked() 
{
    COLOR='\033[01;31m' # bold red
    RESET='\033[00;00m' # normal white
    MESSAGE="$1"  #modified to post the correct option selected
    echo -e "${COLOR}${MESSAGE}${RESET}"
}
clear
show_menu
	while [ opt != '' ]
    do
    if [[ $opt = "" ]]; then 
            exit;
    else
        case $opt in

	1) clear;
		
		
		
        option_picked "1 a) Is the number of petitions with Data Engineer job title increasing over time?";
		
		
        show_menu;
        ;;
        2) clear;
        option_picked "1 b) Find top 5 job titles who are having highest avg growth in applications.[ALL]. ";
        
		pig  /home/hduser/question1b.pig
        show_menu;
        ;;  
	
	3) clear;
		
		
        show_menu;
        ;;
                
	    4) clear;
		echo -e "Enter the year (2011,2012,2013,2014,2015,2016)"   
		read var     
option_picked "2 b) find top 5 locations in the US who have got certified visa for each year.[certified]";
        
		
            hive -e " select worksite,case_status,count(case_status) as job,year from project.h1b_final where year ='$var' and case_status='CERTIFIED' group by worksite,year,case_status order by job desc limit 5;" 
        
	    show_menu;
        ;;  
	   5) clear;
		
		
        option_picked "3) Which industry(SOC_NAME) has the most number of Data Scientist positions? [certified]";
		hive -e 
        show_menu;
        ;;	    

	6) clear;
        option_picked "4)Which top 5 employers file the most petitions each year? - Case Status - ALL";
        echo -e "Enter the year (2011,2012,2013,2014,2015,2016)"
		read var
            show_menu;
	;;

        7) clear;
        option_picked "5 a) Find the most popular top 10 job positions for H1B visa applications for each year for all the applications?";
	    echo -e "Enter the year (2011,2012,2013,2014,2015,2016)"
		read var
		echo "Find the most popular top 10 job positions for H1B visa applications for each year for all the applications?";
		hive -e "select job_title,year,count(case_status ) as job from project.h1b_final where year= '$var' group by job_title,year  order by job desc limit 10; "
	    
        show_menu;
        ;;

        8) clear;
        option_picked "5 b) Find the most popular top 10 job positions for H1B visa applications for each year for only certified applications?";
	    echo -e "Enter the year (2011,2012,2013,2014,2015,2016)"
		read var
		echo "Find the most popular top 10 job positions for H1B visa applications for each year for only certified applications?";
	    hive -e "select job_title,year,case_status,count(case_status ) as job from project.h1b_final where year= '$var' and case_status='CERTIFIED' group by job_title,year,case_status  order by job desc limit 10; "
        show_menu;
        ;;
        
        9) clear;
       	
		option_picked "6) Find the percentage and the count of each case status on total applications for each year. Create a line graph depicting the pattern of All the cases over the period of time.";
		pig -x local /home/hduser/question6.pig
        show_menu;
        ;;
		10) clear;
		
		 option_picked "7) Create a bar graph to depict the number of applications for each year [All]";
		hive -e "select year, count(*) from project.h1b_final group by year order by year;"
        show_menu;
        ;;
		11) clear;
		
        option_picked "8) Find the average Prevailing Wage for each Job for each Year (take part time and full time separate). Arrange the output in descending order - [Certified and Certified Withdrawn.] ";
		echo -e "Enter the choice Full time/ Part time.(Y/N)"
		read var		
		echo -e "Enter the year(2011,2012,2013,2014,2015,2016)"
		read temp
		
		
		hive -e "select job_title,full_time_position,year,avg(prevailing_wage),case_status as average from project.h1b_final where full_time_position = '$var' and year= '$temp' and case_status in ('CERTIFIED','CERTIFIED-WITHDRAWN') group by job_title,full_time_position,year,case_status order by average desc;"        
        show_menu;
        ;;
		12) clear;
		
		option_picked "9) Which are the employers along with the number of petitions who have the success rate more than 70%  in petitions. (total petitions filed 1000 OR more than 1000) ?"
		
		pig  /home/hduser/question9.pig
        show_menu;
        ;;
		13) clear;
		
		option_picked "10) Which are the  job positions along with the number of petitions which have the success rate more than 70%  in petitions (total petitions filed 1000 OR more than 1000)?"
		pig  /home/hduser/question10.pig
		sleep 10;
        show_menu;
        ;;
		14) clear;
		option_picked "11) Export result for question no 10 to MySql database."
		mysql -u root -p'1234' -e 'drop table question11.question11';

		mysql -u root -p'1234' -e 'use question11;create table question11(job_title varchar(100),success_rate float,petitions int);';

		sqoop export --connect jdbc:mysql://localhost/question11 --username root --password 'password@123' --table question11 --update-mode 			allowinsert  --export-dir /Pig/Question10/p* --input-fields-terminated-by '\t' ;

		echo -e '\n\nDisplay contents from MySQL Database.\n\n'

		echo -e '\n10) Which are the top 10 job positions that have  success rate more than 70% in petitions and total petitions filed are equal to or more than 1000?\n\n'

		mysql -u root -p'@password123' -e 'select * from question11.question11 order by success_rate desc ';  
		show_menu;
        ;;
		\n) exit;   
		;;
        *) clear;
        option_picked "Pick an option from the menu";
        show_menu;
        ;;
    esac
fi
done
