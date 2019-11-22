libname mylib '/scratch/cityuhk/xinhe/mylib';

data xml;
set mylib.xml;
run;
PROC PRINT DATA=xml(obs=10);RUN;

data fdq;
set comp.fundq(where=(year(datadate)>=2000));
run;

data fdq;
set fdq(keep=datadate tic conm gvkey cusip rdq exchg fic);
run;
PROC PRINT DATA=fdq(obs=10);RUN;

/* merge whole comp.fundq */

proc sql;
create table m1 as
select * from
xml a left join fdq b on
a.xmlticker=b.tic and
intnx('day',b.rdq,-3,'End') <= a.xmlcalldate <= intnx('day',b.rdq,10,'End')
;
quit;

data m1;
set m1;
no_of_days  = intck ('DAY', xmlcalldate, rdq);
run;

PROC PRINT DATA=m1(obs=100);RUN;
proc export data=m1 outfile="call-comp.csv" dbms=csv replace; run;
