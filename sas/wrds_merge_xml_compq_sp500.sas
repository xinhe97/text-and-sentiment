libname mylib '/scratch/cityuhk/xinhe/mylib';

data xml;
set mylib.xml;
run;
PROC PRINT DATA=xml(obs=10);RUN;

proc sql;
   create table comp500 as
   select a.datadate, a.tic, a.conm, a.gvkey, a.cusip, a.rdq, a.exchg, a.fic,
            b.gvkey, b.iid , b.from, b.thru
   from
     comp.fundq ( where=(DATAFMT='STD' and INDFMT='INDL' and CONSOL='C' and POPSRC='D') ) as a,
     comp.idxcst_his (where=(gvkeyx='000003')) as b
   where a.gvkey=b.gvkey
   and (a.datadate >=b.from)
   and (a.datadate <=b.thru or missing(thru)=1)
   and (year(a.datadate) >= 2000)
   order by datadate;
quit;
run;

PROC PRINT DATA=comp500(obs=100);RUN;
proc export data=comp500 outfile="sp500_compfundq_2000_2019.csv" dbms=csv replace; run;

data fdq;
/* set comp.fundq(where=(year(datadate)>=2000)); */
set comp500(where=(year(datadate)>=2000));
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
proc export data=m1 outfile="call-comp_sp500.csv" dbms=csv replace; run;
