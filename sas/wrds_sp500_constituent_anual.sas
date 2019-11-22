
proc sql;
   create table comp500 as
   select a.datadate, a.at, a.fyear, b.gvkey, b.iid , b.from, b.thru
   from
     comp.funda ( where=(DATAFMT='STD' and INDFMT='INDL' and CONSOL='C' and POPSRC='D') ) as a,
     comp.idxcst_his (where=(gvkeyx='000003')) as b
   where a.gvkey=b.gvkey
   and (a.datadate >=b.from)
   and (a.datadate <=b.thru or missing(thru)=1)
   order by datadate;
quit;
run;

PROC PRINT DATA=comp500(obs=100);RUN;
proc export data=comp500 outfile="sp500_compfunda.csv" dbms=csv replace; run;
