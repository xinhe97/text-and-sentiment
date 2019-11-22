libname mylib '/scratch/cityuhk/xinhe/mylib';

PROC IMPORT OUT= xml
            DATAFILE= "all-year-xml.csv"
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

PROC PRINT DATA=xml(obs=10);RUN;
proc export data=xml outfile="in_xml1.csv" dbms=csv replace; run;

data xml;
set xml;
xmlcalldate = Input( Put( xmlymd, 8.), YYMMDD8.);
format xmlcalldate date9.;
run;

PROC PRINT DATA=xml(obs=10);RUN;
proc export data=xml outfile="in_xml2.csv" dbms=csv replace; run;

data mylib.xml;
set xml;
run;
