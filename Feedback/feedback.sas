option mprint;
* Macro wraps code to enable goto statements.;
%macro init();
%global rc_statusCode rc_message;

%let check_column=_checked;

%* Default rc and results;
%let rc_statusCode = -1;
%let rc_message = N/A;
data work.results;
message = "No results created.";
run;

%* Check if _params is set;
%if not(%symexist(_params)) %then %do;
	%let rc_statusCode = 4;
	%let rc_message = No parameters send. Nothing done!;
	data work.parameters;
	run;
	%goto output;
%end;

%* Extract values from json in _params;
filename jsn "%sysfunc(pathname(work))/jns.json" encoding=utf8;

data _null_;
	length jsn $32000;
	file jsn;
	jsn = urldecode("&_params.");
	put jsn;
	putlog jsn;
run;

libname jsn json automap=create;

data _null_;
	set jsn.alldata;
	putlog P1= Value=;
	call symput(P1, trim(Value));
run;

%let caslib = %scan(%quote(&__pk_tbl.), 1, %quote(.));
%let castbl = %scan(%quote(&__pk_tbl.), 2, %quote(.));
%put &=caslib. &=castbl.;
%let whr = %quote(%trim(&__pk_col.)) eq %quote(&__pk_id.);
%put &=whr.;
%let value = %sysfunc(ifc(%quote(&__pk_val.) eq %quote(true), 1, 0));
%put &=value.;

data work.parameters;
	pk_col_name = "&__pk_col.";
	pk_col_value = "&__pk_id.";
	caslib = "&caslib.";
	castbl = "&castbl.";
	checked_col = "&check_column.";
	checked_val = &value.;
	checked_val_org = "&__pk_val.";
run;

ods html close;
cas casauto;

%* Check if CAS Library and CAS Table exists and fetch columns if so;
proc cas;
session casauto;

table.queryCaslib status=rc / caslib="&caslib.";
print "Status [CASLIB]: " rc;
call symputx('rc_statusCode', rc.statusCode);

if(rc.statusCode == 0) then do;

	table.tableExists status=rc result=r / caslib="&caslib." name="&castbl.";
	print "Status [CASTBL]: " rc;
	call symputx('rc_statusCode', rc.statusCode);

	if(rc.statusCode == 0) then do;

		print "Fetching ColumnInfo.";
		table.columninfo status=rc result=r / table={caslib="&caslib." name="&castbl."};
		saveresult r["ColumnInfo"] dataout=work.columninfo;
		print "Status [ColumnInfo]: " rc;

	end;

end;
run;
quit;
%if &rc_statusCode. ne 0 %then %do;
	%let rc_statusCode = 5;
	%let rc_message = CAS Library [&caslib.] or CAS table [&caslib..&castbl.] not found.;
	%goto output;
%end;

%* Check if columns are in table;
%let idcount = 0;
%if &rc_statusCode. eq 0 %then %do;
	proc sql noprint;
	select count(*) into :idcount from work.columnInfo where lowcase(Column) eq lowcase("%trim(&__pk_col.)");
	select count(*) into :chkcount from work.columnInfo where lowcase(Column) eq lowcase("%trim(&check_column.)");
	select trim(column) into :colnames separated by '], [' from work.columnInfo;
	quit;
%end;
%let colnames = [%trim(%quote(&colnames.))];
%put &=colnames.;
%put &=idcount.;

%if &chkcount. ne 1 or &idcount. ne 1 %then %do;
	%let rc_statusCode = 5;
	%let rc_message = Check column [&check_column.] not found in CAS Table [&caslib..&castbl.]. Available columns are &colnames.;
	%goto output;
%end;

%* Update Microsoft SQL Server table and make everything stay cool;
OPTIONS SET=EASYSOFT_UNICODE=YES;
 
libname test odbc
    dsn="Viya_MSSQL_DB"
    schema="dbo"
    authdomain="Viya_MSSQL_DB"
;

%macro q(str);
%quote(%')&str.%quote(%')
%mend q;

%let sqldt = %sysfunc(datetime(), e8601dt25.3);

proc sql;
CONNECT USING test;
EXECUTE (
INSERT INTO
    [dbo].[Feedback]
(
     [Username],
     [CASLibrary],
     [CASTable],
     [PrimaryKey],
     [Status],
     [feedback_dttm]
) VALUES (
     %q(&sysuserid.),
     %q(&caslib.),
     %q(&castbl.),
     %q(&__pk_id.),
     %q(&value.),
     %q(&sqldt.)
)
) BY test;
DISCONNECT FROM test;
quit;

%* If pass through to MSSQL fails, return error and don''t store in CAS;
%if &syscc. gt 0 %then %do;
	%let rc_statusCode = 5;
	%let rc_message = Storing value in permanent store failed. &SQLXMSG.;
	%let syscc = 0;
	%goto output;
%end;

%* Update CAS Table if column exists and everything is cool and user can see stored value on reload;
%if &idcount. eq 1 %then %do;
	proc cas;
	updtbl.caslib = "&caslib.";
	updtbl.name = "&castbl.";
	updtbl.where = "&whr.";
	table.update result=result status=rc /
		table = updtbl
		set = {
			{
				var="_checked",
				value="&value."
			}
		}
  
	;
	print "Status [UPDATE]: " rc;
	call symputx('rc_statusCode', rc.statusCode);
	
	if(rc.statusCode == 0) then do;
		print "Result: " result;
		call symputx('tableName', result.tableName);
		call symputx('rowsUpdated', result.rowsUpdated);
	end;
	run;
	quit;

	%let rc_statusCode = 0;
	%let rc_message = Status has been updated.;

	data work.results;
	tableName = "&tableName.";
	rowsUpdated = "&rowsUpdated.";
	run;

	cas casauto terminate;
%end;
%else %do;
	%let rc_statusCode = 5;
	%let rc_message = Column [%trim(&__pk_col.)] not found in CAS table [&caslib..&castbl.]. Available columns are &colnames.;
	cas casauto terminate;
	%goto output;
%end;

%output:

%* Send info to user;
filename _webout filesrvc parenturi="&SYS_JES_JOB_URI" 
  name='_webout.xml' contenttype='text/json';

proc json nosastags pretty out=_webout;
write open object;
	write values "rc";
	write open object;
		write values "statusCode" &rc_statusCode.;
		write values "message" "&rc_message.";
	write close;
	write values "parameters";
	write open array;
		export work.parameters;
	write close;
	write values "results";
	write open array;
		export work.results;
	write close;
write close;
run;

%mend init;
%init()
