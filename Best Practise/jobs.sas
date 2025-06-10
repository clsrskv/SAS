/*
Description:
	SAS Metadata - Jobs
*/

%let macro_query_metadata=path/macro_query_metadata.sas;
%let DWH_job_folder=DWH/Jobs/;

%let libref=work;
%let stp_bestpractise_tables=
	&libref..stp_bp_Jobs_BP
	&libref..stp_bp_Jobs_Nummerering
;

*LIBNAME &libref. BASE 'path to persistent data';

proc datasets library=work nolist nodetails nowarn memtype=(view data);
delete
	metadata_job
	_tmp_nummer
	_tmp_props
;
quit;

%include "&macro_query_metadata." / source2;

data _null_;
file xmlreq;
put '<GetMetadataObjects>';
put '	<Reposid>$METAREPOSITORY</Reposid>';
put '	<Type>Job</Type>';
put '	<Objects/>';
put '	<NS>SAS</NS>';
put '	<Flags>388</Flags>';
put '	<Options>';
put '		<Templates>';
put '			<Job Id="" Name="" Desc=""';
put '				>';
put '				<Trees/>';
put '				<JFJobs/>';
put '			</Job>';
put '			<JFJob Id="">';
put '			</JFJob>';
put '			<Tree Name="">'; * Trees/Tree;
put '				<ParentTree/>';
put '			</Tree>';
put '		</Templates>';
*put '	<XMLSelect search="Job[Trees/Tree[@Name ne ''My Folder'']]"/>';
put '	</Options>';
put '</GetMetadataObjects>';
run;

%getMeta(xmllib=Job);
%joinQuery(lib=Job);

proc sql noprint;
select name into :treecols separated by ', '
from dictionary.columns
where libname eq 'WORK'
and memname eq 'METADATA_JOB'
and prxmatch('/^Tree(\d+)?_Name\s+$/', name)
order by npos desc
;
quit;

proc sql;
create table
	work.stp_bp_Jobs(label='Jobs')
as select distinct
	Job_Name label='Job Name',
	catx('/', &treecols.) as Job_Path label='Metadata Path',
	'<div style=''text-wrap: wrap;''>'||trim(Job_Desc)||'</div>' as Job_Desc label='Job Description',
	Job_Id label='JobID',
	CASE JFJob_Id WHEN '' THEN 'No' ELSE 'Yes' END as Deployed label='Deployed'
from
	work.metadata_job
having
	Job_Path not like 'User Folders/%'
order by
	Job_Path,
	Job_Name
;
quit;

data work._tmp_nummer;
set work.stp_bp_Jobs;
if _n_ eq 1 then do;
	retain _re _re2;
	length _expr $64;
	_expr="/^([\d+_]*)/";
	_re=prxparse(_expr);
	_expr="/^\d{2}_\d{2}_\d{3}_\s*$/";
	_re2=prxparse(_expr);
end;
if prxmatch(_re, Job_Name) then do;
	Job_Name_Id = prxposn(_re, 1, Job_Name);
	Job_Name_Id_BP = prxmatch(_re2, Job_Name_Id);
end;
drop _:;
run;

data &libref..stp_bp_Jobs_BP(label='Best practise');
length afvigelse $16 bp $256;
label
	Issue='Issue'
	bp='Best practise'
;
Issue='Chars';
bp='Characters only [A-Za-z0-9_-]';
output;
Issue='Dublicate#';
bp='Job nummer has duplicate';
output;
Issue='Not DWH';
bp='Job placed in folder outside DWH';
output;
Issue='Missing#';
bp='Job does not have number prefix';
output;
Issue='Nummer Format';
bp='Nummer format is not ##_##_###_ (level, subject, serial number)';
output;
Issue='Subfolder';
bp='Job not in correct subfolder (DWH/Jobs/##_subject/)';
output;
run;

proc sql;
create table
	&libref..stp_bp_Jobs_Nummerering(label='Job name not according to best practise')
as select
	catx(', ',
		CASE Job_Name_Id WHEN '' THEN 'Missing#' ELSE
			catx(', ',
				CASE Job_Name_Id_BP WHEN 0 THEN 'Nummer Format' ELSE '' END,
				CASE count(*) WHEN 1 THEN '' ELSE 'Dublicate#' END
			)
		END,
		CASE WHEN Job_Path not like "&DWH_job_folder."||'%' THEN 'Not DWH' ELSE
			CASE countw(Job_Path, '/') WHEN 4 THEN '' ELSE 'Subfolder' END
		END,
		CASE WHEN prxmatch('/^[A-Za-z0-9-_]+\s*$/', Job_Name) THEN '' ELSE 'Chars' END
	)
	as Issue label='Issue',
	NUMMER.Job_Name,
	NUMMER.Job_Path,
	NUMMER.Deployed,
	NUMMER.Job_Desc, 
	NUMMER.Job_Id
from
	work._tmp_nummer as nummer
group by
	Job_Name_Id
having
	Issue ne ''
order by
	Job_Path,
	Job_Name
;
quit;

%macro print_bp(dslist);
%local memlabel;
%let i=1;
ods _all_ close;
ods html file="%sysfunc(getoption(WORK))/best_practise.html";
%do %while(%scan(%quote(&dslist.), &i, %quote( )) ne %quote());
%let memlabel=;
proc contents data=%scan(%quote(&dslist.), &i, %quote( )) noprint out=work._tmp_props;
run;
data _null_;
set work._tmp_props;
call symputx('memlabel', memlabel, 'L');
if _n_ eq 1 then stop;
run;
title "&memlabel.";
proc print noobs data=%scan(%quote(&dslist.), &i, %quote( ));
run;
%let i=%eval(&i.+1);
%end;
ods html close;
proc datasets library=work nolist nodetails nowarn memtype=(view data);
delete _tmp_props;
quit;
%mend print_bp;
%print_bp(&stp_bestpractise_tables.)
