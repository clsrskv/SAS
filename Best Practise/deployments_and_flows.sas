/*
Descrition:
	SAS Metadata - Deployments and Flows.
*/

%let macro_query_metadata=!miljoe/dwh_fiskeri/jobs/00_control_data/macro_query_metadata.sas;
%let path_to_deployed_code=/sasdata/dwh/jobs/98_deployed;
%let path_to_scheduling_server=/opt/sas/config/Lev1/SchedulingServer/;

%let libref=work;
%let stp_bestpractise_tables=
	&libref..stp_bp_deploy_fil_u
	&libref..stp_bp_deploy_job_u
	&libref..stp_bp_deploy_u_flow
	&libref..stp_bp_deploy_u_filer
	&libref..stp_bp_deploy_ne_filnavn
	&libref..stp_bp_deploy_ne_jobnavn
	&libref..stp_bp_deploy_bad_chars
	&libref..stp_bp_deployments_scripts
	&libref..stp_bp_deployments_i_flow
;

*LIBNAME &libref. BASE 'path to persistent data';

%global refresh;
%macro refresh(do_refresh);
%global exists stp_bestpractise_exists;
%let stp_bestpractise_exists=1;
%if %quote(&do_refresh.) eq %quote(false) %then %do;
	%do i=1 %to %sysfunc(countw(%quote(&stp_bestpractise_tables.), %quote( )));
		%if not(%sysfunc(exist(%scan(%quote(&stp_bestpractise_tables.), &i., %quote( ))))) and %quote(&stp_bestpractise_exists.) eq %quote(1) %then %do;
			%let stp_bestpractise_exists=0;
		%end;
		%put %scan(%quote(&stp_bestpractise_tables.), &i., %quote( )) %sysfunc(ifc(%sysfunc(exist(%scan(%quote(&stp_bestpractise_tables.), &i., %quote( )))) eq 1, exists, not exists));
	%end;
%end;
%put &=stp_bestpractise_exists.;
%mend refresh;
%refresh(&refresh.);

%macro data();
%if %quote(&refresh.) ne %quote(false) or %quote(&stp_bestpractise_exists.) eq %quote(0) %then %do;
%include "&macro_query_metadata." / source2;

data _null_;
file xmlreq;
put '<GetMetadataObjects>';
put '	<Reposid>$METAREPOSITORY</Reposid>';
put '	<Type>JFJob</Type>';
put '	<Objects/>';
put '	<NS>SAS</NS>';
put '	<Flags>388</Flags>';
put '	<Options>';
put '		<Templates>';
put '			<JFJob Id=""';
put '				>';
put '				<Trees/>';
put '			</JFJob>';
put '			<Tree Name="">';
put '				<ParentTree/>';
put '			</Tree>';
put '		</Templates>';
put '		<XMLSelect search="JFJob[@PublicType eq ''DeployedJob'' or @PublicType eq ''DeployedFlow'']"/>';
put '	</Options>';
put '</GetMetadataObjects>';
run;

%getMeta(xmllib=JFJob);
%joinQuery(lib=JFJob);

proc sql noprint;
select name into :treecols separated by ', '
from dictionary.columns
where libname eq 'WORK'
and memname eq 'METADATA_JFJOB'
and prxmatch('/^Tree(\d+)?_Name\s+$/', name)
order by npos desc
;
quit;

data work.DeployedPaths;
set work.metadata_JFJob;
Deployment_Path=catx('/', &treecols.);
drop Reposid Type NS Flags;
drop Tree: ParentTree:;
run;

data _null_;
file xmlreq;
put '<GetMetadataObjects>';
put '	<Reposid>$METAREPOSITORY</Reposid>';
put '	<Type>JFJob</Type>';
put '	<Objects/>';
put '	<NS>SAS</NS>';
put '	<Flags>388</Flags>';
put '	<Options>';
put '		<Templates>';
put '			<JFJob Id="" Name=""';
put '				>';
put '				<AssociatedJob/>';
put '				<SourceCode/>';
put '				<Steps/>';
put '			</JFJob>';
put '			<TransformationStep>';
put '				<Activities/>';
put '			</TransformationStep>';
put '			<TransformationActivity Name="">';
put '			</TransformationActivity>';
put '			<Job Name=""';
put '				>';
put '				<Trees/>';
put '			</Job>';
put '			<Tree Name="">';
put '				<ParentTree/>';
put '			</Tree>';
put '			<File FileName="">';
put '				<Directories/>';
put '			</File>';
put '			<Directory DirectoryName="">';
put '			</Directory>';
put '		</Templates>';
put '		<XMLSelect search="JFJob[@PublicType eq ''DeployedJob'']"/>';
put '	</Options>';
put '</GetMetadataObjects>';
run;

%getMeta(xmllib=JFJob);
%joinQuery(lib=JFJob);

proc sql noprint;
select name into :treecols separated by ', '
from dictionary.columns
where libname eq 'WORK'
and memname eq 'METADATA_JFJOB'
and prxmatch('/^Tree(\d+)?_Name\s+$/', name)
order by npos desc
;
quit;

data work.DeployedJobs;
length level $16;
set work.metadata_JFJob;
Job_Path=catx('/', &treecols.);
level=cats(scan(scan(Job_Path, 3, '/'), 1, '.'),'#',scan(scan(Job_Path, 4, '/'), 1, '_'));
if level not in ('#', '00#') or Job_Id eq '';
FlowName=TransformationActivity_Name;
drop Reposid Type NS Flags;
drop Tree: ParentTree: Transformation:;
run;

proc datasets library=work nolist nodetails;
delete metadata_job;
quit;

proc sort data=work.DeployedJobs;
by Job_Path Job_Name;
run;

filename deploy pipe "find &path_to_deployed_code. -type f -maxdepth 1" encoding='utf-8';

data work.files;
infile deploy;
input;
file_name=scan(_infile_, -1, '/');
file_path=substr(_infile_, 1, length(_infile_)-length(file_name)-1);
run;

filename deploy clear;

proc sql;
create table
	work.Deployed
as select
	DEPLOYEDJOBS.Job_Name label='Job Metadata Name',
	DEPLOYEDJOBS.JFJob_Name label='Deployment Metadata Name',
	DEPLOYEDJOBS.FlowName label='Flow Name',
	DEPLOYEDJOBS.Job_Path label='Job Metadata Path',
	DeployedPaths.Deployment_Path label='Deployment Metadata Path',
	FILES.file_name label='File Name',
	FILES.file_path label='File Path'
from
	work.Deployedjobs
full join
	work.files
on
	files.file_name eq Deployedjobs.File_FileName
left join
	work.DeployedPaths
on
	Deployedjobs.JFJob_Id eq DeployedPaths.JFJob_Id
;
quit;

data
	&libref..stp_bp_deploy_fil_u(label='SAS code without deploymet' drop=Job_name JFJob_Name FlowName Job_Path Deployment_path)
	&libref..stp_bp_deploy_job_u(label='Deployments without job' drop=Job_name Job_Path)
	&libref..stp_bp_deploy_u_flow(label='Deployments without flow' drop=FlowName)
	&libref..stp_bp_deploy_u_filer(label='Deployments without SAS code')
	&libref..stp_bp_deploy_m_flows_filer(label='Deployments with SAS code og flows')
	&libref..stp_bp_deploy_ne_filnavn(label='Deploymentnavn different from SAS code file name' keep=JFJob_Name Job_Name Job_Path file_: Deployment_Path)
	&libref..stp_bp_deploy_ne_jobnavn(label='Deploymentnavn different from job name' keep=JFJob_Name Job_Name Job_Path Deployment_Path)
	&libref..stp_bp_deploy_bad_chars(label='Jobnavn with non standard characters' keep=Job_Name Job_Path)
;
set work.deployed;
if jfjob_Name eq '' then output &libref..stp_bp_deploy_fil_u;
else if job_Name eq '' then output &libref..stp_bp_deploy_job_u;
else if file_name eq '' then output &libref..stp_bp_deploy_u_filer;
else if FlowName eq '' then output &libref..stp_bp_deploy_u_flow;
else output &libref..stp_bp_deploy_m_flows_filer;
if
	JFJob_Name ne subpad(file_name, 1, max(0,length(file_name) - 4))
	and JFJob_Name ne ''
	and file_name ne ''
then output &libref..stp_bp_deploy_ne_filnavn;
if
	JFJob_Name ne Job_Name
then output &libref..stp_bp_deploy_ne_jobnavn;
if
	Job_Name ne ''
	and not(prxmatch('/^[A-Za-z0-9_-]+\s*$/', Job_Name))
then output &libref..stp_bp_deploy_bad_chars;
run;

proc sort data=&libref..stp_bp_deploy_ne_jobnavn nodupkey;
by Job_Path Job_Name JFJob_Name;
run;

proc sort data=&libref..stp_bp_deploy_bad_chars nodupkey;
by Job_Path Job_Name;
run;

data _null_;
file xmlreq;
put '<GetMetadataObjects>';
put '	<Reposid>$METAREPOSITORY</Reposid>';
put '	<Type>JFJob</Type>';
put '	<Objects/>';
put '	<NS>SAS</NS>';
put '	<Flags>388</Flags>';
put '	<Options>';
put '		<Templates>';
put '			<JFJob Id="" Name=""';
put '				>';
put '				<AssociatedJob/>';
put '				<Steps/>';
put '				<SourceCode/>';
put '				<Trees/>';
put '			</JFJob>';
put '			<File Name="" FileName="">';
put '				<Directories/>';
put '			</File>';
put '			<Directory Name="" DirectoryName="">';
put '			</Directory>';
put '			<Job Name="">';
put '			</Job>';
put '			<Job Name="">';
put '			</Job>';
put '			<TransformationStep Name="">';
put '				<Activities/>';
put '			</TransformationStep>';
put '			<TransformationActivity Name="">';
put '			</TransformationActivity>';
put '			<Tree Name="">';
put '				<ParentTree/>';
put '			</Tree>';
put '		</Templates>';
put "	<XMLSelect search=""JFJob[@PublicType = 'DeployedJob']""/>";
put '	</Options>';
put '</GetMetadataObjects>';
run;

%getMeta(xmllib=JFJob);
%joinQuery(lib=JFJob);

proc sql noprint;
select name into :treecols separated by ', '
from dictionary.columns
where libname eq 'WORK'
and memname eq 'METADATA_JFJOB'
and prxmatch('/^Tree(\d+)?_Name\s+$/', name)
order by npos desc
;
quit;

data work.JFJobs;
set work.metadata_JFJOB;
length fileexists 3;
JFJob_Path=catx('/', &treecols.);
drop tree: parenttree: reposid ns flags;
run;

proc datasets library=work nolist nodetails;
delete metadata_JFJOB;
quit;

filename depfiles pipe "ls -p &path_to_deployed_code. | grep -v /" encoding='utf-8';

data work.files;
length f $256 path filename $128;
infile depfiles truncover;
input f;
filename = scan(f, -1, '/');
path = substr(f, 1, length(f) - length(filename) - 1);
run;

filename depfiles clear;

proc sql;
update work.JFJobs
set fileexists = (
	select
		count(*)
	from
		work.files
	where
		files.filename eq JFJobs.File_FileName
)
where
	exists (
		select
			*
		from
			work.files
		where
			files.filename eq JFJobs.File_FileName
	)
;
quit;

proc sql;
create table
	&libref..stp_bp_deployments_ej_flow(label="Deployments not in flow")
as select
	JFJOBS.Job_Name label="Jobnavn", 
	JFJOBS.Directory_Name label="Deployment Folder",
	JFJOBS.Directory_DirectoryName label="Path on disk",
	JFJOBS.File_FileName label="File Name",
	JFJOBS.JFJob_Name label="Deployment Name",
	JFJOBS.JFJob_Path label="Metadata Path"
from
	work.JFJobs
where
	TransformationActivity_Name eq ''
order by
	JFJOBS.JFJob_Name
;
*create table
	&libref..stp_bp_deployments_filer_uden(label="SAS code without deployments")
as select
	files.filename label='File Name'
from
	work.files
where
	not exists (
		select
			*
		from
			work.JFJobs
		where
			files.filename eq JFJobs.File_FileName
	)
;
create table
	work.deployments_i_flow
as select
	JFJOBS.JFJob_Name label="Deployment Name",
	CASE WHEN JFJOBS.FileExists GT 0 THEN '<span style="background-color: lightgreen">Yes</span>' ELSE '<span style="background-color: red">No</span>' END as FileExists label="File exist",
	"" as Flows length=512 label="Flows",
	JFJOBS.TransformationActivity_Name label="Flow Name",
	JFJOBS.Directory_Name label="Deployment Folder",
	JFJOBS.Directory_DirectoryName label="Path on disk",
	JFJOBS.File_FileName label="File Name",
	JFJOBS.Job_Name label="Job Name", 
	JFJOBS.JFJob_Path label="Metadata Path"
from
	work.JFJobs
where
	TransformationActivity_Name ne ''
order by
	JFJOBS.JFJob_Name,
	JFJOBS.TransformationActivity_Name
;
quit;

proc transpose data=work.deployments_i_flow out=work.deployments_i_flow_transposed;
by JFJob_Name;
copy FileExists Flows Directory_Name Directory_DirectoryName File_FileName Job_Name JFJob_Path;
var TransformationActivity_Name;
run;

data &libref..stp_bp_deployments_i_flow(label="Deployments in flow");
set work.deployments_i_flow_transposed;
Flows=cats('<span>',catx('<br>', of col:),'</span>');
if col1 ne '';
drop col: _:;
run;

filename schedule pipe "find &path_to_scheduling_server. -name ""*.sh"" -type f -exec grep -naH \\.sas {} \;";

data &libref..stp_bp_deployments_scripts(label='SAS code referred to in scripts');
infile schedule truncover;
input;
length user $64 exist $64 flow sas sti $256 sh $512 _l 8 _line $1024;
sh=scan(_infile_, 1, ':');
user=scan(sh, -3, '/');
if user ne 'F006065' then user='<div style=''color:red;''>'||user||'</div>';
flow=substr(scan(sh, -1, '/'), 1, length(scan(sh, -1, '/')) - length(scan(sh, -1, '_')) - 1);
_l=input(scan(_infile_, 2, ':'),12.);
_line=subpad(_infile_, length(sh)+2+length(scan(_infile_, 2, ':'))+1);
if _n_ eq 1 then do;
	retain _re;
	_expr='/-sysin\s((!miljoe)?\/([^\/\)]+\/)+[^\)]+)/';
	_re=prxparse(_expr);
end;
if prxmatch(_re, _line) then do;
	sas=prxposn(_re, 1, _line);
	exist=ifc(fileexist(sas), '<div style=''background-color:lightgreen;''>J</div>', '<div style=''background-color:red;''>N</div>');
	sti=reverse(substr(reverse(sas), index(reverse(sas), '/')+1));
	sas=substr(sas, length(sti)+2);
end;
else putlog 'WARNING: Unmatched line: ' _n_ _infile_;
if index(sh, 'RunNow_') eq 0 then output;
label
	user='User'
	flow='Flow'
	sti='Path on disk'
	sas='File Name'
	sh='Script'
	exist='Exists'
;
drop _:;
run;

filename schedule clear;

proc sort data=&libref..stp_bp_deployments_scripts;
by user flow sas;
run;
%end;
%mend data;
%data();
