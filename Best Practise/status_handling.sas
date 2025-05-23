/*
Description:
	SAS Metadata - Status Handling.
*/

%let macro_query_metadata=path/macro_query_metadata.sas;
%let status_handling_ds=JobStatus;
%let status_handling_libref=CtrlData;
%let status_handling_user=user;

%let libref=work;
%let stp_bestpractise_tables=
	&libref..stp_bp_stat_hand_none
	&libref..stp_bp_stat_hand_wrong
	&libref..stp_bp_stat_hand_other
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
put '	<Type>Job</Type>';
put '	<Objects/>';
put '	<NS>SAS</NS>';
put '	<Flags>388</Flags>';
put '	<Options>';
put '		<Templates>';
put '			<Job Id="" Name=""';
put '				>';
put '				<Trees/>';
put '				<JFJobs/>';
put '				<ConditionActionSets/>';
put '			</Job>';
put '			<JFJob Name="">';
put '				<SourceCode/>';
put '			</JFJob>';
put '			<File FileName="">';
put '				<!-- Job(JFJobs)/JFJob/SourceCode -->';
put '				<Directories/>';
put '			</File>';
put '			<Directory DirectoryName="">';
put '				<!-- Job(JFJobs)/JFJob(SourceCode)/File(Directories) -->';
put '			</Directory>';
put '			<ConditionActionSet>';
put '				<!-- Job(ConditionActionSets) -->';
put '				<Actions/>';
put '			</ConditionActionSet>';
put '			<Action Name="">';
put '				<!-- Job(ConditionActionSets)/ConditionActionSet(Actions) -->';
put '				<PropertySets/>';
put '			</Action>';
put '			<PropertySet>';
put '				<!-- Job(ConditionActionSets)/ConditionActionSet(Actions)/Action(PropertySets) -->';
put '				<SetProperties/>';
put '			</PropertySet>';
put '			<Property Name="" DefaultValue="">';
put '				<!-- Job(ConditionActionSets)/ConditionActionSet(Actions)/Action(PropertySets)/PropertySet(SetProperties) -->';
put '			</Property>';
put '			<Tree Name="">';
put '				<!-- Job(Trees) -->';
put '				<ParentTree/>';
put '			</Tree>';
put '		</Templates>';
put '	</Options>';
put '</GetMetadataObjects>';
run;

%getMeta(xmllib=Job);
%joinQuery(lib=Job);

data _null_;
file xmlreq;
put '<GetMetadataObjects>';
put '	<Reposid>$METAREPOSITORY</Reposid>';
put '	<Type>Property</Type>';
put '	<Objects/>';
put '	<NS>SAS</NS>';
put '	<Flags>388</Flags>';
put '	<Options>';
put '		<Templates>';
put '			<Property DefaultValue="">';
put '				<AssociatedPropertySet/>';
put '			</Property>';
put '			<PropertySet>';
put '				<OwningObject/>';
put '			</PropertySet>';
put '			<ConditionActionSet>';
put '				<AssociatedTransformation/>';
put '			</ConditionActionSet>';
put '			<Job>';
put '			</Job>';
put '		</Templates>';
put '		<XMLSelect search="Property[@Name eq ''StatusTableUserColumn'']"/>';
put '	</Options>';
put '</GetMetadataObjects>';
run;

data _null_;
infile xmlreq;
input;
put _infile_;
run;

%getMeta(xmllib=Property);
%joinQuery(lib=Property);

proc sql noprint;
select name into :treecols separated by ', '
from dictionary.columns
where libname eq 'WORK'
and memname eq 'METADATA_JOB'
and prxmatch('/^Tree(\d+)?_Name\s+$/', name)
order by npos desc
;
quit;

data work.Jobs;
set work.metadata_job;
drop Reposid Type NS Flags;
Job_Path=catx('/', &treecols.);
drop Action_Id ConditionActionSet_Id Property_Id PropertySet_Id Tree: ParentTree:;
run;

data work._Deployed;
length Value $128;
set
	work.Jobs(where=(Value ne '') rename=(Directory_DirectoryName=Value) in=dir)
	work.Jobs(rename=(Property_DefaultValue=Value))
;
if dir then Property_Name='DirectoryName';
Name=translate(trim(Property_Name),'_',' ');
if Name eq '_' then Name='x';
drop Property_Name;
if
	JFJob_Id ne ''
	and index(Job_Path, 'DWH_Fiskeri') eq 1
;
run;

proc sort data=work._Deployed(keep=
		Job_Path Job_Name Job_Id JFJob_Name JFJob_Id Name
		Value	
	)
	noduprecs
	out=work.__Deployed
;
by Job_Path Job_Name Job_Id JFJob_Name JFJob_Id Name;
run;

data work.___Deployed;
length Value _Value $1024;
set work.__Deployed;
by Job_Path Job_Name Job_Id JFJob_Name JFJob_Id Name;
label
	Job_Path='Metadata Path'
	Job_Name='Metadata Name'
	Job_Id='Job ID'
	JFJob_Name='Metadata Name of deployed job'
	JFJob_Id='Metadata Id of deployed job'
;
retain _Value;
Label=Name;
if first.Name then _Value="";
if Value ne '' then do;
	if Name in ('Message', 'Email address') then
		_Value=catx(', ', cats('[',Value,']'));
	else _Value=Value;
end;
if last.Name then do;
	Value=_Value;
	output;
end;
drop _Value;
run;

proc transpose data=work.___Deployed out=work.____Deployed(drop=_NAME_ x);
by Job_Path Job_Name Job_Id JFJob_Name JFJob_Id;
id Name;
idlabel Label;
var Value;
run;

proc sql;
create table
	work._____Deployed
as select
	____Deployed.*,
	METADATA_Property.Property_DefaultValue as UserColumn
from
	work.____Deployed
left join
	work.METADATA_Property
on
	____Deployed.Job_Id eq METADATA_Property.Job_Id
;
quit;

data work.Deployed;
length Job_Path $512 Job_Name $64 Job_Id $17 JFJob_Name $64 JFJob_Id $17;
length DirectoryName Dataset_Name Libref Email_Address Message $1024 UserColumn $32;
label
	Job_Path='Metadata Path'
	Job_Name='Metadata Name'
	Job_Id='Job ID'
	JFJob_Name='Metadata Name of deployed job'
	JFJob_Name='Metadata Id of deployed job'
	UserColumn='Column Name'
;
set work._____Deployed;
run;

proc sort
	data=work.Deployed
	out=&libref..stp_bp_stat_hand_none(
		label='Deployed jobs without Status Handling'
		keep=Job_Path Job_Name Job_Id JFJob_Name JFJob_Id
	)
;
where Dataset_Name eq '';
by Job_path Job_name;
run;

proc sort
	data=work.Deployed
	out=&libref..stp_bp_stat_hand_other(
		label='Deployede jobs med anden status'
	)
;
where Email_address ne '' or Message ne '';
by Job_path Job_name;
run;

proc sort
	data=work.Deployed
	out=&libref..stp_bp_stat_hand_wrong(
		label='Deployede jobs with wrong Status Handling'
		drop=Email_Address Message
	)
;
where
not (
	(
		Dataset_Name eq ''
		and Libref eq ''
	)
	or (
		Dataset_Name eq "&status_handling_ds."
		and Libref eq "&status_handling_libref."
		and UserColumn eq "&status_handling_user."
	)
)
;
by Job_path Job_name;
run;
%end;
%mend data;
%data();
