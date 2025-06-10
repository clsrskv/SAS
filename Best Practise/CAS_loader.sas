/*
Description:
	SAS Metadata - CAS Loader properties
*/

%let macro_query_metadata=path/macro_query_metadata.sas;
%let metadata_folder_CAS=DWH/Jobs/06. Reporting/;

%let libref=work;
%let stp_bestpractise_tables=
	&libref..stp_bp_CAS_pos_jobs
	&libref..stp_bp_CAS_prop_noreplace
	&libref..stp_bp_CAS_prop_columns
	&libref..stp_bp_CAS_prop_nosave
;

*LIBNAME &libref. BASE 'path to persistent data';

proc datasets library=work nolist nodetails nowarn memtype=(view data);
delete
	metadata_select
	metadata_saslibrary
	_tmp_properties
	_tmp_prop_count
	_tmp_setproperties
	_tmp_properties
	_tmp_setprop_count
	_tmp_jobs_tables
	_tmp_tablesources
	_tmp_columns
	_tmp_casloaders
	_tmp_nosave
	_tmp_noreplace
	_tmp_jobs_columns
;
quit;

%include "&macro_query_metadata." / source2;

data _null_;
file xmlreq;
put '<GetMetadataObjects>';
put '	<Reposid>$METAREPOSITORY</Reposid>';
put '	<Type>SASLibrary</Type>';
put '	<Objects/>';
put '	<NS>SAS</NS>';
put '	<Flags>388</Flags>';
put '	<Options>';
put '		<Templates>';
put '			<SASLibrary Id="" Name="" Desc=""';
put '				MetadataCreated=""';
put '				MetadataUpdated=""';
put '				>';
put '				<UsingPackages/>';
put '			</SASLibrary>';
put '			<Property Name="" DefaultValue="">';
put '			</Property>';
put '			<DatabaseSchema>';
put '				<Tables/>';
put '			</DatabaseSchema>';
put '			<PhysicalTable Name="" Id="" SASTableName="">';
put '				<TargetClassifierMaps/>';
put '				<TargetTransformations/>';
put '			</PhysicalTable>';
put '			<TransformationActivity>';
put '				<Steps/>';
put '			</TransformationActivity>';
put '			<TransformationStep>';
put '				<ReferencedObjects/>';
put '			</TransformationStep>';
put '			<CustomAssociation>';
put '				<OwningObject/>';
put '			</CustomAssociation>';
put '			<Job Name="">';
put '				<Trees/>';
put '			</Job>';
put '			<Tree Name="">';
put '				<ParentTree/>';
put '			</Tree>';
put '		</Templates>';
put '	<XMLSelect search="SASLibrary[@Name contains ''Viya_'']"/>';
put '	</Options>';
put '</GetMetadataObjects>';
run;

%getMeta(xmllib=Job);
%joinQuery(lib=Job);

proc sql noprint;
select name into :treecols separated by ', '
from dictionary.columns
where libname eq 'WORK'
and memname eq 'METADATA_SASLIBRARY'
and prxmatch('/^Tree(\d+)?_Name\s+$/', name)
order by npos desc
;
quit;

proc sql;
create table
	&libref..stp_bp_CAS_pos_jobs(label='CAS loader in wrong level')
as select distinct
	Job_Name label='Jobnavn',
	SASLibrary_Name label='SAS Library',
	PhysicalTable_Name label='Tabelnavn',
	catx('/', &treecols.) as Path label='Sti',
	Job_Id
from
	work.metadata_saslibrary
where
	Job_Name ne ''
having
	Path not like "&metadata_folder_CAS.%"
order by
	Path,
	Job_Name
;
quit;

data _null_;
file xmlreq;
put '<GetMetadataObjects>';
put '	<Reposid>$METAREPOSITORY</Reposid>';
put '	<Type>Select</Type>';
put '	<Objects/>';
put '	<NS>SAS</NS>';
put '	<Flags>388</Flags>';
put '	<Options>';
put '		<Templates>';
put '			<Select Id="" Name=""';
put '				>';
put '				<Steps/>';
put '			</Select>';
put '			<TransformationStep>';
put '				<PropertySets/>';
put '			</TransformationStep>';
put '			<PropertySet>';
put '				<Properties/>';
put '			</PropertySet>';
put '			<Property Id="" Name="" DefaultValue="">';
put '			</Property>';
put '		</Templates>';
put '	<XMLSelect search="Select[@Name eq ''Cloud Analytic Services Table Loader'']"/>';
put '	</Options>';
put '</GetMetadataObjects>';
run;

%getMeta(xmllib=Select);
%joinQuery(lib=Select);

data work._tmp_properties;
set work.metadata_select;
drop reposid type ns flags;
drop PropertySet_Id TransformationStep_Id;
run;

proc sql;
create table
	work._tmp_prop_count
as select
	Property_Name,
	Property_DefaultValue,
	Count(*) as antal
from
	work._tmp_properties
group by
	1,2
;
quit;

data _null_;
file xmlreq;
put '<GetMetadataObjects>';
put '	<Reposid>$METAREPOSITORY</Reposid>';
put '	<Type>Select</Type>';
put '	<Objects/>';
put '	<NS>SAS</NS>';
put '	<Flags>388</Flags>';
put '	<Options>';
put '		<Templates>';
put '			<Select Id="" Name=""';
put '				>';
put '				<Steps/>';
put '			</Select>';
put '			<TransformationStep>';
put '				<PropertySets/>';
put '			</TransformationStep>';
put '			<PropertySet>';
put '				<SetProperties/>';
put '			</PropertySet>';
put '			<Property Id="" Name="" DefaultValue="">';
put '			</Property>';
put '		</Templates>';
put '	<XMLSelect search="Select[@Name eq ''Cloud Analytic Services Table Loader'']"/>';
put '	</Options>';
put '</GetMetadataObjects>';
run;

%getMeta(xmllib=Select);
%joinQuery(lib=Select);

data work._tmp_setproperties;
set work.metadata_select;
drop reposid type ns flags;
drop PropertySet_Id TransformationStep_Id;
run;

data work._tmp_properties;
set
	work._tmp_setproperties
	work._tmp_properties
;
run;

proc sql;
create table
	work._tmp_setprop_count
as select
	Property_Name,
	Property_DefaultValue,
	Count(*) as antal
from
	work._tmp_setproperties
where
	Property_Name ne 'UI_PLACEMENT'
group by
	1,2
;
quit;

data _null_;
file xmlreq;
put '<GetMetadataObjects>';
put '	<Reposid>$METAREPOSITORY</Reposid>';
put '	<Type>Select</Type>';
put '	<Objects/>';
put '	<NS>SAS</NS>';
put '	<Flags>388</Flags>';
put '	<Options>';
put '		<Templates>';
put '			<Select Id="" Name=""';
put '				>';
put '				<ClassifierTargets/>';
put '				<Steps/>';
put '			</Select>';
put '			<PhysicalTable Name="">';
put '			</PhysicalTable>';
put '			<TransformationStep>';
put '				<Activities/>';
put '			</TransformationStep>';
put '			<TransformationActivity>';
put '				<Jobs/>';
put '			</TransformationActivity>';
put '			<Job Name="">';
put '				<Trees/>';
put '			</Job>';
put '			<Tree Name="">';
put '				<ParentTree/>';
put '			</Tree>';
put '		</Templates>';
put '	<XMLSelect search="Select[@Name eq ''Cloud Analytic Services Table Loader'']"/>';
put '	</Options>';
put '</GetMetadataObjects>';
run;

%getMeta(xmllib=Select);
%joinQuery(lib=Select);

proc sql noprint;
select name into :treecols separated by ', '
from dictionary.columns
where libname eq 'WORK'
and memname eq 'METADATA_SELECT'
and prxmatch('/^Tree(\d+)?_Name\s+$/', name)
order by npos desc
;
quit;

data work._tmp_jobs_tables;
set work.metadata_select;
drop reposid type ns flags;
Path=catx('/', &treecols.);
drop Tree: ParentTree:;
run;

data _null_;
file xmlreq;
put '<GetMetadataObjects>';
put '	<Reposid>$METAREPOSITORY</Reposid>';
put '	<Type>Select</Type>';
put '	<Objects/>';
put '	<NS>SAS</NS>';
put '	<Flags>388</Flags>';
put '	<Options>';
put '		<Templates>';
put '			<Select Id="" Name=""';
put '				>';
put '				<ClassifierSources/>';
put '			</Select>';
put '			<PhysicalTable Name="">';
put '			</PhysicalTable>';
put '		</Templates>';
put '	<XMLSelect search="Select[@Name eq ''Cloud Analytic Services Table Loader'']"/>';
put '	</Options>';
put '</GetMetadataObjects>';
run;

%getMeta(xmllib=Select);
%joinQuery(lib=Select);

data work._tmp_tablesources;
set work.metadata_select;
drop reposid type ns flags;
run;

data _null_;
file xmlreq;
put '<GetMetadataObjects>';
put '	<Reposid>$METAREPOSITORY</Reposid>';
put '	<Type>Select</Type>';
put '	<Objects/>';
put '	<NS>SAS</NS>';
put '	<Flags>388</Flags>';
put '	<Options>';
put '		<Templates>';
put '			<Select Id="" Name=""';
put '				>';
put '				<FeatureMaps/>';
put '			</Select>';
put '			<FeatureMap Name="">';
put '				<FeatureSources/>';
put '				<FeatureTargets/>';
put '			</FeatureMap>';
put '			<Column Id="" Name="">';
put '			</Column>';
put '		</Templates>';
put '	<XMLSelect search="Select[@Name eq ''Cloud Analytic Services Table Loader'']"/>';
put '	</Options>';
put '</GetMetadataObjects>';
run;

%getMeta(xmllib=Select);
%joinQuery(lib=Select);

data work._tmp_columns;
set work.metadata_select;
drop reposid type ns flags;
run;

proc datasets library=work nolist nodetails;
delete metadata_select;
quit;

proc sql;
create table
	work._tmp_casloaders
as select distinct
	coalesce(
		J.Select_Id,
		S.Select_Id
	) as Select_Id,
	coalesce(
		J.Select_Name,
		S.Select_Name
	) as Select_Name,
	J.Job_Id,
	J.Job_Name,
	J.PhysicalTable_Name as TargetTable,
	TS.PhysicalTable_Name as SourceTable,
	J.TransformationActivity_Id, 
	J.TransformationStep_Id,
	J.Path,
	S.Property_Id,
	S.Property_Name,
	S.Property_DefaultValue
from
	work._tmp_jobs_tables as j
full join
	work._tmp_properties as s
on
	j.Select_Id eq s.Select_Id
left join
	work._tmp_tablesources as ts
on
	j.Select_Id eq ts.Select_Id
;
quit;

data work._tmp_nosave;
set work._tmp_casloaders;
where
	Property_Name eq 'SAVETABLE'
	and Property_DefaultValue eq 'false'
;
keep path job_id job_name sourcetable targettable;
run;

proc sort data=work._tmp_nosave;
by path job_name;
run;

data work._tmp_noreplace;
set work._tmp_casloaders;
where
	Property_Name eq 'Option.ReplaceType'
	and Property_DefaultValue ne 'REPLACE'
	and TransformationActivity_Id ne ''
;
run;

proc sql;
create table
	work._tmp_jobs_columns
as select distinct
	casloaders.Job_Id,
	casloaders.Job_Name,
	casloaders.Path,
	casloaders.TargetTable,
	casloaders.SourceTable,
	columns.Column_Name,
	columns.Column1_Name
from
	work._tmp_casloaders as casloaders
left join
	work._tmp_columns as columns
on
	casloaders.Select_Id eq columns.Select_Id
where
	lowcase(column_name) ne lowcase(column1_name)
;
quit;

proc sql;
create table
	&libref..stp_bp_CAS_prop_noreplace(label='CAS load without replace')
as select
	Job_Name label='Job Name',
	Job_id label='JobID',
	SourceTable label='Source Table',
	TargetTable label='Target Table',
	Property_DefaultValue label='CAS handling',
	Path label='Metadata Path'
from
	work._tmp_noreplace
;
quit;

proc sql;
create table
	&libref..stp_bp_CAS_prop_nosave(label='CAS load without persisting data')
as select
	Job_Name label='Jobnavn',
	Job_id label='JobID',
	SourceTable label='Source Table',
	TargetTable label='Target Table',
	Path label='Metadata Path'
from
	work._tmp_nosave
;
quit;

proc sql;
create table
	&libref..stp_bp_CAS_prop_columns(label='CAS loader with bad column mapping')
as select distinct
	Job_Name label='Job Name',
	Job_id label='JobID',
	SourceTable label='Source Table',
	TargetTable label='Target Table',
	Path label='Metadata Path'
from
	work._tmp_jobs_columns
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
