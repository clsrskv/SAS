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

data work._properties;
set work.metadata_select;
drop reposid type ns flags;
drop PropertySet_Id TransformationStep_Id;
run;

proc sql;
create table
	work.prop_count
as select
	Property_Name,
	Property_DefaultValue,
	Count(*) as antal
from
	work._properties
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

data work._setproperties;
set work.metadata_select;
drop reposid type ns flags;
drop PropertySet_Id TransformationStep_Id;
run;

data work.properties;
set
	work._setproperties
	work._properties
;
run;

proc sql;
create table
	work.setprop_count
as select
	Property_Name,
	Property_DefaultValue,
	Count(*) as antal
from
	work._setproperties
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

data work.jobs_tables;
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

data work.tablesources;
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

data work.columns;
set work.metadata_select;
drop reposid type ns flags;
run;

proc datasets library=work nolist nodetails;
delete metadata_select;
quit;

proc sql;
create table
	work.casloaders
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
	work.jobs_tables as j
full join
	work.properties as s
on
	j.Select_Id eq s.Select_Id
left join
	work.tablesources as ts
on
	j.Select_Id eq ts.Select_Id
;
quit;

data work.nosave;
set work.casloaders;
where
	Property_Name eq 'SAVETABLE'
	and Property_DefaultValue eq 'false'
;
keep path job_id job_name sourcetable targettable;
run;

proc sort data=work.nosave;
by path job_name;
run;

data work.noreplace;
set work.casloaders;
where
	Property_Name eq 'Option.ReplaceType'
	and Property_DefaultValue ne 'REPLACE'
	and TransformationActivity_Id ne ''
;
run;

proc sql;
create table
	work.jobs_columns
as select distinct
	casloaders.Job_Id,
	casloaders.Job_Name,
	casloaders.Path,
	casloaders.TargetTable,
	casloaders.SourceTable,
	columns.Column_Name,
	columns.Column1_Name
from
	work.casloaders
left join
	work.columns
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
	work.noreplace
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
	work.nosave
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
	work.jobs_columns
;
quit;
%end;
%mend data;
%data();
