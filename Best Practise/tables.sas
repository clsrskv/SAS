/*
Description:
	SAS Metadata - Tables
*/

%let macro_query_metadata=path/macro_query_metadata.sas;

%let libref=work;
%let stp_bestpractise_tables=
	&libref..stp_bp_tabeller_libmiss
	&libref..stp_bp_tabeller_duplicates
	&libref..stp_bp_tabeller_table_name
	&libref..stp_bp_tabeller_libref_prefix
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
put "<GetMetadataObjects>";
put "	<Reposid>$METAREPOSITORY</Reposid>";
put "	<Type>PhysicalTable</Type>";
put "	<Objects/>";
put "	<NS>SAS</NS>";
put "	<Flags>388</Flags>";
put "	<Options>";
put "		<Templates>";
put "			<PhysicalTable Id="""" Name="""" TableName="""" SASTableName="""">";
put "				<Trees/>";
put "				<TablePackage/>";
put "			</PhysicalTable>";
put "			<SASLibrary Name="""" Engine="""" Libref="""">";
put "			</SASLibrary>";
put "			<DatabaseSchema Name="""">";
put "				<UsedByPackages/>";
put "			</DatabaseSchema>";
put "			<Directory DirectoryName="""">";
put "			</Directory>";
put "			<Tree Name="""">";
put "				<ParentTree/>";
put "			</Tree>";
put "		</Templates>";
put "	</Options>";
put "</GetMetadataObjects>";
run;

%getMeta(xmllib=PhysicalTable);
%joinQuery(lib=PhysicalTable);

proc sql noprint;
select name into :treecols separated by ', '
from dictionary.columns
where libname eq 'WORK'
and memname eq 'METADATA_PHYSICALTABLE'
and prxmatch('/^Tree(\d+)?_Name\s+$/', name)
order by npos desc
;
quit;

data work.PhysicalTable;
set work.metadata_PhysicalTable;
TablePath=catx('/', &treecols.);
Engine=COALESCEc(SASLibrary_Engine, SASLibrary1_Engine);
LibId=COALESCEc(SASLibrary_Id, SASLibrary1_Id);
LibName=COALESCEc(SASLibrary_Name, SASLibrary1_Name);
Libref=COALESCEc(SASLibrary_Libref, SASLibrary1_Libref);
rename
	PhysicalTable_Id=TableId
	PhysicalTable_Name=Table_Name
	PhysicalTable_SASTableName=SASTableName
	PhysicalTable_TableName=Table_TableName
;
drop Tree: ParentTree: Type NS Flags Reposid;
drop SASLibrary: DatabaseSchema:;
run;

proc sql;
create table
	&libref..stp_bp_tabeller_libmiss(label="Library missing")
as select distinct
	SASTableName label="Filename",
	Table_Name label="Metadata Name",
	TablePath label="Table Metadata Path"
from
	work.physicaltable
where
	libref eq ''
;
quit;

proc sql;
create table
	&libref..stp_bp_tabeller_duplicates(label="Tables with duplicate metadata objects")
as select distinct
	count(*) as Antal label="Antal",
	SASTableName label="Fysisk navn",
	Libref label="SASLib Libref",
	Table_Name label="Metadatanavn",
	TablePath label="Sti",
	LibName label="SASLib Navn",
	LibId label="SASLib ID"
from
	work.physicaltable
group by
	LibId,
	SASTableName
having
	Antal gt 1
order by
	LibId,
	SASTableName
;
quit;

proc sql;
create table
	&libref..stp_bp_tabeller_table_name(label="Name - metadata name different from table name")
as select distinct
	SASTableName label="Table Name",
	Libref label="SASLib Libref",
	Table_Name label="Metadata Name",
	TablePath label="Table Metadata Path",
	LibName label="SASLib Name",
	LibId label="SASLib ID"
from
	work.physicaltable
where
	(
		UPCASE(Table_Name) ne UPCASE(SASTableName)
		and
		UPCASE(Table_Name) ne UPCASE(CATX('.',Libref,SASTableName))
	)
	and Libref ne ''
order by
	LibId,
	SASTableName
;
quit;

proc sql;
create table
	&libref..stp_bp_tabeller_libref_prefix(label="Name - libref prefix")
as select distinct
	SASTableName label="Table Name",
	Libref label="SASLib Libref",
	Table_Name label="Metadata Name",
	TablePath label="Table Metadata Path",
	LibName label="SASLib Name",
	LibId label="SASLib ID"
from
	work.physicaltable
where
	upcase(scan(Table_Name,1,'.')) ne upcase(Libref)
	and Libref ne ''
order by
	LibId,
	SASTableName
;
quit;

%end;
%mend data;
%data();
