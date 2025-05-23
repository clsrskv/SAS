/*
Description:
	SAS Metadata - SAS Libraries.
*/

%let macro_query_metadata=path/macro_query_metadata.sas;

%let libref=FCtrBstP;
%let stp_bestpractise_tables=
	&libref..stp_bp_Library_libref_dublet
	&libref..stp_bp_Libref_bad_name
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
put '				Libref="" Engine=""';
put '				>';
put '				<UsingPackages/>';
put '				<PropertySets/>';
put '				<Trees/>';
put '			</SASLibrary>';
put '			<Tree Name="">'; * Trees/Tree;
put '				<ParentTree/>';
put '			</Tree>';
put '			<Directory DirectoryName="">'; * UsingPackages/Directory;
put '			</Directory>';
put '			<PropertySet Name="">'; * PropertySets/Property;
put '			</PropertySet>';
put '		</Templates>';
*put '	<XMLSelect search="SASLibrary[@Name contains ''CAS_'']"/>';
put '	</Options>';
put '</GetMetadataObjects>';
run;

%getMeta(xmllib=Lib);
%joinQuery(lib=Lib);

proc sql;
create table
	work.preassigned
as select distinct
	SASLibrary_Id
from
	work.metadata_SASLibrary
where
	PropertySet_Name eq 'PreAssignedLibrarySet'
;
alter table
	work.metadata_SASLibrary
drop column
	PropertySet_Name,
	PropertySet_Id,
	Reposid,
	Type,
	NS,
	Flags
;
quit;

proc sort data=work.metadata_SASLibrary noduprecs;
by SASLibrary_Id;
run;

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
	&libref..stp_bp_Library_libref_dublet(label='Libraries with duplicate libref')
as select distinct
	SASLibrary_Name label='SAS Library',
	SASLibrary_Libref label='Libref',
	SASLibrary_Engine label='Engine',
	CASE preassigned.SASLibrary_Id WHEN ' ' THEN 'No' ELSE 'Yes' END
	AS Preassigned label='Preassigned',
	catx('/', &treecols.) as Path label='Sti'
from
	work.metadata_saslibrary
left join
	work.preassigned
on
	metadata_saslibrary.SASLibrary_Id eq preassigned.SASLibrary_Id
group by
	SASLibrary_Libref
having
	count(*) gt 1
order by
	SASLibrary_Libref,
	SASLibrary_Name
;
quit;

proc sql;
create table
	&libref..stp_bp_Libref_bad_name(label='Not according to naming standard')
as select distinct
	SASLibrary_Name label='SAS Library',
	SASLibrary_Libref label='Libref',
	SASLibrary_Engine label='Engine',
	CASE preassigned.SASLibrary_Id WHEN ' ' THEN 'No' ELSE 'Yes' END
	AS Preassigned label='Preassigned',
	catx('/', &treecols.) as Path label='Metadata Path'
from
	work.metadata_SASLibrary
left join
	work.preassigned
on
	metadata_saslibrary.SASLibrary_Id eq preassigned.SASLibrary_Id
having
	not(scan(path,3,'/') eq '00. Control Data' and index(upcase(SASLibrary_Name), 'CTR') gt 0)
	and not(scan(path,3,'/') eq '01. Source Data' and index(upcase(SASLibrary_Name), 'SRC') gt 0)
	and not(scan(path,3,'/') eq '02. Staging' and index(upcase(SASLibrary_Name), 'STG') gt 0)
	and not(scan(path,3,'/') eq '02. Staging' and index(upcase(SASLibrary_Name), 'STA') gt 0)
	and not(scan(path,3,'/') eq '03. Detail Data Store' and index(upcase(SASLibrary_Name), 'DDS') gt 0)
	and not(scan(path,3,'/') eq '04. Data Marts Staging Area' and index(upcase(SASLibrary_Name), 'DMS') gt 0)
	and not(scan(path,3,'/') eq '05. Data Marts' and index(upcase(SASLibrary_Name), 'DM') gt 0)
	and not(scan(path,3,'/') eq '06. Reporting' and index(SASLibrary_Name, 'REP') eq 1)
order by
	Path,
	SASLibrary_Name
;
quit;
%end;
%mend data;
%data();
