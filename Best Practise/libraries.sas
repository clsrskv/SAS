/*
Description:
	SAS Metadata - SAS Libraries.
*/

%let macro_query_metadata=path/macro_query_metadata.sas;

%let libref=work;
%let stp_bestpractise_tables=
	&libref..stp_bp_Library_libref_dublet
	&libref..stp_bp_Libref_bad_name
;

*LIBNAME &libref. BASE 'path to persistent data';

proc datasets library=work nolist nodetails nowarn memtype=(view data);
delete
	metadata_saslibrary
	_tmp_preassigned
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
	work._tmp_preassigned
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
	work._tmp_preassigned as preassigned
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
