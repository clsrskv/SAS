/*
  Name: macro_query_metadata.sas
  Desc: Include macro used to extract SAS Metadata and present it in a data mart
        Two examples of macro use included in comments below macro code
*/

options spool mprint;
%let metadatarepository=;
 
filename xmlreq temp;

%macro getMeta(xmllib=xmlout, in=xmlreq, metarepo=&metarepository.);
%global metarepository;
%let xmllib = x_%SYSFUNC(subpad(%QUOTE(&xmllib.), 1, 6));
%let mdoutput = &xmllib.;
 
%if %quote(&metarepo.) eq %quote() %then %do;
%put NOTE: Finding id for Foundation Metadata Repository.;
filename xmlini temp;
data _null_;
file xmlini;
put "<GetRepositories><Type>Repository</Type></GetRepositories>";
run;
%getMeta(xmllib=xmlini, in=xmlini, metarepo=get)
 
proc sql noprint;
select repository_id into :metarepository from x_xmlini.Repository
where repository_name eq 'Foundation'
;
quit;
%put NOTE: &=metarepository.;
%let metarep=&metarepository.;
%end;
 
filename &mdoutput. temp encoding='utf-8';
 
proc metadata
 in=&in.
 header=full
 out=&mdoutput.;
run;
 
filename mapfile temp;
 
libname &xmllib. xmlv2 fileref=&mdoutput. automap=replace xmlmap=mapfile;
%mend getMeta;
 
%macro joinQuery(lib=);
%let lib = x_%SYSFUNC(subpad(%QUOTE(&lib.), 1, 6));
%let lib = %upcase(&lib.);
 
proc sql noprint;
%if %sysfunc(exist(&lib..GetMetadataObjects)) %then %do;
    select
        cats('Metadata_', type) into :outDSname
    from
        &lib..GetMetadataObjects
    ;
%end;
%else %do;
    %let outDSname = "Metadata_&lib.";
%end;
 
create table
    work.joinQuery_relations
as select distinct
    d1.memname as parent,
    d2.memname as child,
    d2.name
from
    (
        select memname, name
        from dictionary.columns
        where libname eq "&lib." and name like '%ORDINAL'
    ) as d1
left join
    (
        select memname, name
        from dictionary.columns
        where libname eq "&lib." and name like '%ORDINAL'
    ) as d2
on
    CATS(d1.memname, "_ORDINAL") eq d2.name
    and d1.memname ne d2.memname
where
    d2.memname ne ""
having
    parent not in ("Templates", "Options")
    and child not in ("Templates", "Options")
;
 
create table
    work.joinQuery_columns
as select
    cats(columns.memname, '.', columns.name) as name
from
    dictionary.columns
where
    columns.libname eq "&lib."
    and columns.Name not like '%ORDINAL'
    and columns.memname in (
        select distinct
            memname
        from (
            select parent as memname from work.joinQuery_relations
            )
        union all
            (
            select child as memname from work.joinQuery_relations
            )
    )
;
quit;
 
filename tmp "%sysfunc(pathname(WORK,L))/jointmp.sas";
 
data _null_;
set work.joinQuery_columns end=eof;
file tmp;
if _N_ eq 1 then do;
    put 'proc sql;';
    put 'CREATE TABLE';
    put '09'x "WORK.%TRIM(&outDSname.)(LABEL=&outDSname.)";
    put 'AS SELECT';
end;
 
if not eof then do;
    put '09'x name ',';
end;
else do;
    put '09'x name;
    put 'FROM';
    put '09'x "&lib..GetMetadataObjects";
end;
run;
 
    %macro joinConditions(parent=GetMetadataObjects);
    data _null_;
    file tmp mod;
    set work.joinQuery_relations;
    where parent eq "&parent.";
    length x $1024;
    put 'LEFT JOIN';
    put '09'x "&lib.." child;
    put 'ON';
    x = cats(parent, '.', name, '=', child, '.', name);
    put '09'x x;
    x= cats('%joinConditions(parent=', child, ')');
    call execute(x);
    run;
    %mend joinConditions;
    %joinConditions()
 
data _null_;
file tmp mod;
put ";";
put "quit;";
run;
 
proc sql;
drop table
    work.joinQuery_relations
;
drop table
    work.joinquery_columns
;
quit;
 
%include "%sysfunc(pathname(WORK,L))/jointmp.sas" / source2;
filename tmp clear;
%mend joinQuery;

/*
data _null_;
file xmlreq;
infile datalines truncover;
input;
put _infile_;
datalines4;
<GetMetadataObjects>
	<Reposid>$METAREPOSITORY</Reposid>
	<Type>JFJob</Type>
	<Objects/>
	<NS>SAS</NS>
	<Flags>388</Flags>
	<Options>
		<Templates>
			<JFJob Id="" Name="" Desc=""
				TransformRole=""
				PublicType=""
				MetadataCreated=""
				MetadataUpdated=""
				LockedBy=""
				>
				<JobActivities/>
			</JFJob>
			<JobActivities>
				<JobActivities/>
			</JobActivities>
			<TransformationActivity Name="" Id="" Role="">
				<Steps/>
			</TransformationActivity>
			<Steps>
			</Steps>
			<TransformationStep Id="" Name="">
			</TransformationStep>
		</Templates>
	<XMLSelect search="JFJob[@TransformRole='SCHEDULER_FLOW']"/>
	</Options>
</GetMetadataObjects>
;;;;
run;

%getMeta(xmllib=JFJob);
%joinQuery(lib=JFJob);

data _null_;
file xmlreq;
infile datalines truncover;
input;
put _infile_;
datalines4;
<GetMetadataObjects>
	<Reposid>$METAREPOSITORY</Reposid>
	<Type>Job</Type>
	<Objects/>
	<NS>SAS</NS>
	<Flags>388</Flags>
	<Options>
		<Templates>
			<Job Id="" Name="" Desc=""
				MetadataCreated=""
				MetadataUpdated=""
				LockedBy=""
				>
				<Trees/>
				<JFJobs/>
				<ResponsibleParties/>
			</Job>
			<ResponsibleParties>
				<ResponsibleParty/>
			</ResponsibleParties>
			<ResponsibleParty Name="" Id="" Role="">
			</ResponsibleParty>
			<Trees>
				<Trees/>
			</Trees>
			<Tree Id="" Name="">
			</Tree>
			<JFJobs>
				<JFJobs/>
			</JFJobs>
			<JFJob Id="" Name="" Desc=""
				TransformRole=""
				PublicType=""
				MetadataCreated=""
				MetadataUpdated=""
				LockedBy=""
				>
				<SourceCode/>
				<Steps/>
			</JFJob>
			<SourceCode>
				<SourceCode/>
			</SourceCode>
			<File Id="" Name="" FileName="">
			</File>
			<Steps>
				<Steps/>
			</Steps>
			<TransformationStep Id="">
				<!--Activities/-->
			</TransformationStep>
			<!--TransformationActivity Id="">
				<TransformationActivity/>
			</TransformationActivity-->
		</Templates>
	</Options>
</GetMetadataObjects>
;;;;
run;

%getMeta(xmllib=Jobs);
%joinQuery(lib=Jobs);
*/
