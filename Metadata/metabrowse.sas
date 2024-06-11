/*
  File: metadatabrowse.sas
  SAS Stored Process
  Version: 1.0
  Date: 27JUN2024
*/

%global action;

%if %quote(&action.) eq %quote() %then %do;
data _null_;
file _webout;
put '<!doctype html>';
put '<html lang="en-US">';
put '  <head>';
put '    <title>SAS Metadata Browser</title>';
put '  </head>';
put '  <frameset cols="200px, 500px, *">';
put '    <frame name="init"';
put "      src=""/SASStoredProcess/do?_program=&_program.%quote(&)action=init"" />";
put '    <frame name="type" />';
put '    <frame name="item" />';
put '  </frameset>';
put '</html>';
run;
%end;
%else %do;
data _null_;
file _webout;
put '<div><input type="text" id="myInput" onkeyup="myFunction(this)" placeholder="Filter result..."></div>';
put '<style>';
put 'th {';
put '    cursor: pointer;';
put '}';
put '</style>';
put '<script>';
put 'document.addEventListener(''DOMContentLoaded'', function() {';
put 'const getCellValue = (tr, idx) => tr.children[idx].innerText || tr.children[idx].textContent;';
put ' ';
put 'const comparer = (idx, asc) => (a, b) => ((v1, v2) => ';
put '    v1 !== '' && v2 !== '' && !isNaN(v1) && !isNaN(v2) ? v1 - v2 : v1.toString().localeCompare(v2)';
put '    )(getCellValue(asc ? a : b, idx), getCellValue(asc ? b : a, idx));';
put ' ';
put '// do the work...';
put 'document.querySelectorAll(''th'').forEach(th => th.addEventListener('click', (() => {';
put '    const table = th.closest(''table'').querySelector(''tbody'');';
put '    Array.from(table.querySelectorAll(''tr''))';
put '        .sort(comparer(Array.from(th.parentNode.children).indexOf(th), this.asc = !this.asc))';
put '        .forEach(tr => table.appendChild(tr) );';
put '})));';
put '});';
put '</script>';
put '<script>';
put 'function myFunction(t) {';
put '  // Declare variables';
put '  var input, filter, table, tr, td, i, txtValue;';
put '  input = t';
put '  filter = input.value.toUpperCase();';
put '  tr = document.querySelectorAll("table:not(.systitleandfootercontainer)>tbody>tr");';
put ' ';
put '  for (i = 0; i < tr.length; i++) {';
put '    td = tr[i].getElementsByTagName("td")[0];';
put '    if (tr) {';
put '      txtValue = tr[i].textContent || tr[i].innerText;';
put '      if (txtValue.toUpperCase().indexOf(filter) > -1) {';
put '        tr[i].style.display = "";';
put '      } else {';
put '        tr[i].style.display = "none";';
put '      }';
put '    }';
put '  }';
put '}';
put '</script>';
put '';
run;
%end;

%macro init();

%if %quote(&action.) eq %quote() %then %do;
	%goto exit;
%end;

%goto &action.;

%init:

%stpbegin;
*ProcessBody;

data work.types;
length Type $512 _rc _n 8;
_rc=1;
_n=1;

do while(_rc>0);
	Type="";
	_rc=metadata_getntyp(_n, Type);
	_n+1;
	if _rc gt 0 then do;
		Type = cats(
			'<a href="',
			"/SASStoredProcess/do?_program=&_program",
			'&action=type&type=',
			Type,
			'" target="type">',
			Type,
			'</a>'
		);
		output;
	end;
end;
drop _:;
run;

ods html;

title 'SAS Namespace Metadata Types';

proc print noobs;
run;
ods html close;

%stpend;

%goto exit;

%type:

*options spool mprint;
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

data _null_;
file xmlreq;
put '<GetMetadataObjects>';
put '<Reposid>$METAREPOSITORY</Reposid>';
put "<Type>&type.</Type>";
put '<Objects/>';
put '<NS>SAS</NS>';
put '<Flags>388</Flags>';
put '<Options>';
put '   <Templates>';
put "       <&type.";
put '           Id="" Name=""';
put '           MetadataCreated="" MetadataUpdated=""';
put '           >';
put '			<Trees/>';
put "       </&type.>";
put '   </Templates>';
put '</Options>';
put '</GetMetadataObjects>';
run;

%getMeta(xmllib=metaextr);
 
%joinQuery(lib=metaextr)

%if not(%sysfunc(exist(WORK.METADATA_&type.))) %then %do;
	data _null_;
	file _webout;
	if %length(%quote(&syserrortext.)) eq 0 then do;
		put "<h1>No data found for &type.</h1>";
	end;
	else put "<h1>Forespørgslen genererede en fejl</h1>";
	run;
   %goto exit;
%end;

data _null_;
file xmlreq;
put '<GetMetadataObjects>';
put '	<Reposid>$METAREPOSITORY</Reposid>';
put '	<Type>Tree</Type>';
put '	<Objects/>';
put '	<NS>SAS</NS>';
put '	<Flags>388</Flags>';
put '	<Options>';
put '		<Templates>';
put '			<Tree Id="" Name="" >';
put '				<ParentTree/>';
put '			</Tree>';
put '			<Trees>';
put '				<Tree/>';
put '			</Trees>';
put '			<Tree Name="" Id="">';
put '			</Tree>';
put '		</Templates>';
put '	</Options>';
put '</GetMetadataObjects>';
run;

%getMeta(xmllib=Tree)

%joinQuery(lib=Tree)

%let hasTree=0;

proc sql noprint;
select 1 into :hasTree
from dictionary.columns
where libname="WORK" and memname="METADATA_%upcase(&type.)"
and name eq "Tree_Id";
quit;

%put &=hasTree.;

%if &hasTree. %then %do;
proc sql;
select name into :tree separated by ','
from dictionary.columns
where libname="WORK" and memname="METADATA_TREE"
and name like '%_Name' and name ne "Tree_Name";
quit;

data work.tree;
length Metadata_Path $512;
set work.metadata_tree;
Metadata_Path = catx('/', &tree., Tree_Name);
keep Metadata_Path tree_id tree_name;
run;

proc sort data=work.tree;
by Metadata_Path;
run;

proc datasets library=work nolist nodetails;
change Metadata_&type.=m;
quit;

proc sql;
create table
	work.Metadata_&type.
as select
	m.*,
	t.Metadata_Path
from
	work.m as m
left join
	work.tree as t
on
	m.Tree_id eq t.Tree_id
;
quit;
%end;

%stpbegin;
*ProcessBody;

proc datasets library=work nolist nodetails;
modify metadata_&type.;
rename
	&type._id=Id
	&type._name=n
;*	&type._metadatacreated=Created
;*	&type._metadataupdated=Updated
;
quit;

proc sort
	data=work.metadata_&type.
;
by n;
run;

proc sql;
alter table
	work.metadata_&type.
add
	Name char length=256,
	Created char length=20,
	Updated char length=20
;
update
	work.metadata_&type.
set
	Name = cats('<a href="', "/SASStoredProcess/do?_program=&_program.", '&action=item&type=', "&type.", '&item=', Id, '" target="item">', coalescec(n, '<i>{missing}</i>'), '</a>'),
	Created = cats(put(datepart(input(&type._metadatacreated, anydtdtm.)), yymmddd10.), '_', put(timepart(input(&type._metadatacreated, anydtdtm.)), tod9.)),
	Updated = cats(put(datepart(input(&type._metadataupdated, anydtdtm.)), yymmddd10.), '_', put(timepart(input(&type._metadataupdated, anydtdtm.)), tod9.))
;
quit;

title "&type.";

proc print
	data=work.metadata_&type.(drop=reposid ns flags type)
	noobs
;
var Name Created Updated
	%sysfunc(ifc(%quote(&hasTree.) eq %quote(1),Metadata_Path,))
;
run;

%stpend;

%goto exit;

%item:
option mprint;

%global path;

/*
%if not(%index(%quote(&path.), %quote(&item.))) %then %do;
%end;
*/
%let path=&path.&item.,;

options mprint;
data _null_;
file _webout;
length name type txt $256 id pathitem $20 path $1024;
call missing(id, type);
put '<div style="font-weight: 900; font-size: 1.2em;">';
%do i=1 %to %sysfunc(countw(%quote(&path.),%quote(,)));
if &i. ne 1 then put ' -> ';
pathitem = "%scan(%quote(&path.), &i., %quote(,))";
_rc=metadata_resolve(
	pathitem,
	type,
	id
);
name = "";
_rc = METADATA_GETATTR(pathitem, 'Name', Name);
txt = catx(': ', type, Name);
put "<a href='/SASStoredProcess/do?_program=&_program.%quote(&)action=item&type=Job%quote(&)item=" pathitem '&path=' path '''>' txt '</a>';
path = cats(path, pathitem, ',');
%end;
put '</div>';
run;

data _null_;
length name type $256 id $20 ;
call missing(id, type);
_rc=metadata_resolve(
	"&item.",
	type,
	id
);
_rc = METADATA_GETATTR("&item.", 'Name', Name);
call symput('objtype', type);
call symput('objname', name);
call symput('objid', id);
output;
drop _:;
run;

data work.prop;
length
	Attribute 
	Value $256
	_rc _n 8
;

_rc=1;
_n=1;
call missing(Attribute, Value);

do while(_rc>0);

	/* Walk through all the properties on this machine object. */

	_rc=metadata_getnatr(
		"&item.",
		_n,
		Attribute,
		Value
	);

	if (_rc>0) then output;
	_n+1;
end;
drop _:;
run;

data work.assoc;
length Association $256 Name $1024 RoleType Uri PublicType $256;
label RoleType = 'Role / Type';
_rc=1;
_n=1;

call missing(Association);
do while(_rc>0);
	_rc=metadata_getnasl(
		"&item.",
		_n,
		Association
	);
	_n+1;
	_arc=1;
	name="";
	uri="";
	_an=1;
	do while(_arc gt 0);
		_arc=metadata_getnasn(
			"&item.",
			Association,
			_an,
			uri
		);
		_trc=1;
		Name = "";
		RoleType = "";
		PublicType = "";
		if (_arc>0) then _trc=metadata_getattr(uri,"Name",Name);
		if (_arc>0) then _trc=metadata_getattr(uri,"Role",RoleType);
		if (_arc>0) then _trc=metadata_getattr(uri,"PublicType",PublicType);
		RoleType = coalescec(RoleType, PublicType);
		drop PublicType;
		Name = coalescec(Name, '<i>{missing}</i>');
		if indexw(translate("&path.", ' ', ','), scan(uri, 2, '\')) eq 0 then do;
			Name = cats('<a href="', "/SASStoredProcess/do?_program=&_program", '&action=item&type=', "&type.", '&item=', scan(uri, 2, '\'), '&', "path=&path."">", Name, '</a>');
		end;
		if (_arc gt 0) then output;
		_an+1;
	end;
end;
drop _: uri;
run;

%stpbegin;
*ProcessBody;

title "&objtype.: &objname.";
title2 "Properties";

proc print
	data=work.prop
	noobs
;
run;
title;
title2 "Associations";
proc print
	label
	data=work.assoc
	noobs
;
*by Association;
run;

%stpend;

%goto exit;

%exit:

%mend init;
%init()
