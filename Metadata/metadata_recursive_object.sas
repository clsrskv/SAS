/*
	Macro: %getItem(itm={id/Axxxxxxx.Bxxxxxxx}, level={#/1})
 	Version: 1.0
	Parameters:
		itm: Id of metadata object
		level: Number of recursive levels to parse, 1 returns current object
	Objects are returned as data sets named itm_#_Axxxxxxx.Bxxxxxxx
	Already parsed objects are not reparsed.
	Macro variable noFollowObjects restricts parsing of metadata objects
*/

*%let item=Axxxxxxx.Bxxxxxxx;

%macro getItem(itm=, level=1, __first=y, __lvl=);
%if %quote(&__first.) eq %quote(y) %then %do;
	%global _done_ids;
	%let _done_ids=;
	%let __lvl=&level.;
%end;

%let curLvl = %eval(&__lvl.-&level.+1);

%let noFollowObjects = ResponsibleParties Trees;

%local _ids;
%let _ids=;
%put NOTE: Doing &itm.;

data itm_&curLvl._%sysfunc(tranwrd(%quote(&itm.), %quote(.), %quote(_)));

length
	l 8
	Id $18
	Type $11
	Name 
	Value $1024
	_rc _n 8
;

l=&curLvl.;
Id = "%quote(&itm.)";
Type = "Property";

_rc=1;
_n=1;
call missing(Name, Value);

do while(_rc>0);
	* Walk through all the properties on this object;
	_rc=metadata_getnatr(
		"&itm.",
		_n,
		Name,
		Value
	);

	if (_rc>0) then output;
	_n+1;
end;

length AssocId $18 RoleType _Uri PublicType $256 _ids $5000;
label RoleType = 'Role / Type';

l = &curLvl.;
Id = "&itm.";
Type = "Association";

retain _ids;

call missing(Name, Value);
_rc=1;
_n=0;
do while(_rc gt 0);
	_n+1;
	* Find relevant associations to query;
	_rc=metadata_getnasl(
		"&itm.",
		_n,
		Name
	);
	put _n= Name=;

	_arc=1;
	Value="";
	_Uri="";
	_an=0;
	do while(_arc gt 0);
		_an+1;
		* Quere association;
		_arc=metadata_getnasn(
			"&itm.",
			Name,
			_an,
			_Uri
		);
		if _arc gt 0 then do;
			Value = "";
			RoleType = "";
			PublicType = "";
			_trc=metadata_getattr(_Uri, "Name", Value);
			_trc=metadata_getattr(_Uri, "Role", RoleType);
			_trc=metadata_getattr(_Uri, "PublicType", PublicType);
			RoleType = coalescec(RoleType, PublicType);
			drop PublicType;
			Value = coalescec(Value, '<i>{missing}</i>');
			AssocId = scan(_Uri, 2, '\');
			if AssocId ne ''
				and indexw(_ids, AssocId, " ") eq 0
				and not(indexw("&noFollowObjects.", Name, " "))
			then do;
				_ids = catx(' ', _ids, AssocId);
				call symputx('_ids', _ids, 'L');
			end;
			output;
		end;
	end;
end;
drop _:;
run;

%if &level. gt 1 %then %do;
	%local i curItm;
	%do i=1 %to %sysfunc(countw(%quote(&_ids.), %quote( )));
		%let curItm=%scan(%quote(&_ids.), &i., %quote( ));
		%let _done_ids=&_done_ids.,&curItm.;
		%getItem(itm=&curItm., level=%eval(&level.-1), __first=n, __lvl=&__lvl.)
	%end;
%end;
%mend getItem;
%getItem(itm=&item., level=2)
