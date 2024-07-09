/*
  Filename: translate.sas
  Description: Convert UTF-8 file to ASCII
*/

/*
* With X Command;
filename p pipe 'iconv --verbose -f UTF-8 -t ASCII//TRANSLIT utf-8-file.txt -o ascii-file.txt';

data _null_;
infile p;
input;
put 'PIPE: ' _infile_;
run;
*/

filename utf8file disk "utf-8-file.txt";
filename lat1file disk "ascii-file.txt";

* Translation table;
* Converts single byte UTF-8 characters to ASCII;
proc format;
value $utf8lat
other = .T /* {Skriv på form [_utf8_EFEBEB_];} */
"EFBBBF" = .R /* {Byte-Order Mark - fjernes} */
"C2A0" = 160 /* {non-breaking space} */
"C2A1" = 161 /* ¡ */
"C2A2" = 162 /* ¢ */
"C2A3" = 163 /* £ */
"C2A4" = 164 /* {currency sign} */
"C2A5" = 165 /* ¥ */
"C2A6" = 166 /* {pipe] */
"C2A7" = 167 /* § */
"C2A8" = 168 /* {umlaut} */
"C2A9" = 169 /* © */
"C2AA" = 170 /* ª */
"C2AB" = 171 /* « */
"C2AC" = 172 /* ¬ */
"C2AD" = 173 /* {soft hyphen}­ */
"C2AE" = 174 /* ® */
"C2AF" = 175 /* ¯ */
"C2B0" = 176 /* ° */
"C2B1" = 177 /* ± */
"C2B2" = 178 /* ² */
"C2B3" = 179 /* ³ */
"C2B4" = 180 /* {Acute accent - spacing acute} */
"C2B5" = 181 /* µ */
"C2B6" = 182 /* {Pilcrow sign - paragraph sign} */
"C2B7" = 183 /* · */
"C2B8" = 184 /* {Spacing cedilla} */
"C2B9" = 185 /* ¹ */
"C2BA" = 186 /* º */
"C2BB" = 187 /* » */
"C2BC" = 188 /* {Fraction one quarter} */
"C2BD" = 189 /* {Fraction one half} */
"C2BE" = 190 /* {Fraction three quarters} */
"C2BF" = 191 /* ¿ */
"C380" = 192 /* À */
"C381" = 193 /* Á */
"C382" = 194 /* Â */
"C383" = 195 /* Ã */
"C384" = 196 /* Ä */
"C385" = 197 /* Å */
"C386" = 198 /* Æ */
"C387" = 199 /* Ç */
"C388" = 200 /* È */
"C389" = 201 /* É */
"C38A" = 202 /* Ê */
"C38B" = 203 /* Ë */
"C38C" = 204 /* Ì */
"C38D" = 205 /* Í */
"C38E" = 206 /* Î */
"C38F" = 207 /* Ï */
"C390" = 208 /* Ð */
"C391" = 209 /* Ñ */
"C392" = 210 /* Ò */
"C393" = 211 /* Ó */
"C394" = 212 /* Ô */
"C395" = 213 /* Õ */
"C396" = 214 /* Ö */
"C397" = 215 /* × */
"C398" = 216 /* Ø */
"C399" = 217 /* Ù */
"C39A" = 218 /* Ú */
"C39B" = 219 /* Û */
"C39C" = 220 /* Ü */
"C39D" = 221 /* Ý */
"C39E" = 222 /* Þ */
"C39F" = 223 /* ß */
"C3A0" = 224 /* à */
"C3A1" = 225 /* á */
"C3A2" = 226 /* â */
"C3A3" = 227 /* ã */
"C3A4" = 228 /* ä */
"C3A5" = 229 /* å */
"C3A6" = 230 /* æ */
"C3A7" = 231 /* ç */
"C3A8" = 232 /* è */
"C3A9" = 233 /* é */
"C3AA" = 234 /* ê */
"C3AB" = 235 /* ë */
"C3AC" = 236 /* ì */
"C3AD" = 237 /* í */
"C3AE" = 238 /* î */
"C3AF" = 239 /* ï */
"C3B0" = 240 /* ð */
"C3B1" = 241 /* ñ */
"C3B2" = 242 /* ò */
"C3B3" = 243 /* ó */
"C3B4" = 244 /* ô */
"C3B5" = 245 /* õ */
"C3B6" = 246 /* ö */
"C3B7" = 247 /* ÷ */
"C3B8" = 248 /* ø */
"C3B9" = 249 /* ù */
"C3BA" = 250 /* ú */
"C3BB" = 251 /* û */
"C3BC" = 252 /* ü */
"C3BD" = 253 /* ý */
"C3BE" = 254 /* þ */
"C3BF" = 255 /* ÿ */
;
run;

data work.convertedChars(keep=_l: _utf8_char pval); 
length pval $1 _utf8_length 8 _utf8_char $20 _l_in _l_out 8 _hexout $16;
infile utf8file recfm=N;
file lat1file recfm=N;
input pval $char1. @@;

retain _utf8_length 0 _utf8_char "";

if _utf8_length eq 0 and index(put(pval, binary8.), "0") gt 1 then _utf8_length = index(put(pval, binary8.), "0")-1;
if length(pval) ne 0 and _utf8_length gt 0 then do;
	_utf8_char = cats(_utf8_char, put(pval, $hex3.));
	_utf8_length + -1;
	if _utf8_length eq 0 then do;
		*putlog 'NOTE:' _utf8_length= _utf8_char= _utf8_char $utf8lat.;
		select (put(_utf8_char, $utf8lat.));
			when (.T) do;
				* Multibyte UTF-8 characters are written to output file as [_utf8_XXXXXX_];
				_hexout=cats('[_utf8_', substr(_utf8_char, 1), '_]');
				*putlog 'NOTE:' _utf8_length= _utf8_char= _utf8_char $utf8lat.;
				do i=1 to length(_hexout);
					pval=substr(_hexout, i, 1);
					*putlog pval= _hexout= _n_;
					put pval $char1. @@;
					_l_out + 1;
				end;
			end;
			when (.R);
			when (.) putlog 'ERROR: UTF-8 translation went wrong: ' _N_=  _utf8_char= _n_ _utf8_char= pval=;
			otherwise pval = byte(put(_utf8_char, $utf8lat.));
		end;
		_l_in = _n_;
		output work.convertedChars;
		if _utf8_length eq 0 then _utf8_char = '';
	end;
end;
else do;
	put pval $char1. @@;
	_l_out + 1;
end;
if _utf8_length lt 0 then do;
	putlog 'ERROR: Something went wrong: ' _utf8_length=;
	stop;
end;
run;

* List statistics on converted UTF-8 characters;
proc sql;
select distinct count(*) as antal, _utf8_char, pval, put(_utf8_char, $utf8lat.) as ascii from work.convertedChars group by _utf8_char;
quit;
