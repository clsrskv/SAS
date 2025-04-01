# SAS Metadata

* [macro_query_metadata.sas](./macro_query_metadata)
  Makro til udtræk af metadata og omdannelse til mart.
  Anvendes i SAS-kode med %include '<sti>/macro_query_metadata.sas' /source2;
  Efterfølgende dannes XML i fil til forespørgsel i data-step med statement file xmlreq;
  Metadata hentes med funktionen %getMeta(xmllib=<libnavn>);
  Mart dannes med funktionen %joinQuery(lib=<libnavn>);
  - libnavn: op til 6 karakterer - anvendes til libref til XML SAS Library med metadata navngivet X_<libnavn>
  Se [eksempler her](./examples).
* [metabrowse.sas](./metabrowse.sas)
  SAS Stored Process til erstatning af metabrowse fra SAS Display Manager.
  Oprettes i metadata via SAS Data Integration Studio med referance til koden med %include '<sti>/metabrowse.sas' /source2;
* [metadata_recursive_object.sas](./metadata_recursive_object.sas)
  Makro der finder metadataobjekt samt nedarvede objekter rekursivt.
  Anvendes ved at kalde %getItem(itm=<metadata ID>, level=<niveauer>):
  - itm: Metadata ID (to gange 8 karakterer opdelt med punktum)
  - niveauer: Antal objektniveauer under det overordnede niveau
