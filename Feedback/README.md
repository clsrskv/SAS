# Formål

Eksempel på feedback med Data Drive Content i SAS Visual Analytics med beskrivelse af, hvordan det kan anvendes i rapportering.

* [Anvendelse](#user-content-anvendelse)
* Design
* Kode
  * Rapporteksempel
  * Data til eksempel
  * HTML og JavaScript
  * Job Execution Server
  * Microsoft SQL Server
* Forbedringspotentialer

# Anvendelse

Data leveres fra eksempelvis SAS 9.4 til CAS i SAS Viya. I forberedelsen af data skal der dannes et unikt id, som persisteres (fastholdes) for den pågældende række i datasættet. Id'et er forudsætning for, at status på rækken kan lagres.

Tabellen skal desuden være udstyret med kolonnen _checked, som anvendes til at registrere brugerens feedback.

Data overføres til CAS, og rapport udarbejdes. I rapporten kan listetabel kombineres med Data Driven Content. Her anvendes følgende URL:

https://fst-viya-exp01u.prod.sitad.dk/htmlcommons/test/feedback.html

Denne fil indeholder JavaScript, som håndterer interaktion med bruger og server-side script til lagring af data.

Data Driven Content tilføjes fra Objekter i panelet i venstre side af rapportbyggeren:
<!--
@startuml
!theme mars
<style>
skinparam titleBorderRoundCorner 15
skinparam titleBorderThickness 2
skinparam titleBorderColor red
skinparam titleBackgroundColor Aqua-CadetBlue
</style>
Actor User
group Feedback from SAS Visual Analytics
else Initialize Data Driven Content
== Initialize Data Driven Content ==
autonumber 1.1 "[##.0]"
!pragma teoz true
box "SAS Viya" #AquaMarine
box "SAS Visual Analytics" #LightBlue
participant Report
participant "Data Driven Content" as DDC
end box
participant "JobExecution" as JES
participant CAS
participant MSSQL
end box
User -> Report : Click on row
Report -> DDC : messageEvent
DDC -> User : GUI Checkbox
== Update status ==
autonumber 2.1 "[##.0]"
User -> DDC : Change Checkbox
DDC -> JES : Submit data
JES -> MSSQL : Persist Data
JES -> CAS : Update Data
JES -> DDC : Result
DDC -> User : GUI Status
end
@enduml
-->
Feedback/assets/images/Interaktion_og_kommunikation.svg
![/Interaktion_og_kommunikation.svg](Feedback/assets/images/Interaktion_og_kommunikation.svg)
<img src="Feedback/assets/images/Interaktion_og_kommunikation.svg">

