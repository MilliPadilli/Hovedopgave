# Automatiseret fakturabehandlingspipeline

Dette repository indeholder min hovedopgave, udviklet i forbindelse med mit praktikophold hos Rebound.
Projektet implementerer en automatiseret, event-drevet pipeline til behandling af fakturaer ved hjælp af
AWS-services og Python-baserede Lambda-funktioner.

## Repository-struktur

- `bi-invoice-processing/`  
  Indeholder selve fakturabehandlingslogikken, AWS Lambda-funktioner samt infrastruktur- og
  konfigurationsrelateret kode.

- `compact_core_layer/`  
  Et delt Python Lambda layer, som anvendes af flere funktioner.  
  Den centrale implementeringskode findes her:  
  `compact_core_layer/python/compact_core/compact/`

## Note til eksaminator

Repository-strukturen afspejler et produktionsnært setup.
Nogle mapper indeholder build-, pakke- og deploy-relaterede filer, mens den centrale
forretningslogik er dokumenteret og let at lokalisere.
