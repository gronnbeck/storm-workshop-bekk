### Laste ned datasettet fra
[Last ned herfra](http://data.gov.uk/dataset/road-accidents-safety-data/resource/80b76aec-a0a1-4e14-8235-09cc6b92574a)

### Rydde opp i datasettet
```sh
awk -F "," '{ if ($1 in stored_lines) x=1; else print; stored_lines[$1]=1 }' Vehicles7904.csv.duplicates > outfile.csv
```

### Begynne på oppgavene beskrevet i slidesene
