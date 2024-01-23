# Getting Catalog
https://soar.esac.esa.int/soar-sl-tap/tap/tables

```java
https://soar.esac.esa.int/soar-sl-tap/tap/sync?"
            + "REQUEST=doQuery&LANG=ADQL&FORMAT=CSV"
            + "&QUERY=SELECT+distinct(logical_source),logical_source_description"
            + "+FROM+soar.v_cdf_dataset"
            + "+WHERE+logical_source+LIKE%20%27solo_L2_%25%25%27"
```

# Getting Info
Availability.  
```
wget -O - "https://soar.esac.esa.int/soar-sl-tap/tap/sync?REQUEST=doQuery&LANG=ADQL&FORMAT=CSV&QUERY=SELECT+begin_time,end_time,filepath,filename+FROM+soar.v_sc_data_item+WHERE+instrument='MAG'+AND+level='L2'" > foo.csv
```

Info for Data items
```java
"https://soar.esac.esa.int/soar-sl-tap/tap/sync?REQUEST=doQuery&LANG=ADQL&FORMAT=json&QUERY=select%20*%20from%20soar.v_cdf_plot_metadata%20where%20logical_source%20=%20%27"+id+"%27";
wget -O - 'https://soar.esac.esa.int/soar-sl-tap/tap/sync?REQUEST=doQuery&LANG=ADQL&FORMAT=json&QUERY=SELECT+filename,+filepath+FROM+v_sc_data_item+WHERE+begin_time%3E%272020-08-29+00:00:00%27+AND+end_time%3C%272020-09-30+00:00:00%27+AND+data_item_id+LIKE+%27solo_L2_rpw-lfr-surv-asm%25%27'
```

# Test URLs
http://localhost:8080/HapiServer/hapi/data?id=solo_L2_mag-rtn-normal&parameters=EPOCH&start=2023-09-01T00:00Z&stop=2023-09-05T00:00Z
http://localhost:8080/HapiServer/hapi/data?id=solo_L2_mag-rtn-normal&start=2023-09-01T00:00Z&stop=2023-09-05T00:00Z
http://localhost:8080/HapiServer/hapi/data?id=solo_L2_mag-rtn-normal&start=2023-09-03T00:00Z&stop=2023-09-04T00:00Z
This shows where a 1-day list of files results in no files:
https://soar.esac.esa.int/soar-sl-tap/tap/sync?REQUEST=doQuery&LANG=ADQL&FORMAT=csv&QUERY=SELECT+begin_time,end_time,filename,filepath+FROM+v_sc_data_item+WHERE+end_time%3E%272023-09-03T00:00:00.000000000Z%27+AND+begin_time%3C%272023-09-04T00:00:00.000000000Z%27+AND+data_item_id+LIKE+%27solo_L2_mag-rtn-normal%25%27+ORDER+BY+begin_time+ASC

# Introduction
config.json shows an example configuration file.  Note the jar file location
will need to be updated.

EsdcCatalogSource computes the catalog.

EsdcInfoSource computes the infos.

EsdcRecordSource computes the data, using the granule iterator (Iterator<int[]>)
breaking the request into 1-file reads, and then a parameter-subset to get
the data iterator (Iterator<HapiRecord>)

