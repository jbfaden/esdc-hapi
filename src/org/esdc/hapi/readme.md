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
```

