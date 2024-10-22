
package org.hapiserver.source.tap;

import java.io.IOException;
import java.net.URL;
import java.text.ParseException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.hapiserver.AbstractHapiRecord;
import org.hapiserver.AbstractHapiRecordSource;
import org.hapiserver.CsvDataFormatter;
import org.hapiserver.HapiRecord;
import org.hapiserver.TimeUtil;
import org.hapiserver.source.AggregationGranuleIterator;
import org.hapiserver.source.SourceUtil;

/**
 * Source for availability
 * @author jbf
 */
public class TapAvailabilitySource extends AbstractHapiRecordSource {
    
    private static final Logger logger = Logger.getLogger("hapi.cef");

    private static Set<String> exclude= Collections.EMPTY_SET;
    
    /**
     * get the catalog
     * @return
     * @throws IOException 
     */
    public static String getCatalog() throws IOException {
        try {
            String catalogString= CsaInfoCatalogSource.getCatalog();
            JSONObject catalogContainer= new JSONObject(catalogString);
            JSONArray catalog= catalogContainer.getJSONArray("catalog");
            int n= catalog.length();
            for ( int i=0; i<n; i++ ) {
                JSONObject jo= catalog.getJSONObject(i);
                jo.put( "id", jo.getString("id")+"/availability" );
                if ( jo.has("title") ) {
                    jo.put("title","Availability of "+jo.getString("title") );
                }
                catalog.put( i, jo );
            }
            catalogContainer.put("catalog", catalog);
            return catalogContainer.toString(4);
            
        } catch (JSONException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    /**
     * get the info for the id.
     * @param idavail the dataset id, ending in "/availability"
     * @return 
     */
    public static String getInfo( String idavail ) {
        int i= idavail.indexOf("/");
        String id= idavail.substring(0,i);
        String sampleTime= CsaInfoCatalogSource.getSampleTime(id);
        String sampleStartDate, sampleStopDate;
        sampleStartDate= "2019-04-01T00:00:00.000Z";
        sampleStopDate="2019-05-01T00:00:00.000Z";
        if ( sampleTime!=null ) {
            String[] ss= sampleTime.split("/");
            sampleStartDate= ss[0];
            sampleStopDate= ss[1];
            if ( sampleStartDate.charAt(7)=='-' ) { // allow for $Y-$j
                try {
                    int[] ssd= TimeUtil.parseISO8601Time( sampleStartDate );
                    sampleStartDate= String.format("%04d-%02d-01T00:00Z", ssd[0],ssd[1] );
                    sampleStopDate= TimeUtil.formatIso8601TimeBrief(
                            TimeUtil.add( new int[] { ssd[0], ssd[1], 1, 0, 0, 0, 0 }, new int[] { 0, 1, 0, 0, 0, 0, 0 } ) );
                } catch (ParseException ex) {
                    // just use the default 2019-04.

                }
            } 
            
        }
        
        return "{\n" +
"    \"HAPI\": \"3.0\",\n" +
"    \"modificationDate\": \"2023-03-22T13:26:43.835Z\",\n" +
"    \"parameters\": [\n" +
"        {\n" +
"            \"fill\": null,\n" +
"            \"length\": 24,\n" +
"            \"name\": \"StartTime\",\n" +
"            \"type\": \"isotime\",\n" +
"            \"units\": \"UTC\"\n" +
"        },\n" +
"        {\n" +
"            \"fill\": null,\n" +
"            \"length\": 24,\n" +
"            \"name\": \"StopTime\",\n" +
"            \"type\": \"isotime\",\n" +
"            \"units\": \"UTC\"\n" +
"        },\n" +
"        {\n" +
"            \"fill\": null,\n" +
"            \"name\": \"count\",\n" +
"            \"type\": \"integer\",\n" +
"            \"units\": null\n" +
"        }\n" +
"    ],\n" +
"    \"sampleStartDate\": \""+ sampleStartDate + "\",\n" +
"    \"sampleStopDate\": \""+ sampleStopDate + "\",\n" +
"    \"startDate\": \"2000-01-01T00:00:00.000Z\",\n" +
"    \"status\": {\n" +
"        \"code\": 1200,\n" +
"        \"message\": \"OK request successful\"\n" +
"    },\n" +
"    \"stopDate\": \"lasthour\"\n" +
"}";
    }
    
    String id;
    
    public TapAvailabilitySource( String idavail ) {
        int i= idavail.indexOf("/");
        id= idavail.substring(0,i);
    }
    
    @Override
    public boolean hasGranuleIterator() {
        return true;
    }

    @Override
    public boolean hasParamSubsetIterator() {
        return false;
    }
    
    @Override
    public Iterator<int[]> getGranuleIterator(int[] start, int[] stop) {
        return new AggregationGranuleIterator( "$Y-$m", start, stop );
    }

    @Override
    public Iterator<HapiRecord> getIterator(int[] start, int[] stop) {
        String templ= "https://csa.esac.esa.int/csa-sl-tap/tap/sync?REQUEST=doQuery&LANG=ADQL&FORMAT=CSV&QUERY=SELECT+start_time,end_time,num_instances+FROM+csa.v_dataset_inventory+WHERE+dataset_id='%s'+AND+start_time>='%s'+AND+start_time<'%s'+AND+num_instances>0+ORDER+BY+start_time";
        
        String startStr= TimeUtil.formatIso8601TimeBrief(start);
        String stopStr= TimeUtil.formatIso8601TimeBrief(stop);
        
        String url= String.format( templ, id, startStr, stopStr );
        logger.log(Level.INFO, "readData URL: {0}", url);
        
        try {
            Iterator<String> stringRecords= SourceUtil.getFileLines( new URL(url) );
            stringRecords.next(); // skip the header line
            return fromStrings( stringRecords );
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    
    private static Iterator<HapiRecord> fromStrings( Iterator<String> ins ) {
        return new Iterator<HapiRecord>() {
            
            @Override
            public boolean hasNext() {
                return ins.hasNext();
            }

            @Override
            public HapiRecord next() {
                String nextRecord= ins.next();
                String[] fields= SourceUtil.stringSplit(nextRecord);
                return new AbstractHapiRecord() {
                    @Override
                    public int length() {
                        return 3;
                    }

                    @Override
                    public String getIsoTime(int i) {
                        String f= fields[i];
                        int n1= f.length()-1;
                        if ( f.charAt(0)=='"' && f.charAt(n1)=='"' ) {
                            return f.substring(1,n1);
                        } else {
                            return f;
                        }
                    }

                    @Override
                    public int getInteger(int i) {
                        return Integer.parseInt(fields[i]); 
                    }
                };
            }
        };
    }
    
    /**
     * get the data for the id, within the start and stop times.
     * @param idavail the dataset id
     * @param start iso8601 formatted time
     * @param stop iso8601 formatted time
     * @return
     * @throws IOException 
     */
    public static Iterator<HapiRecord> getData( String idavail, String start, String stop ) throws IOException {
        
        String templ= "https://csa.esac.esa.int/csa-sl-tap/tap/sync?REQUEST=doQuery&LANG=ADQL&FORMAT=CSV&QUERY=SELECT+start_time,end_time,num_instances+FROM+csa.v_dataset_inventory+WHERE+dataset_id='%s'+AND+start_time>='%s'+AND+start_time<'%s'+AND+num_instances>0+ORDER+BY+start_time";
        
        int i= idavail.indexOf("/");
        String id= idavail.substring(0,i);
        
        String url= String.format( templ, id, start, stop );
        logger.log(Level.INFO, "readData URL: {0}", url);
        
        Iterator<String> lines= SourceUtil.getFileLines( new URL(url) );
        if ( lines.hasNext() ) lines.next(); // header line.
        
        return fromStrings( lines );
        
    }
    
    
    private static void printHelp() {
        System.err.println("TapAvailabilitySource [id] [start] [stop]");
        System.err.println("   no arguments will provide the catalog response");
        System.err.println("   if only id is present, then return the info response for the id");
        System.err.println("   if id,start,stop then return the data response.");
    }
    
    public static void main( String[] args ) throws IOException {
        
        switch (args.length) {
            case 0:
                System.out.println( getCatalog() );
                break;
            case 1:
                System.out.println( getInfo(args[0]) );
                break;
            case 3:
                Iterator<HapiRecord> iter = TapAvailabilitySource.getData( args[0], args[1], args[2] );
                if ( iter.hasNext() ) {
                    CsvDataFormatter format= new CsvDataFormatter();
                    try {
                        format.initialize( new JSONObject( getInfo(args[0]) ),System.out,iter.next() );
                    } catch (JSONException ex) {
                        throw new RuntimeException(ex);
                    }
                    do {
                        HapiRecord r= iter.next();
                        format.sendRecord( System.out, r );
                    } while ( iter.hasNext() );
                }   
                break;
            default:
                printHelp();
        }
                
    }
    
}
