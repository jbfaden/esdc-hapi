
package org.esdc.hapi;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.hapiserver.TimeUtil;
import org.hapiserver.source.SourceUtil;

/**
 * returns an info response for availability--just shows what files are found when.  This
 * tests for the extent of each data set, and also returns an sampleStartDate and sampleStopDate
 * of the last data granule.
 * @author jbf
 */
public class EsdcAvailabilityInfoSource {
        
    public static String getCatalog( ) {
        try {
            return EsdcCatalogSource.getCatalog("availability/");
        } catch ( IOException | JSONException ex ) {
            throw new RuntimeException(ex);
        }
    }
    
    static Map<String,String[]> extents;
    
    /**
     * return the extent of data coverage for the id.  This makes two calls to the TAP server,
     * getting the first and last files of a time-sorted list of files.
     * @param id the SOAR ID
     * @return a four-element string containing the start, stop, sampleStart, and sampleStop.
     */
    public static String[] getExtent( String id ) {
        
        // put a little cache of extents to aid in development.  TODO: This should be removed at some point before
        // putting this into production.
        synchronized ( EsdcAvailabilityInfoSource.class ) {
            if ( extents==null ) {
                HashMap extents1= new HashMap<>();
                InputStream ins= EsdcAvailabilityInfoSource.class.getResourceAsStream("id.extent.sample.time.txt");
                try ( BufferedReader read= new BufferedReader( new InputStreamReader(ins) ) ) {
                    String s= read.readLine();
                    while ( s!=null ) {
                        if ( s.trim().length()>0 && !s.startsWith("#") ) { // a record
                            String[] ss= new String[4];
                            String[] ss1= s.split(",",-2);
                            if ( ss1.length==5 ) {
                                System.arraycopy( ss1, 1, ss, 0, 4 );
                            }
                            extents1.put(ss1[0],ss);
                        }
                        s= read.readLine();                        
                    }
                } catch ( IOException ex ) {
                    
                }
                extents= extents1;
            }
        }
        
        String[] r= extents.get(id);
        if ( r!=null ) {
            return r;
        }
        
        try {
            
            // SELECT TOP 1 begin_time,end_time,filename,data_item_id FROM v_sc_data_item WHERE data_item_id LIKE 'solo_L2_swa-pas-grnd-mom* ORDER BY begin_time'
            //          'https://soar.esac.esa.int/soar-sl-tap/tap/sync?REQUEST=doQuery&LANG=ADQL&FORMAT=csv&QUERY=SELECT+TOP+1+begin_time,end_time,filename,data_item_id+FROM+v_sc_data_item+WHERE+data_item_id+LIKE+%27solo_L2_swa-pas-grnd-mom%25%27ORDER+BY+begin_time'
            //          'https://soar.esac.esa.int/soar-sl-tap/tap/sync?REQUEST=doQuery&LANG=ADQL&FORMAT=csv&QUERY=SELECT+TOP+1+begin_time,end_time,filename,data_item_id+FROM+v_sc_data_item+WHERE+data_item_id+LIKE+%27solo_L2_swa-pas-grnd-mom%25%27ORDER+BY+begin_time+DESC'
            String url;
            Iterator<String> iter;
            String[] fields;
            
            String startTime, stopTime;
            
            url = "https://soar.esac.esa.int/soar-sl-tap/tap/sync?REQUEST=doQuery&LANG=ADQL&FORMAT=csv&QUERY=SELECT+TOP+1+begin_time,end_time,filename,data_item_id+FROM+v_sc_data_item+WHERE+data_item_id+LIKE+%27"+id+"%25%27+ORDER+BY+begin_time+ASC";
            
            BufferedReader reader= new BufferedReader( new InputStreamReader( new URL(url).openStream() ) );
            
            String line= reader.readLine();
            
            if ( !line.startsWith("begin_time") ) {
                throw new IllegalArgumentException("expected begin_time");
            } 
            
            String firstGranule= reader.readLine();
            fields= SourceUtil.stringSplit(firstGranule);
            startTime= fields[0];
            
            url = "https://soar.esac.esa.int/soar-sl-tap/tap/sync?REQUEST=doQuery&LANG=ADQL&FORMAT=csv&QUERY=SELECT+TOP+1+begin_time,end_time,filename,data_item_id+FROM+v_sc_data_item+WHERE+data_item_id+LIKE+%27"+id+"%25%27+ORDER+BY+begin_time+DESC";
            reader= new BufferedReader( new InputStreamReader( new URL(url).openStream() ) );
            line= reader.readLine();
            if ( !line.startsWith("begin_time") ) {
                throw new IllegalArgumentException("expected begin_time");
            } 
            String lastGranule= reader.readLine();
            fields= SourceUtil.stringSplit(lastGranule);
            stopTime= fields[1];
            
            String sampleStartTime= fields[0];
            String sampleStopTime= fields[1];
            
            return new String[] { startTime, stopTime, sampleStartTime, sampleStopTime };
            
            
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    public static String getInfo( String availId ) {
        // get the start and stop of the mission, using TAP "TOP" keyword.
        
        String id= availId.substring("availability/".length());
        String[] extent= getExtent(id);
        
        try {
            JSONObject result= new JSONObject();
            JSONArray parameters= new JSONArray();

            JSONObject parameter;
            
            parameter = new JSONObject();
            parameter.put("name","start");
            parameter.put("type","isotime");
            parameter.put("length",24);
            parameter.put("units","UTC");
            parameter.put("fill", org.codehaus.jettison.json.JSONObject.NULL);
            parameters.put( 0, parameter );

            parameter= new JSONObject();
            parameter.put("name","stop");
            parameter.put("type","isotime");
            parameter.put("length",24);
            parameter.put("units","UTC");
            parameter.put("fill", org.codehaus.jettison.json.JSONObject.NULL);
            parameters.put( 1, parameter );
            
            parameter= new JSONObject();
            parameter.put("name","filename");
            parameter.put("type","string");
            parameter.put("length",40);
            parameter.put("units", org.codehaus.jettison.json.JSONObject.NULL);
            parameter.put("fill", org.codehaus.jettison.json.JSONObject.NULL);
            parameters.put( 2, parameter );
                                    
            result.put("parameters",parameters);
            
            result.put("startDate",TimeUtil.reformatIsoTime("2000-01-01T00:00:00.000Z", extent[0]));
            result.put("stopDate",TimeUtil.reformatIsoTime("2000-01-01T00:00:00.000Z", extent[1]));

            result.put("sampleStartDate",TimeUtil.reformatIsoTime("2000-01-01T00:00:00.000Z", extent[2]));
            result.put("sampleStopDate",TimeUtil.reformatIsoTime("2000-01-01T00:00:00.000Z", extent[3]));
    
            result.put("HAPI","3.1");
            result.put("x_version","20240215_0905");
            
            return result.toString(4);
        } catch ( Exception e ) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * solo_L2a_swa-eas1-nm3d-psd fails for some reason.
     */
    public static void main( String[] args ) throws IOException, JSONException, InterruptedException {
        File out= new File("/home/jbf/tmp/out.availability.dat");
        PrintStream p= new PrintStream( new FileOutputStream(out) );
        JSONObject jo= new JSONObject( EsdcCatalogSource.getCatalog() );
        JSONArray cc= jo.getJSONArray("catalog");
        for ( int i=0; i<cc.length(); i++ ) {
            String id= cc.getJSONObject(i).getString("id");
            System.err.println("Loading sample time and file for "+id);
            try {
                String[] ss= EsdcAvailabilityInfoSource.getExtent(id);
                p.println( String.format( "%s,%s,%s,%s,%s", id, ss[0], ss[1], ss[2], ss[3] ) );
                EsdcRecordSource recsrc= new EsdcRecordSource("solo_L2_epd-ept-south-rates",null);
                System.err.println( recsrc.getSampleCdfFile() );
                Thread.sleep(1000);
            } catch ( Exception ex ) {
                ex.printStackTrace();
                System.err.println("fail: "+id);
            }
        }
        
        p.close();
    }
}
