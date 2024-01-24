
package org.esdc.hapi;

import java.io.IOException;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * returns an info response for availability--just shows what files are found when.
 * @author jbf
 */
public class EsdcAvailabilityInfoSource {
        
    public static String getCatalog( ) {
        try {
            return EsdcCatalogSource.getCatalogCsv("availability/");
        } catch ( IOException | JSONException ex ) {
            throw new RuntimeException(ex);
        }
    }
    
    public static String getInfo( String id ) {
        //String uri= "SELECT+begin_time,end_time,filepath,filename+FROM+"+id+"+WHERE+instrument='MAG'+AND+level='L2'+ORDER+BY+begin_time";
        //return null;
        try {
            JSONObject result= new JSONObject();
            JSONArray parameters= new JSONArray();

            JSONObject parameter;
            
            parameter = new JSONObject();
            parameter.put("name","start");
            parameter.put("type","isotime");
            parameter.put("length","24");
            parameters.put( 0, parameter );

            parameter= new JSONObject();
            parameter.put("name","stop");
            parameter.put("type","isotime");
            parameter.put("length","24");
            parameters.put( 1, parameter );
            
            parameter= new JSONObject();
            parameter.put("name","filename");
            parameter.put("type","string");
            parameter.put("length","40");
            parameters.put( 2, parameter );
                                    
            result.put("parameters",parameters);
            
            result.put("startDate","2023-01-01T00:00Z");
            result.put("stopDate","2024-01-01T00:00Z");

            result.put("sampleStartDate","2023-09-01T00:00Z");
            result.put("sampleStopDate","2023-10-01T00:00Z");
            
            result.put("HAPI","3.1");
            
            return result.toString(4);
        } catch ( Exception e ) {
            throw new RuntimeException(e);
        }
    }
}
