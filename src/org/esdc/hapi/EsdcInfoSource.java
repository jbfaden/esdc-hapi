package org.esdc.hapi;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.hapiserver.source.SourceUtil;

/**
 * provide info responses to the server.  this is based on
 * https://soar.esac.esa.int/soar-sl-tap/tap/sync
 *   ?REQUEST=doQuery&LANG=ADQL&FORMAT=json
 *   &QUERY=select%20*%20from%20soar.v_cdf_plot_metadata%20where%20logical_source%20=%20%27solo_L2_rpw-lfr-surv-swf-b%27
 * @author jbf
 */
public class EsdcInfoSource {
    
    public static String getInfo( String id ) throws IOException, JSONException {
        try {
            String urlString= "https://soar.esac.esa.int/soar-sl-tap/tap/sync?REQUEST=doQuery&LANG=ADQL&FORMAT=json&QUERY=select%20*%20from%20soar.v_cdf_plot_metadata%20where%20logical_source%20=%20%27"+id+"%27";
            String tapJsonSrc= SourceUtil.getAllFileLines( new URL(urlString) );
            
            JSONObject tapResponse= new JSONObject(tapJsonSrc);
            
            JSONObject result= new JSONObject();
            JSONArray parameters= new JSONArray();
            JSONObject parameter= new JSONObject();
            parameter.put("name","Epoch");
            parameter.put("type","isotime");
            
            parameters.put( 0, parameter );
                
            JSONArray tapParameters= tapResponse.getJSONArray("data");
            
            for ( int i=0; i<tapParameters.length(); i++ ) {
                JSONArray tapParameter= tapParameters.getJSONArray(i);
                
                parameter= new JSONObject();
                parameter.put( "name", tapParameter.getString(23) );
                parameter.put( "type", "float" );
                String size= tapParameter.getString(25);
                if ( size!=null ) {
                    int i2= size.lastIndexOf(",");
                    parameter.put( "size", "["+size+"]" );
                }
                parameters.put( i+1, parameter );
            }
            
            result.put("parameters",parameters);
            result.put("startDate","2023-01-01T00:00Z");
            result.put("stopDate","2024-01-01T00:00Z");
            result.put("HAPI","3.1");
            
            return result.toString(4);
            
        } catch (MalformedURLException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    public static void main( String[] args ) throws IOException, JSONException {
        System.err.println( getInfo("solo_L2_rpw-lfr-surv-cwf-b"));
    }
}
