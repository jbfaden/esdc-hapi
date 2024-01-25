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
            
            // Here's what we need: depend0, var_name, var_sizes
            String svr= "https://soar.esac.esa.int/soar-sl-tap/tap/sync";
            String select= "depend0,var_name,var_sizes,var_units";
            String urlString= svr + "?REQUEST=doQuery&LANG=ADQL&FORMAT=json&QUERY=select%20"+select+"%20from%20soar.v_cdf_plot_metadata%20where%20logical_source%20=%20%27"+id+"%27";
            String tapJsonSrc= SourceUtil.getAllFileLines( new URL(urlString) );
            
            JSONObject tapResponse= new JSONObject(tapJsonSrc);
            JSONArray tapParameters= tapResponse.getJSONArray("data");
            
            JSONObject result= new JSONObject();
            JSONArray parameters= new JSONArray();
            JSONObject parameter= new JSONObject();
            
            String depend0name= tapParameters.getJSONArray(0).getString(0);
            
            parameter.put("name",depend0name);
            parameter.put("type","isotime");
            parameter.put("length","24");
            parameter.put("units","UTC");
            
            parameters.put( 0, parameter );
                
            
            for ( int i=0; i<tapParameters.length(); i++ ) {
                JSONArray tapParameter= tapParameters.getJSONArray(i);
                
                parameter= new JSONObject();
                parameter.put( "name", tapParameter.getString(1) );
                parameter.put( "type", "double" );
                String size= tapParameter.getString(2);
                if ( size!=null ) {
                    JSONArray sizeArray= new JSONArray();
                    String[] ss= size.split(",");
                    for ( int j=0; j<ss.length; j++ ) {
                        sizeArray.put(j,Integer.parseInt(ss[j]));
                    }
                    parameter.put( "size", sizeArray );
                }
                parameter.put( "units", tapParameter.getString(3) );
                parameters.put( i+1, parameter );
            }
            
            result.put("parameters",parameters);
            result.put("startDate","2023-01-01T00:00Z");
            result.put("stopDate","2024-01-01T00:00Z");

            result.put("sampleStartDate","2023-09-01T00:00Z");
            result.put("sampleStopDate","2023-09-02T00:00Z");
            
            result.put("HAPI","3.1");
            
            return result.toString(4);
            
        } catch (MalformedURLException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    public static void main( String[] args ) throws IOException, JSONException {
        //System.err.println( getInfo("solo_L2_rpw-lfr-surv-cwf-b"));
        System.err.println( getInfo("solo_L2_mag-rtn-normal"));
    }
}
