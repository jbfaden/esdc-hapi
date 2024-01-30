
package org.esdc.hapi;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.HashSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * Produce the catalog response for the ESDC-SOAR HAPI server
 * 
 * https://soar.esac.esa.int/soar-sl-tap/tap/sync?REQUEST=doQuery&LANG=ADQL&FORMAT=json&QUERY=SELECT+distinct(logical_source),logical_source_description+FROM+soar.v_cdf_plot_metadata+WHERE+logical_source+LIKE%20%27solo_L2_%25%25%27
 * 
 * @author jbf
 */
public class EsdcCatalogSource {
    
    private static final Logger logger= Logger.getLogger("hapi.esdc");
    
    private static JSONObject getOKStatus() throws JSONException {
        JSONObject status = new JSONObject();
        status.put("code", 1200);
        status.put("message", "OK request successful");
        return status;
    }    
    
    /**
     * Read the CSV data from the URL and return it as a JSON catalog response.
     * https://soar.esac.esa.int/soar-sl-tap/tap/sync?"
     *       + "REQUEST=doQuery&LANG=ADQL&FORMAT=CSV"
     *       + "&QUERY=SELECT+distinct(logical_source),logical_source_description"
     *       + "+FROM+soar.v_cdf_dataset"
     *       + "+WHERE+logical_source+LIKE%20%27solo_L2_%25%25%27"
     * @param prefix prefix all ids with this (to support availability)
     * @return JSON catalog response with the schema https://github.com/hapi-server/data-specification-schema/blob/jon-jeremy-mess-3.0/3.1/catalog.json
     * @throws IOException
     * @throws JSONException 
     */
    public static String getCatalog( String prefix ) throws IOException, JSONException {
        URL url = new URL("https://soar.esac.esa.int/soar-sl-tap/tap/sync?"
            + "REQUEST=doQuery&LANG=ADQL&FORMAT=CSV"
            + "&QUERY=SELECT+distinct(logical_source),logical_source_description"
            + "+FROM+soar.v_cdf_dataset"
            + "+WHERE+logical_source+LIKE%20%27solo_L2_%25%25%27");
        HashSet exclude= new HashSet();
        JSONArray catalog = new JSONArray();
        try (InputStream in = url.openStream()) {
            BufferedReader ins = new BufferedReader(new InputStreamReader(in));
            String s = ins.readLine();
            if (s != null) {
                s = ins.readLine(); // skip the first header line
            }
            while (s != null) {
                int i = s.indexOf(",");
                JSONObject jo = new JSONObject();
                String id= s.substring(0, i).trim();
                if (id.startsWith("\"") && id.endsWith("\"")) {
                    id = id.substring(1, id.length() - 1);
                }
                if ( exclude.contains(id) ) {
                    logger.log(Level.FINE, "excluding dataset id {0}", id);
                    s = ins.readLine();
                    continue;
                }
                jo.put("id", prefix + id);
                String t = s.substring(i + 1).trim();
                if (t.startsWith("\"") && t.endsWith("\"")) {
                    t = t.substring(1, t.length() - 1);
                }
                jo.put("title", t);
                catalog.put(catalog.length(), jo);
                s = ins.readLine();
            }
        }
        JSONObject result = new JSONObject();
        result.setEscapeForwardSlashAlways(false);
        result.put("HAPI", "3.0");
        result.put("catalog", catalog);
        result.put("status", getOKStatus());
        result.put("x_tap_url",url);    

        if (logger.isLoggable(Level.FINER)) {
            logger.finer(result.toString(4));
        }

        return result.toString(4);

    }
    
    /**
     * return the catalog response by parsing the XML returned by 
     * https://soar.esac.esa.int/soar-sl-tap/tap/tables.
     * @return
     * @throws IOException 
     */
    public static String getCatalog() throws IOException {
        try {
            return getCatalog( "" );
        } catch ( IOException | JSONException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static void main( String[] args ) throws IOException {
        System.err.println(EsdcCatalogSource.getCatalog() );
    }
}
