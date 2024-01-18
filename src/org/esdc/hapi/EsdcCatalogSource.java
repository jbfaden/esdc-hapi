
package org.esdc.hapi;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.hapiserver.source.SourceUtil;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

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
            + "REQUEST=doQuery&LANG=ADQL&FORMAT=CSV"
            + "&QUERY=SELECT+distinct(logical_source),logical_source_description"
            + "+FROM+soar.v_cdf_dataset"
            + "+WHERE+logical_source+LIKE%20%27solo_L2_%25%25%27"
     * @return JSON catalog response with the schema https://github.com/hapi-server/data-specification-schema/blob/jon-jeremy-mess-3.0/3.1/catalog.json
     * @throws IOException
     * @throws JSONException 
     */
    private static String getCatalogCsv( ) throws IOException, JSONException {
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
                jo.put("id", id);
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
            if ( true ) {
                return getCatalogCsv( );
            } else {
                // "https://csa.esac.esa.int/csa-sl-tap/tap/sync?REQUEST=doQuery&LANG=ADQL&FORMAT=CSV&QUERY=SELECT+dataset_id,title+FROM+csa.v_dataset";
                // "https://soar.esac.esa.int/soar-sl-tap/tap/sync?REQUEST=doQuery&LANG=ADQL&FORMAT=CSV&QUERY=SELECT+distinct(logical_source),logical_source_description+FROM+soar.v_cdf_dataset+WHERE+logical_source+LIKE%20%27solo_L2_%25%25%27";

                URL url= new URL("https://soar.esac.esa.int/soar-sl-tap/tap/tables");
                //URL url= new File("/home/jbf/temp/esdc-hapi/src/org/esdc/hapi/tables-2.xml").toURI().toURL();
                Document doc= SourceUtil.readDocument(url);
                XPathFactory factory = XPathFactory.newInstance();
                XPath xpath = (XPath) factory.newXPath();
                NodeList nodes = (NodeList) xpath.evaluate( "//tableset/schema/table/name/text()", doc, XPathConstants.NODESET );

                int ic= 0;
                JSONArray catalog= new JSONArray();
                for ( int i=0; i<nodes.getLength(); i++ ) {
                    Node node= nodes.item(i);
                    NamedNodeMap attrs= node.getAttributes();

                    if ( true ) {
                        String name= node.getTextContent();

                        if ( true ) {
                            JSONObject jo= new JSONObject();
                            jo.put( "id", name );
                            catalog.put( ic++, jo );
                        }
                    }

                }

                JSONObject result= new JSONObject();
                result.put( "catalog", catalog );
                return result.toString(4);
            }
        } catch (MalformedURLException | SAXException | ParserConfigurationException | XPathExpressionException | JSONException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static void main( String[] args ) throws IOException {
        System.err.println( getCatalog() );
    }
}
