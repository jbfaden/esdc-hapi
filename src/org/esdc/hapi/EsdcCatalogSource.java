
package org.esdc.hapi;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
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
 *
 * @author jbf
 */
public class EsdcCatalogSource {
    
    /**
     * return the catalog response by parsing the XML returned by 
     * https://soar.esac.esa.int/soar-sl-tap/tap/tables.
     * @return
     * @throws IOException 
     */
    public static String getCatalog() throws IOException {
        try {
            // "https://csa.esac.esa.int/csa-sl-tap/tap/sync?REQUEST=doQuery&LANG=ADQL&FORMAT=CSV&QUERY=SELECT+dataset_id,title+FROM+csa.v_dataset";
            //URL url= new URL("https://soar.esac.esa.int/soar-sl-tap/tap/tables");
            URL url= new File("/home/jbf/temp/esdc-hapi/src/org/esdc/hapi/tables-2.xml").toURI().toURL();
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
        } catch (MalformedURLException | SAXException | ParserConfigurationException | XPathExpressionException | JSONException ex) {
            throw new RuntimeException(ex);
        }
        
    }

    public static void main( String[] args ) throws IOException {
        System.err.println( getCatalog() );
    }
}
