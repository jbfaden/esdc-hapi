
package org.hapiserver.source.das2server;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.hapiserver.source.SourceUtil;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

/**
 * https://jupiter.physics.uiowa.edu/das/server?server=dataset&dataset=Juno/JED/ElectronSpectra&start_time=2013-10-09&end_time=2013-10-10
 * @author jbf
 */
public class Das2ServerInfoSource {
    
    public static int STANDARD_STEP_SIZE_SECONDS=30;
    
    private static String[] parseTimeRange( String timerange ) {
        int i= timerange.indexOf("|");
        if ( i>-1 ) {
            timerange= timerange.substring(0,i);
        }
        String[] ss= timerange.split("to");
        return new String[] { ss[0].trim(), ss[1].trim() };
    }
        
    /**
     * this is going to make a request to the Das2Server, and will take the Das2Stream response and convert it to
     * a HAPI header.
     * 
     * @param config
     * @param id the dataset id
     * @return
     * @throws IOException
     * @throws JSONException
     * @throws SAXException
     * @throws ParserConfigurationException
     * @throws XPathExpressionException 
     */
    public static String getInfo( String config, String id ) throws IOException, JSONException, SAXException, ParserConfigurationException, XPathExpressionException {
        URL url= Das2ServerCatalogSource.class.getResource(config);
        String configJson= SourceUtil.getAllFileLines(url);
        JSONObject jo = new JSONObject(configJson);
        String das2server= jo.getString("server");
        String dsdf= SourceUtil.getAllFileLines( new URL(das2server+"?server=dsdf&dataset="+id ) );
        String xml= dsdf.substring(10);
        Document doc= SourceUtil.readDocument(xml);
        XPath xpath= XPathFactory.newInstance().newXPath();
        String testRange = xpath.compile("//stream/properties/@testRange").evaluate(doc);
        String testInterval = xpath.compile("//stream/properties/@testInterval").evaluate(doc);
        if ( testRange.length()==0 ) {
            testRange= xpath.compile("//stream/properties/@exampleRange").evaluate(doc);
            testInterval= xpath.compile("//stream/properties/@exampleInterval").evaluate(doc);
        }
        if ( testRange.length()==0 ) {
            testRange= xpath.compile("//stream/properties/@exampleRange_00").evaluate(doc);
            testInterval= xpath.compile("//stream/properties/@exampleInterval_00").evaluate(doc);
        }
        StringBuilder dataRequest= new StringBuilder(das2server).append("?server=dataset&dataset=").append(id);
        if ( testRange.length()>0 ) {
            String[] isoTimes= parseTimeRange(testRange);
            dataRequest.append("&").append("start_time=").append(isoTimes[0]).append("&end_time=").append(isoTimes[1]);
            if ( testInterval.length()>0 ) {
                dataRequest.append("&").append("interval=").append(STANDARD_STEP_SIZE_SECONDS);
            }
        } else {
            throw new IllegalArgumentException("unable to identify time range to download example.");
        }
    dataRequest.append("&ascii=true"); // TODO: support for native types
        JSONObject result= parseDas2StreamForInfo( new URL(dataRequest.toString()) );
        if ( testInterval.length()>0 ) {
            dataRequest.append("&interval=").append(testInterval);
            result.put("x_interval",STANDARD_STEP_SIZE_SECONDS);
        }
        String[] validRange = parseTimeRange( xpath.compile("//stream/properties/@validRange").evaluate(doc) );
        result.put("startDate",validRange[0]);
        result.put("stopDate", validRange[1]);
        
        String[] exampleRange = parseTimeRange( xpath.compile("//stream/properties/@exampleRange_00").evaluate(doc) );
        result.put("sampleStartDate",exampleRange[0]);
        result.put("sampleStopDate", exampleRange[1]);
        
        return result.toString(4);
        
    }
    
    /**
     * can we make a light-weight das2stream parser which gets the description from the header.  For the Das2Servers, the contents
     * of the stream is not known until a request is made.
     * @param das2StreamUrl
     * @return 
     */
    public static JSONObject parseDas2StreamForInfo( URL das2StreamUrl ) throws IOException, SAXException, ParserConfigurationException, XPathExpressionException {
        InputStream ins= das2StreamUrl.openStream();
        JSONObject result= new Das2StreamParser(ins).getInfo();
        return result;
    }
    
    public static void main( String[] args ) throws Exception {
        //args= new String[] { "AC_AT_DEF" };
        args= new String[0];
        
        if ( args.length==0 ) {
            System.out.println( Das2ServerInfoSource.getInfo("jupiter-d2s.json","Juno/Ephemeris/Jovicentric") );
        } 
    }
}
