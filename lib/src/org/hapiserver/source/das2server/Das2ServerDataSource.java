package org.hapiserver.source.das2server;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.hapiserver.AbstractHapiRecordSource;
import org.hapiserver.HapiRecord;
import org.hapiserver.HapiRecordSource;
import org.hapiserver.TimeUtil;
import org.hapiserver.source.SourceUtil;
import org.xml.sax.SAXException;

/**
 * Read data from the Das2Server to create a HapiRecordSource.  This source
 * only produces complete records (ParamSubset=False).
 * 
 * For reference, here is a Das2Server URL:
 * https://jupiter.physics.uiowa.edu/das/server?server=dataset&start_time=2021-04-15T00%3A00%3A00.000Z&end_time=2021-04-17T00%3A00%3A00.000Z&dataset=Juno%2FEphemeris%2FJovicentric
 * @author jbf
 */
public class Das2ServerDataSource extends AbstractHapiRecordSource {

    private static final Logger logger = Logger.getLogger("hapi.das2server");

    private String id;
    private String das2server;

    JSONObject info;
    JSONObject data;
    String root;

    private Das2ServerDataSource(String das2server, String id, JSONObject info) {
        logger.entering("CdawebServicesHapiRecordSource", "constructor");
        this.das2server = das2server;
        this.id = id;
        this.info = info;
        logger.exiting("CdawebServicesHapiRecordSource", "constructor");
    }

    /**
     * the server will call this method to get the record source.
     *
     * @param config
     * @param id
     * @param info
     * @return
     * @throws java.io.IOException
     * @throws org.codehaus.jettison.json.JSONException
     */
    public static HapiRecordSource getRecordSource(String config, String id, JSONObject info) throws IOException, JSONException {
        URL url = Das2ServerCatalogSource.class.getResource(config);
        String configJson = SourceUtil.getAllFileLines(url);
        JSONObject jo = new JSONObject(configJson);
        String das2server = jo.getString("server");
        return new Das2ServerDataSource(das2server, id, info);
    }

    @Override
    public boolean hasGranuleIterator() {
        return false;
    }

    @Override
    public boolean hasParamSubsetIterator() {
        return false;
    }

    @Override
    public Iterator<HapiRecord> getIterator(int[] start, int[] stop) {
        String sstart= TimeUtil.formatIso8601TimeBrief(start);
        String sstop= TimeUtil.formatIso8601TimeBrief(stop);
        StringBuilder url= new StringBuilder(this.das2server);
        url.append("?server=dataset&dataset=").append(id).append("&start_time=").append(sstart).append("&end_time=").append(sstop);
        int interval= info.optInt("x_interval",0);
        if ( interval>0 ) {
            url.append("&interval=").append(interval);
        }
    url.append("&ascii=true");
        InputStream ins;
        try {
            URL das2StreamUrl= new URL( url.toString() );
            ins= das2StreamUrl.openStream();
            return new Das2StreamParser(ins).getHapiRecordIterator();
        } catch ( MalformedURLException ex ) {
            throw new RuntimeException(ex);
        } catch (IOException | SAXException | ParserConfigurationException | XPathExpressionException ex) {
            throw new RuntimeException(ex);
        }


    }

}
