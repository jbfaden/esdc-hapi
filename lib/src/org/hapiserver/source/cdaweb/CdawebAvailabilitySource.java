
package org.hapiserver.source.cdaweb;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
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
import org.hapiserver.AbstractHapiRecord;
import org.hapiserver.AbstractHapiRecordSource;
import org.hapiserver.CsvDataFormatter;
import org.hapiserver.HapiRecord;
import org.hapiserver.TimeUtil;
import org.hapiserver.source.AggregationGranuleIterator;
import org.hapiserver.source.SourceUtil;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * return availability, showing when file granules are found.
 * @author jbf
 */ 
public class CdawebAvailabilitySource extends AbstractHapiRecordSource {

    private static final Logger logger= Logger.getLogger("hapi.cdaweb");
    
    /**
     * the field containing the partial filename.
     */
    public static int FIELD_FILENAME= 2;

    String spid;
    int rootlen;
    String root;
    
    public CdawebAvailabilitySource(  String hapiHome, String idavail, JSONObject info, JSONObject data ) {
        int i= idavail.indexOf("/");
        spid= idavail.substring(i+1);
        try {
            JSONArray array= info.getJSONArray("parameters");
            JSONObject p= array.getJSONObject(2); // the filename parameter
            JSONObject stringType= p.getJSONObject("x_stringType");
            JSONObject urin= stringType.getJSONObject("uri");
            rootlen= urin.getString("base").length();
            if ( !urin.getString("base").contains("sp_phys/") ) {
                rootlen= rootlen + 4; //TODO: Bernie's server says "sp_phys" while all.xml says "pub".
            }
            root= urin.getString("base");
        } catch (JSONException ex) {
            throw new RuntimeException(ex);
        }
        
    }
    
    /**
     * return the root for references in availability response
     * @return 
     */
    public String getRoot() {
        return this.root;
    }
    
    /**
     * get the catalog
     * @return
     * @throws IOException 
     */
    public static String getCatalog() throws IOException {
        try {
            String catalogString= CdawebInfoCatalogSource.getCatalog();
            JSONObject catalogContainer= new JSONObject(catalogString);
            JSONArray catalog= catalogContainer.getJSONArray("catalog");
            int n= catalog.length();
            for ( int i=0; i<n; i++ ) {
                JSONObject jo= catalog.getJSONObject(i);
                jo.setEscapeForwardSlashAlways(false);
                jo.put( "id", "availability/" + jo.getString("id") );
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
     * return a sample time for the id.  This will be the last full month containing data.  If all
     * data is contained within just one month, then this is that month.
     * 
     * @param id the data id, such as AC_H1_EPM
     * @return null or an iso8601 range.
     */
    public static String getSampleTime(String id) {
        String range= CdawebInfoCatalogSource.coverage.get(id);
        if ( range==null ) {
            try {
                CdawebInfoCatalogSource.getCatalog20230629();
                range= CdawebInfoCatalogSource.coverage.get(id);
            } catch (IOException ex) {
                logger.log(Level.SEVERE, null, ex);
            }
            if ( range==null ) {
                logger.fine("Expect sample times in catalog");
                return null;
            }
        }
        try {
            int[] irange= TimeUtil.parseISO8601TimeRange(range);
            int[] startTime= TimeUtil.getStartTime(irange);
            int[] stopTime= TimeUtil.getStopTime(irange);
            stopTime[2]=1;
            stopTime[3]=0;
            stopTime[4]=0;
            stopTime[5]=0;
            stopTime[6]=0;
            if ( TimeUtil.gt( startTime, stopTime ) ) { // whoops, went back too far
                stopTime[1]= stopTime[1]+1;
                TimeUtil.normalizeTime(stopTime);
            }
            startTime= TimeUtil.subtract( stopTime, new int[] { 0, 1, 0, 0, 0, 0, 0 } );
            return TimeUtil.formatIso8601TimeBrief( startTime ) + "/" +  TimeUtil.formatIso8601TimeBrief( stopTime );
        } catch (ParseException ex) {
            logger.log(Level.SEVERE, null, ex);
            return null;
        }
    }
    
    /**
     * get the info for the id.
     * @param idavail the dataset id, ending in "/availability"
     * @return 
     */
    public static String getInfo( String idavail ) {
        int i= idavail.indexOf("/");
        String id= idavail.substring(i+1);
        String sampleTime= getSampleTime(id);
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
        String root;
        int filenameLen=0;
        String filenaming= CdawebInfoCatalogSource.filenaming.get(id);
        int iroot= filenaming.indexOf("%");
        iroot= filenaming.lastIndexOf("/",iroot);
        root= filenaming.substring(0,iroot+1);
        for ( int ii=iroot; ii<filenaming.length(); ii++ ) {
            if ( filenaming.charAt(ii)=='%' ) {
                filenameLen+=1;
                char f= filenaming.charAt(ii+1);
                switch (f) {
                    case 'Y':
                        filenameLen+=4;
                        break;
                    case 'Q':
                        filenameLen+=4; // we don't really know, unfortunately.
                        break;
                    default:
                        filenameLen+=2;
                        break;
                }
                ii=ii+1;
            } else {
                filenameLen+=1;
            }
        }
        
        String stringType= "{ \"uri\": { \"base\": \"" + root + "\" } }";
        
        String startDate="1980-01-01";
        String stopDate="lastmonth";
        
        String coverage= CdawebInfoCatalogSource.coverage.get(id);
        if ( coverage!=null ) {
            String[] ss= coverage.split("/");
            startDate= ss[0];
            stopDate= ss[1];
        } 
        
        return "{\n" +
"    \"HAPI\": \"3.1\",\n" +
"    \"modificationDate\": \"2023-05-12T13:26:43.835Z\",\n" +
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
"            \"name\": \"filename\",\n" +
"            \"type\": \"string\",\n" +
"            \"x_stringType\":" + stringType + ",\n" +
"            \"length\": "+filenameLen + ",\n" +
"            \"units\": null\n" +
"        }\n" +
"    ],\n" +
"    \"sampleStartDate\": \""+ sampleStartDate + "\",\n" +
"    \"sampleStopDate\": \""+ sampleStopDate + "\",\n" +
"    \"startDate\": \""+startDate+"\",\n" +
"    \"status\": {\n" +
"        \"code\": 1200,\n" +
"        \"message\": \"OK request successful\"\n" +
"    },\n" +
"    \"stopDate\": \""+stopDate+"\"\n" +
"}";
    }    
    
    @Override
    public boolean hasGranuleIterator() {
        return true;
    }
    
    @Override
    public Iterator<int[]> getGranuleIterator(int[] start, int[] stop) {
        return new AggregationGranuleIterator( "$Y-$m", start, stop );
    }

    @Override
    public boolean hasParamSubsetIterator() {
        return false;
    }

    @Override
    public Iterator<HapiRecord> getIterator(int[] start, int[] stop) {
        
        try {
            
            String sstart= String.format( "%04d%02d%02dT%02d%02d%02dZ", start[0], start[1], start[2], start[3], start[4], start[5] );
            String sstop= String.format( "%04d%02d%02dT%02d%02d%02dZ", stop[0], stop[1], stop[2], stop[3], stop[4], stop[5] );
            
            URL url = new URL(String.format( CdawebInfoCatalogSource.CDAWeb + "WS/cdasr/1/dataviews/sp_phys/datasets/%s/orig_data/%s,%s", spid, sstart, sstop) );
            
            logger.log(Level.INFO, "readData URL: {0}", url);
            
            System.out.println("url: "+url );
            
            try {
                Document doc= SourceUtil.readDocument( url );
                XPathFactory factory = XPathFactory.newInstance();
                XPath xpath = (XPath) factory.newXPath();
                NodeList starts = (NodeList) xpath.evaluate( "//DataResult/FileDescription/StartTime", doc, XPathConstants.NODESET );
                NodeList stops = (NodeList) xpath.evaluate( "//DataResult/FileDescription/EndTime", doc, XPathConstants.NODESET );
                NodeList files = (NodeList) xpath.evaluate( "//DataResult/FileDescription/Name", doc, XPathConstants.NODESET );
                //NodeList lengths = (NodeList) xpath.evaluate( "//DataResult/FileDescription/Length", doc, XPathConstants.NODESET );
                return fromNodes( starts, stops, rootlen, files );
            } catch (IOException | SAXException | ParserConfigurationException | XPathExpressionException ex) {
                throw new RuntimeException(ex);
            }
        } catch (MalformedURLException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    
    private static Iterator<HapiRecord> fromNodes( final NodeList starts, final NodeList stops, int rootlen, final NodeList files ) {
        final int len= starts.getLength();
                
        return new Iterator<HapiRecord>() {
            int i=0;
            
            @Override
            public boolean hasNext() {
                return i<len;
            }

            @Override
            public HapiRecord next() {
                String[] fields= new String[] { starts.item(i).getTextContent(), stops.item(i).getTextContent(), files.item(i).getTextContent() };
                i=i+1;
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

                    @Override
                    public String getString(int i) {
                        return fields[i].substring(rootlen);
                    }
                    
                };
            }
        };
    }
    
    private static void printHelp() {
        System.err.println("TapAvailabilitySource [id] [start] [stop]");
        System.err.println("   no arguments will provide the catalog response");
        System.err.println("   if only id is present, then return the info response for the id");
        System.err.println("   if id,start,stop then return the data response.");
    }
    
    public static void main( String[] args ) throws IOException, ParseException {
        
        //args= new String[] { };
        //args= new String[] { "availability/AC_K1_SWE" };
        //args= new String[] { "availability/BAR_1A_L2_SSPC" };
        //args= new String[] { "availability/AC_K1_SWE", "2022-01-01T00:00Z", "2023-05-01T00:00Z" };
        args= new String[] { "availability/RBSP-A-RBSPICE_LEV-2_ESRHELT", "2014-01-01T00:00Z", "2014-02-01T00:00Z" };
        
        switch (args.length) {
            case 0:
                System.out.println( getCatalog() );
                break;
            case 1:
                System.out.println( getInfo(args[0]) );
                break;
            case 3:
                JSONObject info;
                try {
                    info= new JSONObject( getInfo(args[0]) );
                } catch (JSONException ex) {
                    throw new RuntimeException(ex);
                }
                Iterator<HapiRecord> iter = 
                        new CdawebAvailabilitySource("",args[0],info,null).getIterator( 
                                TimeUtil.parseISO8601Time(args[1]), 
                                TimeUtil.parseISO8601Time(args[2]) );
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
