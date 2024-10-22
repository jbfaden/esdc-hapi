package org.hapiserver.source.tap;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.hapiserver.TimeUtil;
import org.hapiserver.source.SourceUtil;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * returns catalog and info responses for the TAP server at ESAC. Requests are made to https://csa.esac.esa.int/csa-sl-tap/tap/sync
 * and reformatted into HAPI responses. This can be used on the command line (see main method) or the getCatalog and getInfo methods
 * can be called using the classpath source.
 *
 * Here is an example configuration:
 * <pre>
 * {
 *     "HAPI": "3.0",
 *     "catalog": [
 *         {
 *             "x_group_id": "csa",
 *             "x_source": "classpath",
 *             "x_class": "org.hapiserver.source.tap.CsaInfoCatalogSource",
 *             "x_method": "getCatalog",
 *             "x_config": {
 *                 "info": {
 *                     "x_source":"classpath",
 *                     "x_class":"org.hapiserver.source.tap.CsaInfoCatalogSource",
 *                     "x_method": "getInfo",
 *                     "x_args": [ "${id}" ]
 *                 },
 *                 "data": {
 *                     "source": "classpath",
 *                     "class":"org.hapiserver.source.tap.TAPDataSource",
 *                     "args":["https://csa.esac.esa.int/csa-sl-tap/","${id}"]
 *                 }
 *             }
 *         }
 *     ],
 *     "status": {
 *         "code": 1200,
 *         "message": "OK request successful"
 *     }
 * }
 * </pre>
 *
 * @author jbf
 */
public class CsaInfoCatalogSource {

    private static final Logger logger = Logger.getLogger("hapi.cef");

    private static Map<String,String> sampleTimes= new HashMap<>();
    
    static {
        long t0= System.currentTimeMillis();
        try {
            String[] avails= new String[] { "esac.avail.C1.txt", "esac.avail.C2.txt", "esac.avail.C3.txt", "esac.avail.C4.txt",
                "esac.avail.D1.txt", "esac.avail.D2.txt" };
            for ( String avail : avails ) {
                Iterator<String> lines= SourceUtil.getFileLines( CsaInfoCatalogSource.class.getResource(avail) );
                while ( lines.hasNext() ) {
                    String line = lines.next();
                    String[] fields=line.trim().split(" ");
                    if ( fields[0].equals("C4_CP_CIS-CODIF_HS_O1_PEF") ) {
                        System.err.println("Stop here");
                    }
                    if ( fields.length==3 ) {
                        sampleTimes.put( fields[0],fields[1]+"/"+fields[2] );
                    }
                }
            }
        } catch (IOException ex) {
            logger.log(Level.SEVERE, null, ex);
        }
        logger.log(Level.INFO, "read in sample times (ms): {0}", System.currentTimeMillis()-t0);
    
    }
    
    /**
     * return the sample time identified for this dataset id. 
     * @param id the dataset id, for example C1_PP_WHI
     * @return null if none is available, the time as a formatted isotime otherwise.
     */
    public static String getSampleTime( String id ) {
        String s= sampleTimes.get(id);
        return s;
    }
            
    private static Document readDoc(InputStream is) throws SAXException, IOException, ParserConfigurationException {
        DocumentBuilder builder;
        builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        InputSource source = new InputSource(new InputStreamReader(is));
        Document document = builder.parse(source);
        return document;
    }
    
    /**
     * get the subnode PARAMETER_ID.  I'm sure there's an easier way to do this.
     * @param p
     * @return 
     */
    private static String getNameForNode( Node p ) {
        NodeList n = p.getChildNodes();
        for (int j = 0; j < n.getLength(); j++) {
            Node c= n.item(j);
            String nodeName= c.getNodeName();
            if ( nodeName.equals("PARAMETER_ID") ) {
                return c.getTextContent();
            }
        }
        throw new IllegalArgumentException("expected to find PARAMETER_ID");
    }

    /**
     * 
     * @param timeRange formatted time range, like 2022-02-03T12:14.421/2023-03-04T00:21.224
     * @param digit the digit, for example TimeUtil.COMPONENT_HOURS means minutes and seconds will be zero.
     * @return rounded time range like 2022-02-03T12:00/2023-03-04T01:00 expressed in 14 components.
     * @throws ParseException 
     */
    public static int[] roundOut( String timeRange, int digit ) throws ParseException {
        int[] tr= TimeUtil.parseISO8601TimeRange(timeRange);
        int[] t1= TimeUtil.getStartTime(tr);
        int[] t2= TimeUtil.getStopTime(tr);
        boolean isRoundUp=false;
        if ( digit<TimeUtil.COMPONENT_DAY ) throw new IllegalArgumentException("digit must be DAY, HOUR, or finer");
        for ( int i=digit+1; i<TimeUtil.TIME_DIGITS; i++ ) {
            if ( t2[i]>0 ) isRoundUp=true;
        }
        for ( int i=digit+1; i<TimeUtil.TIME_DIGITS; i++ ) {
            t1[i]=0;
            t2[i]=0;
        }
        if ( isRoundUp ) t2[digit]++;
        return TimeUtil.createTimeRange( t1, t2 );
    }
            
    /**
     * remove the redundant dataset id from parameter ids.
     */
    private static final boolean popLabel= true;
    
    /**
     * produce the info response for a given ID. This assumes the response will be cached and performance is not an issue.
     *
     * @param id the dataset id.
     * @return the JSON formatted response.
     * @throws IOException
     */
    public static String getInfo(String id) throws IOException {

        // get the start and stop date by posting another request.  TODO: consider if this should be done during the catalog request.
        String startDate, stopDate;
        String trurl = String.format("https://csa.esac.esa.int/csa-sl-tap/tap/sync?REQUEST=doQuery&LANG=ADQL&FORMAT=CSV&QUERY=SELECT+dataset_id,title,start_date,end_date+FROM+csa.v_dataset+where+dataset_id=%%27%s%%27", id);
        logger.log(Level.FINE, "wrap {0}", trurl);
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new URL(trurl).openStream()))) {
            String header = reader.readLine();
            logger.log(Level.FINER, "header: {0}", header);
            String data = reader.readLine();
            String[] ss = SourceUtil.stringSplit(data);
            if (ss.length != 4) {
                throw new IllegalArgumentException("expected four fields when getting time range");
            }
            startDate = ss[ss.length - 2];
            stopDate = ss[ss.length - 1];
        }

        String url = String.format("https://csa.esac.esa.int/csa-sl-tap/data?retrieval_type=HEADER&DATASET_ID=%s&FORCEPACK=false", id);
        JSONObject jo = new JSONObject();
        jo.setEscapeForwardSlashAlways(false);
        
        try (InputStream ins = new URL(url).openStream()) {
            //String s= SourceUtil.getAllFileLines( new URL(url) );

            Document document;

            try {
                document = readDoc(ins);
            } catch (SAXException | ParserConfigurationException ex) {
                logger.log(Level.WARNING, "Unable to parse document returned by {0}", url);
                throw new RuntimeException(ex);
            }

            jo.put("HAPI", "3.0");

            XPathFactory factory = XPathFactory.newInstance();
            XPath xpath = (XPath) factory.newXPath();

            String sval = (String) xpath.evaluate("/DATASETS/DATASET_METADATA/DATASET_DESCRIPTION", document, XPathConstants.STRING);
            jo.put("x_description", sval.trim());

            sval = (String) xpath.evaluate("/DATASETS/DATASET_METADATA/TIME_RESOLUTION/text()", document, XPathConstants.STRING);
            if (sval != null) {
                jo.put("cadence", "PT" + sval + "S");
            }

            NodeList nl = (NodeList) xpath.evaluate("/DATASETS/DATASET_METADATA/PARAMETERS/*", document, XPathConstants.NODESET);
            JSONArray parameters = new JSONArray();

            String[] constantData= new String[nl.getLength()];
            JSONObject definitions = new JSONObject();
            boolean hasDefinitions= false;
            for (int i = 0; i < nl.getLength(); i++) { // scan through looking for non-time-varying data
                Node p = nl.item(i);
                String name= getNameForNode(p);
                String units=null;
                NodeList n = p.getChildNodes();
                for (int j = 0; j < n.getLength(); j++) {
                    Node c = n.item(j); // parameter
                    String nodeName= c.getNodeName();
                    if ( nodeName.equals("DATA") ) {
                        String sdata= c.getTextContent();
                        constantData[i]= sdata; 
                        String[] ss= constantData[i].trim().split("\\s+");
                        if ( ss.length>1 ) {
                            JSONObject data= new JSONObject();
                            data.put( "name", name );
                            JSONArray ja= new JSONArray();
                            for ( int z= 0; z<ss.length; z++ ) {
                                ja.put( z, Double.parseDouble(ss[z]) );
                            }
                            data.put( "centers", ja );
                            if ( units!=null ) {
                                data.put( "units", units );
                            } else {
                                data.put( "units", JSONObject.NULL  );
                            }
                            definitions.put( name, data );
                            hasDefinitions= true;                        
                        }
                    }
                }
            }
                    
            for (int i = 0; i < nl.getLength(); i++) {
                boolean isTime;
                Node p = nl.item(i);
                NodeList n = p.getChildNodes();
                
                String name=null;
                String units=null;

                for (int j = 0; j < n.getLength(); j++) {                    
                    Node c = n.item(j); // parameter
                    String nodeName= c.getNodeName();
                    if ( nodeName.equals("PARAMETER_ID") ) {
                        name= c.getTextContent();
                    } else if ( nodeName.equals("UNITS") ) {
                        units= c.getTextContent();
                    }
                }
                
                if ( name==null ) throw new IllegalArgumentException("unnamed parameter");
               
                JSONObject parameter = new JSONObject();
                for (int j = 0; j < n.getLength(); j++) {
                    Node c = n.item(j); // parameter
                    String nodeName= c.getNodeName();
                    if ( nodeName.equals("VALUE_TYPE") ) {
                        String t = c.getTextContent();
                        switch (t) {
                            case "ISO_TIME":
                                parameter.put("type", "isotime");
                                parameter.put("units", "UTC" );
                                break;
                            case "ISO_TIME_RANGE":
                                parameter.put("type", "isotime");
                                parameter.put("x_type", "ISO_TIME_RANGE");
                                parameter.put("units", "UTC" );
                                break;
                            case "FLOAT":
                            case "DOUBLE":
                                parameter.put("type", "double");
                                break;
                            case "INT":
                                parameter.put("type", "integer");
                                break;
                            case "CHAR":
                                parameter.put("type", "string");
                                break;
                            default:
                                break;
                        }             
                    }
                }
                
                if ( !parameter.has("type") ) {
                    throw new IllegalArgumentException("Expected type for id: "+id);
                } else {
                    isTime= parameter.getString("type").equals("isotime");
                }
                
                List<String> sizes= new ArrayList<>();
                List<String> depends= new ArrayList<>();
                
                for (int j = 0; j < n.getLength(); j++) {
                    Node c = n.item(j); // parameter
                    String nodeName= c.getNodeName();
                    String nodeValue= c.getTextContent();
                    switch (c.getNodeName()) {
                        case "PARAMETER_ID":
                            if ( popLabel ) {
                                nodeValue= nodeValue.substring(0,nodeValue.length()-2-id.length());
                            }
                            parameter.put("name", nodeValue);
                            break;
                        case "UNITS":
                            if ( isTime ) {
                                parameter.put("units", "UTC" );
                            } else {
                                if (nodeValue.equals("unitless")) {
                                    parameter.put("units", JSONObject.NULL);
                                } else {
                                    parameter.put("units", nodeValue);
                                }
                            }
                            break;
                        case "DEPEND_1":
                        case "DEPEND_2":
                        case "DEPEND_3":
                        case "DEPEND_4":
                            int index= Integer.parseInt( nodeName.substring(7) );
                            depends.add( index-1, nodeValue );
                            
                        case "SIGNIFICANT_DIGITS":
                            String type = parameter.optString("type", "");
                            if ( isTime || type.equals("string")) {
                                if (parameter.optString("x_type", "").equals("ISO_TIME_RANGE")) {
                                    parameter.put("length", 25);
                                } else {
                                    parameter.put("length", Integer.parseInt(nodeValue));
                                }
                            }
                            break;
                        case "SIZES":
                            sizes.add(c.getTextContent());
                            if ( parameter.get("type").equals("string") ) {
                                parameter.put("length", Integer.parseInt(c.getTextContent()) );
                            }
                            break;
                        case "CATDESC":
                            parameter.put("description", nodeValue);
                            break;
                        case "FIELDNAM":
                            parameter.put("label", nodeValue);
                            break;
                        case "FILLVAL":
                            if ( isTime ) {
                                parameter.put("fill", JSONObject.NULL );
                            } else {
                                parameter.put("fill", nodeValue);
                            }
                            break;
                        default:
                            break;
                    }
                }

                if ( sizes.isEmpty() || ( sizes.size()==1 && sizes.get(0).equals("1") ) ) {
                    // no need to do anything, typical non-array case;
                } else {

                    JSONArray bins= new JSONArray();

                    JSONArray array = new JSONArray();
                    for ( int ia=0; ia<sizes.size(); ia++ ) {
                        try {     
                            array.put(ia, Integer.parseInt(sizes.get(ia)));
                            if ( depends.size()==sizes.size() ) {
                                JSONObject bin= new JSONObject();
                                bin.setEscapeForwardSlashAlways(false);
                                if ( definitions.has( depends.get(ia) ) ) {
                                    //TODO: I can't figure out why it always escapes the backslashes here
                                    bin.put( "$ref", "#/definitions/"+ depends.get(ia) );
                                } else {
                                    bin.put( "name", depends.get(ia)+"__ref" );
                                    bin.put( "centers", depends.get(ia) );
                                }
                                
                                bins.put( ia, bin );
                            }
                            
                        } catch (JSONException ex) {
                            logger.log(Level.SEVERE, null, ex);
                        }
                    }
                    if ( depends.size()==sizes.size() ) {
                        parameter.put( "bins", bins );
                    }
                    parameter.put( "size", array );
                }

                if ( constantData[i]!=null && parameter.has("size") ) {
                    
                    
                } else {                
                    parameters.put(parameters.length(), parameter);

                }
            }
            
            if ( hasDefinitions ) {
                jo.put( "definitions", definitions );
            }
            
            jo.put("parameters", parameters);
            
            try {
                int[] tr= roundOut( startDate + "/" + stopDate, TimeUtil.COMPONENT_HOUR );
                startDate= TimeUtil.formatIso8601TimeBrief( TimeUtil.getStartTime(tr) );
                stopDate= TimeUtil.formatIso8601TimeBrief( TimeUtil.getStopTime(tr) );
                
            } catch ( ParseException ex ) {
                throw new RuntimeException(ex);
            }
            
            jo.put("startDate", startDate);
            jo.put("stopDate", stopDate);
            jo.put("x_tap_url", url);
            
            String sampleTimeRange= sampleTimes.get(id);
            if ( sampleTimeRange!=null ) {
                String[] ss= sampleTimeRange.split("/",-2);
                String sampleStartDate= ss[0];
                String sampleStopDate= ss[1];
                sampleStartDate= TimeUtil.reformatIsoTime( startDate, sampleStartDate );
                sampleStopDate= TimeUtil.reformatIsoTime( stopDate, sampleStopDate );
                if ( sampleStopDate.compareTo(startDate)<0 ) sampleStartDate= startDate;
                if ( sampleStopDate.compareTo(stopDate)>0 ) sampleStopDate= stopDate;
                try { // make sure sample times are no greater than one or two days.
                    int[] tr= roundOut( sampleStartDate + "/" + sampleStopDate, TimeUtil.COMPONENT_HOUR );
                    int[] t1= TimeUtil.getStartTime(tr);
                    int[] t2= TimeUtil.getStopTime(tr);
                    int[] dt= TimeUtil.subtract( t2,t1 );
                    if ( dt[TimeUtil.COMPONENT_DAY]>1 ) {
                        t2= TimeUtil.add( t1, new int[] { 0,0,1,0,0,0,0 } );
                    }
                    sampleStartDate= TimeUtil.formatIso8601TimeBrief(t1);
                    sampleStopDate= TimeUtil.formatIso8601TimeBrief(t2);
                    if ( sampleStopDate.compareTo(sampleStartDate)>0 ) {
                        jo.put("sampleStartDate", sampleStartDate );
                        jo.put("sampleStopDate", sampleStopDate );
                    }                    
                } catch (ParseException ex) {
                    logger.log(Level.SEVERE, null, ex);
                }
                String zz= "0000-00-00T00:00:00.000Z";
                if ( sampleStopDate.length()<zz.length() ) {
                    int n= sampleStopDate.length()-1; // remove Z
                    sampleStopDate= sampleStopDate.substring(0,n)+zz.substring(n);
                }
                if ( sampleStartDate.length()<zz.length() ) {
                    int n= sampleStartDate.length()-1; // remove Z
                    sampleStartDate= sampleStartDate.substring(0,n)+zz.substring(n);
                }
                // this is just to aid in debugging.
                String queryString = "https://csa.esac.esa.int/csa-sl-tap/data?RETRIEVAL_TYPE=product&RETRIEVAL_ACCESS=streamed&DATASET_ID=" + id
                + "&START_DATE=" + sampleStartDate + "&END_DATE=" + sampleStopDate;
                jo.put("x_tap_data_url", queryString );

            } else {
                // this is just to aid in debugging.
                String queryString = "https://csa.esac.esa.int/csa-sl-tap/data?RETRIEVAL_TYPE=product&RETRIEVAL_ACCESS=streamed&DATASET_ID=" + id
                + "&START_DATE=" + stopDate + "&END_DATE=" + stopDate;
                jo.put("x_tap_data_url", queryString );
                
            }
            
            
            return jo.toString(4);

        } catch (JSONException | XPathExpressionException ex) {
            throw new RuntimeException(ex);
        }

    }

    /**
     * return the catalog as a HAPI catalog response
     * @return @throws IOException
     */
    public static String getCatalog() throws IOException {
        try {
            loadExcludeList();
            JSONArray catalog = new JSONArray();
            String url = "https://csa.esac.esa.int/csa-sl-tap/tap/sync?REQUEST=doQuery&LANG=ADQL&FORMAT=CSV&QUERY=SELECT+dataset_id,title+FROM+csa.v_dataset";
            try (InputStream in = new URL(url).openStream()) {
                BufferedReader ins = new BufferedReader(new InputStreamReader(in));
                String s = ins.readLine();
                if (s != null) {
                    s = ins.readLine(); // skip the first header line
                }
                while (s != null) {
                    int i = s.indexOf(",");
                    JSONObject jo = new JSONObject();
                    String id= s.substring(0, i).trim();
                    if ( exclude.contains(id) ) {
                        logger.log(Level.FINE, "excluding dataset id {0}", id);
                        s = ins.readLine();
                        continue;
                    }
                    //if ( id.startsWith("C2") ) {
                        jo.put("id", id);
                        String t = s.substring(i + 1).trim();
                        if (t.startsWith("\"") && t.endsWith("\"")) {
                            t = t.substring(1, t.length() - 1);
                        }
                        jo.put("title", t);
                        catalog.put(catalog.length(), jo);
                    //}
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

        } catch (JSONException ex) {
            throw new RuntimeException(ex);

        }
    }

    private static JSONObject getOKStatus() throws JSONException {
        JSONObject status = new JSONObject();
        status.put("code", 1200);
        status.put("message", "OK request successful");
        return status;
    }

    private static void printHelp() {
        System.err.println("CsaInfoCatalogSource [id]");
        System.err.println("   [id] if present, then return the info response for the id");
        System.err.println("   [id] if missing, then return the catalog response");
    }

    private static Set<String> exclude;
    
    private static void loadExcludeList() throws IOException {
        exclude= new HashSet<>();
        try (InputStream ins = CsaInfoCatalogSource.class.getResourceAsStream("CsaCatalogExclude.txt") ) {
            BufferedReader read= new BufferedReader( new InputStreamReader( ins ) );
            String line= read.readLine();
            while ( line!=null ) {
                int i= line.indexOf("#");
                if ( i>-1 ) line= line.substring(0,i);
                line= line.trim();
                if ( line.length()>0 ) {
                    exclude.add(line);
                }
                line= read.readLine();
            }
        }
    }
    /**
     *
     * @param args
     */
    public static void main(String[] args) {
        if (args.length == 1) {
            if (args[0].equals("--help")) {
                printHelp();
                System.exit(1);
            } else if ( args[0].equals("--depth=all")) {
                // this was to entertain the idea of doing a full catalog with every response info generated.  
                // I realized I could generate this just as easily by running the "makeGiantCatalog.jy" script
                // on my instance of this, which has the side-effect of generating all the info responses.
                try {
                    String s = getCatalog();
                    JSONObject jo= new JSONObject(s);
                    JSONArray ja= jo.getJSONArray("catalog");
                    JSONArray ja1= new JSONArray();
                    int n= ja.length();
                    for ( int i=0; i<n; i++ ) {
                        JSONObject jo1= ja.getJSONObject(i);
                        String id= jo1.getString("id");
                        String sinfo= getInfo(id);
                        ja1.put(i,new JSONObject(sinfo));
                        System.err.println("read infos: "+i+" of "+n);
                    }
                    jo.put("catalog",ja1);
                    System.out.println(jo);
                    
                } catch ( IOException ex ) {
                    ex.printStackTrace();
                    System.exit(-1);
                } catch (JSONException ex) {
                    Logger.getLogger(CsaInfoCatalogSource.class.getName()).log(Level.SEVERE, null, ex);
                }
            } else if ( args[0].equals("--case=4")) { 
                try {
                    String s = getInfo("C4_CP_CIS-CODIF_HS_O1_PEF");
                    System.out.println(s);
                } catch (IOException ex) {
                    Logger.getLogger(CsaInfoCatalogSource.class.getName()).log(Level.SEVERE, null, ex);
                }
            } else if ( args[0].equals("--case=5") ) {
                try {
                    String s = getInfo("C1_CP_PEA_3DRH_PSD");
                    System.out.println(s);
                } catch (IOException ex) {
                    Logger.getLogger(CsaInfoCatalogSource.class.getName()).log(Level.SEVERE, null, ex);
                }
            } else if ( args[0].equals("--case=8") ) {
                try {
                    String s = getInfo("C1_PP_WHI");
                    System.out.println(s);
                } catch (IOException ex) {
                    Logger.getLogger(CsaInfoCatalogSource.class.getName()).log(Level.SEVERE, null, ex);
                }
                //C1_CP_PEA_3DRH_PSD&parameters=time_tags__C1_CP_PEA_3DRH_PSD,Angle_SR2phi__C1_CP_PEA_3DRH_PSD&timerange=2019-08-01+0:00+to+0:10
            } else {
                try {
                    String s = getInfo(args[0]);
                    System.out.println(s);
                    System.exit(0);
                } catch (IOException ex) {
                    ex.printStackTrace();
                    System.exit(-2);
                }
            }
        } else if (args.length == 0) {
            try {
                String s = getCatalog();
                System.out.println(s);
                System.exit(0);
            } catch (IOException ex) {
                ex.printStackTrace();
                System.exit(-1);
            }

        }
    }
}
