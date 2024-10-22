
package org.hapiserver.source.cdaweb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
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
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
        
/**
 * Returns catalog response based on all.xml, and info responses from
 * either Bob's process, Nand's existing server, or a future implementation (and this
 * documentation needs to be updated).
 * @author jbf
 * @see https://cdaweb.gsfc.nasa.gov/pub/catalogs/all.xml
 */
public class CdawebInfoCatalogSource {
    
    private static final Logger logger= Logger.getLogger("hapi.cdaweb");
    
    public static final String CDAWeb = "https://cdaweb.gsfc.nasa.gov/";
    
    protected static Map<String,String> coverage= new HashMap<>();
    protected static Map<String,String> filenaming= new HashMap<>();

    private static String getURL( String id, Node dataset ) {
        NodeList kids= dataset.getChildNodes();
        String lookfor= "ftp://cdaweb.gsfc.nasa.gov/pub/istp/";
        String lookfor2= "ftp://cdaweb.gsfc.nasa.gov/pub/cdaweb_data";
        for ( int j=0; j<kids.getLength(); j++ ) {
            Node childNode= kids.item(j);
            if ( childNode.getNodeName().equals("access") ) {
                NodeList kids2= childNode.getChildNodes();
                for ( int k=0; k<kids2.getLength(); k++ ) {
                    if ( kids2.item(k).getNodeName().equals("URL") ) {
                        if ( kids2.item(k).getFirstChild()==null ) {
                            logger.log(Level.FINE, "URL is missing for {0}, data cannot be accessed.", id);
                            return null;
                        }
                        
                        String url= kids2.item(k).getFirstChild().getTextContent().trim();
                        if ( url.startsWith( lookfor ) ) {
                            // "ftp://cdaweb.gsfc.nasa.gov/pub/istp/ace/mfi_h2"
                            //  http://cdaweb.gsfc.nasa.gov/istp_public/data/
                            url= CDAWeb + "sp_phys/data/" + url.substring(lookfor.length());
                        }
                        if ( url.startsWith(lookfor2) ) {
                            url= CDAWeb + "sp_phys/data/" + url.substring(lookfor2.length());
                        }
                        String templ= url + "/";
                        String subdividedby= childNode.getAttributes().getNamedItem("subdividedby").getTextContent();
                        String filenaming= childNode.getAttributes().getNamedItem("filenaming").getTextContent();
                        
                        if ( !subdividedby.equals("None") ) {
                            templ= templ + subdividedby + "/";
                        }
                        templ= templ + filenaming;
                        CdawebInfoCatalogSource.filenaming.put(id,templ);
                        return url;
                    }
                }
            }
        }
        return null;
    }

    private static HashSet<String> skips;
    private static HashSet<Pattern> skipsPatterns;
    
    private static void readSkips() throws IOException {
        logger.info("reading skips");
        skips= new HashSet<>();
        skipsPatterns= new HashSet<>();
        URL skipsFile= CdawebInfoCatalogSource.class.getResource("skips.txt");
        try (BufferedReader r = new BufferedReader(new InputStreamReader( skipsFile.openStream() ))) {
            String s = r.readLine();
            while ( s!=null ) {  
                String[] ss= s.split(",",-2);
                if ( ss.length==2 ) {
                    if ( ss[0].contains(".") ) {
                        skipsPatterns.add( Pattern.compile(ss[0]) );
                    } else {
                        skips.add(ss[0].trim());
                    }
                }
                s = r.readLine();
            }
        }
    }
    
    /**
     * read all available cached infos and form a catalog.
     * @return
     * @throws IOException 
     */
    public static String getCatalog20230629() throws IOException {
        File cache= new File("/home/jbf/ct/autoplot/project/cdf/2023/bobw/nl/");
        File[] ff= cache.listFiles((File dir, String name) -> name.endsWith(".json"));

        ArrayList<File> catalog= new ArrayList<>( Arrays.asList(ff) );
        Collections.sort(catalog, (File o1, File o2) -> o1.getName().compareTo(o2.getName()));
        
        JSONObject result= new JSONObject();
        JSONArray jscat= new JSONArray();

        try {
            for ( File f : catalog ) {
                JSONObject item= new JSONObject();
                String n= f.getName();
                item.put("id",n.substring(0,n.length()-5));
                jscat.put( jscat.length(), item);            
            }
            result.put( "catalog", jscat );
        } catch (JSONException ex) {
            throw new RuntimeException(ex);
        }
        
        return result.toString();
    }
    
    /**
     * return the catalog response by parsing all.xml.
     * @return
     * @throws IOException 
     */
    public static String getCatalog() throws IOException {
        readSkips();
        try {
            URL url= new URL("https://cdaweb.gsfc.nasa.gov/pub/catalogs/all.xml");
            Document doc= SourceUtil.readDocument(url);
            XPathFactory factory = XPathFactory.newInstance();
            XPath xpath = (XPath) factory.newXPath();
            NodeList nodes = (NodeList) xpath.evaluate( "//sites/datasite/dataset", doc, XPathConstants.NODESET );

            int ic= 0;
            JSONArray catalog= new JSONArray();
            for ( int i=0; i<nodes.getLength(); i++ ) {
                Node node= nodes.item(i);
                NamedNodeMap attrs= node.getAttributes();

                String st= attrs.getNamedItem("timerange_start").getTextContent();
                String en= attrs.getNamedItem("timerange_stop").getTextContent();
                String nssdc_ID= attrs.getNamedItem("nssdc_ID").getTextContent();
                if ( st.length()>1 && Character.isDigit(st.charAt(0))
                        && en.length()>1 && Character.isDigit(en.charAt(0))
                        && nssdc_ID.contains("None") ) {
                    String name= attrs.getNamedItem("serviceprovider_ID").getTextContent();

                    if ( skips.contains(name) ) {
                        logger.log(Level.FINE, "skipping {0}", name);
                        continue;
                    }
                    boolean doSkip= false;
                    for ( Pattern p: skipsPatterns ) {
                        if ( p.matcher(name).matches() ) {
                            doSkip= true;
                            logger.log(Level.FINE, "skipping {0} because of match", name);
                        }
                    }
                    if ( doSkip ) continue;

                    String sourceurl= getURL(name,node);
                    if ( sourceurl!=null && 
                            ( sourceurl.startsWith( CDAWeb ) ||
                            sourceurl.startsWith("ftp://cdaweb.gsfc.nasa.gov" ) ) && !sourceurl.startsWith("/tower3/private" ) ) {
                        JSONObject jo= new JSONObject();
                        jo.put( "id", name );
                        try {
                            st = TimeUtil.formatIso8601TimeBrief( TimeUtil.parseISO8601Time(st) );
                            en = TimeUtil.formatIso8601TimeBrief( TimeUtil.parseISO8601Time(en) );
                            String range= st+"/"+en;
                            jo.put( "x_range", range );
                            CdawebInfoCatalogSource.coverage.put( name, range );
                        } catch (ParseException ex) {
                            logger.log(Level.SEVERE, null, ex);
                        }

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
    
    /**
     * return the info response generated by combining several sources.  These info
     * responses are stored (presently) at http://mag.gmu.edu/git-data/cdaweb-hapi-metadata/hapi/bw/CDAWeb/info/.
     * @param id the dataset id.
     * @param srcid nl (to call the old server) or bw (to use Bob's info calculations) or "" to use new code.
     * @return the info response.
     * @throws MalformedURLException
     * @throws IOException 
     */
    public static String getInfo( String id, String srcid ) throws MalformedURLException, IOException {
        int i= id.indexOf('_');
        String g;
        if ( i>-1 ) {
            g= id.substring(0,i);
        } else {
            throw new IllegalArgumentException("bad id: "+id);
        }
        if ( srcid.equals("bw") ) {
            URL url = new URL( "http://mag.gmu.edu/git-data/cdaweb-hapi-metadata/hapi/bw/CDAWeb/info/"+id+".json" );
            String src= SourceUtil.getAllFileLines( url );
            try {
                JSONObject jo= new JSONObject(src);
                jo.put("x_info_author", "bw");
                return jo.toString(4);
            } catch ( JSONException ex ) {
                throw new IllegalArgumentException("bad thing that will never happen");
            }
        } else if ( srcid.equals("jf") ) {
            try {
                URL url = new URL( "file:/home/jbf/ct/autoplot/project/cdf/2023/bobw/nl/"+id+".json" );
                String src= SourceUtil.getAllFileLines( url );
                JSONObject jo= new JSONObject(src);
                jo.put("x_info_author", "jfnl");
                return jo.toString(4);
            } catch (JSONException ex) {
                throw new IllegalArgumentException("bad thing that will never happen");
            }
        } else if ( srcid.equals("nl") ) {
            try {
                URL url = new URL( "https://cdaweb.gsfc.nasa.gov/hapi/info?id="+id );
                String src= SourceUtil.getAllFileLines( url );
                JSONObject jo= new JSONObject(src);
                jo.put("x_info_author", "nl");
                return jo.toString(4);
            } catch (JSONException ex) {
                throw new IllegalArgumentException("bad thing that will never happen");
            }
        } else {
            throw new IllegalArgumentException("info method not supported");
        }
        
    }
    
    public static void main( String[] args ) throws IOException {
        //args= new String[] { "AC_AT_DEF" };
        args= new String[0];
        
        if ( args.length==0 ) {
            System.out.println( CdawebInfoCatalogSource.getCatalog20230629() );
        } else if ( args.length==1 ) {
            System.out.println( CdawebInfoCatalogSource.getInfo( args[0], "bw" ) );
        }
    }
    
}
