package org.hapiserver.source.das2server;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.regex.Pattern;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.hapiserver.source.SourceUtil;

/**
 * return a catalog
 * @author jbf
 */
public class Das2ServerCatalogSource {
    
    
    private static JSONObject makeOne( String id, String description ) throws JSONException {
        JSONObject jo= new JSONObject();
        jo.setEscapeForwardSlashAlways(false);
        jo.put("id",id);
        if ( description.length()>0 ) {
            jo.put("description", description );
        }
        return jo;
    }
    /**
     * config is a file located relative to this source.
     * @param config the configuration json file
     * @return the catalog response
     * @throws java.io.IOException 
     * @throws org.codehaus.jettison.json.JSONException 
     */
    public static String getCatalog( String config ) throws IOException, JSONException {
        URL url= Das2ServerCatalogSource.class.getResource(config);
        String configJson= SourceUtil.getAllFileLines(url);
        JSONObject jo = new JSONObject(configJson);
        String das2server= jo.getString("server");
        JSONArray includeRegexArray= jo.getJSONArray("include"); // default is to include everything
        JSONArray excludeRegexArray= jo.optJSONArray("exclude"); // default is to exclude nothing
        
        Pattern[] includePatternArray;
        if ( includeRegexArray==null ) {
            includePatternArray= new Pattern[0];
        } else {
            includePatternArray= new Pattern[includeRegexArray.length()];
        }
        for ( int i=0; i<includePatternArray.length; i++ ) {
            includePatternArray[i]= Pattern.compile(includeRegexArray.getString(i));
        }
        Pattern[] excludePatternArray;
        if ( excludeRegexArray==null ) {
            excludePatternArray = new Pattern[0];
        } else {
            excludePatternArray = new Pattern[excludeRegexArray.length()];
        }
        for ( int i=0; i<excludePatternArray.length; i++ ) {
            excludePatternArray[i]= Pattern.compile(excludeRegexArray.getString(i));
        }
        
        Iterator<String> lines= SourceUtil.getFileLines( new URL(das2server+"?server=list") );
        ArrayList<JSONObject> dsids= new ArrayList<>();
        while ( lines.hasNext() ) {
            String line= lines.next();
            String id,description;
            int i= line.indexOf("|");
            if ( i>-1 ) {
                id =  line.substring(0,i);
                description= line.substring(i+1);
            } else {
                id= line.trim();
                description= "";
            }
            if ( id.endsWith("/") ) {
                continue;
            }
            
            boolean include= false;
            if ( includePatternArray.length==0 ) {
                include= true;
            } else {
                for ( Pattern p: includePatternArray ) {
                    if ( p.matcher(id).matches() ) include=true;
                }
            }
            if ( include ) {
                if ( excludePatternArray.length>0 ) {
                    for ( Pattern p: excludePatternArray ) {
                        if ( p.matcher(id).matches() ) include=false;
                    }
                }                
            }
            if ( include ) {
                dsids.add(makeOne(id,description));
            }
        }
        JSONObject catalog= new JSONObject();
        catalog.put("catalog", dsids);
                
        return catalog.toString();
    }
    
    public static void main( String[] args ) throws IOException, JSONException {
        //args= new String[] { "AC_AT_DEF" };
        args= new String[0];
        
        if ( args.length==0 ) {
            System.out.println( Das2ServerCatalogSource.getCatalog("jupiter-d2s.json") );
        } 
    }
}
