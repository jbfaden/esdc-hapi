package org.esdc.hapi;

import gov.nasa.gsfc.spdf.cdfj.CDFException;
import gov.nasa.gsfc.spdf.cdfj.CDFReader;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.hapiserver.TimeUtil;
import org.hapiserver.source.SourceUtil;

/**
 * provide info responses to the server.  this is based on
 * https://soar.esac.esa.int/soar-sl-tap/tap/sync
 *   ?REQUEST=doQuery&LANG=ADQL&FORMAT=json
 *   &QUERY=select%20*%20from%20soar.v_cdf_plot_metadata%20where%20logical_source%20=%20%27solo_L2_rpw-lfr-surv-swf-b%27
 * @author jbf
 */
public class EsdcInfoSource {
    
    private static final Logger logger= Logger.getLogger("hapi.esdc");
    
    private static String getUnits( CDFReader reader, String name ) throws CDFException.ReaderError {
        Object o= reader.getAttribute(name,"UNITS");
        String u= null;
        if ( o instanceof Vector ) {    // yuck
            u= (String)((Vector)o).get(0);
        }
        return u;
    }
    
    private static Map<File,Map<String,JSONObject>> cache;
    
    private static JSONObject getBins( File cdfFile, CDFReader reader, String name, int len ) throws CDFException.ReaderError, JSONException {
        
        if ( cache==null ) {
            synchronized ( EsdcInfoSource.class ) {
                if ( cache==null ) {
                    cache= new HashMap<>();
                }
            }
        }
        
        Map<String,JSONObject> cacheForFile= cache.get(cdfFile);
        
        if ( cacheForFile!=null ) {
            if ( cacheForFile.containsKey(name) ) {
                JSONObject result= cacheForFile.get(name);
                return result;
            }
        }
        
        Object o = reader.getOneD(name, true);
        if ( o==null && !( o instanceof double[]) ) return null;
        double[] dd= (double[]) o;
        
        JSONObject bins;
        
        if ( dd.length!=len ) {
            bins= null;
            
        } else {
            bins= new JSONObject();
            JSONArray centers= new JSONArray();
            for ( int i=0; i<len; i++ ) {
                centers.put(i,dd[i]);
            }
            bins.put("centers", centers);
            bins.put("name",name );
            String u= getUnits( reader, name ).trim();
            if ( u!=null && u.length()>0 ) {
                bins.put("units", u );
            } else {
                bins.put("units", JSONObject.NULL );
            }
        }
        
        if ( cacheForFile==null ) {
            synchronized ( EsdcInfoSource.class ) {
                cacheForFile= new HashMap<>();
                cache.put( cdfFile, cacheForFile );
            }
        }
        
        cacheForFile.put( name, bins );
        
        return bins;
    }
    
    private static String getFillValue( CDFReader cdfReader, String name ) throws CDFException.ReaderError {
        Vector v= (Vector)cdfReader.getAttribute(name,"FILLVAL");
        if ( v.size()==1 ) {
            if ( v.get(0) instanceof double[] ) {
                double[] v1= (double[])v.get(0);
                if ( v1.length==1 ) {
                    double v2= v1[0];
                    String s= String.valueOf(v2);
                    if ( s.contains("9.99999") ) {
                        return String.format("%.2e",v2);
                    } else {
                        return s;
                    }
                }
            }
        } 
        return null;
    }
    
    public static String getInfo( String id ) throws IOException, JSONException, CDFException.ReaderError {
        try {
            
            // Here's what we need: depend0, var_name, var_sizes
            String svr= "https://soar.esac.esa.int/soar-sl-tap/tap/sync";
            String select= "depend0,var_name,var_sizes,var_units,depend1,depend2,depend3";
            String tapQuery= svr + "?REQUEST=doQuery&LANG=ADQL&FORMAT=json&QUERY=select%20"+select+"%20from%20soar.v_cdf_plot_metadata%20where%20logical_source%20=%20%27"+id+"%27";
            String tapJsonSrc= SourceUtil.getAllFileLines( new URL(tapQuery) );
            
            String[] extent= EsdcAvailabilityInfoSource.getExtent(id);
            
            JSONObject tapResponse= new JSONObject(tapJsonSrc);
            JSONArray tapParameters= tapResponse.getJSONArray("data");
            
            JSONObject result= new JSONObject();
            JSONArray parameters= new JSONArray();
            JSONObject parameter= new JSONObject();
            
            String depend0name= tapParameters.getJSONArray(0).getString(0);
            
            parameter.put("name",depend0name);
            parameter.put("type","isotime");
            parameter.put("length",24);
            parameter.put("units","UTC");
            parameter.put("fill",JSONObject.NULL);
            
            parameters.put( 0, parameter );
            
            File cdfFile;
            EsdcRecordSource recsrc= new EsdcRecordSource(id,null);
            cdfFile= recsrc.getSampleCdfFile();
           
            CDFReader cdfReader= new CDFReader(cdfFile.toString());

            for ( int i=0; i<tapParameters.length(); i++ ) {
                JSONArray tapParameter= tapParameters.getJSONArray(i);
                
                String name = tapParameter.getString(1);
                parameter= new JSONObject();
                parameter.put( "name", name );
                parameter.put( "type", "double" );
                String size= tapParameter.getString(2);
                if ( size!=null ) {
                    JSONArray sizeArray= new JSONArray();
                    String[] ss= size.split(",");
                    for ( int j=0; j<ss.length; j++ ) {
                        sizeArray.put(j,Integer.parseInt(ss[j]));
                    }
                    parameter.put( "size", sizeArray );
                    JSONArray binsArray= new JSONArray();
                    boolean binsHasNonNull= false;
                    for ( int j=0; j<ss.length; j++ ) {
                        JSONObject bins;
                        try {
                            String depname= tapParameter.getString(4+j);
                            int len= sizeArray.getInt(j);
                            if ( depname!=null ) {
                                bins = getBins(cdfFile,cdfReader,depname,len);
                                if ( bins==null ) {
                                    binsArray.put(j,JSONObject.NULL);
                                } else {
                                    binsArray.put(j,bins);
                                    binsHasNonNull= true;
                                }
                            }
                        } catch (CDFException.ReaderError ex) {
                            logger.log(Level.SEVERE, null, ex);
                        }
                    }
                    if ( binsHasNonNull && ss.length>0 && binsArray.length()>0 ) parameter.put( "bins", binsArray );
                }
                String units= tapParameter.getString(3).trim();
                if ( units.length()>0 ) {
                    parameter.put( "units", tapParameter.getString(3) );
                } else {
                    parameter.put( "units", JSONObject.NULL );
                }
                parameter.put( "fill", getFillValue(cdfReader,name) );
                parameter.setEscapeForwardSlashAlways(false);
                parameters.put( i+1, parameter );
            }
            
            result.put("parameters",parameters);
            result.put("startDate",TimeUtil.reformatIsoTime("2000-01-01T00:00:00.000Z", extent[0]));
            result.put("stopDate",TimeUtil.reformatIsoTime("2000-01-01T00:00:00.000Z", extent[1]));

            result.put("sampleStartDate",TimeUtil.reformatIsoTime("2000-01-01T00:00:00.000Z", extent[2]));
            result.put("sampleStopDate",TimeUtil.reformatIsoTime("2000-01-01T00:00:00.000Z", extent[3]));
            
            result.put("HAPI","3.1");
            result.put("x_version", "20240202.3");
            result.put("x_tap_query", tapQuery );
            result.put("x_cdf_file", cdfFile.toString() );
            
            result.setEscapeForwardSlashAlways(false);
            return result.toString(4);
            
        } catch (MalformedURLException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    public static void main( String[] args ) throws IOException, JSONException, CDFException.ReaderError {
        //System.err.println( getInfo("solo_L2_rpw-lfr-surv-cwf-b"));
        //System.err.println( getInfo("solo_L2_rpw-lfr-surv-swf-b"));
        //System.err.println( getInfo("solo_L2_mag-rtn-normal")); //
        //System.err.println( getInfo("solo_L2_rpw-tnr-surv")); //spectrograms  rank 2 frequencies: file:///home/tomcat/tmp/esdc/jbf/solo_L2_rpw-tnr-surv_20230920_V01.cdf?FREQUENCY
        //System.err.println( getInfo("solo_L2_epd-ept-south-rates")); // spectrograms
        
        System.err.println( getInfo("solo_L2a_swa-eas1-nm3d-psd") );
        System.err.println( getInfo("solo_L2_mag-rtn derived from LL data") );
        System.err.println( getInfo("solo_L2_mag-srf derived from LL data") );
        System.err.println( getInfo("solo_L2a_swa-eas1-nm3d-def") );
        System.err.println( getInfo("solo_L2a_swa-eas1-nm3d-dnf") );

    }
}
