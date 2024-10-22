package org.hapiserver.source.tap;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.hapiserver.AbstractHapiRecordSource;
import org.hapiserver.HapiRecord;
import org.hapiserver.TimeUtil;

/**
 * provide the data stream from the ESAC TAP server
 * @author jbf
 */
public class TAPDataSource extends AbstractHapiRecordSource {

    private static final Logger logger = Logger.getLogger("hapi.cef");

    private final String tapServerURL;
    private final String id;
    
    private InputStream in=null;

    public TAPDataSource(String tapServerURL, String id) {
        this.tapServerURL = tapServerURL;
        this.id = id;

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
        String startTimeString;
        String stopTimeString;
        int minimumDurationNs=200000000;
        int[] duration = TimeUtil.subtract(stop, start);
        if ( duration[0]==0 && duration[1]==0 && duration[2]==0 
                && duration[3]==0 && duration[4]==0 && duration[5]==0 
                && duration[6]<minimumDurationNs ) {
            startTimeString = formatTime(start);
            stopTimeString = formatTime( TimeUtil.add( start, new int[] { 0, 0, 0, 0, 0, 0, minimumDurationNs } ) );
        } else {
            startTimeString = formatTime(start);
            stopTimeString = formatTime(stop);
        }
        
        String queryString = tapServerURL + "data?RETRIEVAL_TYPE=product&RETRIEVAL_ACCESS=streamed&DATASET_ID=" + id
            + "&START_DATE=" + startTimeString + "&END_DATE=" + stopTimeString;
        logger.log(Level.FINE, "Querying: {0}", queryString);
        try {
            URL uu = new URL(queryString);
            in = uu.openStream();
            ReadableByteChannel lun = Channels.newChannel(in);
            CefFileIterator iter = new CefFileIterator(lun);

            return iter;
        } catch (IOException e) {
            try {
                if ( in!=null ) in.close();
            } catch (IOException ex) {
                logger.log(Level.SEVERE, null, ex);
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public void doFinalize() {
        if ( in!=null ) {
            try {
                in.close();
            } catch ( IOException ex ) {
                logger.log( Level.WARNING, ex.getMessage(), ex );
            }
        }
    }

    
    private String formatTime(int[] time) {
        String timeString = String.format("%4d-%02d-%02dT%02d:%02d:%02dZ",
            time[0], time[1], time[2], time[3], time[4], time[5]);
        return timeString;
    }

    public static void mainCase1( String[] args ) {
        String tapServerURL="https://csa.esac.esa.int/csa-sl-tap/";
        String id= "CL_SP_WHI";
        int[] start= new int[] { 2012, 12, 25, 0, 0, 0, 0 };
        int[] stop= new int[] { 2012, 12, 26, 0, 0, 0, 0 };
        Iterator<HapiRecord> iter= new TAPDataSource(tapServerURL, id).getIterator(start, stop);
        while ( iter.hasNext() ) {
            HapiRecord r= iter.next();
            System.err.println( String.format( "%s %d fields", r.getIsoTime(0), r.length() ) );
        }
    }
    
    public static void mainCase2( String[] args ) {
        String tapServerURL="https://csa.esac.esa.int/csa-sl-tap/";
        String id= "D1_CG_STA-DWP_COMBI_PNG";
        int[] start= new int[] { 2012, 12, 25, 0, 0, 0, 0 };
        int[] stop= new int[] { 2012, 12, 26, 0, 0, 0, 0 };
        Iterator<HapiRecord> iter= new TAPDataSource(tapServerURL, id).getIterator(start, stop);
        while ( iter.hasNext() ) {
            HapiRecord r= iter.next();
            System.err.println( String.format( "%s %d fields", r.getIsoTime(0), r.length() ) );
        }
    }
    
    public static void mainCase3( String[] args ) throws ParseException {
        String tapServerURL="https://csa.esac.esa.int/csa-sl-tap/";
        String id="CM_CG_WBD_OVERVIEW_500_19_PNG";
        String tr= "2023-01-18T17:00/18:00";
        int[] timerange = TimeUtil.parseISO8601TimeRange(tr);
        int[] start= Arrays.copyOfRange( timerange, 0, 7 );
        int[] stop= Arrays.copyOfRange( timerange, 7, 14 );
        Iterator<HapiRecord> iter= new TAPDataSource(tapServerURL, id).getIterator(start, stop);
        while ( iter.hasNext() ) {
            HapiRecord r= iter.next();
            System.err.println( String.format( "%s %d fields", r.getIsoTime(0), r.length() ) );
        }
    }
    
    public static void mainCase4( String[] args ) throws ParseException {
        String tapServerURL="https://csa.esac.esa.int/csa-sl-tap/";
        String id="C4_CP_CIS-CODIF_HS_O1_PEF";
        String tr= "2021-12-01T00:00/00:02";
        int[] timerange = TimeUtil.parseISO8601TimeRange(tr);
        int[] start= Arrays.copyOfRange( timerange, 0, 7 );
        int[] stop= Arrays.copyOfRange( timerange, 7, 14 );
        Iterator<HapiRecord> iter= new TAPDataSource(tapServerURL, id).getIterator(start, stop);
        while ( iter.hasNext() ) {
            HapiRecord r= iter.next();
            System.err.println( String.format( "%s %d fields", r.getIsoTime(0), r.length() ) );
        }
    }
    
    public static void mainCase5( String[] args ) throws ParseException {
        String tapServerURL="https://csa.esac.esa.int/csa-sl-tap/";
        String id="C1_CP_PEA_3DRH_PSD";
        String tr= "2019-08-01T00:00/0:10";
        int[] timerange = TimeUtil.parseISO8601TimeRange(tr);
        int[] start= Arrays.copyOfRange( timerange, 0, 7 );
        int[] stop= Arrays.copyOfRange( timerange, 7, 14 );
        Iterator<HapiRecord> iter= new TAPDataSource(tapServerURL, id).getIterator(start, stop);
        while ( iter.hasNext() ) {
            HapiRecord r= iter.next();
            System.err.println( String.format( "%s %d fields", r.getIsoTime(0), r.length() ) );
        }
    }

    /**
     * This returns 769 fields while the info thinks it should be 897 (128 more).
     * @see https://github.com/hapi-server/server-java/issues/21
     * @param args
     * @throws ParseException 
     */
    public static void mainCase6( String[] args ) throws ParseException {
        String tapServerURL="https://csa.esac.esa.int/csa-sl-tap/";
        String id="C4_CP_STA_CS_NBR";
        String tr= "2022-07-31T11:00Z/2022-08-01T00:00Z";
        int[] timerange = TimeUtil.parseISO8601TimeRange(tr);
        int[] start= Arrays.copyOfRange( timerange, 0, 7 );
        int[] stop= Arrays.copyOfRange( timerange, 7, 14 );
        Iterator<HapiRecord> iter= new TAPDataSource(tapServerURL, id).getIterator(start, stop);
        while ( iter.hasNext() ) {
            HapiRecord r= iter.next();
            System.err.println( String.format( "%s %d fields", r.getIsoTime(0), r.length() ) );
        }
    }
        
/**
     * This returns 769 fields while the info thinks it should be 897 (128 more).
     * @see https://github.com/hapi-server/server-java/issues/21
     * @param args
     * @throws ParseException 
     */
    public static void mainCase7( String[] args ) throws ParseException {
        String tapServerURL="https://csa.esac.esa.int/csa-sl-tap/";
        String id="C1_PP_EDI";
        String tr= "2018-10-24T18:59:56Z/2018-10-25T00:00:04Z";
        int[] timerange = TimeUtil.parseISO8601TimeRange(tr);
        int[] start= Arrays.copyOfRange( timerange, 0, 7 );
        int[] stop= Arrays.copyOfRange( timerange, 7, 14 );
        Iterator<HapiRecord> iter= new TAPDataSource(tapServerURL, id).getIterator(start, stop);
        while ( iter.hasNext() ) {
            HapiRecord r= iter.next();
            System.err.println( String.format( "%s %d fields", r.getIsoTime(0), r.length() ) );
        }
    }    
        
/**
     * This returns 769 fields while the info thinks it should be 897 (128 more).
     * @see https://github.com/hapi-server/server-java/issues/21
     * @param args
     * @throws ParseException 
     */
    public static void mainCase8( String[] args ) throws ParseException {
        String tapServerURL="https://csa.esac.esa.int/csa-sl-tap/";
        String id="C1_PP_WHI";
        String tr= "2012-12-15T20:00Z/2012-12-15T20:07Z";
        int[] timerange = TimeUtil.parseISO8601TimeRange(tr);
        int[] start= Arrays.copyOfRange( timerange, 0, 7 );
        int[] stop= Arrays.copyOfRange( timerange, 7, 14 );
        Iterator<HapiRecord> iter= new TAPDataSource(tapServerURL, id).getIterator(start, stop);
        while ( iter.hasNext() ) {
            HapiRecord r= iter.next();
            System.err.println( String.format( "%s %d fields", r.getIsoTime(0), r.length() ) );
        }
    }    
    
    public static void main(String[] args ) throws Exception {
        //mainCase1(args);
        //mainCase2(args);
        //mainCase3(args);
        //mainCase4(args);
        //mainCase5(args);
        //mainCase6(args);
        //mainCase7(args);
        mainCase8(args);
    }
}
