
package org.hapiserver.source.cdaweb;

import java.util.Iterator;
import java.util.logging.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.hapiserver.AbstractHapiRecordSource;
import org.hapiserver.HapiRecord;

/**
 * CdawebServicesHapiRecordSource creates a HapiRecord iterator from CDF files,
 * using the WebServices or reading directly from files.
 * @author jbf
 */
public class CdawebServicesHapiRecordSource extends AbstractHapiRecordSource {
    
    private static final Logger logger= Logger.getLogger("hapi.cdaweb");
    
    private String id;
    JSONObject info;
    JSONObject data;
    AvailabilityIterator availabilityIterator;
    String root;
    
    public CdawebServicesHapiRecordSource( String hapiHome, String id, JSONObject info, JSONObject data ) {
        logger.entering("CdawebServicesHapiRecordSource","constructor");
        this.id= id;
        this.info= info;
        this.data= data;
        logger.exiting("CdawebServicesHapiRecordSource","constructor");
    }
    
    @Override
    public boolean hasGranuleIterator() {
        return true;
    }
    
    @Override
    public Iterator<int[]> getGranuleIterator(int[] start, int[] stop) {
        logger.entering("CdawebServicesHapiRecordSource","getGranuleIterator");
        String availInfo= CdawebAvailabilitySource.getInfo( "availability/"+id );
        JSONObject jsonObject;
        try {
            jsonObject = new JSONObject(availInfo);
        } catch (JSONException ex) {
            throw new RuntimeException(ex);
        }
        CdawebAvailabilitySource source= new CdawebAvailabilitySource( "notUsed", "availability/"+id, jsonObject, new JSONObject() );
        Iterator<HapiRecord> it = source.getIterator(start, stop);
        this.root= source.getRoot();
        
        availabilityIterator= new AvailabilityIterator(it);
        logger.exiting("CdawebServicesHapiRecordSource","getGranuleIterator");
        return availabilityIterator;
    }
    
    @Override
    public boolean hasParamSubsetIterator() {
        return true;
    }

    @Override
    public Iterator<HapiRecord> getIterator(int[] start, int[] stop, String[] params) {
        logger.entering("CdawebServicesHapiRecordSource","getIterator");
        String f= this.root + availabilityIterator.getFile();
        
        CdawebServicesHapiRecordIterator result=new CdawebServicesHapiRecordIterator(id, info, start, stop, params, f );
        
        logger.exiting("CdawebServicesHapiRecordSource","getIterator");
        return result;
    }    
 
}
