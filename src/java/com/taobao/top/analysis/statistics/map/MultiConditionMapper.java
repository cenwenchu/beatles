package com.taobao.top.analysis.statistics.map;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.top.analysis.node.job.JobTask;
import com.taobao.top.analysis.statistics.data.ReportEntry;
import com.taobao.top.analysis.util.AnalysisConstants;

/**
 * MultiConditionMapper.java
 * @author yunzhan.jtq
 * 
 * @since 2012-2-15 下午07:29:49
 */
public class MultiConditionMapper extends DefaultMapper {

    /**
     * 
     */
    private static final long serialVersionUID = -5604020449659822007L;

    private static final Log logger = LogFactory.getLog(MultiConditionMapper.class);
    private static final int c = ';';
    private ConcurrentHashMap<String, List<MapAndParam>> mapParamsClass =
            new ConcurrentHashMap<String, List<MapAndParam>>();


    @Override
    public String generateKey(ReportEntry entry, String[] contents, JobTask jobtask) {
        String mapParams = entry.getMapParams();
        if (StringUtils.isEmpty(mapParams)) {
            return super.generateKey(entry, contents, jobtask);
        }
        else {
            List<MapAndParam> multiMap = mapParamsClass.get(mapParams);
            if (multiMap == null) {
                multiMap = analysisMapParams(mapParams);
                mapParamsClass.put(mapParams, multiMap);
            }
            if (multiMap.size() == 0) {
                return AnalysisConstants.IGNORE_PROCESS;
            }
            MapAndParam generateKeyMap = multiMap.get(0);
            ReportEntry rEntry = null;
            for (int i = 1, n = multiMap.size(); i < n; i++) {
                try {
                    rEntry = entry.clone();
                }
                catch (CloneNotSupportedException e) {
                    logger.error(e);
                }
                rEntry.setMapClass(multiMap.get(i).getMapInstance());
                rEntry.setMapParams(multiMap.get(i).getMapParam());
                if (AnalysisConstants.IGNORE_PROCESS.equals(multiMap.get(i).getMapInstance()
                    .generateKey(rEntry, contents, jobtask))) {
                    return AnalysisConstants.IGNORE_PROCESS;
                }
            }
            try {
                rEntry = entry.clone();
            }
            catch (CloneNotSupportedException e) {
                logger.error(e);
            }
            rEntry.setMapClass(generateKeyMap.getMapInstance());
            rEntry.setMapParams(generateKeyMap.getMapParam());
            String key =
                    generateKeyMap.getMapInstance().generateKey(rEntry, contents, jobtask);
            return key;
        }
    }
    
    @Override
    protected Object generateValue(ReportEntry entry,
            Object[] contents, JobTask jobtask) {
        String mapParams = entry.getMapParams();
        if (StringUtils.isEmpty(mapParams)) {
            return super.generateValue(entry, contents, jobtask);
        } else {
            List<MapAndParam> multiMap = mapParamsClass.get(mapParams);
            if (multiMap == null) {
                multiMap = analysisMapParams(mapParams);
                mapParamsClass.put(mapParams, multiMap);
            }
            if (multiMap.size() == 0) {
                return super.generateValue(entry, contents, jobtask);
            }
            MapAndParam generateKeyMap = multiMap.get(0);
            ReportEntry rEntry = null;
            try {
                rEntry = entry.clone();
            }
            catch (CloneNotSupportedException e) {
                logger.error(e);
            }
            rEntry.setMapClass(generateKeyMap.getMapInstance());
            rEntry.setMapParams(generateKeyMap.getMapParam());
            return generateKeyMap.getMapInstance().generateValue(rEntry, contents, jobtask);
        }
    }


    private List<MapAndParam> analysisMapParams(String mapParams) {
        String[] params = StringUtils.split(mapParams, ",");
        List<MapAndParam> mapAndParamList = new ArrayList<MapAndParam>(params.length);

        for (int i = 0, n = params.length; i < n; i++) {
            String mapClassStr = null;
            String mapParam = null;

            int sepIndex = params[i].indexOf(c);
            if (sepIndex > -1) {
                mapClassStr = params[i].substring(0, sepIndex);
                mapParam = params[i].substring(sepIndex + 1, params[i].length());
            }
            else {
                mapClassStr = params[i].trim();
            }

            try {
                mapAndParamList.add(new MapAndParam((AbstractMapper) Class.forName(mapClassStr).newInstance(), mapParam));
            }
            catch (Exception e) {
                logger.error(e, e);
                continue;
            }
        }
        if (mapAndParamList.size() == 0) {
            logger.warn(new StringBuilder().append(" mapParam:").append(mapParams).append(" is not valid."));
        }
        return mapAndParamList;
    }

    /**
     * 保存一个map的实例以及这个map的输入参数
     * 
     * @author zhenzi 2010-12-7 上午07:58:22
     */
    private class MapAndParam {
        private AbstractMapper mapInstance;
        private String mapParam;


        public MapAndParam(AbstractMapper mapInstance, String mapParam) {
            this.mapInstance = mapInstance;
            this.mapParam = mapParam;
        }


        public AbstractMapper getMapInstance() {
            return mapInstance;
        }


        public String getMapParam() {
            return mapParam;
        }
    }

}
