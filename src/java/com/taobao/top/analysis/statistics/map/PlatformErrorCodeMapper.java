package com.taobao.top.analysis.statistics.map;

import java.util.ArrayList;
import java.util.List;

import com.taobao.top.analysis.node.job.JobTask;
import com.taobao.top.analysis.statistics.data.ReportEntry;
import com.taobao.top.analysis.util.AnalysisConstants;

/**
 * PlatformErrorCodeMapper.java
 * @author yunzhan.jtq
 * 
 * @since 2012-4-23 下午04:56:51
 */
public class PlatformErrorCodeMapper extends DefaultMapper {

    /**
     * 
     */
    private static final long serialVersionUID = 6003575691837084728L;

    private static final String ERROR_CODE = "errorCode";
    private static final List<String> errorCodes = new ArrayList<String>();

    static {
        errorCodes.add("3");
        errorCodes.add("10");
    };

    @Override
    public String generateKey(ReportEntry entry, String[] contents, JobTask jobtask) {
        String key = super.generateKey(entry, contents, jobtask);

        if (AnalysisConstants.IGNORE_PROCESS.equals(key))
            return AnalysisConstants.IGNORE_PROCESS;

        int position = jobtask.getStatisticsRule().getAliasPool().get(ERROR_CODE).getKey();

        String errorCode = contents[position - 1];

        if (!errorCodes.contains(errorCode))
            return AnalysisConstants.IGNORE_PROCESS;

        return key;
    }
}
