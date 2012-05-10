package com.taobao.top.analysis.statistics.map;

import java.util.ArrayList;
import java.util.List;

import com.taobao.top.analysis.node.job.JobTask;
import com.taobao.top.analysis.statistics.data.ReportEntry;
import com.taobao.top.analysis.util.AnalysisConstants;

/**
 * IspErrorCodeMapper.java
 * @author yunzhan.jtq
 * 
 * @since 2012-2-15 下午09:39:39
 */
public class IspErrorCodeMapper extends DefaultMapper {

    /**
     * 
     */
    private static final long serialVersionUID = -1438780686686027335L;

    private static final String ERROR_CODE = "errorCode";
    private static final String SUB_ERROR_CODE = "subErrorCode";
    private static final List<String> subErrorCodes = new ArrayList<String>();

    static {

        subErrorCodes.add("900");
        subErrorCodes.add("901");
        subErrorCodes.add("902");
    };

    /* (non-Javadoc)
     * @see com.taobao.top.analysis.statistics.map.AbstractMapper#generateKey(com.taobao.top.analysis.statistics.data.ReportEntry, java.lang.String[], com.taobao.top.analysis.node.job.JobTask)
     */
    @Override
    protected String generateKey(ReportEntry entry, String[] contents, JobTask jobtask) {
        String key = super.generateKey(entry, contents, jobtask);

        if (AnalysisConstants.IGNORE_PROCESS.equals(key))
            return AnalysisConstants.IGNORE_PROCESS;
        int position = jobtask.getStatisticsRule().getAliasPool().get(ERROR_CODE).getKey();
        String errorCode = contents[Integer.valueOf(position) - 1];
        int errorCode_num = 0;
        try {
            errorCode_num = Integer.parseInt(errorCode);
        } catch (RuntimeException e) {
            return AnalysisConstants.IGNORE_PROCESS;
        }

        position = jobtask.getStatisticsRule().getAliasPool().get(SUB_ERROR_CODE).getKey();
        String subErrorCode = contents[Integer.valueOf(position) - 1];

        if ((errorCode_num > 100 || errorCode_num == 15)
                && null != subErrorCode
                && (subErrorCodes.contains(subErrorCode)
                        || subErrorCode.startsWith("isp.") || "null"
                        .equals(subErrorCode))) {
            return key;
        } else {
            return AnalysisConstants.IGNORE_PROCESS;
        }
    }

}
