package com.taobao.top.analysis.statistics.map;

import java.util.ArrayList;
import java.util.List;

import com.taobao.top.analysis.node.job.JobTask;
import com.taobao.top.analysis.statistics.data.ReportEntry;
import com.taobao.top.analysis.util.AnalysisConstants;

/**
 * IsvSysErrorCodeMapper.java
 * @author yunzhan.jtq
 * 
 * @since 2012-4-23 下午04:50:58
 */
public class IsvSysErrorCodeMapper extends DefaultMapper {

    /**
     * 
     */
    private static final long serialVersionUID = 6724630027576963970L;

    private static final String ERROR_CODE = "errorCode";
    private static final List<String> errorCodes = new ArrayList<String>();
    static {

        errorCodes.add("4");
        errorCodes.add("5");
        errorCodes.add("6");
        errorCodes.add("7");
        errorCodes.add("8");
        errorCodes.add("9");
        errorCodes.add("11");
        errorCodes.add("12");
        errorCodes.add("13");
        errorCodes.add("20");
        errorCodes.add("21");
        errorCodes.add("22");
        errorCodes.add("24");
        errorCodes.add("25");
        errorCodes.add("26");
        errorCodes.add("27");
        errorCodes.add("28");
        errorCodes.add("29");
        errorCodes.add("30");
        errorCodes.add("31");
        errorCodes.add("32");
        errorCodes.add("33");
        errorCodes.add("34");
        errorCodes.add("42");
    };

    @Override
    public String generateKey(ReportEntry entry, String[] contents, JobTask jobtask) {
        String key = super.generateKey(entry, contents, jobtask);

        if (AnalysisConstants.IGNORE_PROCESS.equals(key))
            return AnalysisConstants.IGNORE_PROCESS;
        int position = jobtask.getStatisticsRule().getAliasPool().get(ERROR_CODE).getKey();
        String errorCode = contents[position - 1];

        if (errorCodes.contains(errorCode)) {
            return key;
        } else {
            return AnalysisConstants.IGNORE_PROCESS;
        }
    }
}
