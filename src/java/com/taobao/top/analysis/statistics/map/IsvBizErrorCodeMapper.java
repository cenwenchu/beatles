package com.taobao.top.analysis.statistics.map;

import java.util.ArrayList;
import java.util.List;

import com.taobao.top.analysis.node.job.JobTask;
import com.taobao.top.analysis.statistics.data.ReportEntry;
import com.taobao.top.analysis.util.AnalysisConstants;

/**
 * IsvBizErrorCodeMapper.java
 * @author yunzhan.jtq
 * 
 * @since 2012-2-15 下午09:51:50
 */
public class IsvBizErrorCodeMapper extends DefaultMapper {

    /**
     * 
     */
    private static final long serialVersionUID = 6751234200700102109L;

    private static final String ERROR_CODE = "errorCode";
    private static final String SUB_ERROR_CODE = "subErrorCode";
    private static final List<String> subErrorCodes = new ArrayList<String>();
    private static final List<String> errorCodes = new ArrayList<String>();
    static {

        errorCodes.add("23");
        errorCodes.add("40");
        errorCodes.add("41");
        errorCodes.add("43");
    };
    static {

        subErrorCodes.add("501");
        subErrorCodes.add("503");
        subErrorCodes.add("504");
        subErrorCodes.add("505");
        subErrorCodes.add("506");
        subErrorCodes.add("507");
        subErrorCodes.add("508");
        subErrorCodes.add("509");
        subErrorCodes.add("511");
        subErrorCodes.add("554");
        subErrorCodes.add("556");
        subErrorCodes.add("558");
        subErrorCodes.add("559");
        subErrorCodes.add("562");
        subErrorCodes.add("563");
        subErrorCodes.add("564");
        subErrorCodes.add("566");
        subErrorCodes.add("567");
        subErrorCodes.add("568");
        subErrorCodes.add("569");
        subErrorCodes.add("571");
        subErrorCodes.add("581");
        subErrorCodes.add("582");
        subErrorCodes.add("583");
        subErrorCodes.add("594");
        subErrorCodes.add("595");
        subErrorCodes.add("596");
        subErrorCodes.add("597");
        subErrorCodes.add("601");
        subErrorCodes.add("611");
        subErrorCodes.add("612");
        subErrorCodes.add("613");
        subErrorCodes.add("614");
        subErrorCodes.add("621");
        subErrorCodes.add("622");
        subErrorCodes.add("623");
        subErrorCodes.add("651");
        subErrorCodes.add("653");
        subErrorCodes.add("654");
        subErrorCodes.add("655");
        subErrorCodes.add("656");
        subErrorCodes.add("657");
        subErrorCodes.add("658");
        subErrorCodes.add("659");
        subErrorCodes.add("661");
        subErrorCodes.add("662");
        subErrorCodes.add("663");
        subErrorCodes.add("664");

        subErrorCodes.add("510");
        subErrorCodes.add("550");
        subErrorCodes.add("551");
        subErrorCodes.add("552");
        subErrorCodes.add("553");
        subErrorCodes.add("555");
        subErrorCodes.add("557");
        subErrorCodes.add("560");
        subErrorCodes.add("561");
        subErrorCodes.add("570");
        subErrorCodes.add("580");
        subErrorCodes.add("590");
        subErrorCodes.add("591");
        subErrorCodes.add("592");
        subErrorCodes.add("540");
        subErrorCodes.add("541");
        subErrorCodes.add("542");
        subErrorCodes.add("610");
        subErrorCodes.add("615");
        subErrorCodes.add("620");
        subErrorCodes.add("630");
        subErrorCodes.add("650");
        subErrorCodes.add("652");
        subErrorCodes.add("660");
        subErrorCodes.add("670");
        subErrorCodes.add("673");
        subErrorCodes.add("674");
        subErrorCodes.add("710");

        subErrorCodes.add("ID is illegal");
        subErrorCodes.add("invalid parameter");
        subErrorCodes.add("invalid-parameter");
        subErrorCodes.add("invalid-permission");
        subErrorCodes.add("MEDIA_CAT_NAME_IS_EXISTS");
        subErrorCodes.add("MEDIA_EXSIT_FILE_NAME");
        subErrorCodes.add("MEDIA_FILE_NAME_OVER_MAXLENGTH");
        subErrorCodes.add("MEDIA_VIDEO_FILE_NEED_2SCALE");
        subErrorCodes.add("MEIA_NAME_NOT_ALLOW_CHAR");
        subErrorCodes.add("missing-parameter");
        subErrorCodes.add("parameters-mismatch");
        subErrorCodes.add("PICTURE_ERROR_FORMAT");
        subErrorCodes.add("PICTURE_OVER_AVAILSPACE");
        subErrorCodes.add("PICTURE_PARAMETER_ERROR");
        subErrorCodes.add("trade-not-exist");
        subErrorCodes.add("user-not-exist");

    };

    @Override
    public String generateKey(ReportEntry entry, String[] contents, JobTask jobtask) {
        String key = super.generateKey(entry, contents, jobtask);

        if (AnalysisConstants.IGNORE_PROCESS.equals(key))
            return AnalysisConstants.IGNORE_PROCESS;
        int position = jobtask.getStatisticsRule().getAliasPool().get(ERROR_CODE).getKey();
        String errorCode = contents[Integer.valueOf(position) - 1];
        position = jobtask.getStatisticsRule().getAliasPool().get(SUB_ERROR_CODE).getKey();
        String subErrorCode = contents[Integer.valueOf(position) - 1];
        int errorCode_num = 0;
        try {
            errorCode_num = Integer.parseInt(errorCode);
        } catch (RuntimeException e) {
            return AnalysisConstants.IGNORE_PROCESS;
        }

        if (errorCodes.contains(errorCode)
                || subErrorCodes.contains(subErrorCode)
                || (errorCode_num > 100 || errorCode_num == 15)
                && (null != subErrorCode && subErrorCode.startsWith("isv"))) {
            return key;
        } else {
            return AnalysisConstants.IGNORE_PROCESS;
        }
    }
}
