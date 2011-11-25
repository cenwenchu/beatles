package com.taobao.top.analysis.node.base.map;

import java.util.List;
import java.util.Map;

import com.taobao.top.analysis.node.base.IReportMap;
import com.taobao.top.analysis.statistics.data.Alias;
import com.taobao.top.analysis.statistics.data.InnerKey;
import com.taobao.top.analysis.statistics.data.ReportEntry;
import com.taobao.top.analysis.util.AnalysisConstants;
import com.taobao.top.analysis.util.ReportUtil;

public class TimeMap implements IReportMap {

	@Override
	public String generateKey(ReportEntry entry, String[] contents,
			Map<String, Alias> aliasPool, String tempMapParams,List<InnerKey> innerKeyPool) {

		String key = ReportUtil.generateKey(entry, contents,innerKeyPool);

		if (AnalysisConstants.IGNORE_PROCESS.equals(key))
			return AnalysisConstants.IGNORE_PROCESS;

		long currentTime = Long.valueOf(contents[Integer.valueOf(entry
				.getKeys()[0]) - 1]);

		StringBuilder otherkeys = new StringBuilder();

		if (entry.getKeys().length > 1) {
			for (int i = 1; i < entry.getKeys().length; i++) {
				otherkeys.append(AnalysisConstants.SPLIT_KEY).append(
						contents[Integer.valueOf(entry.getKeys()[i]) - 1]);
			}
		}

		String mapParams = tempMapParams != null ? tempMapParams : entry
				.getMapParams();

		if (mapParams != null && !mapParams.equals("")) {
			StringBuilder result = new StringBuilder();
			long currentLongMinute = currentTime / (60 * 1000);

			// 分钟方式
			if (mapParams.startsWith("minute")) {
				// 可定制化
				int interval = 0;
				int currentMinute = (int) (currentLongMinute % 60);
				int addMinute = 0;

				if (!mapParams.equals("minute"))
					interval = Integer.valueOf(mapParams.substring("minute:"
							.length()));

				if (interval > 0) {
					while (currentMinute >= addMinute) {
						addMinute += interval;
					}
				}

				result.append(currentLongMinute + addMinute - currentMinute);

			}// 日方式
			else {
				result.append(currentTime / (86400000));
			}

			if (otherkeys.length() > 0)
				result.append(otherkeys);

			return result.toString();
		}// 没有任何参数
		else {
			StringBuilder result = new StringBuilder();

			result.append(currentTime / (3600000));

			if (otherkeys.length() > 0)
				result.append(otherkeys);

			return result.toString();

		}

	}

}
