package com.taobao.top.analysis.statistics.map;

import com.taobao.top.analysis.node.job.JobTask;
import com.taobao.top.analysis.statistics.data.ReportEntry;
import com.taobao.top.analysis.util.AnalysisConstants;

/**
 * 时间类型的MapClass实现
 * 
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-26
 *
 */
public class TimeKeyMapper extends DefaultMapper {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 7987742959483474779L;
	
	

	@Override
	protected String generateKey(ReportEntry entry,
			String[] contents, JobTask jobtask) {
		long currentTime = Long.valueOf(contents[Integer.valueOf(entry
				.getKeys()[0]) - 1]);

		StringBuilder otherkeys = new StringBuilder();

		if (entry.getKeys().length > 1) {
			for (int i = 1; i < entry.getKeys().length; i++) {
				otherkeys.append(AnalysisConstants.SPLIT_KEY).append(
						contents[Integer.valueOf(entry.getKeys()[i]) - 1]);
			}
		}

		String mapParams = entry.getMapParams();

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
