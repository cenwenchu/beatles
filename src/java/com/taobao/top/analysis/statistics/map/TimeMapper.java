package com.taobao.top.analysis.statistics.map;

import java.util.Calendar;
import java.util.Date;

import com.taobao.top.analysis.node.job.JobTask;

/**
 * 时间类型的MapClass实现
 * 
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-26
 *
 */
public class TimeMapper extends TimeKeyMapper {
	
	

	/**
	 * 
	 */
	private static final long serialVersionUID = 971861884374324170L;

	@Override
	protected Object generateValue(DefaultExpReportEntry entry,
			Object[] contents, JobTask jobtask) {


		Calendar calendar = Calendar.getInstance();
		
		long timestamp = Long.valueOf(contents[Integer.valueOf(entry
				.getKeys()[0]) - 1].toString());

		calendar.setTime(new Date(timestamp));

		if (entry.getMapParams() != null
				&& !entry.getMapParams().equals("")) {
			if (entry.getMapParams().startsWith("minute")) {
				// 可定制化
				int interval = 0;
				int minute = calendar.get(Calendar.MINUTE);

				if (!entry.getMapParams().equals("minute"))
					interval = Integer.valueOf(entry.getMapParams()
							.substring("minute:".length()));

				if (interval > 0) {
					int _timeCounter = 0;

					while (minute >= _timeCounter) {
						_timeCounter += interval;
					}

					minute = _timeCounter;

					if (minute == 60) {
						minute = 0;
						calendar.add(Calendar.HOUR, 1);
					}

				}

				int hour = calendar.get(Calendar.HOUR_OF_DAY);
				String hourStr;

				if (hour < 10)
					hourStr = new StringBuilder().append("0").append(hour)
							.toString();
				else
					hourStr = String.valueOf(hour);

				if (minute < 10)
					return new StringBuilder()
							.append(calendar.get(Calendar.YEAR)).append("-")
							.append(calendar.get(Calendar.MONTH) + 1)
							.append("-")
							.append(calendar.get(Calendar.DAY_OF_MONTH))
							.append(" ").append(hourStr).append(":0")
							.append(minute).toString();
				else
					return new StringBuilder()
							.append(calendar.get(Calendar.YEAR)).append("-")
							.append(calendar.get(Calendar.MONTH) + 1)
							.append("-")
							.append(calendar.get(Calendar.DAY_OF_MONTH))
							.append(" ").append(hourStr).append(":")
							.append(minute).toString();
			} else if (entry.getReduceParams().startsWith("Lminute")) {
				// 可定制化
				int interval = 0;
				int minute = calendar.get(Calendar.MINUTE);

				if (!entry.getReduceParams().equals("Lminute"))
					interval = Integer.valueOf(entry.getReduceParams()
							.substring("Lminute:".length()));

				if (interval > 0) {
					int _timeCounter = 0;

					while (minute >= _timeCounter) {
						_timeCounter += interval;
					}

					minute = _timeCounter;

					if (minute == 60) {
						minute = 0;
						calendar.add(Calendar.HOUR, 1);
					}

				}

				calendar.set(Calendar.MINUTE, minute);
				calendar.set(Calendar.SECOND, 0);
				calendar.set(Calendar.MILLISECOND, 0);

				return String.valueOf(calendar.getTimeInMillis());

			} else {
				int day = calendar.get(Calendar.DAY_OF_MONTH);

				if (day < 10)
					return new StringBuilder()
							.append(calendar.get(Calendar.YEAR)).append("-")
							.append(calendar.get(Calendar.MONTH) + 1)
							.append("-0")
							.append(calendar.get(Calendar.DAY_OF_MONTH))
							.toString();
				else
					return new StringBuilder()
							.append(calendar.get(Calendar.YEAR)).append("-")
							.append(calendar.get(Calendar.MONTH) + 1)
							.append("-")
							.append(calendar.get(Calendar.DAY_OF_MONTH))
							.toString();
			}
		} else {
			
			int hour = calendar.get(Calendar.HOUR_OF_DAY);

			if (hour < 10)
				return new StringBuilder().append(calendar.get(Calendar.YEAR))
						.append("-").append(calendar.get(Calendar.MONTH) + 1)
						.append("-")
						.append(calendar.get(Calendar.DAY_OF_MONTH))
						.append(" 0").append(hour).toString();
			else
				return new StringBuilder().append(calendar.get(Calendar.YEAR))
						.append("-").append(calendar.get(Calendar.MONTH) + 1)
						.append("-")
						.append(calendar.get(Calendar.DAY_OF_MONTH))
						.append(" ").append(hour).toString();
		}

	
	}
	
	
	

}
