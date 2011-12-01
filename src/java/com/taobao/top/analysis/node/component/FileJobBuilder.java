/**
 * 
 */
package com.taobao.top.analysis.node.component;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.top.analysis.config.JobConfig;
import com.taobao.top.analysis.config.MasterConfig;
import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.node.IJobBuilder;
import com.taobao.top.analysis.node.job.Job;
import com.taobao.top.analysis.node.job.JobTask;
import com.taobao.top.analysis.statistics.data.Alias;
import com.taobao.top.analysis.statistics.data.InnerKey;
import com.taobao.top.analysis.statistics.data.Report;
import com.taobao.top.analysis.statistics.data.ReportEntry;
import com.taobao.top.analysis.statistics.data.ReportEntryValueType;
import com.taobao.top.analysis.statistics.data.Rule;
import com.taobao.top.analysis.util.ReportUtil;

/**
 * 
 * 通过读取文件，创建多个任务
 * 
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-24
 *
 */
public class FileJobBuilder implements IJobBuilder{
	
	private final Log logger = LogFactory.getLog(FileJobBuilder.class);
	private MasterConfig config;
	private boolean needRebuild = false;
	/**
	 * 可用于rebuild，缓存上次的编译文件路径
	 */
	private String jobSource;
	
	@Override
	public boolean isNeedRebuild() {
		return needRebuild;
	}

	@Override
	public void setNeedRebuild(boolean needRebuild) {
		this.needRebuild = needRebuild;
	}
	
	@Override
	public MasterConfig getConfig() {
		return config;
	}


	@Override
	public void setConfig(MasterConfig config) {
		this.config = config;
	}
	
	public Map<String,Job> build() throws AnalysisException
	{
		if(config == null)
			throw new AnalysisException("master config is null!");
			
		return build(config.getJobsSource());
	}
	
	/**
	 * 从某一个位置获取任务集
	 * @param 可以自己扩展是从本地文件载入还是http等其他方式载入
	 * @return
	 * @throws AnalysisException
	 */
	public Map<String,Job> build(String config) throws AnalysisException 
	{
		if (logger.isInfoEnabled())
			logger.info("start build job from :" + config);
		
		jobSource = config;
		
		Map<String,Job> jobs = new HashMap<String,Job>();
		
		InputStream in = null;
		
		try
		{
			in = ReportUtil.getInputStreamFromFile(config);
			
			Properties prop = new Properties();
			prop.load(in);
			
			String js = (String)prop.get("jobs");
			
			if (js != null)
			{
				String[] instances = StringUtils.split(js,",");
				
				for(String j : instances)
				{
					Job job = new Job();
					Rule rule = new Rule();
					JobConfig jobconfig = new JobConfig();
					job.setStatisticsRule(rule);
					job.setJobConfig(jobconfig);
					job.setJobName(j);
					
					getConfigFromProps(j,jobconfig,prop);
					
					if (jobconfig.getReportConfigs() == null 
							|| (jobconfig.getReportConfigs() != null && jobconfig.getReportConfigs().length == 0))
					{
						throw new AnalysisException("job Config files should not be null!");
					}
						
					for(String conf : jobconfig.getReportConfigs() )
						buildReportModule(conf, rule);
					
					buildTasks(job);
					jobs.put(job.getJobName(), job);
				}
			}
		}
		catch(IOException ex)
		{
			logger.error(ex);
		}
		finally
		{
			if (in != null)
			{
				try 
				{
					in.close();
				} catch (IOException e) {
					logger.error(e);
				}
			}
		}
		
		return jobs;
	}
	
	void getConfigFromProps(String jobName,JobConfig jobConfig,Properties prop)
	{
		String prefix = new StringBuilder(jobName).append(".").toString();
		Map<String,String> tmp = new HashMap<String,String>();
		
		Iterator<Object> keys = prop.keySet().iterator();
		
		while(keys.hasNext())
		{
			String key = (String)keys.next();
			
			if (key.startsWith(prefix))
			{
				tmp.put(key.substring(prefix.length()), prop.getProperty(key));
			}
		}
		
		jobConfig.addAllToConfig(tmp);
	}
	
	/**
	 * 编译分析规则模型
	 * 
	 * @param 配置文件
	 * @param entry定义池
	 * @param 父级entry定义池
	 * @param 报表定义池
	 * @param 别名定义池
	 * @param 告警定义池
	 * @throws IOException 
	 * @throws XMLStreamException 
	 */
	public void buildReportModule(String configFile, Rule rule)
			throws AnalysisException, IOException {
		InputStream in = null;
		XMLEventReader r = null;
		Report report = null;
		StringBuilder globalConditions = new StringBuilder();
		StringBuilder globalValuefilter = new StringBuilder();
		List<String> globalMapClass = new ArrayList<String>();

		String domain = null;

		String localdir = new StringBuilder()
				.append(System.getProperty("user.dir"))
				.append(File.separatorChar).toString();

		if (configFile == null || "".equals(configFile)) {
			String error = "configFile can not be null !";
			logger.error(error);
			throw new AnalysisException(error);
		}

		try {
			XMLInputFactory factory = XMLInputFactory.newInstance();
			ClassLoader loader = Thread.currentThread().getContextClassLoader();

			if (configFile.startsWith("file:")) {
				try {
					in = new java.io.FileInputStream(new File(
							configFile.substring(configFile.indexOf("file:")
									+ "file:".length())));
				} catch (Exception e) {
					logger.error(e);
				}

				if (in == null)
					in = new java.io.FileInputStream(new File(localdir
							+ configFile.substring(configFile.indexOf("file:")
									+ "file:".length())));
			} else {
				URL url = loader.getResource(configFile);

				if (url == null) {
					String error = "configFile: " + configFile + " not exist !";
					logger.error(error);
					throw new java.lang.RuntimeException(error);
				}

				in = url.openStream();
			}

			r = factory.createXMLEventReader(in);
			List<String> parents = new ArrayList<String>();

			while (r.hasNext()) {
				XMLEvent event = r.nextEvent();

				if (event.isStartElement()) {
					StartElement start = event.asStartElement();

					String tag = start.getName().getLocalPart();

					if (tag.equalsIgnoreCase("domain")) {
						if (start.getAttributeByName(new QName("", "value")) != null) {
							domain = start.getAttributeByName(
									new QName("", "value")).getValue();
							rule.setDomain(domain);
						}

						continue;
					}

					if (tag.equalsIgnoreCase("alias")) {
						Alias alias = new Alias();
						alias.setName(start.getAttributeByName(
								new QName("", "name")).getValue());
						alias.setKey(start.getAttributeByName(
								new QName("", "key")).getValue());

						rule.getAliasPool().put(alias.getName(), alias);

						continue;
					}
					
					if (tag.equalsIgnoreCase("inner-key")){
						InnerKey innerKey = new InnerKey();
						
						innerKey.setKey(start.getAttributeByName(
								new QName("", "key")).getValue());
						
						boolean isExist = false;
						
						for(InnerKey ik : rule.getInnerKeyPool())
						{
							if (ik.getKey().equals(innerKey.getKey()))
							{
								logger.error("duplicate innerkey define, key :" + innerKey.getKey());
								
								isExist = true;
								break;
							}
						}
						
						if (!isExist)
						{
							if (innerKey.setFile(start.getAttributeByName(
									new QName("", "file")).getValue()))
								rule.getInnerKeyPool().add(innerKey);
							else
								logger.error("inner-key set error, file : " + innerKey.getFile());
							
						}
						
						continue;
						
					}

					if (tag.equalsIgnoreCase("global-condition")) {
						if (start.getAttributeByName(new QName("", "value")) != null) {
							if (globalConditions.length() > 0) {
								globalConditions.append("&"
										+ start.getAttributeByName(
												new QName("", "value"))
												.getValue());
							} else {
								globalConditions.append(start
										.getAttributeByName(
												new QName("", "value"))
										.getValue());
							}

						}

						continue;
					}

					if (tag.equalsIgnoreCase("global-mapClass")) {
						if (start.getAttributeByName(new QName("", "value")) != null) {
							globalMapClass.add(start.getAttributeByName(
									new QName("", "value")).getValue());
						}

						continue;
					}

					if (tag.equalsIgnoreCase("global-valuefilter")) {
						if (start.getAttributeByName(new QName("", "value")) != null) {
							globalValuefilter.append(
									start.getAttributeByName(
											new QName("", "value")).getValue())
									.append("&");
						}

						continue;
					}

					if (tag.equalsIgnoreCase("ReportEntry")
							|| tag.equalsIgnoreCase("entry")) {
						ReportEntry entry = new ReportEntry();
						if (tag.equalsIgnoreCase("ReportEntry"))
							setReportEntry(true, start, entry, report,
									rule.getEntryPool(), rule.getAliasPool(),
									globalConditions, globalValuefilter,
									globalMapClass, parents);
						else {
							setReportEntry(false, start, entry, report,
									rule.getEntryPool(), rule.getAliasPool(),
									globalConditions, globalValuefilter,
									globalMapClass, parents);
						}

						if (entry.getId() != null) {
							if (rule.getEntryPool().get(entry.getId()) != null)
								throw new java.lang.RuntimeException(
										"ID confict:" + entry.getId());
							rule.getEntryPool().put(entry.getId(), entry);
						}

						// 增加引用标识
						if (tag.equalsIgnoreCase("entry")) {
							if (entry.getId() != null)
								rule.getReferEntrys().put(entry.getId(), entry);
							else if (report.getReportEntrys() != null
									&& report.getReportEntrys().size() > 0)
								rule.getReferEntrys().put(
										report.getReportEntrys()
												.get(report.getReportEntrys()
														.size() - 1).getId(),
										report.getReportEntrys()
												.get(report.getReportEntrys()
														.size() - 1));
						}

						ReportEntry _tmpEntry = entry;

						if (_tmpEntry.getId() == null
								&& report.getReportEntrys() != null
								&& report.getReportEntrys().size() > 0)
							_tmpEntry = report.getReportEntrys().get(
									report.getReportEntrys().size() - 1);

						if (_tmpEntry.getBindingStack() != null) {
							if (_tmpEntry.getValueExpression() != null
									&& _tmpEntry.getValueExpression().indexOf(
											"entry(") >= 0)
								for (String k : _tmpEntry.getBindingStack()) {
									rule.getReferEntrys().put(k, null);
								}
						}

						continue;
					}

					if (tag.equalsIgnoreCase("report")) {
						if (report != null) {
						    if(report.getId() != null) {
						        if(rule.getReportPool().get(report.getId()) != null)
						            throw new java.lang.RuntimeException(
                                        "ID confict:" + report.getId());
						    }
							rule.getReportPool().put(report.getId(), report);
						}

						report = new Report();
						setReport(start, report, rule.getReportPool());
						continue;
					}

					if (tag.equalsIgnoreCase("entryList")) {
						report.setReportEntrys(new ArrayList<ReportEntry>());
						continue;
					}

				}

				if (event.isEndElement()) {
					EndElement end = event.asEndElement();

					String tag = end.getName().getLocalPart();

					if (tag.equalsIgnoreCase("reports") && report != null) {
						rule.getReportPool().put(report.getId(), report);
						continue;
					}

				}

			}
			// 给刚刚记录的parent Entry对象打上标识
			for (Iterator<String> iterator = parents.iterator(); iterator
					.hasNext();) {
				String parent = iterator.next();
				ReportEntry parentEntry = rule.getEntryPool().get(parent);
				rule.getParentEntryPool().put(parent, parentEntry);
			}

			// 删除没有被引用的公用的定义
			if (rule.getReferEntrys() != null
					&& rule.getReferEntrys().size() > 0) {
				Iterator<Entry<String, ReportEntry>> iter = rule.getEntryPool()
						.entrySet().iterator();
				StringBuilder invalidKeys = new StringBuilder();

				while (iter.hasNext()) {
					Entry<String, ReportEntry> e = iter.next();

					if (!rule.getReferEntrys().containsKey(e.getKey())) {
						iter.remove();
						invalidKeys.append(e.getKey()).append(",");
					}

				}

				if (invalidKeys.length() > 0)
					logger.error("File: " + configFile
						+ " ----- remove invalid entry define : "
						+ invalidKeys.toString());
			}

		} 
		catch(XMLStreamException ex)
		{
			logger.error(ex);
		}
		finally {
			if (r != null)
				try {
					r.close();
				} catch (XMLStreamException e) {
					logger.error(e);
				}

			if (in != null)
				in.close();

			r = null;
			in = null;
		}
	}
	
	/**
	 * 构建报表对象
	 * 
	 * @param 数据节点
	 * @param 上一个节点
	 * @param 报表池
	 */
	public void setReport(StartElement start, Report report,
			Map<String, Report> reportPool) {

		if (start.getAttributeByName(new QName("", "id")) != null) {
			report.setId(start.getAttributeByName(new QName("", "id"))
					.getValue());
		}

		if (start.getAttributeByName(new QName("", "file")) != null) {
			report.setFile(start.getAttributeByName(new QName("", "file"))
					.getValue());
		}

		if (start.getAttributeByName(new QName("", "period")) != null) {
			report.setPeriod(Boolean.valueOf(start.getAttributeByName(
					new QName("", "period")).getValue()));
		}
		
		if (start.getAttributeByName(new QName("", "append")) != null) {
			report.setAppend(Boolean.valueOf(start.getAttributeByName(
					new QName("", "append")).getValue()));
		}
		
		
		if (start.getAttributeByName(new QName("", "exportInterval")) != null) {
			report.setExportInterval(Long.valueOf(start.getAttributeByName(
					new QName("", "exportInterval")).getValue()));
		}


		if (start.getAttributeByName(new QName("", "orderby")) != null) {
			report.setOrderby(start
					.getAttributeByName(new QName("", "orderby")).getValue());
		}

		if (start.getAttributeByName(new QName("", "rowCount")) != null) {
			report.setRowCount(Integer.valueOf(start.getAttributeByName(
					new QName("", "rowCount")).getValue()));
		}

		// 以下新增条件设置 add by fangliang 2010-05-26
		Attribute attr = start.getAttributeByName(new QName("", "condition"));
		if (attr != null) {
			report.setConditions(attr.getValue());
		}
	}

	/**
	 * 构建报表entry对象
	 * 
	 * @param 是否是公用的entry
	 *            ，非公用的定义在report对象中
	 * @param 数据节点
	 * @param 当前处理的entry
	 * @param 当前隶属的report
	 *            ，这个值只有在非公用的entry解析的时候用到
	 * @param entry池
	 * @param 别名池
	 * @param 全局条件池
	 * @param 全局的valueFilter定义
	 * @param 全局的mapClass定义
	 * @param 父entry定义池
	 */
	public void setReportEntry(boolean isPublic, StartElement start,
			ReportEntry entry, Report report,
			Map<String, ReportEntry> entryPool, Map<String, Alias> aliasPool,
			StringBuilder globalConditions, StringBuilder globalValuefilter,
			List<String> globalMapClass, List<String> parents) {

		if (start.getAttributeByName(new QName("", "refId")) != null) {
			ReportEntry node = entryPool.get(start.getAttributeByName(
					new QName("", "refId")).getValue());

			if (node != null) {
				if (report.getConditions() != null
						&& report.getConditions().length() > 0) { // entry时如果report有conditions则克隆
					ReportEntry cloneReportEntry = node.clone();
					String id = start.getAttributeByName(new QName("", "id"))
							.getValue();
					if (id != null && !"".equals(id.trim())) {
						cloneReportEntry.setId(id);
					} else {
						cloneReportEntry.setId(report.getId() + "_"
								+ cloneReportEntry.getId());
					}

					cloneReportEntry.appendConditions(report.getConditions(),
							aliasPool);
				
					report.getReportEntrys().add(cloneReportEntry);
					if (entryPool.get(cloneReportEntry.getId()) != null)
						throw new java.lang.RuntimeException("ID confict:"
								+ cloneReportEntry.getId());
					entryPool.put(cloneReportEntry.getId(), cloneReportEntry);
				} else {
					report.getReportEntrys().add(node);
				}
			} else {
				String errorMsg = new StringBuilder()
						.append("ref Entry not exist :")
						.append(start
								.getAttributeByName(new QName("", "refId"))
								.getValue()).toString();

				throw new java.lang.RuntimeException(errorMsg);
			}

			return;
		}
		// 引用的方式,只在非共享模式下有用
		if (!isPublic && start.getAttributeByName(new QName("", "id")) != null
				&& start.getAttributeByName(new QName("", "name")) == null) {

			ReportEntry node = entryPool.get(start.getAttributeByName(
					new QName("", "id")).getValue());

			if (node != null) {
				if (report.getConditions() != null
						&& report.getConditions().length() > 0) { // entry时如果report有conditions则克隆
					ReportEntry cloneReportEntry = node.clone();
					cloneReportEntry.setId(report.getId() + "_"
							+ cloneReportEntry.getId());
					cloneReportEntry.appendConditions(report.getConditions(),
							aliasPool);
				
					report.getReportEntrys().add(cloneReportEntry);
					if (entryPool.get(cloneReportEntry.getId()) != null)
						throw new java.lang.RuntimeException("ID confict:"
								+ cloneReportEntry.getId());
					entryPool.put(cloneReportEntry.getId(), cloneReportEntry);
				} else {
					report.getReportEntrys().add(node);
				}
			} else {
				String errorMsg = new StringBuilder()
						.append("reportEntry not exist :")
						.append(start.getAttributeByName(new QName("", "id"))
								.getValue()).toString();

				throw new java.lang.RuntimeException(errorMsg);
			}

			return;
		}

		if (start.getAttributeByName(new QName("", "name")) != null) {
			entry.setName(start.getAttributeByName(new QName("", "name"))
					.getValue());
		}

		if (start.getAttributeByName(new QName("", "id")) != null) {
			entry.setId(start.getAttributeByName(new QName("", "id"))
					.getValue());
		} else {
			if (!isPublic && report != null) {
				entry.setId(new StringBuilder().append("report:")
						.append(report.getId()).append(entry.getName())
						.toString());
			}
		}

		if (entry.getId() == null)
			throw new java.lang.RuntimeException("entry id can't be null...");

		if (start.getAttributeByName(new QName("", "parent")) != null) {
			String parent = start.getAttributeByName(new QName("", "parent"))
					.getValue();
			entry.setParent(parent);
			parents.add(parent);

		}

		if (start.getAttributeByName(new QName("", "key")) != null) {
			entry.setKeys(start.getAttributeByName(new QName("", "key"))
					.getValue().split(","));

			// 用alias替换部分key
			ReportUtil.transformVars(entry.getKeys(), aliasPool);
		}

		if (start.getAttributeByName(new QName("", "value")) != null) {
			String content = start.getAttributeByName(new QName("", "value"))
					.getValue();
			String type = content.substring(0, content.indexOf("("));
			String expression = content.substring(content.indexOf("(") + 1,
					content.lastIndexOf(")"));
			entry.setValueType(ReportEntryValueType.getType(type));
			entry.setValueExpression(expression, aliasPool);
		}

		if (start.getAttributeByName(new QName("", "mapClass")) != null) {
			entry.setMapClass(start.getAttributeByName(
					new QName("", "mapClass")).getValue());
		}

		if (start.getAttributeByName(new QName("", "reduceClass")) != null) {
			entry.setReduceClass(start.getAttributeByName(
					new QName("", "reduceClass")).getValue());
		}

		if (start.getAttributeByName(new QName("", "mapParams")) != null) {
			entry.setMapParams(start.getAttributeByName(
					new QName("", "mapParams")).getValue());
		}

		if (start.getAttributeByName(new QName("", "reduceParams")) != null) {
			entry.setReduceParams(start.getAttributeByName(
					new QName("", "reduceParams")).getValue());
		}

		if (start.getAttributeByName(new QName("", "engine")) != null) {
			entry.setEngine(start.getAttributeByName(new QName("", "engine"))
					.getValue());
		}

		if (start.getAttributeByName(new QName("", "lazy")) != null) {
			entry.setLazy(Boolean.valueOf(start.getAttributeByName(
					new QName("", "lazy")).getValue()));
		}
		
		if (start.getAttributeByName(new QName("", "useCompressKeyMode")) != null) {
			entry.setUseCompressKeyMode(start.getAttributeByName(
					new QName("", "useCompressKeyMode")).getValue());
		}
		
		if (start.getAttributeByName(new QName("", "compressedDestMaxLength")) != null) {
			entry.setCompressedDestMaxLength(Integer.parseInt(start.getAttributeByName(
					new QName("", "compressedDestMaxLength")).getValue()));
		}
		

		// 以下修改conditions的设置方式 modify by fangliang 2010-05-26
		StringBuilder conditions = new StringBuilder();
		if (globalConditions != null && globalConditions.length() > 0) {
			conditions.append(globalConditions);
		}
		if (report != null && report.getConditions() != null
				&& report.getConditions().length() > 0 && !isPublic) { // 在非共享模式下
																		// 设置conditions
			conditions.append("&" + report.getConditions());
		}
		Attribute attr = start.getAttributeByName(new QName("", "condition"));
		if (attr != null) {
			conditions.append("&" + attr.getValue());
		}
		if (conditions.length() > 0) {
			entry.setConditions(conditions.toString(), aliasPool);
		}

		if (start.getAttributeByName(new QName("", "valuefilter")) != null) {
			if (globalValuefilter != null && globalValuefilter.length() > 0)
				entry.setValuefilter(new StringBuilder(globalValuefilter)
						.append(start.getAttributeByName(
								new QName("", "valuefilter")).getValue())
						.toString());
			else
				entry.setValuefilter(start.getAttributeByName(
						new QName("", "valuefilter")).getValue());
		} else {
			if (globalValuefilter != null && globalValuefilter.length() > 0)
				entry.setValuefilter(globalValuefilter.toString());
		}

		if (globalMapClass != null && globalMapClass.size() > 0)
			entry.setGlobalMapClass(globalMapClass);

		if (report != null)
			report.getReportEntrys().add(entry);

	}

	@Override
	public void init() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void releaseResource() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String,Job> rebuild() throws AnalysisException {
		if (this.needRebuild && jobSource != null)
		{
			this.needRebuild = false;
			return build(this.jobSource);
		}
		else
			return null;
	}

	@Override
	public void buildTasks(Job job) throws AnalysisException {
		
		if (job.getJobTasks() != null)
			job.getJobTasks().clear();
		
		JobConfig jobConfig = job.getJobConfig();
		
		if (jobConfig == null)
			throw new AnalysisException("generateJobTasks error, jobConfig is null.");
		
		//允许定义多个job通过逗号分割
		if (jobConfig.getInputParams() == null && jobConfig.getInput().indexOf(",") <= 0)
		{
			JobTask jobTask = new JobTask(jobConfig);
			jobTask.setStatisticsRule(job.getStatisticsRule());
			jobTask.setTaskId(job.getJobName() + "-" + job.getTaskCount());
			jobTask.setJobName(job.getJobName());
			job.addTaskCount();
			job.getJobTasks().add(jobTask);
		}
		else
		{
			if (jobConfig.getInputParams() != null)
			{
				String[] p = StringUtils.split(jobConfig.getInputParams(),":");
				String key = new StringBuilder("$").append(p[0]).append("$").toString();
				
				if (p.length != 2 || jobConfig.getInput().indexOf(key) < 0)
					throw new AnalysisException("inputParams invalidate : " + jobConfig.getInputParams());
				
				String[] params = StringUtils.split(p[1],",");
				
				for(String ps : params)
				{
					JobTask jobTask = new JobTask(jobConfig);
					jobTask.setStatisticsRule(job.getStatisticsRule());
					jobTask.setTaskId(job.getJobName() + "-" + job.getTaskCount());
					jobTask.setJobName(job.getJobName());
					jobTask.setInput(jobConfig.getInput().replace(key, ps));
					job.addTaskCount();
					job.getJobTasks().add(jobTask);
				}
			}
			else
			{
				String[] inputs = StringUtils.split(jobConfig.getInput(),",");
				
				for(String input : inputs)
				{
					JobTask jobTask = new JobTask(jobConfig);
					jobTask.setStatisticsRule(job.getStatisticsRule());
					jobTask.setTaskId(job.getJobName() + "-" + job.getTaskCount());
					jobTask.setJobName(job.getJobName());
					jobTask.setInput(input);
					job.addTaskCount();
					job.getJobTasks().add(jobTask);
				}
				
			}
		}	
	}

}
