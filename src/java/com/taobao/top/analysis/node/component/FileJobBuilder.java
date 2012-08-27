/**
 * 
 */
package com.taobao.top.analysis.node.component;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

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
import com.taobao.top.analysis.node.job.JobResource;
import com.taobao.top.analysis.node.job.JobTask;
import com.taobao.top.analysis.node.operation.JobDataOperation;
import com.taobao.top.analysis.statistics.data.Alias;
import com.taobao.top.analysis.statistics.data.InnerKey;
import com.taobao.top.analysis.statistics.data.ObjectColumn;
import com.taobao.top.analysis.statistics.data.Report;
import com.taobao.top.analysis.statistics.data.ReportEntry;
import com.taobao.top.analysis.statistics.data.Rule;
import com.taobao.top.analysis.statistics.data.impl.SimpleCalculator;
import com.taobao.top.analysis.statistics.data.impl.SimpleCondition;
import com.taobao.top.analysis.statistics.data.impl.SimpleFilter;
import com.taobao.top.analysis.statistics.map.DefaultMapper;
import com.taobao.top.analysis.statistics.map.IMapper;
import com.taobao.top.analysis.statistics.reduce.DefaultReducer;
import com.taobao.top.analysis.statistics.reduce.IReducer;
import com.taobao.top.analysis.statistics.reduce.group.GroupFunctionFactory;
import com.taobao.top.analysis.util.AnalyzerFilenameFilter;
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
	
	private static final Log logger = LogFactory.getLog(FileJobBuilder.class);
	private MasterConfig config;
	private AtomicBoolean needRebuild = new AtomicBoolean(false);
	
	private IMapper defaultMapper = new DefaultMapper();
	private IReducer defaultReducer = new DefaultReducer();
	
	/**
	 * 可用于rebuild，缓存上次的编译文件路径
	 */
	private String jobResource;
	
	/**
	 * 将读取的job规则配置进行缓存
	 */
	private Map<String, JobResource> jobConfigs;
	
	/**
	 * jobs.properties上次修改时间
	 */
	private long lastFileModify;
	
	@Override
	public boolean isNeedRebuild() {
		return needRebuild.get();
	}

	@Override
	public void setNeedRebuild(boolean needRebuild) {
		this.needRebuild.compareAndSet(false, needRebuild);
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
		
		jobResource = config;
		
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
				Set<String> allMasters = new HashSet<String>();
				
				for(String j : instances)
				{
                    try {
                        Job job = new Job();
                        Rule rule = new Rule();
                        JobConfig jobconfig = new JobConfig();
                        job.setStatisticsRule(rule);
                        job.setJobConfig(jobconfig);
                        job.setJobName(j);

                        getConfigFromProps(j, jobconfig, prop);

                        if (jobconfig.getReportConfigs() == null
                                || (jobconfig.getReportConfigs() != null && jobconfig.getReportConfigs().length == 0)) {
                            throw new AnalysisException("job Config files should not be null!");
                        }

                        buildRule(jobconfig.getReportConfigs(), rule);
                        
                        //增加一个获得当前临时文件数据源游标的操作
//                        JobDataOperation jobDataOperation = new JobDataOperation(job,
//                        		AnalysisConstants.JOBMANAGER_EVENT_LOADDATA,this.config);
//                        jobDataOperation.run();
                        JobDataOperation.getSourceTimeStamp(job, this.config);
//                        JobDataOperation.loadDataToTmp(job, this.config);
//                        JobDataOperation.loadData(job, this.config);
                        
                        buildTasks(job);
                        jobs.put(job.getJobName(), job);
                        this.jobConfigs.put(job.getJobName(),
                            new JobResource(job.getJobName(), jobconfig.getReportConfigs()));
                        if (job.getJobConfig().getSaveTmpResultToFile() == null && this.config != null)
                            job.getJobConfig().setSaveTmpResultToFile(
                                String.valueOf(this.config.getSaveTmpResultToFile()));
                        if (job.getJobConfig().getAsynLoadDiskFilePrecent() < 0 && this.config != null)
                            job.getJobConfig().setAsynLoadDiskFilePrecent(
                                String.valueOf(this.config.getAsynLoadDiskFilePrecent()));
                    }
                    catch (Throwable e) {
                        logger.error("build job error : " + j, e);
                    }
                }
				
				//编译好rule后针对当前是否有mastergroup来做多master的report分配
				if (this.config != null && StringUtils.isNotEmpty(this.config.getMasterGroup()))
				{
					String[] ms = StringUtils.split(this.config.getMasterGroup(),",");
					List<String> masters = new ArrayList<String>();
					List<String> reports = new ArrayList<String>();
					for(String m : ms)
						masters.add(m);
							
					for(Job j : jobs.values())
					{
						Rule rule = j.getStatisticsRule();
						reports.clear();
						
						for(Report r : rule.getReportPool().values())
						{
							reports.add(new StringBuilder().append(r.getId())
									.append("|").append(r.getWeight()).toString());
						}
						
						//做一下改进，如果原来已经有分配的，为了保证数据一致性，则不再分配(保证中间结果的连贯性)
						//考虑原来就是比较平均分配的，然后将新来业务平均分配也是一样的
//						Map<String, String> report2Master = ReportUtil.SimpleAllocationAlgorithm(masters, reports, "|");
						Map<String, String> report2Master = new HashMap<String, String>();
						
						//此处将report2Master传入方法中进行修改，并非好的代码处理方式
//						AnalyzerUtil.loadReportToMaster(masters, reports, report2Master, j);
						if(this.config.getReportToMaster() != null && this.config.getReportToMaster().size() > 0) {
						    report2Master.putAll(this.config.getReportToMaster());
						}
						for(Report r : rule.getReportPool().values()) {
                            if(!report2Master.containsKey(r.getId()) && this.config.getDispatchMaster()) {
                                report2Master.put(r.getId(), ReportUtil.getIp() + ":" + this.config.getMasterPort());
                            }
                        }
						
						for(Entry<String,String> rm : report2Master.entrySet())
//							if (rule.getReport2Master().get(rm.getKey()) == null)
							rule.getReport2Master().put(rm.getKey(), rm.getValue());
						
						if (logger.isWarnEnabled() && rule.getReport2Master() != null)
						{
							StringBuilder report2MasterStr = new StringBuilder("report2Master Info : ");
							
							for(Entry<String,String> r : rule.getReport2Master().entrySet())
							{
								report2MasterStr.append("report: ")
									.append(r.getKey()).append(" -> master: ").append(r.getValue()).append(" , ");
							}
							
							logger.error(report2MasterStr.toString());
						}
//						AnalyzerUtil.exportReportToMaster(report2Master, j);
						allMasters.addAll(report2Master.values());
					}
					
				} 
                if (this.config != null) {
                    allMasters.add(ReportUtil.getIp() + ":" + this.config.getMasterPort());
                    StringBuilder sb = new StringBuilder("allMasters:");
                    for (String master : allMasters) {
                        sb.append(master).append(",");
                    }
                    logger.error(sb.toString());
                }
				for(Job j : jobs.values()) {
                    j.getStatisticsRule().setMasters(allMasters);
                }
				
			}
			lastFileModify = (new File(this.jobResource.substring(jobResource
	            .indexOf("file:") + "file:".length()))).lastModified();
		}
		catch(IOException ex)
		{
			logger.error(ex,ex);
		}
		finally
		{
			if (in != null)
			{
				try 
				{
					in.close();
				} catch (IOException e) {
					logger.error(e,e);
				}
			}
		}
		if (logger.isInfoEnabled())
            logger.info("build job complete from :" + config);
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
	 * @param configs
	 * @param rule
	 * @throws IOException 
	 * @throws AnalysisException 
	 */
    private void buildRule(String[] configs, Rule rule) throws AnalysisException, IOException {
        if (configs != null) {
            for (String config : configs) {
                if (config.startsWith("dir:")) {
                    File[] files =
                            new File(config.substring(config.indexOf("dir:") + "dir:".length()))
                                .listFiles(new AnalyzerFilenameFilter(".xml"));
                    if(files == null) {
                        logger.error("please have a check at " + config);
                    }
                    for (File file : files) {
                        this.buildReportModule(new StringBuilder("file:").append(file.getAbsolutePath()).toString(),
                            rule);
                    }
                    rule.setVersion(Calendar.getInstance().getTimeInMillis());
                }
                else {
                    this.buildReportModule(config, rule);
                    rule.setVersion(Calendar.getInstance().getTimeInMillis());
                }

            }
            // 遍历所有报表的entry，再遍历这些entry中所使用的其他的entry
            // 将entry与report的关联关系理清楚，因为在数据清理，多master数据发送的时候，都是按照entry来进行的
            for(Report report : rule.getReportPool().values()) {
                for(ReportEntry entry : report.getReportEntrys()) {
                    Set<String> referEntries = ((SimpleCalculator) entry.getCalculator()).getReferEntries();
                    if(referEntries == null)
                        continue;
                    for(String key : referEntries) {
                        setReport(entry, rule, rule.getEntryPool().get(key));
                    }
                }
            }
        }
    }
    
    /**
     * 采用递归来设置每一个entry的报表
     */
    private void setReport(ReportEntry entry, Rule rule, ReportEntry referEntry) {
        if(referEntry == null) {
            logger.error("please have a check at the referEntry. How can it be null?");
            return;
        }
        for(String reportName : entry.getReports()) {
            referEntry.addReport(rule.getReportPool().get(reportName));
        }
        Set<String> referEntries = ((SimpleCalculator) referEntry.getCalculator()).getReferEntries();
        if(referEntries == null)
            return;
        for(String refer : referEntries) {
            setReport(referEntry, rule, rule.getEntryPool().get(refer));
        }
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
	    if(logger.isInfoEnabled()) {
	        logger.info("start build rule in " + configFile);
	    }
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
					logger.error(e,e);
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
						alias.setKey(Integer.valueOf(start.getAttributeByName(
								new QName("", "key")).getValue()));

						rule.getAliasPool().put(alias.getName(), alias);

						continue;
					}
					
					if (tag.equalsIgnoreCase("inner-key")){
						InnerKey innerKey = new InnerKey();
						
						innerKey.setKey(Integer.parseInt(start.getAttributeByName(
								new QName("", "key")).getValue()));
						
						boolean isExist = false;
						
						for(InnerKey ik : rule.getInnerKeyPool())
						{
							if (ik.getKey() == innerKey.getKey())
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
						
						List<Object> bindingStack = ((SimpleCalculator)_tmpEntry.getCalculator()).getBindingStack();
						String valueExpression = ((SimpleCalculator)_tmpEntry.getCalculator()).getValue();
						if (bindingStack != null) {
							if (valueExpression != null
									&& valueExpression.indexOf(
											"entry(") >= 0)
								for (Object k : bindingStack) {
									rule.getReferEntrys().put((String)k, null);
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
            // 考虑到每一个job（也就是过去的instance的概念）对应于同一个rule，并非是一个配置文件对应一个rule，因此
            // rule中删除不用的key这一操作，应该是在所有配置文件读取完后进行
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
		catch(Throwable ex)
		{
			logger.error("the error config file is " + configFile,ex);
		}
		finally {
			if (r != null)
				try {
					r.close();
				} catch (XMLStreamException e) {
					logger.error(e,e);
				}

			if (in != null)
				in.close();

			r = null;
			in = null;
		}
		if(logger.isInfoEnabled()) {
            logger.info("complete build rule in " + configFile);
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
		
		if (start.getAttributeByName(new QName("", "weight")) != null) {
			report.setWeight(Integer.valueOf(start.getAttributeByName(new QName("", "weight"))
					.getValue()));
		}
		
		if (start.getAttributeByName(new QName("", "key")) != null) {
			report.setKey(start.getAttributeByName(new QName("", "key"))
					.getValue());
		}
		
		if (start.getAttributeByName(new QName("", "condition")) != null) {
			report.setCondition(start.getAttributeByName(new QName("", "condition"))
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
	 * @throws AnalysisException 
	 */
	public void setReportEntry(boolean isPublic, StartElement start,
			ReportEntry entry, Report report,
			Map<String, ReportEntry> entryPool, Map<String, Alias> aliasPool,
			StringBuilder globalConditions, StringBuilder globalValuefilter,
			List<String> globalMapClass, List<String> parents) throws AnalysisException {

		// 引用的方式,只在非共享模式下有用
		if ((!isPublic && start.getAttributeByName(new QName("", "id")) != null
				&& start.getAttributeByName(new QName("", "name")) == null) 
				||(start.getAttributeByName(new QName("", "refId")) != null))
		{

			ReportEntry node = entryPool.get(start.getAttributeByName(
					new QName("", "id")).getValue());

			if (node != null) {
				report.getReportEntrys().add(node);
				
				//给node增加report的属性
				node.addReport(report);
			} else {
				String errorMsg = new StringBuilder()
						.append("reportEntry not exist :")
						.append(start.getAttributeByName(new QName("", "id"))
								.getValue()).toString();

				throw new java.lang.RuntimeException(errorMsg);
			}

			return;
		}
		
		if (report != null)
		{
			//给node增加report的属性
			entry.addReport(report);
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
		
		if (start.getAttributeByName(new QName("", "mapClass")) != null) {
			String className = start.getAttributeByName(
					new QName("", "mapClass")).getValue();
			IMapper mapper = ReportUtil.getInstance(IMapper.class,
					Thread.currentThread().getContextClassLoader(),className,
					true);
			assert mapper != null;
			entry.setMapClass(mapper);
		}else{
			entry.setMapClass(defaultMapper);
		}

		if (start.getAttributeByName(new QName("", "reduceClass")) != null) {
			String className = start.getAttributeByName(
					new QName("", "reduceClass")).getValue();
			IReducer reducer = ReportUtil.getInstance(IReducer.class,
					Thread.currentThread().getContextClassLoader(),className,
					true);
			assert reducer != null;
			entry.setReduceClass(reducer);
		}else{
			entry.setReduceClass(defaultReducer);
		}
		
        // 添加period字段
        if (start.getAttributeByName(new QName("", "period")) != null) {
            entry.setPeriod(Boolean.valueOf(start.getAttributeByName(new QName("", "period")).getValue()));
        }

		if (start.getAttributeByName(new QName("", "mapParams")) != null) {
			entry.setMapParams(start.getAttributeByName(
					new QName("", "mapParams")).getValue());
		}

		if (start.getAttributeByName(new QName("", "reduceParams")) != null) {
			entry.setReduceParams(start.getAttributeByName(
					new QName("", "reduceParams")).getValue());
		}
		
		if (start.getAttributeByName(new QName("", "additions")) != null) {
			entry.setAdditions(start.getAttributeByName(
					new QName("", "additions")).getValue());
		}

		if (start.getAttributeByName(new QName("", "key")) != null) {
			
			String[] ks = start.getAttributeByName(new QName("", "key")).getValue().split(",");
			List<ObjectColumn> subKeys = new ArrayList<ObjectColumn>();
			
			int[] keys = ReportUtil.transformVars(ks, aliasPool,subKeys);

			// 用alias替换部分key
			entry.setKeys(keys);
			
			if (subKeys.size() > 0)
				entry.setSubKeys(subKeys);
		}
		else
		{
			//直接继承report的key
			if (!isPublic && report != null && report.getKey() != null)
			{
				String[] ks = report.getKey().split(",");
				List<ObjectColumn> subKeys = new ArrayList<ObjectColumn>();
				
				int[] keys = ReportUtil.transformVars(ks, aliasPool,subKeys);

				// 用alias替换部分key
				entry.setKeys(keys);
				
				if (subKeys.size() > 0)
					entry.setSubKeys(subKeys);
			}
			else
				throw new AnalysisException("entry key should not be null! entry name :" + entry.getName() + ", report:" + report.getFile());
		}
		

		if (start.getAttributeByName(new QName("", "value")) != null) {
			String content = start.getAttributeByName(new QName("", "value"))
					.getValue();
			String type = content.substring(0, content.indexOf("("));
			String expression = content.substring(content.indexOf("(") + 1,
					content.lastIndexOf(")"));
			entry.setGroupFunction(GroupFunctionFactory.getFunction(type));
			if (content.indexOf("entry(") >= 0){
				entry.setLazy(true);
			}
			entry.setCalculator(new SimpleCalculator(expression, aliasPool));
		}


		if (start.getAttributeByName(new QName("", "lazy")) != null) {
			entry.setLazy(Boolean.valueOf(start.getAttributeByName(
					new QName("", "lazy")).getValue()));
		}


		// 以下修改conditions的设置方式 modify by fangliang 2010-05-26
		StringBuilder conditions = new StringBuilder();
		if (globalConditions != null && globalConditions.length() > 0) {
			conditions.append(globalConditions);
		}
		
		//add by fangweng report 也可以有condition
		if (report != null && report.getCondition() != null && report.getCondition().length() > 0)
		{
			conditions.append("&").append(report.getCondition());
		}
		
		Attribute attr = start.getAttributeByName(new QName("", "condition"));
		if (attr != null) {
			conditions.append("&" + attr.getValue());
		}
		if (conditions.length() > 0) {
			entry.setCondition(new SimpleCondition(conditions.toString(), aliasPool));
		}
		String filter = null;
		if (start.getAttributeByName(new QName("", "valuefilter")) != null) {
			if (globalValuefilter != null && globalValuefilter.length() > 0)
				filter = new StringBuilder(globalValuefilter)
				.append(start.getAttributeByName(
				new QName("", "valuefilter")).getValue()).toString();
			else
				filter = start.getAttributeByName(new QName("", "valuefilter")).getValue();
		} else {
			if (globalValuefilter != null && globalValuefilter.length() > 0)
				filter = globalValuefilter.toString();
		}
		entry.setValueFilter(new SimpleFilter(filter));

		if (report != null)
			report.getReportEntrys().add(entry);

	}

	@Override
	public void init() {
	    this.jobConfigs = new HashMap<String, JobResource>();
	}

	@Override
	public void releaseResource() {
		this.jobConfigs.clear();
		this.jobConfigs = null;
	}

	@Override
	public Map<String,Job> rebuild(Map<String,Job> jobs) throws AnalysisException {
		if (this.needRebuild.getAndSet(false))
		{
		    Map<String,Job> result = build();
		    if(jobs != null) {
		        for(Entry<String, Job> entry : result.entrySet()) {
		            if(jobs.containsKey(entry.getKey())) {
//		                entry.getValue().getEpoch().set(jobs.get(entry.getKey()).getEpoch().incrementAndGet());
//		                entry.getValue().setJobSourceTimeStamp(jobs.get(entry.getKey()).getJobSourceTimeStamp());
//		                entry.getValue().setLastExportTime(jobs.get(entry.getKey()).getLastExportTime());
//		                entry.getValue().setJobResult(jobs.get(entry.getKey()).getJobResult());
		                jobs.get(entry.getKey()).rebuild(1, entry.getValue(), null);
		            } else {
		                jobs.put(entry.getKey(), entry.getValue());
		                jobs.get(entry.getKey()).rebuild(2, entry.getValue(), null);
		            }
		        }
		        for(Entry<String, Job> entry : jobs.entrySet()) {
		            if(!result.containsKey(entry.getKey())) {
		                entry.getValue().rebuild(-1, entry.getValue(), null);
		            }
		        }
		    }
		    return jobs;
		}
		else
			return null;
	}
	
	private String generateJobInputAddition(String input,Job job)
	{
		StringBuilder result = new StringBuilder(input);
		if (input != null && input.startsWith("http"))
		{
			result.append("&jobSourceTimeStamp=").append(job.getJobSourceTimeStamp())
				.append("&epoch=").append(job.getEpoch());
		}
		
		return result.toString();
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
			jobTask.setJobName(job.getJobName());
			jobTask.setUrl(jobTask.getInput());
			jobTask.setJobSourceTimeStamp(job.getJobSourceTimeStamp());
			
			jobTask.setInput(generateJobInputAddition(jobTask.getInput(),job));
			jobTask.setTaskId(getTaskIdFromUrl(job.getJobName(), jobTask.getUrl(), job.getTaskCount()));
			
			/**
			 * 目前使用master游标管理方式的只有hub
			 */
			Long begin = jobConfig.getBegin();
			if(begin == null)
			    begin = 0L;
			if(jobTask.getUrl().startsWith("hub://")) {
			    String key = jobTask.getUrl().substring(0, jobTask.getUrl().indexOf('?'));
			    job.getCursorMap().putIfAbsent(key, begin);
			    job.getTimestampMap().putIfAbsent(key, -1L);
			    jobTask.setJobSourceTimeStamp(job.getTimestampMap().get(key));
			}
			job.addTaskCount();
			jobTask.getTailCursor().set(jobConfig.getInit());
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
					jobTask.setJobName(job.getJobName());
					jobTask.setUrl(jobConfig.getInput().replace(key, ps));
					jobTask.setJobSourceTimeStamp(job.getJobSourceTimeStamp());
					jobTask.setInput(generateJobInputAddition(jobConfig.getInput().replace(key, ps),job));
					jobTask.setTaskId(getTaskIdFromUrl(job.getJobName(), jobTask.getUrl(), job.getTaskCount()));
					/**
		             * 目前使用master游标管理方式的只有hub
		             */
					Long begin = jobConfig.getBegin();
		            if(begin == null)
		                begin = 0L;
		            if(jobTask.getUrl().startsWith("hub://")) {
		                String keyU = jobTask.getUrl().substring(0, jobTask.getUrl().indexOf('?'));
		                job.getCursorMap().putIfAbsent(keyU, begin);
		                job.getTimestampMap().putIfAbsent(keyU, -1L);
		                jobTask.setJobSourceTimeStamp(job.getTimestampMap().get(keyU));
		            }
		            jobTask.getTailCursor().set(jobConfig.getInit());
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
					jobTask.setJobName(job.getJobName());
					jobTask.setInput(generateJobInputAddition(input,job));
					jobTask.setUrl(input);
					jobTask.setTaskId(getTaskIdFromUrl(job.getJobName(), jobTask.getUrl(), job.getTaskCount()));
                    /**
                     * 目前使用master游标管理方式的只有hub
                     */
                    Long begin = jobConfig.getBegin();
                    if(begin == null)
                        begin = 0L;
                    if(jobTask.getUrl().startsWith("hub://")) {
                        String key = jobTask.getUrl().substring(0, jobTask.getUrl().indexOf('?'));
                        job.getCursorMap().putIfAbsent(key, begin);
                        job.getTimestampMap().putIfAbsent(key, -1L);
                        jobTask.setJobSourceTimeStamp(job.getTimestampMap().get(key));
                    }
                    jobTask.getTailCursor().set(jobConfig.getInit());
					job.addTaskCount();
					job.getJobTasks().add(jobTask);
				}
				
			}
		}	
	}

	/*
	 * (non-Javadoc)
	 * @see com.taobao.top.analysis.node.IJobBuilder#getJobResource()
	 */
    public String getJobResource() {
        return jobResource;
    }

    /* (non-Javadoc)
     * @see com.taobao.top.analysis.node.IJobBuilder#isModified()
     */
    @Override
    public boolean isModified() {
        if(this.jobResource == null)
            return false;
        File file = new File(this.jobResource.substring(jobResource
            .indexOf("file:") + "file:".length()));
        if(file.lastModified() > lastFileModify)
            return true;
        for(String job : jobConfigs.keySet()) {
            if(jobConfigs.get(job).isModify())
                return true;
        }
        return false;
    }
    
    private String getTaskIdFromUrl(final String jobName, final String url, final int taskCount) {
        String temp = url.substring(url.indexOf("://") + 3);
        if(temp.indexOf(':') >= 0)
            temp = temp.substring(0, temp.indexOf(':'));
        else if(temp.indexOf('/') >= 0)
            temp = temp.substring(0, temp.indexOf('/'));
        temp = jobName + "-" + temp + "-" + taskCount;
        return temp;
    }

}
