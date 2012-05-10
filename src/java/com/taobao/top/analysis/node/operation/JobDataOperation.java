/**
 * 
 */
package com.taobao.top.analysis.node.operation;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import junit.framework.Assert;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.taobao.top.analysis.config.MasterConfig;
import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.node.job.Job;
import com.taobao.top.analysis.statistics.data.DistinctCountEntryValue;
import com.taobao.top.analysis.util.AnalysisConstants;
import com.taobao.top.analysis.util.AnalyzerFilenameFilter;
import com.taobao.top.analysis.util.ReportUtil;

/**
 * 对于某一个Job的数据做操作，支持Job的中间结果导入，导出，删除
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-29
 *
 */
public class JobDataOperation implements Runnable {

	private static final Log logger = LogFactory.getLog(JobDataOperation.class);
	Job job;
	String operation;
	MasterConfig config;
	String bckPrefix;
	
	public JobDataOperation(Job job,String operation,MasterConfig config)
	{
		this.job = job;
		this.operation = operation;
		this.config = config;
	}
	
	public JobDataOperation(Job job,String operation,MasterConfig config,String bckPrefix)
	{
		this.job = job;
		this.operation = operation;
		this.config = config;
		this.bckPrefix = bckPrefix;
	}
	
	@Override
	public void run() 
	{
		//先设置主干为空，然后再导出，不影响后续的合并处理，适用于磁盘换内存模式的导出
		if (operation.equals(AnalysisConstants.JOBMANAGER_EVENT_SETNULL_EXPORTDATA))
		{
			if (logger.isWarnEnabled())
				logger.warn(job.getJobName() +  " start clear trunk data and exportData now...");
				
			exportData(true);
			
			if (logger.isWarnEnabled())
                logger.warn(job.getJobName() +  " end clear trunk data and exportData ");
			return;
		}
		
		if(operation.equals(AnalysisConstants.JOBMANAGER_EVENT_EXPORTDATA))
		{
			if (logger.isWarnEnabled())
				logger.warn(job.getJobName() +  " start exportData now...");
				
			exportData(false);
			job.setLastExportTime(System.currentTimeMillis());
			
			if (logger.isWarnEnabled())
                logger.warn(job.getJobName() +  " end exportData ");
			
			return;
		}
		
		if (operation.equals(AnalysisConstants.JOBMANAGER_EVENT_DEL_DATAFILE))
		{
			if (logger.isWarnEnabled())
				logger.warn(job.getJobName() +  " delete exportData now...");
				
			deleteData();
			
			return;
		}
		
		if (operation.equals(AnalysisConstants.JOBMANAGER_EVENT_LOADDATA))
		{
			try 
			{
				if (logger.isWarnEnabled())
					logger.warn(job.getJobName() +  " start loadData now...");
				
				loadData(job, config);
				
				if (logger.isWarnEnabled())
                    logger.warn(job.getJobName() +  " end loadData");
			} 
			catch (AnalysisException e) 
			{
				logger.error("loadData error.",e);
			}
			
			return;
		}
			
		if (operation.equals(AnalysisConstants.JOBMANAGER_EVENT_LOADDATA_TO_TMP))
		{
			try 
			{
				if (logger.isWarnEnabled())
					logger.warn(job.getJobName() +  " start loadDataToTmp now...");
				
				JobDataOperation.loadDataToTmp(job, config);
				
				if (logger.isWarnEnabled())
                    logger.warn(job.getJobName() +  " end loadDataToTmp");
			} 
			catch (AnalysisException e) 
			{
				logger.error("loadDataToTmp error.",e);
			}
            catch (InterruptedException e) {
                logger.error("loadDataToTmp error.",e);
            }
			
			return;
		}
		
		if (operation.equals(AnalysisConstants.JOBMANAGER_EVENT_LOAD_BACKUPDATA))
		{
			try 
			{
				if (logger.isWarnEnabled())
					logger.warn(job.getJobName() +  " start loadData ,bckPrefix: " + bckPrefix);
				
				loadBackupData(bckPrefix);
				
				if (logger.isWarnEnabled())
                    logger.warn(job.getJobName() +  " end loadData ,bckPrefix: " + bckPrefix);
			} 
			catch (AnalysisException e) 
			{
				logger.error("load backupdata error.",e);
			}
			
			return;
		}
		
		if (operation.equals(AnalysisConstants.JOBMANAGER_EVENT_GET_SOURCETIMESTAMP))
		{
			try 
			{
				if (logger.isWarnEnabled())
					logger.warn(job.getJobName() +  " start get sourceTimeStamp.");
				
				JobDataOperation.getSourceTimeStamp(job, config);
				
				if (logger.isWarnEnabled())
                    logger.warn(job.getJobName() +  " end get sourceTimeStamp.");
			} 
			catch (AnalysisException e) 
			{
				logger.error("getSourceTimeStamp error.",e);
			}
			
			return;
		}
		
	}
	
	public static void getSourceTimeStamp(Job job, MasterConfig config) throws AnalysisException
	{
		innerLoad(true, job, config);
	}
	
	void loadBackupData(String bckPrefix) throws AnalysisException 
	{
		if (StringUtils.isEmpty(bckPrefix))
		{
			logger.error("epoch should not be empty! loadBackupData error! bckPrefix : " + bckPrefix);
			return;
		}
		
		Map<String, Map<String, Object>> resultPool = null;
		
		String destDir = getDestDir(job, config);	
		
		File dest = new File(destDir);
		if (!dest.exists() || (dest.exists() && !dest.isDirectory()))
		{
			logger.error(destDir + " dir not exist! loadBackupData fail.");
			return;
		}
		
		File[] bckfiles = dest.listFiles(new AnalyzerFilenameFilter(AnalysisConstants.IBCK_DATAFILE_SUFFIX));
		File backupData = null;
		
		for(File bf : bckfiles)
		{
			if (bf.getName().endsWith(new StringBuilder("-").append(bckPrefix)
					.append(AnalysisConstants.IBCK_DATAFILE_SUFFIX).toString()))
			{
				backupData = bf;
				break;
			}
		}
		
		if (backupData == null)
		{
			logger.error(new StringBuilder("loadBackupData fail! jobName: ")
								.append(job.getJobName()).append(" bckPrefix:").append(bckPrefix).append(" not exist!").toString());
			return;
		}
		
		List<Map<String, Map<String, Object>>> results = load(backupData,false,job,false);

		if (results == null || (results != null && results.size() == 0))
			return;
		
		resultPool = results.get(results.size() - 1);
		
		WriteLock writeLock = job.getTrunkLock().writeLock();
		
		try 
		{
			if (writeLock.tryLock(10, TimeUnit.MINUTES))
			{
				try
				{
					Map<String, Map<String, Object>> jobResult = job.getJobResult();
					if (jobResult != null)
						jobResult.clear();
					jobResult = null;
					
					job.setJobResult(resultPool);
					job.getEpoch().set(Integer.valueOf(1));
					
					logger.warn("success load data to jobTrunk.");
				}
				finally
				{
					writeLock.unlock();
				}
			}
			else
			{
				logger.error("loadData error, can't get writeLock! ");
			}
		} catch (InterruptedException e) {
			//do nothing
		}
				
	}
	
	
	static Map<String, Map<String, Object>> innerLoad(boolean onlyLoadSourceTimestamp, Job job, MasterConfig config) throws AnalysisException
	{
		String destDir = getDestDir(job, config);	
		
		File dest = new File(destDir);
		if (!dest.exists() || (dest.exists() && !dest.isDirectory()))
			return null;
		
		String _fileSuffix = AnalysisConstants.INNER_DATAFILE_SUFFIX;
		String _bckSuffix = AnalysisConstants.IBCK_DATAFILE_SUFFIX;
		
		
		File[] files = dest.listFiles(new AnalyzerFilenameFilter(_fileSuffix));
		File[] bckfiles = dest.listFiles(new AnalyzerFilenameFilter(_bckSuffix));
		
		if (files.length + bckfiles.length == 0)
			return null;
		
		File[] totalFiles = new File[files.length + bckfiles.length];
		
		if (files.length == 0)
			totalFiles = bckfiles;
		else
		{
			if (bckfiles.length == 0)
				totalFiles = files;
			else
			{
				System.arraycopy(files, 0, totalFiles, 0, files.length);
				System.arraycopy(bckfiles, 0, totalFiles, files.length, bckfiles.length);
			}
		}

		return load(totalFiles,onlyLoadSourceTimestamp, job);
	}
	
	void deleteData()
	{
		String destDir = getDestDir(job, config);	
		
		File dest = new File(destDir);
		if (!dest.exists() || (dest.exists() && !dest.isDirectory()))
			return;
		
		String _fileSuffix = AnalysisConstants.INNER_DATAFILE_SUFFIX;
		String _bckSuffix = AnalysisConstants.IBCK_DATAFILE_SUFFIX;
		
		
		File[] files = dest.listFiles(new AnalyzerFilenameFilter(_fileSuffix));
		File[] bckfiles = dest.listFiles(new AnalyzerFilenameFilter(_bckSuffix));
		
		if (files.length + bckfiles.length == 0)
			return;
		
		File[] totalFiles = new File[files.length + bckfiles.length];
		
		if (files.length == 0)
			totalFiles = bckfiles;
		else
		{
			if (bckfiles.length == 0)
				totalFiles = files;
			else
			{
				System.arraycopy(files, 0, totalFiles, 0, files.length);
				System.arraycopy(bckfiles, 0, totalFiles, files.length, bckfiles.length);
			}
		}
		
		// 当天的备份数据
		Calendar calendar = Calendar.getInstance();
		
		String prefix = new StringBuilder()
				.append(calendar.get(Calendar.YEAR)).append("-")
				.append(String.valueOf(calendar.get(Calendar.MONTH) + 1))
				.append("-").append(calendar.get(Calendar.DAY_OF_MONTH)).append("-")
				.append(job.getJobName())
				.toString();
		
		for (File f : totalFiles) 
		{
			if (!f.getName().startsWith(prefix))
				continue;
			
			f.delete();
		}
	}
	
	public static void loadDataToTmp(Job job, MasterConfig config) throws AnalysisException, InterruptedException
 {
        job.getLoadLock().lock();

        try {
            Map<String, Map<String, Object>> resultPool = innerLoad(false, job, config);

            if (resultPool == null)
                return;

            job.setDiskResult(resultPool);

            logger.warn("success load data to jobTmpTrunk.");
        }
        finally {
            job.getLoadLock().unlock();
        }
    }
	
	public static void loadData(Job job, MasterConfig config) throws AnalysisException 
	{
		Map<String, Map<String, Object>> resultPool = innerLoad(false, job, config);	
		
		if (resultPool == null)
			return;
		
		
		WriteLock writeLock = job.getTrunkLock().writeLock();
		
		try 
		{
			if (writeLock.tryLock(10, TimeUnit.MINUTES))
			{
				try
				{
					Map<String, Map<String, Object>> jobResult = job.getJobResult();
					if (jobResult != null)
						jobResult.clear();
					jobResult = null;
					
					job.setJobResult(resultPool);
					
					logger.warn("success load data to jobTrunk.job" + job.getJobName() + " resultPool:" + resultPool.size());
				}
				finally
				{
					writeLock.unlock();
				}
			}
			else
			{
				logger.error("loadData error, can't get writeLock! ");
			}
		} catch (InterruptedException e) {
			//do nothing
		}
				
	}
	
	/**
	 * 私有扁平化载入
	 * @param totalFiles
	 * @return
	 * @throws AnalysisException 
	 */
	private static Map<String, Map<String, Object>> load(File[] totalFiles,boolean onlyLoadSourceTimestamp, Job job) throws AnalysisException
	{
		boolean error=false;
		
		// 当天的备份数据
		Calendar calendar = Calendar.getInstance();
		String prefix = new StringBuilder()
			.append(calendar.get(Calendar.YEAR)).append("-")
			.append(String.valueOf(calendar.get(Calendar.MONTH) + 1))
			.append("-").append(calendar.get(Calendar.DAY_OF_MONTH)).append("-")
			.append(job.getJobName())
			.toString();
		
		for (File f : totalFiles) 
		{
			if (!f.getName().startsWith(prefix))
				continue;
			
			try
			{
				List<Map<String, Map<String, Object>>> resultPools = load(f,true,job,onlyLoadSourceTimestamp);
				
				if(resultPools != null && resultPools.size() > 0)
					return resultPools.get(resultPools.size() - 1);
			}
			catch(Exception ex)
			{
				logger.error("Load file "+f.getName()+" Error", ex);
				error = true;
			}
		}
		
		if (error)
			throw new AnalysisException("load init data error!");
		else
			return null;
	}
	
	void exportData(boolean setTrunkNull) {
		
		Map<String, Map<String, Object>> resultPool = job.getJobResult();
		
		
		if (resultPool != null && resultPool.size() > 0) {
			
			String destfile=null;
			try 
			{
				String destDir = getDestDir(job, config);
						
				File dest = new File(destDir);
				if (!dest.exists() || (dest.exists() && !dest.isDirectory()))
					dest.mkdirs();
				
				Calendar calendar = Calendar.getInstance();

				String dir = new File(destDir).getAbsolutePath();
									
				String _fileSuffix = AnalysisConstants.INNER_DATAFILE_SUFFIX;
				String _bckSuffix = AnalysisConstants.IBCK_DATAFILE_SUFFIX;
									
				if (!dir.endsWith(File.separator))
					dir += File.separator;

				destfile = new StringBuilder(dir)
					.append(calendar.get(Calendar.YEAR))
					.append("-")
					.append(String.valueOf(calendar.get(Calendar.MONTH) + 1))
					.append("-")
					.append(calendar.get(Calendar.DAY_OF_MONTH))
					.append("-")
					.append(job.getJobName())
					.append("-")
					.append(ReportUtil.getIp())
					.append(_fileSuffix).toString();
				
				//根据job版本来保存文件，版本信息保存在文件名中最后一位
				//先删除掉所有跨天创建的文件，保证不会把磁盘撑爆掉
				try
				{
					int oldDataKeepMinutes = 120;
					
					if (config != null)
						oldDataKeepMinutes = config.getOldDataKeepMinutes();
					
					long checkTimeLine = System.currentTimeMillis() - oldDataKeepMinutes * 60 * 1000;
					
					File tmpDir = new File(destDir);
					File[] bckFiles = tmpDir.listFiles(new AnalyzerFilenameFilter(_bckSuffix));
					
					for(File f : bckFiles)
					{
						if (f.lastModified() < checkTimeLine)
							f.delete();
					}
				}
				catch(Exception ex)
				{
					logger.error("delete old data file error!",ex);
				}
				
				//fix by fangweng 2012 增加分析器恢复能力，当前每10分钟备份一个
				_bckSuffix = new StringBuilder("-").append(calendar.get(Calendar.HOUR_OF_DAY)).append("-")
						.append(calendar.get(Calendar.MINUTE)/10).append(_bckSuffix).toString();
							
				File f = new File(destfile);
				if (f.exists() && f.isFile())
				{
					File bckFile = new File(destfile.replace(_fileSuffix,_bckSuffix));
					if (bckFile.exists())
						bckFile.delete();
						
					f.renameTo(new File(destfile.replace(_fileSuffix,_bckSuffix)));
				}
				
				if (setTrunkNull)
				{
					//需要支持如果结果里面包含distinct count 的情况，这种情况，部分数据不导出到磁盘（distinct部分）
					Map<String, Map<String, Object>> newTrunk = cloneResultFromTrunk(resultPool);
					
					job.setJobResult(newTrunk);
					export(resultPool,destfile,true,true);
				}
				else
				{
					ReadLock readLock = job.getTrunkLock().readLock();
					
					try
					{
						if (readLock.tryLock(10, TimeUnit.MINUTES))
						{
							try
							{
								export(resultPool,destfile,true,true);
							}
							finally{
								readLock.unlock();
							}
							
						}
						else
						{
							logger.error("exportData error! can't got readLock!");
						}
						
					}
					catch(InterruptedException ie)
					{
						//do nothing
					}
				}
				job.getTrunkExported().compareAndSet(false, true);
			}
			catch (Throwable ex) 
			{
				logger.error(ex.getMessage(), ex);
			} 
		}
	}
	
	
	/**
	 * 将主干上属于distinct count部分数据保留下来不输出，其他内容都输出且清空
	 * @return
	 */
	public Map<String, Map<String, Object>> cloneResultFromTrunk(Map<String, Map<String, Object>> trunk)
	{
		Map<String, Map<String, Object>> r = new HashMap<String,Map<String,Object>>();
		
		Iterator<String> keys = trunk.keySet().iterator();
		
		while(keys.hasNext())
		{
			String key = keys.next();
			
			Map<String,Object> imap = trunk.get(key);
			
			Iterator<String> ikeys = imap.keySet().iterator();
			
			while(ikeys.hasNext())
			{
				String ikey = ikeys.next();
				
				if (imap.get(ikey) instanceof DistinctCountEntryValue)
				{
					if (r.get(key) == null)
					{
						r.put(key, new HashMap<String,Object>());
					}
					
					r.get(key).put(ikey, imap.get(ikey));
					
					ikeys.remove();
				}
			}
			
			if (trunk.get(key).size() == 0)
				keys.remove();
		}
		
		if (r.size() == 0)
			return null; 
		else	
			return r;
	}
	
	public static String getDestDir(Job job, MasterConfig config)
	{
		String output = job.getJobConfig().getOutput();
		
		if (output.indexOf(":") > 0)
			output = output.substring(output.indexOf(":")+1);
		
		if (!output.endsWith(File.separator))
			output = output + File.separator;
		
		if (config != null && config.getMasterName() != null)
			return output + config.getMasterName() + File.separator + "tmp" + File.separator;
		else
			return output + "tmp" + File.separator;
	}
	
	/**
	 * 内部协议导入文件生成对象
	 * @param file
	 * @param 文件是否有压缩
	 * @return
	 * @throws AnalysisException 
	 */
	public static List<Map<String, Map<String, Object>>> load(File file,boolean useCompress,Job job,boolean onlyLoadSourceTimestamp) throws AnalysisException
	{
		BufferedReader breader = null;
		List<Map<String,Map<String,Object>>> resultPools = new ArrayList<Map<String,Map<String,Object>>>();
		
		Map<String, Map<String, Object>> resultPool = null;
		long beg = System.currentTimeMillis();
		
		try
		{	
			
			if(useCompress)
				breader = new BufferedReader(new InputStreamReader(new InflaterInputStream(new FileInputStream(file))));
			else
				breader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
			
			String line;
			boolean isFirstLine = true;
			while((line = breader.readLine()) != null)
			{
				//fix by fangweng 2012 增加分析器恢复能力
				if (isFirstLine)
				{
					try
					{
						long jobSourceTimeStamp = Long.parseLong(line);
						job.setJobSourceTimeStamp(jobSourceTimeStamp);
						
						if (onlyLoadSourceTimestamp)
							return resultPools;
					}
					catch(Exception ex)
					{
						// ignore 
					}
					
					isFirstLine = false;
				}
				
				if (line.equals(AnalysisConstants.EXPORT_DATA_SPLIT))
				{
					if(resultPool != null)
						resultPools.add(resultPool);
					
					resultPool = new HashMap<String,Map<String,Object>>();
					
					continue;
				}
				
				if (resultPool == null)
					resultPool = new HashMap<String,Map<String,Object>>();
				
				String[] contents = StringUtils.splitByWholeSeparator(line, AnalysisConstants.EXPORT_RECORD_SPLIT);
				
				Map<String,Object> m = new HashMap<String,Object>();

				resultPool.put(contents[0], m);
				
				for(int i = 1; i < contents.length; i++)
				{
					String[] tt = StringUtils.splitByWholeSeparator(contents[i], AnalysisConstants.EXPORT_COLUMN_SPLIT);
					
					if (tt.length >= 2)
						if (tt.length == 2)
							m.put(tt[0], tt[1]);
						else
							m.put(tt[0], Double.parseDouble(tt[2]));
				}
				
			}
			
			if (resultPool != null && resultPool.size() > 0)
			{
				resultPools.add(resultPool);
			}
			
			if (logger.isWarnEnabled())
				logger.warn("Load file "+ file.getName() +" Success , use : " + String.valueOf(System.currentTimeMillis() - beg));
			
			return resultPools;
		}
		catch(Exception ex)
		{
			logger.error("Load file "+ file.getName() +" Error", ex);
			throw new AnalysisException("Load file "+ file.getName() +" Error", ex);
		}
		finally
		{
			if (breader != null)
			{
				try {
					breader.close();
				} catch (IOException e) {
					logger.error(e.getMessage(), e);
				}
			}
		}
	}
	
	/**
	 * 内部协议导出对象
	 * @param 结果对象
	 * @param 文件名
	 * @param 是否需要压缩
	 * @param 是否需要覆盖原来的文件
	 */
	public static void export(Map<String, Map<String, Object>> resultPool,
			String FileName,boolean needCompress,boolean needOverwrite)
	{
	    if(resultPool == null)
	        return;
		BufferedWriter bwriter = null;
		
		try{
			if (needOverwrite)
				new File(FileName).createNewFile();
			else
			{
				File dest = new File(FileName);
				if (!dest.exists() || (dest.exists() && dest.isDirectory()))
					new File(FileName).createNewFile();
			}
			
			//这种模式下是否覆盖原文件参数无效，每次都会覆盖
			if (needCompress)
			{
				Deflater def = new Deflater(Deflater.BEST_SPEED, false);
			
				bwriter = new BufferedWriter(new OutputStreamWriter(new DeflaterOutputStream(
					new FileOutputStream(FileName), def)));
			}
			else		
				bwriter = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(FileName,!needOverwrite)));
			
			//fix by fangweng 2012 增加分析器恢复能力
			bwriter.write(String.valueOf(System.currentTimeMillis()));
			bwriter.write(ReportUtil.getSeparator());
		
			//写个开头
			bwriter.write(AnalysisConstants.EXPORT_DATA_SPLIT);
			bwriter.write(ReportUtil.getSeparator());
			
			Iterator<String> keys = resultPool.keySet().iterator();
			
			while(keys.hasNext())
			{
				String key = keys.next();
				
				bwriter.write(key);
				bwriter.write(AnalysisConstants.EXPORT_RECORD_SPLIT);
				
				Map<String,Object> m = resultPool.get(key);
							
				Iterator<String> mkeys = m.keySet().iterator();
				
				while(mkeys.hasNext())
				{
					String k = mkeys.next();
					bwriter.write(k);
					bwriter.write(AnalysisConstants.EXPORT_COLUMN_SPLIT);
					
					if (m.get(k) instanceof Double)
						bwriter.write(AnalysisConstants.EXPORT_DOUBLE_SPLIT);
					
					bwriter.write(m.get(k).toString());
					bwriter.write(AnalysisConstants.EXPORT_RECORD_SPLIT);
				}
				
				bwriter.write(ReportUtil.getSeparator());
				
			}
			bwriter.flush();
			
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
		finally {
			if (bwriter != null) {
				try {
					bwriter.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			
		}
	}
	
	public static void main(String[] args) throws AnalysisException
	{
		Map<String, Map<String, Object>> resultPool = new HashMap<String, Map<String, Object>>();
		Job job = new Job();
		
		Map<String,Object> innPool = new HashMap<String,Object>();
		innPool.put("key1", "value1");
		innPool.put("key2", "value2");
		innPool.put("key3", "value3");
		innPool.put("key4", "value4");
		
		resultPool.put("entry1", innPool);
		
		JobDataOperation.export(resultPool, "resultPool.tmp",false,true);
		JobDataOperation.export(resultPool, "resultPool.tmp",false,false);
		
		List<Map<String, Map<String, Object>>> resultPools = JobDataOperation.load(new File("resultPool.tmp"),false,job,false);
		
		Assert.assertEquals(2,resultPools.size());
		Assert.assertEquals("value4", resultPools.get(0).get("entry1").get("key4"));
		Assert.assertEquals("value4", resultPools.get(1).get("entry1").get("key4"));
		
		
		JobDataOperation.export(resultPool, "resultPool.tmp",true,true);
		resultPools = JobDataOperation.load(new File("resultPool.tmp"),true,job,false);
		
		Assert.assertEquals(1,resultPools.size());
		Assert.assertEquals("value4", resultPools.get(0).get("entry1").get("key4"));
		
		File destDir = new File("temp");
		
		File[] files = destDir.listFiles(new AnalyzerFilenameFilter(AnalysisConstants.TEMP_MASTER_DATAFILE_SUFFIX));
		
		System.out.println(files[0].getName().substring(files[0].getName().indexOf(AnalysisConstants.SPLIT_KEY)+AnalysisConstants.SPLIT_KEY.length()
				,files[0].getName().indexOf(AnalysisConstants.TEMP_MASTER_DATAFILE_SUFFIX)));
		
	}
	

}
