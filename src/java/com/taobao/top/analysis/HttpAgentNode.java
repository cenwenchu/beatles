package com.taobao.top.analysis;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.taobao.top.analysis.node.IJobManager;
import com.taobao.top.analysis.util.ExtFilenameFilter;
/**
 * 
 * @author zijiang.jl
 * 
 * 
 * 分布式http节点，提供分析器与外部应用的通道，基于http协议。
 * 
 * 
 * 
 * 1.健康检查
 * 
 * http://url/web?command=check
 * 
 * 
 * 2.运行时内存数据管理
 * 
 * 
 * http://url/web?command=manage&instance=in&method=mh&entry=en&key=k&value=val
 * 
 * 
 * method 支持：
 * get     获取数据
 * put     创建数据
 * post    修改数据
 * delete  删除数据
 * 
 * 
 * 3.属性文件和配置文件管理
 * 
 * http://url/web?command=config&instance=in&type=0&method=get
 * 
 * type:
 * 0   属性文件
 * 1   规则文件
 * 
 * method:
 * get 获取文件
 * post 推送文件
 * 
 *
 */

public class HttpAgentNode extends Thread {
	private static final Log logger = LogFactory.getLog(HttpAgentNode.class);
	private static final String COMMAND="command";
	private static final String COMMAND_CHECK="check";
	private static final String COMMAND_CONFIG="config";
	private static final String COMMAND_MANAGE="manage";
	
	
	private static final String METHOD="method";
	private static final String TYPE="type";
	private static final String INSTANCE="instance";
	private static final String LEVEL="level";
	private static final String ENTRY="entry";
	private static final String ALLENTRY="a_check_for_all_reference_entry";
	public static final String _DEFAULT_REGISTER_ = "_DEFAULT_RULE_REGISTRY_";
	
	
	
	private IJobManager jobManager;
	

	/**
     * @param jobManager the jobManager to set
     */
    public void setJobManager(IJobManager jobManager) {
        this.jobManager = jobManager;
    }

    private static String ip;
	static {
		try {
			ip = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
		}
	}



	interface Callback{
		public void callback(int code,long len) throws IOException;
	}
	

	@Override
	public void run() {
		try {
		    Thread.sleep(2000);
		    logger.error("jobManager:" + jobManager + ", config:" + jobManager.getConfig());
			HttpServer httpServer = HttpServer.create(
					new InetSocketAddress(jobManager.getConfig().getHttpPort()), 0);
			httpServer.createContext(
			    jobManager.getConfig().getHttpContext(),
					new HttpHandler() {
						private AtomicInteger checkCnt=new AtomicInteger(0);
						private AtomicInteger configCnt=new AtomicInteger(0);
						@Override
						public void handle(final HttpExchange arg0)throws IOException {
								try{
									String qu=arg0.getRequestURI().getQuery();
									if(qu==null) qu="command=check";
									java.util.Map<String, String> quMap=new java.util.HashMap<String, String>();
									StringBuilder sb = new StringBuilder();
									StringTokenizer st = new StringTokenizer(qu, "&");
								    while (st.hasMoreTokens()) {
							            String pair = st.nextToken();
							            int pos = pair.indexOf('=');
							            if (pos != -1) {
								            String key = parseName(pair.substring(0, pos), sb);
								            String val = parseName(pair.substring(pos+1, pair.length()), sb);
								            quMap.put(key, java.net.URLDecoder.decode(val,"UTF-8"));
							            }
								     }
								    
									String command=quMap.get(COMMAND);
									
									if(command==null||"".equals(command)) command=COMMAND_CHECK;
									
									logger.warn("http agent node accept command "+command);
									java.lang.StringBuilder exetime=new StringBuilder("the command:");
									exetime.append(command);
									exetime.append(" execute time:");
									
									
									if(COMMAND_CHECK.equals(command)){
										long start=System.currentTimeMillis();
										handleCheck(arg0.getRequestBody(),arg0.getResponseBody(),new Callback(){
											@Override
											public void callback(int code,long len) throws IOException{
												arg0.sendResponseHeaders(code, len);
												
											}
											
										});
										long end=System.currentTimeMillis();
										checkCnt.incrementAndGet();
										exetime.append(end-start);
										exetime.append(",execute count:");
										exetime.append(checkCnt.get());
										logger.warn(exetime.toString());
										return;
									}
									
									if(COMMAND_MANAGE.equals(command)){
                                        long start=System.currentTimeMillis();
                                        String method=quMap.get(METHOD);
                                        String level=quMap.get(LEVEL);
                                        String instance=quMap.get(INSTANCE);
                                        String entry=quMap.get(ENTRY);
                                        String key=quMap.get("key");
                                        String value=quMap.get("value");
                                        handleManage(arg0.getRequestBody(),arg0.getResponseBody(),new Callback(){
                                            @Override
                                            public void callback(int code,long len) throws IOException{
                                                arg0.sendResponseHeaders(code, len);
                                                
                                            }
                                            
                                        },method,level,instance,entry,key,value);
                                        
                                        long end=System.currentTimeMillis();
                                        exetime.append(end-start);
                                        exetime.append(",execute count:");
                                        logger.warn(exetime.toString());
                                        return;
                                    }
									
									if(COMMAND_CONFIG.equals(command)){
										long start=System.currentTimeMillis();
										String instance=quMap.get(INSTANCE);
										String type=quMap.get(TYPE);
										String method=quMap.get(METHOD);
										handleConfig(arg0.getRequestBody(),arg0.getResponseBody(),new Callback(){
											@Override
											public void callback(int code,long len) throws IOException{
												arg0.sendResponseHeaders(code, len);
												
											}
											
										},type,method,instance);
										long end=System.currentTimeMillis();
										configCnt.incrementAndGet();
										exetime.append(end-start);
										exetime.append(",execute count:");
										exetime.append(configCnt.get());
										logger.warn(exetime.toString());
										return;
									}
									
									logger.error("http agent node command:"+command+" was not supported");
									
								}catch(Throwable t){
									logger.error("http handle error", t);
								}
						}

					});
			httpServer.start();
		} catch (Throwable e) {
			logger.error("nested http server error", e);
		}
	}
	
	/**
     * //未做并发保护，谨慎使用
     * @param arg0
     * @param method
     * @param instance
     * @param entry
     * @param key
     * @param value
     */
    private void handleManage(InputStream is,OutputStream os,Callback callback,String method,String level,String instance,String entry,String key,String value){
        StringBuilder node = new StringBuilder("");
        try {
            if(instance!=null){
                if(jobManager.getJobs().get(instance)!=null){
                    if("get".equals(method)){
                        if(entry!=null){
                            if(jobManager.getJobs().get(instance).getJobResult().get(entry)!=null){
                                if(key!=null){
                                    node.append(jobManager.getJobs().get(instance).getJobResult().get(key));
                                }else{
                                    Map<String, Object> map=jobManager.getJobs().get(instance).getJobResult().get(entry);
                                    java.util.Iterator<String> it=map.keySet().iterator();
                                    while(it.hasNext()){
                                        node.append(it.next());
                                        node.append(",");
                                    }
                                }
                            }else{
                                node.append("not find entry "+entry+" in memory");
                                node.append("\r\n");
                                callback.callback(415,node.toString().getBytes("UTF-8").length);
                                os.write(node.toString().getBytes("UTF-8"));
                                os.flush();
                                os.close();
                                return;
        
                            }
                            
                        }else{
                            java.util.Iterator<String> it=jobManager.getJobs().get(instance).getJobResult().keySet().iterator();
                            node.append(ALLENTRY).append(",");
                            while(it.hasNext()){
                                node.append(it.next());
                                node.append(",");
                            }
                        }
                    } else if("put".equals(method)){
                        if(jobManager.getJobs().get(instance).getJobResult().get(entry)==null) jobManager.getJobs().get(instance).getJobResult().put(entry, new java.util.HashMap<String, Object>());
                        if(key!=null&&value!=null) jobManager.getJobs().get(instance).getJobResult().get(entry).put(key, value);
                    } else if("post".equals(method)){
                        if(entry!=null&&key!=null&&value!=null&&jobManager.getJobs().get(instance).getJobResult().get(entry)!=null) jobManager.getJobs().get(instance).getJobResult().get(entry).put(key, value);
                    }else if("delete".equals(method)){
                        if("1".equals(level)){
                            Map<String, Map<String, Object>> m=jobManager.getJobs().get(instance).getJobResult();
                            if(m!=null) m.clear();
                        }
                        if("2".equals(level)){
                            Map<String, Map<String, Object>> m=jobManager.getJobs().get(instance).getJobResult();
                            if(m!=null){
                                if(ALLENTRY.equals(entry)){
                                    java.util.Iterator<String> it=m.keySet().iterator();
                                    while(it.hasNext()){
                                        String st=it.next();
                                        if(jobManager.getJobs().get(instance).getStatisticsRule().getEntryPool().containsKey(st)) continue;
                                        Map<String, Object> b=m.remove(st);
                                        if(b!=null) b.clear();
                                    }   
                                }else{
                                    Map<String, Object> b=m.remove(entry);
                                    if(b!=null) b.clear();
                                }
                            }
                        }
                        
                        if("3".equals(level)){
                            Map<String, Map<String, Object>> m=jobManager.getJobs().get(instance).getJobResult();
                            if(m!=null){
                                Map<String, Object> b=m.get(entry);
                                if(b!=null) b.remove(key);
                            }
                        }
                    }else{
                        node.append("not support!method error");
                        node.append("\r\n");
                        callback.callback(415,node.toString().getBytes("UTF-8").length);
                        os.write(node.toString().getBytes("UTF-8"));
                        os.flush();
                        os.close();
                        return;
                    }
                }else{
                    node.append("not find instance "+instance+" in memory");
                    node.append("\r\n");
                    callback.callback(415,node.toString().getBytes("UTF-8").length);
                    os.write(node.toString().getBytes("UTF-8"));
                    os.flush();
                    os.close();
                    return;
                }
            }else{
                if(jobManager != null && jobManager.getConfig() != null){
                    java.util.Iterator<String> in=jobManager.getJobs().keySet().iterator();
                    while(in.hasNext()){
                        node.append(in.next());
                        node.append(",");
                    }
                }else{
                    node.append(_DEFAULT_REGISTER_);
                }

            }
        
            callback.callback(HttpServletResponse.SC_OK,node.toString().getBytes("UTF-8").length);
            os.write(node.toString().getBytes("UTF-8"));
            os.flush();
            os.close();
        } catch (IOException e) {
            try {
                if(os!=null) os.close();
            } catch (IOException ex) {
                logger.error(ex.getMessage(), ex);
            }
            logger.error(e.getMessage(), e);
        }
    }
	
	/**
	 * 
	 * @param arg0
	 */
	private void handleCheck(InputStream is,OutputStream os,Callback callback){
		StringBuilder node = new StringBuilder("");
		Set<String> ins = jobManager.getJobs().keySet();
		String in;
		java.util.Iterator<String> its = ins
				.iterator();
		while (its.hasNext()) {
			in = its.next();
			node.append(in);
			node.append(":");
			node.append("success");

			node.append(":");
			node.append("0");
			node.append("\r\n");
		}
		
		try {
			callback.callback(HttpServletResponse.SC_OK,node.toString().getBytes("UTF-8").length);
			os.write(node.toString().getBytes("UTF-8"));
			os.flush();
			os.close();
		} catch (IOException e) {
			try {
				if(os!=null) os.close();
			} catch (IOException ex) {
				logger.error(ex.getMessage(), ex);
			}
			logger.error(e.getMessage(), e);
			
		}
	}
	
		
	/**
	 * 
	 * 
	 * sendResponseHeaders  这方法存在重复设置的问题。但一般情况下不会出现。
	 * 
	 * 
	 * @param arg0
	 * @param type
	 * @param method
	 * @param instance
	 */
	private void handleConfig(InputStream is,OutputStream os,Callback callback,String type,String method,String instance){

		java.io.DataInputStream dis=new java.io.DataInputStream(new BufferedInputStream(is));
		java.io.DataOutputStream dos=new java.io.DataOutputStream(new BufferedOutputStream(os));
		FileInputStream fis=null;
		FileOutputStream fos=null;
		try{
			if(instance!=null&&this.jobManager.getJobs().get(instance)!=null){
				//属性文件
				if("0".equals(type)){
					if("get".equals(method)){
						File file=new File(this.jobManager.getConfig().getConfigFile());
						byte[] fd=file.getAbsolutePath().getBytes("UTF-8");
						if(file.exists()){
							fis=new java.io.FileInputStream(file);
							callback.callback(HttpServletResponse.SC_OK,4+4+fd.length+4+fis.available());
							dos.writeInt(1);
							dos.writeInt(fd.length);
							dos.write(fd);
							dos.writeInt(fis.available());
							byte[] buf=new byte[1024];
							int pos=-1;
							while((pos=fis.read(buf))!=-1){
								dos.write(buf, 0, pos);
								dos.flush();
							}
							fis.close();
							dos.close();
						}else{
							callback.callback(415,0);
							dos.write("property not exist".getBytes("UTF-8"));
							dos.flush();
							dos.close();
						}
					}
					
					if("post".equals(method)){
						int flen=dis.readInt();
						byte[] fb=new byte[flen];
						dis.read(fb);
						String fName=new String(fb,"UTF-8");
						boolean load=false;
						if(fName.endsWith(this.jobManager.getConfig().getConfigFile())) load=true;
						if(load){
							//临时文件输出
							File temp=new File(this.jobManager.getConfig().getConfigFile()+".temp");
							if(temp.exists()) temp.delete();
							fos=new FileOutputStream(temp);
							
							int a=-1;
							byte[] buf=new byte[1024];
							while((a=dis.read(buf))!=-1){
								fos.write(buf, 0, a);
								fos.flush();
								
							}
							dis.close();
							fos.close();
							File file=new File(this.jobManager.getConfig().getConfigFile());
							File bak=new File(this.jobManager.getConfig().getConfigFile()+".bak");
							if(bak.exists()) bak.delete();
							if(file.exists()){
								file.renameTo(bak);
							}
							temp.renameTo(file);
							callback.callback(HttpServletResponse.SC_OK,0);
							dos.flush();
							dos.close();
						}else{
							callback.callback(415,0);
							dos.write("file can't loaded".getBytes("UTF-8"));
							dos.flush();
							dos.close();
						}

					}
					return;
				}
				
				
				//XML文件,只能修改本地文件
				if("1".equals(type)){
					if(jobManager != null && jobManager.getConfig() != null){
						if("get".equals(method)){
							String[] configFiles=jobManager.getJobs().get(instance).getJobConfig().getReportConfigs();
							FileAttr files=new FileAttr();
							for (String config : configFiles) {
								String temp=config.substring(config.indexOf(":")+1);
								File cfg=new File(temp);
								listFile(files,cfg);
							}
							callback.callback(HttpServletResponse.SC_OK,4+8*files.size()+files.getFileLen()+files.getFnLen());
							dos.writeInt(files.size());
							for(int i=0;i<files.size();i++){
								dos.writeInt(files.getFileName().get(i).length);
								dos.write(files.getFileName().get(i));
								dos.writeInt(files.getFisList().get(i).available());
								byte[] buf=new byte[1024];
								int pos=-1;
								while((pos=files.getFisList().get(i).read(buf))!=-1){
									dos.write(buf, 0, pos);
									dos.flush();
								}
								files.getFisList().get(i).close();
							}
							dos.close();	
						}
						
						
						if("post".equals(method)){
							String[] configFiles=jobManager.getJobs().get(instance).getJobConfig().getReportConfigs();
							java.util.List<String> dirs=new java.util.ArrayList<String>();
							for (String config : configFiles) {
								String temp=config.substring(config.indexOf(":")+1);
								File cfg=new File(temp);
								if(cfg.isDirectory()){
									dirs.add(cfg.getAbsolutePath());
								}
							}
							
							int flen=dis.readInt();
							byte[] fb=new byte[flen];
							dis.read(fb);
							String fname=new String(fb,"UTF-8");
							
							//对于上传的文件需要判断能否被分析器加载。
							boolean load=false;
							for(String dir:dirs){
								if(fname.startsWith(dir)) load=true;
							}
							if(load){
								
								File temp=new File(fname+".temp");
								if(temp.exists()) temp.delete();
								fos=new FileOutputStream(temp);
								int a=-1;
								byte[] buf=new byte[1024];
								while((a=dis.read(buf))!=-1){
									fos.write(buf, 0, a);
									fos.flush();
									
								}
								dis.close();
								fos.close();

								File file=new File(fname);
								File bak=new File(fname+".bak");
								
								if(bak.exists()) bak.delete();
								if(file.exists()) {
									file.renameTo(bak);
								}
								temp.renameTo(file);
								callback.callback(HttpServletResponse.SC_OK,0);
								dos.flush();
								dos.close();
							}else{
								callback.callback(415,0);
								dos.write("file can't loaded".getBytes("UTF-8"));
								dos.flush();
								dos.close();
							}
						}
						
						return;
					}else{
						callback.callback(415,0);
						dos.write("slave can't find xml file".getBytes("UTF-8"));
						dos.flush();
						dos.close();
					}

				}
				
				callback.callback(415,0);
				dos.write("parameter error".getBytes("UTF-8"));
				dos.flush();
				dos.close();
			}else{
				callback.callback(415,0);
				dos.write("Can't find instance:".getBytes("UTF-8"));
				dos.write(instance.getBytes("UTF-8"));
				dos.flush();
				dos.close();
			}	
		}catch(Throwable t){
			try {
				callback.callback(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,0);
				dos.flush();
				dos.close();
			} catch (IOException e) {
				logger.error(e.getMessage(), e);
				
			}
		}finally{
			try {
				if(fis!=null) fis.close();
				if(fos!=null) fos.close();
				if(dis!=null) dis.close();
				if(dos!=null) dos.close();
			} catch (IOException e) {
				logger.error(e.getMessage(), e);
				
			}
		}
	}
    
	private static class FileAttr{
		private java.util.List<FileInputStream> fis=new java.util.ArrayList<FileInputStream>();
		private java.util.List<byte[]> fileName=new java.util.ArrayList<byte[]>();
		
		private int fileLen;
		private int fnLen;
		
	
		
		public int size(){
			if(fis.size()==fileName.size()) return fis.size();
			throw new java.lang.RuntimeException("the size is wrong");
		}
		
		public final java.util.List<byte[]> getFileName() {
			return fileName;
		}
		public final void addFileName(byte[] fileName) {
			this.fileName.add(fileName);
		}
		
		public final java.util.List<FileInputStream> getFisList() {
			return fis;
		}
		public final void addFileInputStream(java.io.FileInputStream is) {
			fis.add(is);
		}
		
		public final void removeFileInputStream(java.io.FileInputStream is) {
			fis.remove(is);
		}
		
		public final int getFileLen() {
			return fileLen;
		}
		public final void addFileLen(int fileLen) {
			this.fileLen+=fileLen;
		}
		public final int getFnLen() {
			return fnLen;
		}
		public final void addFnLen(int fnLen) {
			this.fnLen += fnLen;
		}
		
		
		
		
	}
	
	/**
	 * 递归枚举所有的文件
	 * @param files
	 * @param file
	 */
	public static void listFile(FileAttr files,File file){
		if(file.isFile()) {
			java.io.FileInputStream fs=null;
			boolean flag=false;
			try {
				fs = new java.io.FileInputStream(file);
				flag=true;
				files.addFileInputStream(fs);
				files.addFileLen(fs.available());
				byte[] a=file.getAbsolutePath().getBytes("UTF-8");
				files.addFileName(a);
				files.addFnLen(a.length);
			} catch (Exception e) {
				if(flag&&fs!=null) files.removeFileInputStream(fs);
			}
			return;
		}
		if(file.isDirectory()){
			File[] temps = file.listFiles(new ExtFilenameFilter(".xml"));
			for(File f:temps){
				listFile(files, f);
			}
		}
	}
	
	/**
	 * 解析字符或者编码后的字符
	 * @param s
	 * @param sb
	 * @return
	 */
    private String parseName(String s, StringBuilder sb) {
        sb.setLength(0);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i); 
            switch (c) {
            case '+':
                sb.append(' ');
                break;
            case '%':
                try {
                    sb.append((char) Integer.parseInt(s.substring(i+1, i+3), 
                                                      16));
                    i += 2;
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException();
                } catch (StringIndexOutOfBoundsException e) {
                    String rest  = s.substring(i);
                    sb.append(rest);
                    if (rest.length()==2)
                        i++;
                }
                
                break;
            default:
                sb.append(c);
                break;
            }
        }
        return sb.toString();
    }
}
