/**
 * 
 */
package com.taobao.top.analysis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;

/**
 * @author fangweng
 * @Email fangweng@taobao.com
 * 2011-11-26
 * 
 * 用于提供Http数据来源的模拟服务器
 *
 */
public class TestServer {
	
	public static void main(String[] args) throws Exception
	{
		Handler handler=new AbstractHandler()
		{
		    public void handle(String target, HttpServletRequest request, HttpServletResponse response, int dispatch)
		        throws IOException, ServletException
		    {
		        response.setContentType("text/html");
		        response.setStatus(HttpServletResponse.SC_OK);
		        
		        BufferedReader br = null;
		        URL fileResource = ClassLoader.getSystemResource("top-access.log");
		        
		        try
		        {
		        	br = new BufferedReader(new java.io.InputStreamReader(fileResource.openStream(),"UTF-8"));
		        	
		        	String s;
		        	
		        	while((s = br.readLine()) != null)
		        	{
		        		response.getWriter().println(s);
		        	}
		        }
		        catch(Exception ex)
		        {
		        	ex.printStackTrace();
		        }
		        finally
		        {
		        	((Request)request).setHandled(true);
		        	
		        	if (br != null)
		        		br.close();
		        }
		    }
		};
		
		Server server = new Server(8181);
		server.setHandler(handler);
		server.start();
	}
	 
	

}
