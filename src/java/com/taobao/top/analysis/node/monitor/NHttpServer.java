package com.taobao.top.analysis.node.monitor;

import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.StringWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.HttpStatus;
import org.apache.http.MethodNotSupportedException;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.impl.nio.DefaultHttpServerIODispatch;
import org.apache.http.impl.nio.DefaultNHttpServerConnection;
import org.apache.http.impl.nio.DefaultNHttpServerConnectionFactory;
import org.apache.http.impl.nio.reactor.DefaultListeningIOReactor;
import org.apache.http.nio.NHttpConnection;
import org.apache.http.nio.NHttpConnectionFactory;
import org.apache.http.nio.NHttpServerConnection;
import org.apache.http.nio.entity.NFileEntity;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.nio.protocol.BasicAsyncRequestConsumer;
import org.apache.http.nio.protocol.BasicAsyncResponseProducer;
import org.apache.http.nio.protocol.HttpAsyncExchange;
import org.apache.http.nio.protocol.HttpAsyncRequestConsumer;
import org.apache.http.nio.protocol.HttpAsyncRequestHandler;
import org.apache.http.nio.protocol.HttpAsyncRequestHandlerRegistry;
import org.apache.http.nio.protocol.HttpAsyncService;
import org.apache.http.nio.reactor.IOEventDispatch;
import org.apache.http.nio.reactor.ListeningIOReactor;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.params.CoreProtocolPNames;
import org.apache.http.params.HttpParams;
import org.apache.http.params.SyncBasicHttpParams;
import org.apache.http.protocol.ExecutionContext;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpProcessor;
import org.apache.http.protocol.ImmutableHttpProcessor;
import org.apache.http.protocol.ResponseConnControl;
import org.apache.http.protocol.ResponseContent;
import org.apache.http.protocol.ResponseDate;
import org.apache.http.protocol.ResponseServer;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.runtime.resource.ResourceManagerImpl;

import com.taobao.top.analysis.config.MasterConfig;
import com.taobao.top.analysis.exception.AnalysisException;
import com.taobao.top.analysis.node.IComponent;
import com.taobao.top.analysis.node.component.MasterMonitor;


/**
 * 使用apache http components的一个简单HttpServer
 * @author sihai
 *
 */
public class NHttpServer implements IComponent<MasterConfig>{

	private static final Log logger = LogFactory.getLog(NHttpServer.class);
	
	/**
	 * Master端配置
	 */
	private MasterConfig config;
	
	/**
	 * Master的监控组件, 提供监控数据
	 */
	private MasterMonitor monitor;
	
	/**
	 * 模板编码
	 */
	private String templateEncode = "utf-8";
	
	/**
	 * 模板缓存
	 */
	private ConcurrentHashMap<String, Template> templateCache;
	
	/**
	 * 
	 */
	private boolean velocityOK = false;
	
	/**
	 * 
	 */
	private ListeningIOReactor ioReactor;
	
	@Override
	public MasterConfig getConfig() {
		return config;
	}

	@Override
	public void init() throws AnalysisException {
		
		// 
		templateCache = new ConcurrentHashMap<String, Template>();
		
		// 初始化模板引擎
		try {
			Velocity.setProperty(Velocity.RESOURCE_MANAGER_CLASS, ResourceManagerImpl.class.getName());
			Velocity.setProperty(Velocity.FILE_RESOURCE_LOADER_PATH, config.getMonitorDocRoot());
			Velocity.setProperty(Velocity.INPUT_ENCODING, "utf-8");
			Velocity.setProperty(Velocity.OUTPUT_ENCODING, "utf-8");
			Velocity.init();
			velocityOK = true;
		} catch (Exception e) {
			velocityOK = false;
			logger.error("Exception: init velocity:", e);
		}
		// init the server
		
		// HTTP parameters for the server
        HttpParams params = new SyncBasicHttpParams();
        params.setIntParameter(CoreConnectionPNames.SO_TIMEOUT, 5000)
            .setIntParameter(CoreConnectionPNames.SOCKET_BUFFER_SIZE, 8 * 1024)
            .setBooleanParameter(CoreConnectionPNames.TCP_NODELAY, true)
            .setParameter(CoreProtocolPNames.ORIGIN_SERVER, "HttpTest/1.1");
        // Create HTTP protocol processing chain
        HttpProcessor httpproc = new ImmutableHttpProcessor(new HttpResponseInterceptor[] {
                // Use standard server-side protocol interceptors
                new ResponseDate(),
                new ResponseServer(),
                new ResponseContent(),
                new ResponseConnControl()
        });
        // Create request handler registry
        HttpAsyncRequestHandlerRegistry reqistry = new HttpAsyncRequestHandlerRegistry();
        // Register the default handler for all URIs
        reqistry.register("*", new HttpFileHandler(new File(config.getMonitorDocRoot())));
        // Create server-side HTTP protocol handler
        HttpAsyncService protocolHandler = new HttpAsyncService(
                httpproc, new DefaultConnectionReuseStrategy(), reqistry, params) {

            @Override
            public void connected(final NHttpServerConnection conn) {
                System.out.println(conn + ": connection open");
                super.connected(conn);
            }

            @Override
            public void closed(final NHttpServerConnection conn) {
                System.out.println(conn + ": connection closed");
                super.closed(conn);
            }

        };
        // Create HTTP connection factory
        NHttpConnectionFactory<DefaultNHttpServerConnection> connFactory;
        connFactory = new DefaultNHttpServerConnectionFactory(params);
        // Create server-side I/O event dispatch
        final IOEventDispatch ioEventDispatch = new DefaultHttpServerIODispatch(protocolHandler, connFactory);
        // Create server-side I/O reactor
        new Thread(new Runnable(){
        	
        	@Override
        	public void run() {
	        	try {
	        		ioReactor = new DefaultListeningIOReactor();
	                // Listen of the given port
	                ioReactor.listen(new InetSocketAddress(config.getMonitorPort()));
	                // Ready to go!
	                ioReactor.execute(ioEventDispatch);
	            } catch (InterruptedIOException ex) {
	                System.err.println("Interrupted");
	                logger.error("NHttpServer failed", ex);
	            } catch (IOException e) {
	                System.err.println("I/O error: " + e.getMessage());
	                logger.error("NHttpServer failed", e);
	            }
        	}
        }, "NHttp-Server-Thread").start();
	}

	@Override
	public void releaseResource() {
		templateCache.clear();
		if(ioReactor != null) {
			try {
				ioReactor.shutdown();
			} catch (IOException e) {
				logger.error(e);
			}
		}
	}

	@Override
	public void setConfig(MasterConfig config) {
		this.config = config;
	}
	
	public MasterMonitor getMonitor() {
		return monitor;
	}

	public void setMonitor(MasterMonitor monitor) {
		this.monitor = monitor;
	}
	
	/**
	 * 
	 * @author sihai
	 *
	 */
	private class HttpFileHandler implements HttpAsyncRequestHandler<HttpRequest> {

        private final File docRoot;

        public HttpFileHandler(final File docRoot) {
            super();
            this.docRoot = docRoot;
        }

        public HttpAsyncRequestConsumer<HttpRequest> processRequest(
                final HttpRequest request,
                final HttpContext context) {
            // Buffer request content in memory for simplicity
            return new BasicAsyncRequestConsumer();
        }

        public void handle(
                final HttpRequest request,
                final HttpAsyncExchange httpexchange,
                final HttpContext context) throws HttpException, IOException {
            HttpResponse response = httpexchange.getResponse();
            handleInternal(request, response, context);
            httpexchange.submitResponse(new BasicAsyncResponseProducer(response));
        }

        private void handleInternal(
                final HttpRequest request,
                final HttpResponse response,
                final HttpContext context) throws HttpException, IOException {

            String method = request.getRequestLine().getMethod().toUpperCase(Locale.ENGLISH);
            if (!method.equals("GET") && !method.equals("HEAD") && !method.equals("POST")) {
                throw new MethodNotSupportedException(method + " method not supported");
            }

            String target = URLDecoder.decode(request.getRequestLine().getUri(), "UTF-8");
            if(target.endsWith(".jhtml")) {
            	if(velocityOK) {
	            	Template template = templateCache.get(target);
	            	if(template == null) {
		            	try {
		            		String vmFileName = target.replace(".jhtml", ".vm").replace("/", "\\").substring(1);
		            		final File file = new File(this.docRoot, target.replace(".jhtml", ".vm"));
		            		if(!file.exists()) {
		            			response.setStatusCode(HttpStatus.SC_NOT_FOUND);
		    	                NStringEntity entity = new NStringEntity(
		    	                        "<html><body><h1>File" + file.getPath() +
		    	                        " not found</h1></body></html>",
		    	                        ContentType.create("text/html", "UTF-8"));
		    	                response.setEntity(entity);
		    	                System.out.println("File " + file.getPath() + " not found");
		            		} else {
			            		template = Velocity.getTemplate(vmFileName, templateEncode);
			            		// 
			            		templateCache.putIfAbsent(target, template);
		            		}
		            	} catch (Exception e) {
		            		logger.error("Exception: get template", e);
		            	}
	            	} 
	            	
	            	// 渲染VM
            		VelocityContext cxt = new VelocityContext();
            		monitor.getData(cxt);
            		Writer writer = new StringWriter();
            		template.merge(cxt, writer);
            		response.setStatusCode(HttpStatus.SC_OK);
            		NStringEntity entity = new NStringEntity(writer.toString(), ContentType.create("text/html", "UTF-8"));
            		response.setEntity(entity);
            	} else {
            		response.setStatusCode(HttpStatus.SC_NOT_FOUND);
	                NStringEntity entity = new NStringEntity("Velocity not init succeed",
	                        ContentType.create("text/html", "UTF-8"));
	                response.setEntity(entity);
	                System.out.println("Velocity not init succeed");
            	}
            } else {
            	final File file = new File(this.docRoot, target);
	            if (!file.exists()) {
	                response.setStatusCode(HttpStatus.SC_NOT_FOUND);
	                NStringEntity entity = new NStringEntity(
	                        "<html><body><h1>File" + file.getPath() +
	                        " not found</h1></body></html>",
	                        ContentType.create("text/html", "UTF-8"));
	                response.setEntity(entity);
	                System.out.println("File " + file.getPath() + " not found");
	
	            } else if (!file.canRead() || file.isDirectory()) {
	
	                response.setStatusCode(HttpStatus.SC_FORBIDDEN);
	                NStringEntity entity = new NStringEntity(
	                        "<html><body><h1>Access denied</h1></body></html>",
	                        ContentType.create("text/html", "UTF-8"));
	                response.setEntity(entity);
	                System.out.println("Cannot read file " + file.getPath());
	
	            } else {
	            	NHttpConnection conn = (NHttpConnection) context.getAttribute(
	                        ExecutionContext.HTTP_CONNECTION);
	                response.setStatusCode(HttpStatus.SC_OK);
	                NFileEntity body = new NFileEntity(file, ContentType.create("text/html"));
	                response.setEntity(body);
	                System.out.println(conn + ": serving file " + file.getPath());
	            }
            }
        }
    }
}
