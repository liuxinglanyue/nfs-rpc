package code.google.nfs.rpc.netty4.server;

/**
 * nfs-rpc
 *   Apache License
 *   
 *   http://code.google.com/p/nfs-rpc (c) 2011
 */
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import code.google.nfs.rpc.ProtocolFactory;
import code.google.nfs.rpc.netty4.serialize.Netty4ProtocolDecoder;
import code.google.nfs.rpc.netty4.serialize.Netty4ProtocolEncoder;
import code.google.nfs.rpc.server.Server;

/**
 * Netty4 Server
 * 
 * @author <a href="mailto:coderplay@gmail.com">Min Zhou</a>
 */
public class Netty4Server implements Server {

	private static final Log LOGGER = LogFactory.getLog(Netty4Server.class);
	private AtomicBoolean startFlag = new AtomicBoolean(false);

	private NioEventLoopGroup bossGroup;
	private NioEventLoopGroup ioGroup;
	private EventExecutorGroup businessGroup;
	private final int businessThreads;

	public Netty4Server(int businessThreads) {
		this.businessThreads = businessThreads;
	}

	public void start(int listenPort, final ExecutorService ignore)
	        throws Exception {
		if (!startFlag.compareAndSet(false, true)) {
			return;
		}
		bossGroup = new NioEventLoopGroup();
		ioGroup = new NioEventLoopGroup();
		businessGroup = new DefaultEventExecutorGroup(businessThreads);

		ServerBootstrap b = new ServerBootstrap();
		b.group(bossGroup, ioGroup).channel(NioServerSocketChannel.class)
		        .childOption(ChannelOption.TCP_NODELAY, 
		                     Boolean.parseBoolean(System.getProperty(
		                             "nfs.rpc.tcp.nodelay", "true")))
		        .childOption(ChannelOption.SO_REUSEADDR, 
		                     Boolean.parseBoolean(System.getProperty(
		                             "nfs.rpc.tcp.reuseaddress", "true")))
		        .childHandler(new ChannelInitializer<SocketChannel>() {
			        @Override
			        public void initChannel(SocketChannel ch)
			                throws Exception {
						ch.pipeline().addLast("decoder",
								new Netty4ProtocolDecoder());
						ch.pipeline().addLast("encoder",
								new Netty4ProtocolEncoder());
						ch.pipeline().addLast(businessGroup, "handler",
								new Netty4ServerHandler());
			        }
		        });

		b.bind(listenPort).sync();
		LOGGER.warn("Server started,listen at: " + listenPort + ", businessThreads is " + businessThreads);
	}

	public void registerProcessor(int protocolType, String serviceName,
	        Object serviceInstance) {
		ProtocolFactory.getServerHandler(protocolType).registerProcessor(
		        serviceName, serviceInstance);
	}

	public void stop() throws Exception {
		LOGGER.warn("Server stop!");
		bossGroup.shutdownGracefully();
		ioGroup.shutdownGracefully();
		businessGroup.shutdownGracefully();
		startFlag.set(false);
	}

}
