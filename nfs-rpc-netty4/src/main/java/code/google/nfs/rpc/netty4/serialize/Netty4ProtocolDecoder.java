package code.google.nfs.rpc.netty4.serialize;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import code.google.nfs.rpc.protocol.ProtocolUtils;
/**
 * nfs-rpc
 *   Apache License
 *   
 *   http://code.google.com/p/nfs-rpc (c) 2011
 */
/**
 * decode byte[]
 * 	change to pipeline receive requests or responses,let's IO thread do less thing
 * 
 * @author <a href="mailto:coderplay@gmail.com">Min Zhou</a>
 */
public class Netty4ProtocolDecoder extends ByteToMessageDecoder {
	private static final Log LOGGER = LogFactory.getLog(Netty4ProtocolDecoder.class);

	@Override
	protected void decode(ChannelHandlerContext ctx, ByteBuf in,
			List<Object> out) throws Exception {
		Netty4ByteBufferWrapper wrapper = new Netty4ByteBufferWrapper(in);
		Object msg = ProtocolUtils.decode(wrapper, null);
		if (msg != null)
			out.add(msg);
	}

}
