/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.utility;

import io.grpc.netty.shaded.io.netty.bootstrap.ServerBootstrap;
import io.grpc.netty.shaded.io.netty.buffer.Unpooled;
import io.grpc.netty.shaded.io.netty.channel.ChannelFuture;
import io.grpc.netty.shaded.io.netty.channel.ChannelFutureListener;
import io.grpc.netty.shaded.io.netty.channel.ChannelHandlerContext;
import io.grpc.netty.shaded.io.netty.channel.ChannelInitializer;
import io.grpc.netty.shaded.io.netty.channel.ChannelPipeline;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.SimpleChannelInboundHandler;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.SocketChannel;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioServerSocketChannel;
import io.grpc.netty.shaded.io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.grpc.netty.shaded.io.netty.handler.codec.http.FullHttpRequest;
import io.grpc.netty.shaded.io.netty.handler.codec.http.FullHttpResponse;
import io.grpc.netty.shaded.io.netty.handler.codec.http.HttpHeaderNames;
import io.grpc.netty.shaded.io.netty.handler.codec.http.HttpObjectAggregator;
import io.grpc.netty.shaded.io.netty.handler.codec.http.HttpResponseStatus;
import io.grpc.netty.shaded.io.netty.handler.codec.http.HttpServerCodec;
import io.grpc.netty.shaded.io.netty.handler.codec.http.HttpVersion;
import io.grpc.netty.shaded.io.netty.handler.logging.LogLevel;
import io.grpc.netty.shaded.io.netty.handler.logging.LoggingHandler;
import io.grpc.netty.shaded.io.netty.util.CharsetUtil;
import java.net.InetSocketAddress;

public class HttpTinyServer {
    public static String content = "foobar";

    final int port;
    final EventLoopGroup bossGroup;
    final EventLoopGroup workerGroup;

    public HttpTinyServer(int port) {
        this.port = port;
        this.bossGroup = new NioEventLoopGroup();
        this.workerGroup = new NioEventLoopGroup();
    }

    public void start() throws Exception {
        ServerBootstrap bootstrap = new ServerBootstrap();
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        bootstrap.group(bossGroup, workerGroup)
                 .handler(new LoggingHandler(LogLevel.DEBUG))
                 .channel(NioServerSocketChannel.class)
                 .childHandler(new HttpServerInitializer());

        ChannelFuture f = bootstrap.bind(new InetSocketAddress(port)).sync();
        //        f.channel().closeFuture().sync();
    }

    public void shutdown() {
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }

    static class HttpServerInitializer extends ChannelInitializer<SocketChannel> {
        @Override
        protected void initChannel(SocketChannel channel) {
            ChannelPipeline pipeline = channel.pipeline();
            pipeline.addLast(new HttpServerCodec());
            pipeline.addLast("httpAggregator", new HttpObjectAggregator(512 * 1024));
            pipeline.addLast(new HttpRequestHandler());
        }

    }

    static class HttpRequestHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            ctx.flush();
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest req) {
            // create http response
            FullHttpResponse response = new DefaultFullHttpResponse(
                    HttpVersion.HTTP_1_1,
                    HttpResponseStatus.OK,
                    Unpooled.copiedBuffer(content, CharsetUtil.UTF_8));
            // set header
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/html; charset=UTF-8");
            ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        }
    }
}
