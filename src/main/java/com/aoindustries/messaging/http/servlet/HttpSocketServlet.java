/*
 * ao-messaging - Asynchronous bidirectional messaging over various protocols.
 * Copyright (C) 2014, 2015, 2016  AO Industries, Inc.
 *     support@aoindustries.com
 *     7262 Bull Pen Cir
 *     Mobile, AL 36695
 *
 * This file is part of ao-messaging.
 *
 * ao-messaging is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ao-messaging is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with ao-messaging.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.aoindustries.messaging.http.servlet;

import com.aoindustries.io.AoByteArrayOutputStream;
import com.aoindustries.io.Encoder;
import com.aoindustries.messaging.Message;
import com.aoindustries.messaging.MessageType;
import com.aoindustries.messaging.Socket;
import com.aoindustries.messaging.base.AbstractSocket;
import com.aoindustries.messaging.base.AbstractSocketContext;
import com.aoindustries.messaging.http.HttpSocket;
import com.aoindustries.nio.charset.Charsets;
import com.aoindustries.security.Identifier;
import com.aoindustries.util.concurrent.Callback;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Server component for bi-directional messaging over HTTP.
 * This is a synchronous implementation compatible with older environments.
 */
abstract public class HttpSocketServlet extends HttpServlet {

	private static final Logger logger = Logger.getLogger(HttpSocketServlet.class.getName());

	private static final long serialVersionUID = 1L;

	private static final boolean DEBUG = false;

	/** Server should normally respond within 60 seconds even if no data coming back. */
	private static final int LONG_POLL_TIMEOUT = HttpSocket.READ_TIMEOUT / 2;

	public static class ServletSocket extends AbstractSocket {

		private final String serverName;

		final Map<Long,Message> inQueue = new HashMap<Long,Message>();
		long inSeq = 1; // Synchronized on inQueue

		private final Queue<Message> outQueue = new LinkedList<Message>();
		private Thread outQueueCurrentThread; // Synchronized on outQueue
		private long outSeq = 1; // Synchronized on outQueue

		ServletSocket(
			ServletSocketContext socketContext,
			Identifier id,
			long connectTime,
			SocketAddress remoteSocketAddress,
			String serverName
		) {
			super(
				socketContext,
				id,
				connectTime,
				remoteSocketAddress
			);
			this.serverName = serverName;
		}

		/**
		 * All requests for this socket must use the same server name.
		 */
		public String getServerName() {
			return serverName;
		}

		@Override
		public void close() throws IOException {
			try {
				super.close();
			} finally {
				synchronized(outQueue) {
					outQueue.notifyAll();
				}
			}
		}

		@Override
		public String getProtocol() {
			return HttpSocket.PROTOCOL;
		}

		// Expose to this package
		@Override
		protected Future<?> callOnMessages(List<? extends Message> messages) throws IllegalStateException {
			return super.callOnMessages(messages);
		}

		// Expose to this package
		@Override
		protected Future<?> callOnError(Exception exc) throws IllegalStateException {
			return super.callOnError(exc);
		}

		@Override
		protected void startImpl(Callback<? super Socket> onStart, Callback<? super Exception> onError) {
			// Nothing to do
			if(onStart!=null) onStart.call(this);
		}

		@Override
		protected void sendMessagesImpl(Collection<? extends Message> messages) {
			synchronized(outQueue) {
				outQueue.addAll(messages);
				outQueue.notifyAll();
			}
		}

		/**
		 * Gets the messages to be sent back, blocks for a maximum of
		 * LONG_POLL_TIMEOUT milliseconds.
		 * If a new thread comes-in, the first thread will be notified to return immediately.
		 */
		Map<Long,? extends Message> getOutMessages() {
			long endMillis = (System.nanoTime() / 1000000) + LONG_POLL_TIMEOUT;
			final Thread currentThread = Thread.currentThread();
			synchronized(outQueue) {
				// If more than one out thread, notify previous
				if(outQueueCurrentThread != null) outQueue.notifyAll();
				// Replace any previous with new current
				outQueueCurrentThread = currentThread;
				try {
					while(true) {
						if(!outQueue.isEmpty()) {
							Map<Long,Message> messages = new LinkedHashMap<Long,Message>(outQueue.size()*4/3+1);
							while(!outQueue.isEmpty()) {
								messages.put(outSeq++, outQueue.remove());
							}
							outQueue.notifyAll();
							return Collections.unmodifiableMap(messages);
						}
						// Check if closed
						if(isClosed()) return Collections.emptyMap();
						// Check if replaced by a newer thread
						if(outQueueCurrentThread != currentThread) return Collections.emptyMap();
						// Check if time expired
						long timeRemaining = endMillis - (System.nanoTime() / 1000000);
						if(timeRemaining <= 0) return Collections.emptyMap();
						try {
							outQueue.wait(timeRemaining);
						} catch(InterruptedException e) {
							logger.log(Level.WARNING, null, e);
							// Restore the interrupted status
							Thread.currentThread().interrupt();
						}
					}
				} finally {
					if(outQueueCurrentThread == currentThread) outQueueCurrentThread = null;
				}
			}
		}
	}

	public static class ServletSocketContext extends AbstractSocketContext<ServletSocket> {
		// Expose to this package
		@Override
		protected Identifier newIdentifier() {
			return super.newIdentifier(); //To change body of generated methods, choose Tools | Templates.
		}

		// Expose to this package
		@Override
		protected void addSocket(ServletSocket newSocket) {
			super.addSocket(newSocket);
		}
	}

	protected final ServletSocketContext socketContext = new ServletSocketContext();

	private final Encoder textInXhtmlEncoder;

	protected HttpSocketServlet(Encoder textInXhtmlEncoder) {
		this.textInXhtmlEncoder = textInXhtmlEncoder;
	}

	/**
	 * Last modified times must never be used.
	 */
	@Override
	final protected long getLastModified(HttpServletRequest request) {
		return -1;
	}

	/**
	 * Get requests must never be used.
	 */
	@Override
	final protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		response.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
	}

	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String action = request.getParameter("action");
		if("connect".equals(action)) {
			long connectTime = System.currentTimeMillis();
			Identifier id = socketContext.newIdentifier();
			// Build the response
			AoByteArrayOutputStream bout = new AoByteArrayOutputStream();
			try {
				Writer out = new OutputStreamWriter(bout, Charsets.UTF_8);
				try {
					out.write("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n"
						+ "<connection id=\"");
					out.write(id.toString());
					out.write("\"/>");
				} finally {
					out.close();
				}
			} finally {
				bout.close();
			}
			response.setContentType("application/xml");
			response.setCharacterEncoding("UTF-8");
			response.setContentLength(bout.size());
			OutputStream out = response.getOutputStream();
			try {
				out.write(bout.getInternalByteArray(), 0, bout.size());
			} finally {
				out.close();
			}
			// Determine the port
			int remotePort = request.getRemotePort();
			if(remotePort < 0) remotePort = 0; // < 0 when unknown such as old AJP13 protocol
			if(DEBUG) System.err.println("DEBUG: HttpSocketServlet: doPost: remotePort="+remotePort);
			ServletSocket servletSocket = new ServletSocket(
				socketContext,
				id,
				connectTime,
				new InetSocketAddress(
					request.getRemoteAddr(),
					remotePort
				),
				request.getServerName()
			);
			socketContext.addSocket(servletSocket);
		} else if("messages".equals(action)) {
			Identifier id = Identifier.valueOf(request.getParameter("id"));
			if(DEBUG) System.err.println("DEBUG: HttpSocketServlet: doPost: id="+id);
			ServletSocket socket = socketContext.getSocket(id);
			if(socket==null) {
				if(DEBUG) System.err.println("DEBUG: HttpSocketServlet: doPost: socket not found");
				response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Socket id not found");
			} else {
				try {
					// Handle incoming messages
					{
						int size = Integer.parseInt(request.getParameter("l"));
						if(DEBUG) System.err.println("DEBUG: HttpSocketServlet: doPost: size="+size);
						// Add all messages to the inQueue by sequence to handle out-of-order messages
						List<Message> messages;
						synchronized(socket.inQueue) {
							for(int i=0; i<size; i++) {
								// Get the sequence
								Long seq = Long.parseLong(request.getParameter("s"+i));
								// Get the type
								MessageType type = MessageType.getFromTypeChar(request.getParameter("t"+i).charAt(0));
								// Get the message string
								String encodedMessage = request.getParameter("m"+i);
								// Decode and add
								if(socket.inQueue.put(seq, type.decode(encodedMessage)) != null) {
									throw new IOException("Duplicate incoming sequence: " + seq);
								}
							}
							// Gather as many messages that have been delivered in-order
							messages = new ArrayList<Message>(socket.inQueue.size());
							while(true) {
								Message message = socket.inQueue.remove(socket.inSeq);
								if(message != null) {
									messages.add(message);
									socket.inSeq++;
								} else {
									// Break in the sequence
									break;
								}
							}
						}
						if(!messages.isEmpty()) socket.callOnMessages(Collections.unmodifiableList(messages));
					}
					// Handle outgoing messages
					{
						Map<Long,? extends Message> outMessages = socket.getOutMessages();
						// Build the response
						AoByteArrayOutputStream bout = new AoByteArrayOutputStream();
						try {
							Writer out = new OutputStreamWriter(bout, Charsets.UTF_8);
							try {
								out.write("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n"
									+ "<messages>\n");
								for(Map.Entry<Long,? extends Message> entry : outMessages.entrySet()) {
									Long seq = entry.getKey();
									Message message = entry.getValue();
									out.write("  <message seq=\"");
									out.write(seq.toString());
									out.write("\" type=\"");
									out.write(message.getMessageType().getTypeChar());
									out.write("\">");
									textInXhtmlEncoder.write(message.encodeAsString(), out);
									out.write("</message>\n");
								}
								out.write("</messages>");
							} finally {
								out.close();
							}
						} finally {
							bout.close();
						}
						response.setContentType("application/xml");
						response.setCharacterEncoding("UTF-8");
						response.setContentLength(bout.size());
						OutputStream out = response.getOutputStream();
						try {
							out.write(bout.getInternalByteArray(), 0, bout.size());
						} finally {
							out.close();
						}
					}
				} catch(Exception e) {
					socket.callOnError(e);
					if(e instanceof ServletException) throw (ServletException)e;
					if(e instanceof IOException     ) throw (IOException)e;
					if(e instanceof RuntimeException) throw (RuntimeException)e;
					throw new ServletException(e);
				}
			}
		} else {
			response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Unexpected action: " + action);
		}
	}

	/**
	 * When the servlet is destroyed, all active sockets are also closed.
	 */
	@Override
	public void destroy() {
		socketContext.close();
	}
}
