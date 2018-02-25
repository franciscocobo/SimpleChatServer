package es.uc3m.it.aptel;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import es.uc3m.it.aptel.connection.State;
import es.uc3m.it.aptel.messages.Message;

public class Server {

	// CONSTANTS
	
	// COMMANDS
	private static final short SEND = 0x02;
	private static final short ACK = 0x03;
	private static final short NACK = 0x04;
	private static final short HELLO = 0x05;
	private static final int SERV_ID = 0x00;
	
	// LENGTH
	private static final int NACK_LEN = 0;
	private static final int ACK_LEN = 0;

	// GLOBAL VARIABLES
	List<State> clients = new ArrayList<State>();

	Selector selector = null;
	ServerSocketChannel ss;
	Logger logger = Logger.getLogger(Server.class.getCanonicalName());

	public static void main(String args[]) {
		try {
			Server srv = new Server(8888);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public Server(int port) throws IOException {

		ss = ServerSocketChannel.open();
		SocketAddress sa = new InetSocketAddress(port);
		ss.bind(sa, 10);

		selector = Selector.open();

		// set passive socket non blocking
		ss.configureBlocking(false);

		ss.register(selector, SelectionKey.OP_ACCEPT);

		logger.log(Level.INFO, "Listening at " + port);
		// move to server blk
		ServerSelector();
	}

	/**
	 * @throws IOException
	 */
	public void ServerSelector() throws IOException {

		ss.configureBlocking(false);

		while (true) {
			SocketChannel activeSocket = null;
			State state = null;
			try {
				int readyChannels;

				readyChannels = selector.select();

				if (readyChannels == 0) {
					logger.info("No channels (sockets) selected");
					// continue;
				}

				Set<SelectionKey> selectedKeys = selector.selectedKeys();

				Iterator<SelectionKey> keyIterator = selectedKeys.iterator();

				while (keyIterator.hasNext()) {

					SelectionKey currentKey = keyIterator.next();
					// we already get the key in currentKey, remove it from iterator
					keyIterator.remove();

					if (currentKey.isAcceptable()) {
						logger.info("A new connection arrives");
						// get the socket that has an incoming connection
						// (passive)
						ServerSocketChannel ssChannel = (ServerSocketChannel) currentKey.channel();
						// accept the connection and log it
						activeSocket = ssChannel.accept();
						InetSocketAddress saddrs = (InetSocketAddress) activeSocket.getRemoteAddress();
						logger.info("Client from " + saddrs.getAddress().getHostAddress() + " : " + saddrs.getPort()
								+ " - is now connected");
						// create a new Connection class to store the connection
						// state
						// State state = new State(activeSocket);
						state = new State(activeSocket);
						// the active socket should be controlled with the
						// selector
						// an active socket can read and write
						// we will register the active socket with the selector
						// and use state as attachment
						activeSocket.configureBlocking(false);
						activeSocket.register(selector, SelectionKey.OP_READ, state);

					} else if (currentKey.isConnectable()) {
						logger.info("connectable...");

					} else if (currentKey.isReadable()) {
						logger.info("A new I/O operation on an active socket");
						// get the socket that has an incoming connection
						// (active)
						activeSocket = (SocketChannel) currentKey.channel();
						// check if the socket is closed
						if (!currentKey.isValid()) {
							if (!activeSocket.isConnected()) {
								// close the socket
								currentKey.cancel();
								activeSocket.close();
								break;
							}
						}

						state = (State) currentKey.attachment();
						// read data
						synchronized (state.getIn()) {
							int numBytesRead;

							while ((numBytesRead = activeSocket.read(state.getIn())) != 0) {
								if (numBytesRead == -1) {
									logger.info("A client closed");
									throw new ClosedChannelException();
								}

								logger.info("loop reading " + numBytesRead + " from socket");
								logger.info("Total bytes read " + state.getIn().position());

								//In order to add more robustness to the server:
								while (state.getIn().hasRemaining()) {
									processMessage(state);
									// prepare buffer for next message
								}
								state.getIn().clear();
							}
						}

					} else {
						logger.severe("Unexpected key");
					}
				}
			} catch (Exception e) { //We catch a generic exception for both IO and ClosedChannel
				// TODO Auto-generated catch block
				// e.printStackTrace();
				// clean list
				if (activeSocket != null) {
					activeSocket.close();
					clients.remove(state);
				}
			}
		}
	}

	public int writeMessage(State state, Message outgoing) {
		int count = 0;
		synchronized (state.getOut()) {

			try {
				// clean the outgoing buffer
				state.getOut().clear();
				// Fill the message
				outgoing.serialize(state.getOut());
				// prepare buffer for reading to copu to socket
				state.getOut().flip();

				while (state.getOut().hasRemaining()) {

					count += state.getSocket().write(state.getOut());

				}
				logger.info("sending " + count + " bytes through the socket");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		return count;

	}

	private void processMessage(State sender) {
		// process message
		// flip the incoming buffer
		ByteBuffer in = sender.getIn();
		in.flip();
		// get a message from the buffer
		Message incoming = new Message(in);

		// Here starts the magic:
		short cmd = incoming.getCode();

		switch (cmd) {
		case HELLO:
			// Check if the sender is online.
			if (isOnline(sender.getID())) {

				// NACK CASE
				Message nack = new Message(SERV_ID, incoming.getSource(), incoming.getID(), NACK, NACK_LEN, null);
				this.writeMessage(sender, nack);
			} else {
				// ACK CASE
				// We pass the ack message to the client.
				Message ack = new Message(SERV_ID, incoming.getSource(), incoming.getID(), ACK, ACK_LEN, null);
				this.writeMessage(sender, ack);
				// We store the ID of the client in the id field.
				sender.setID(incoming.getSource());
				// We add the client to the list of clients.
				clients.add(sender);
			}

			break;
		case SEND:
			// Check if the destination is online.
			int destId = incoming.getDest();
			Optional<State> destState = clients.stream().filter(p -> p.getID() == destId).findFirst();
			if (destState.isPresent()) {
				// SEND MESSAGE TO DESTINATION
				logger.info("A send message arrived ");
				Message mssg = new Message(sender.getID(), destId, incoming.getID(), SEND, incoming.getpLen(),
						incoming.getPayload());
				
				// SEND MESSAGE TO THE DESTINATION: response -> destination
				this.writeMessage(destState.get(), mssg);
				// WE CHECK IF THERE WAS NO ERROR
				Message ack = new Message(SERV_ID, incoming.getSource(), incoming.getID(), ACK, ACK_LEN, null);
				// SEND ACK TO THE SOURCE: state -> source
				this.writeMessage(sender, ack);
			} else {
				// NACK CASE
				Message nack = new Message(SERV_ID, incoming.getSource(), incoming.getID(), NACK, NACK_LEN, null);
				this.writeMessage(sender, nack);
			}
		default:
			break;

		}
	}

	public boolean isOnline(int clientID) {
		return clients.stream().filter(p -> p.getID() == clientID).findFirst().isPresent();
	}

	public State isInList(int clientID) {
		State existe = null;
		for (State st : clients) {
			if (st.getID() == clientID)
				existe = st;
		}
		return existe;
	}

}
