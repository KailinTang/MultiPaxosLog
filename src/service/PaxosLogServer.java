package service;

import message.ClientToServerMsg;
import message.HeartBeatMsg;
import message.Message;
import message.ServerToClientMsg;
import thread.HeartBeatTracker;
import thread.ThreadHandler;
import util.AddressPortPair;
import util.LogWriter;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class PaxosLogServer {

    private final static int HEART_BEAT_PERIOD_MILLS = 5000;

    private final int serverId;
    private final String serverAddr;
    private final int serverPort;

    private volatile boolean isLeader;
    private int viewNumber;

    private final int totalNumOfReplicas;
    private final List<AddressPortPair> allReplicasInfo;

    private final int skipSlotSeqNum;
    private final double messageLossRate;

    private final List<Socket> allReceiveSockets;

    // for allReplicaSendSockets, the key is the replica ID and value is the socket used to send message to other replicas
    private final Map<Integer, Socket> allReplicaSendSockets;

    // for allClientSendSockets, the key is the client ID and value is the socket used to send message to client
    private final Map<Long, Socket> allClientSendSockets;


    private final Queue<String> messageQueue;
    private final Queue<String> clientChatMessageQueue;

    private final HeartBeatTracker tracker;
    private final LogWriter logWriter;

    public PaxosLogServer(
            final int serverId,
            final String serverAddr,
            final int serverPort,
            boolean isLeader,
            int viewNumber,
            final int numOfToleratedFailures,
            final List<AddressPortPair> allReplicasInfo,
            final int skipSlotSeqNum,
            final double messageLossRate) {
        this.serverId = serverId;
        this.serverAddr = serverAddr;
        this.serverPort = serverPort;
        this.isLeader = isLeader;
        this.viewNumber = viewNumber;
        this.totalNumOfReplicas = numOfToleratedFailures * 2 + 1;
        this.allReplicasInfo = allReplicasInfo;
        this.skipSlotSeqNum = skipSlotSeqNum;
        this.messageLossRate = messageLossRate;
        this.allReceiveSockets = new Vector<>();
        this.allReplicaSendSockets = new ConcurrentHashMap<>();
        this.allClientSendSockets = new ConcurrentHashMap<>();
        this.messageQueue = new ConcurrentLinkedQueue<>();
        this.clientChatMessageQueue = new ConcurrentLinkedQueue<>();
        this.tracker = new HeartBeatTracker(
                this::increaseViewNumber,
                this::tryToBecomeLeader,
                System.currentTimeMillis(),
                HEART_BEAT_PERIOD_MILLS);
        this.logWriter = new LogWriter(serverId);
        System.out.println("Server with ID: " + serverId + " initialize at address: " + serverAddr + ':' + serverPort);
    }

    /**
     * Entrance of the server
     */
    public void start() {
        final Thread inComingSocketHandler = new Thread(new IncomingSocketHandler(serverPort));
        final Thread heartBeatLogger = new Thread(new HeartBeatLogger());
        inComingSocketHandler.start();  // start listing to its port for incoming sockets
        createSendSocketsForReplicasIfNecessary();  // try to connect all other replicas at beginning
        heartBeatLogger.start();    // start heartbeat logger
        tracker.start();    // start heartbeat tracker

    }

    /**
     * Create replica sending sockets if we don't have 2f such sockets or some such sockets are died
     */
    private void createSendSocketsForReplicasIfNecessary() {
        // If all replicas sending sockets are alive and # of those equal to 2f, we no longer
        if (areAllSendSocketsAlive() && allReplicaSendSockets.size() == totalNumOfReplicas) {
            return;
        } else {
            createSendSocketsForReplicas();
        }
    }

    /**
     * Create replica sending sockets if we don't have that connection and save it to allReplicaSendSockets
     */
    private void createSendSocketsForReplicas() {
        for (int i = 0; i < allReplicasInfo.size(); i++) {
            try {
                if (!allReplicaSendSockets.containsKey(i)) {
                    final Socket socket = new Socket(allReplicasInfo.get(i).getIp(), allReplicasInfo.get(i).getPort());
                    if (socket != null && socket.isConnected()) {
                        allReplicaSendSockets.put(i, socket);
                    }
                }
            } catch (Exception e) {
                System.out.println("Replica whose address is " + allReplicasInfo.get(i).getIp()
                        + ':' + allReplicasInfo.get(i).getPort() + " is not accessible now");
            }
        }
    }

    /**
     * @return Whether all sockets in allReplicaSendSockets are alive
     */
    private boolean areAllSendSocketsAlive() {
        if (allReplicaSendSockets.size() == 0) {
            return false;
        }
        for (final Integer replicaID : this.allReplicaSendSockets.keySet()) {
            if (!allReplicaSendSockets.get(replicaID).isConnected()) {
                allReplicaSendSockets.remove(replicaID);   // remove the dead sockets if necessary
                return false;
            }
        }
        return true;
    }

    /**
     * A worker for listening the port of server and create receiving sockets for any incoming sockets (client & other replicas)
     * Note that the sockets created in this worker is only responsible for receiving messages
     */
    public class IncomingSocketHandler implements Runnable {

        private ServerSocket serverSocket;

        public IncomingSocketHandler(final int port) {
            try {
                serverSocket = new ServerSocket(port);
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("Server with ID: " + serverId + "fail to listen to port:" + serverPort + ". Terminating...");
                System.exit(1);
            }
        }

        @Override
        public void run() {
            while (true) {
                try {
                    Socket acceptedSocket = serverSocket.accept();
                    allReceiveSockets.add(acceptedSocket);
                    new Thread(new ReceiveMessageHandler(acceptedSocket)).start();
                } catch (IOException e) {
                    e.printStackTrace();
                    System.out.println("Server with ID: " + serverId + "fail to accept connection");
                }
            }
        }
    }

    /**
     * A worker for receive messages from each socket which is responsible for receiving messages
     * According to the different message received (client, heartbeat or replica message), perform different operations
     */
    public class ReceiveMessageHandler extends ThreadHandler {

        public ReceiveMessageHandler(Socket socket) {
            super(socket);
        }

        @Override
        public void run() {
            try {
                String line;
                while ((line = super.bufferedReader.readLine()) != null) {
                    System.out.println(line);
                    switch (Message.getMessageType(line)) {
                        case CLIENT_TO_SERVER:
                            switch (ClientToServerMsg.getClientToServerType(line)) {
                                case HELLO:
                                    handleClientHello(ClientToServerMsg.HelloMsg.fromString(line));
                                    break;
                                case CHAT:
                                    clientChatMessageQueue.offer(line);
                                    break;
                                default:
                                    throw new IllegalStateException("Unresolvable client to server message!");
                            }
                            break;
                        case SERVER_TO_CLIENT:
                            throw new IllegalStateException("Server should never receive the message that supposed to be sent to client!");
                        case HEART_BEAT:
                            handleClientHeartBeat(HeartBeatMsg.fromString(line));
                            break;
                        case PREPARE:
                        case PREPARE_RESPONSE:
                        case ACCEPT:
                        case ACCEPT_RESPONSE:
                        case SUCCESS:
                        case SUCCESS_RESPONSE:
                            messageQueue.offer(line);
                        default:
                            throw new IllegalStateException("Unresolvable message received!");
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void handleClientHeartBeat(final HeartBeatMsg heartBeatMsg) {
        updateViewNumber(heartBeatMsg.getViewNumber());
        tracker.setLatestReceivedTimeStamp(heartBeatMsg.getTimeStamp());
    }

    private void handleClientHello(final ClientToServerMsg.HelloMsg helloMsg) {
        if (!allClientSendSockets.containsKey(helloMsg.getClientID())) {
            try {
                final Socket clientSocket = new Socket(helloMsg.getListeningIPAddr(), helloMsg.getListeningPort());
                if (clientSocket != null && clientSocket.isConnected()) {
                    allClientSendSockets.put(helloMsg.getClientID(), clientSocket);
                }
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("Failed to connect to client with " + helloMsg.getListeningIPAddr() + ":" + helloMsg.getListeningPort());
            }
        }
        final Socket clientSocket = allClientSendSockets.get(helloMsg.getClientID());
        try {
            final PrintWriter writer = new PrintWriter(clientSocket.getOutputStream(), true);
            if (isLeader) {
                writer.println(new ServerToClientMsg.ServerAckMsg().toString());
            } else {
                writer.println(new ServerToClientMsg.ServerNackMsg(getCurrentLeader()).toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Fail to send message to client");
        }
    }

    private void increaseViewNumber() {
        this.viewNumber += 1;
    }

    private void updateViewNumber(final int newViewNumber) {
        this.viewNumber = newViewNumber > this.viewNumber ? newViewNumber : this.viewNumber;
    }

    private void tryToBecomeLeader() {
        if (getCurrentLeader() == this.serverId) {
            System.out.println("Proposer " + serverId + " is trying to become leader");
            this.isLeader = true;
        }
    }

    private int getCurrentLeader() {
        return this.viewNumber % this.totalNumOfReplicas;
    }

    /**
     * Broadcast a message to all replicas through send replica socket.
     *
     * @param message
     * @throws IOException
     */
    private void broadcastToAllReplicas(final String message) throws IOException {
        createSendSocketsForReplicasIfNecessary();
        for (final Socket replicaSendSocket : allReplicaSendSockets.values()) {
            final PrintWriter writer = new PrintWriter(replicaSendSocket.getOutputStream(), true);
            writer.println(message);
        }
    }

    /**
     * If the current process is leader, it will send heartbeat messages to all other replicas periodically.
     */
    public class HeartBeatLogger implements Runnable {

        @Override
        public void run() {
            while (true) {
                if (getCurrentLeader() != serverId) {
                    isLeader = false;
                }
                if (isLeader) {
                    try {
                        final long currentTimeStamp = System.currentTimeMillis();
                        broadcastToAllReplicas("HEART_BEAT:" + viewNumber + ":" + currentTimeStamp);
                        Thread.sleep(HEART_BEAT_PERIOD_MILLS);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
