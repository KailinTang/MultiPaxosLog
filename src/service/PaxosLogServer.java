package service;

import message.Message;
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
    private final List<Socket> allReplicaSendSockets;
    private final Map<AddressPortPair, Socket> allClientSendSockets;
    private final Queue<Message> messageQueue;

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
        this.allReplicaSendSockets = new Vector<>();
        this.allClientSendSockets = new ConcurrentHashMap<>();
        this.messageQueue = new ConcurrentLinkedQueue<>();
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
                if (!containSendSockets(allReplicasInfo.get(i))) {
                    final Socket socket = new Socket(allReplicasInfo.get(i).getIp(), allReplicasInfo.get(i).getPort());
                    if (socket != null && socket.isConnected()) {
                        allReplicaSendSockets.add(socket);
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
        for (final Socket socket : this.allReplicaSendSockets) {
            if (!socket.isConnected()) {
                allReplicaSendSockets.remove(socket);   // remove the dead sockets if necessary
                return false;
            }
        }
        return true;
    }

    /**
     * @param sendAddressPortPair Input address port pair
     * @return Whether we have already create a socket for the given address port pair
     */
    private boolean containSendSockets(final AddressPortPair sendAddressPortPair) {
        for (final Socket socket : this.allReplicaSendSockets) {
            if (socket.getInetAddress().getHostAddress().equals(sendAddressPortPair.getIp()) && socket.getPort() == sendAddressPortPair.getPort()) {
                return true;
            }
        }
        return false;
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
                    if (Message.getMessageType(line).equals(Message.MESSAGE_TYPE.HEART_BEAT)) {
                        updateViewNumber(Integer.parseInt(line.split(":")[1]));
                        tracker.setLatestReceivedTimeStamp(Long.parseLong(line.split(":")[2]));
                    }
                    messageQueue.offer(new Message(line));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
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
        for (final Socket replicaSendSocket : allReplicaSendSockets) {
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
            while(true) {
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
