package client;


import java.io.IOException;

import message.ClientToServerMsg;
import message.Message;
import message.ServerToClientMsg;
import thread.ThreadHandler;
import util.AddressPortPair;

import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Queue;
import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedQueue;

import java.util.Calendar;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Scanner;


public class PaxosLogClient {

    private final long clientId;
    private final String clientAddr;
    private final int clientPort;
    private int clientSequence;

    private final int totalNumOfReplicas;
    private final List<AddressPortPair> allReplicasInfo;
    private boolean receivedLastSendMsgResponse;
    private int leaderServerID;

    private final List<Socket> allReplicaSendSockets;
    private final Queue<ClientToServerMsg.ChatMsg> sendMessageQueue;
    private final Queue<String> receiveMessageQueue;
    private ClientToServerMsg.ChatMsg nextSendMsg;
    private Message nextMsg;

    private final ClientToServerMsg.HelloMsg MsgHello;

    public PaxosLogClient(
            final String clientAddr,
            final int clientPort,
            final List<AddressPortPair> allReplicasInfo
    ) {
        this.clientId = System.currentTimeMillis();
        this.clientAddr = clientAddr;
        this.clientPort = clientPort;
        this.clientSequence = 0;
        this.allReplicasInfo = allReplicasInfo;
        this.totalNumOfReplicas = allReplicasInfo.size();
        this.allReplicaSendSockets = new Vector<>();
        this.receiveMessageQueue = new ConcurrentLinkedQueue<>();
        this.sendMessageQueue = new ConcurrentLinkedQueue<>();
        this.receivedLastSendMsgResponse = false;
        this.leaderServerID = 0;
        this.MsgHello = new ClientToServerMsg.HelloMsg(clientId, clientAddr, clientPort);

        System.out.println("Client with ID: " + clientId + " initialize at address: " + clientAddr + ':' + clientPort);
    }


    /**
     * Entrance of the client
     */
    public void start() {
        new Thread(new IncomingSocketHandler(clientPort)).start();
        new Thread(new ScannerHandler()).start();
        createSendSocketsForReplicasIfNecessary();
        queueHandler();
    }

    /**
     * Create replica sending sockets if we don't have 2f such sockets or some such sockets are died
     */
    private void createSendSocketsForReplicasIfNecessary() {
        // If all replicas sending sockets are alive and # of those equal to 2f, we no longer
        if (areAllSendSocketsAlive() && allReplicaSendSockets.size() == totalNumOfReplicas) {
            return;
        } else {
            createSendSocketsForClients();
        }
    }

    /**
     * Create replica sending sockets if we don't have that connection and save it to allReplicaSendSockets
     */
    private void createSendSocketsForClients() {
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
        for (final Socket socket : this.allReplicaSendSockets) {
            System.out.println(socket.getPort());
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
                System.out.println("Client with ID: " + clientId + "fail to listen to port:" + clientPort + ". Terminating...");
                System.exit(1);
            }
        }

        @Override
        public void run() {
            while (true) {
                try {
                    Socket acceptedSocket = serverSocket.accept();
                    // allReceiveSockets.add(acceptedSocket);
                    new Thread(new ReceiveMessageHandler(acceptedSocket)).start();
                } catch (IOException e) {
                    e.printStackTrace();
                    System.out.println("Client with ID: " + clientId + "fail to accept connection");
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
                    receiveMessageQueue.offer(line);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /* Create worker to deal with input message from scanner and send to leader*/


    public class ScannerHandler implements Runnable {

        private Scanner scanner;

        public ScannerHandler() {
            try {
                this.scanner = new Scanner(System.in);
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Fail to build scanner");

            }
        }

        @Override
        public void run() {
            while (true) {
                try {
                    String nextLine = scanner.nextLine();
                    sendMessageQueue.offer(new ClientToServerMsg.ChatMsg(clientId, clientSequence, nextLine));
                    clientSequence += 1;
                    System.out.println("client want to send message: " + nextLine);
                    System.out.println("is connected " + allReplicaSendSockets.get(0).isConnected());
                    System.out.println("is closed " + allReplicaSendSockets.get(0).isClosed());
                    if (sendMessageQueue.size() == 1) {
                        receivedLastSendMsgResponse = true;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("Fail to add msg to sendMessageQueue");
                }
            }
        }
    }


    /*create main process to handle msg in sendMessageQueue and receiveMessageQueue*/

    public void queueHandler() {

        while (true) {
            if (receivedLastSendMsgResponse == true) {
                nextSendMsg = sendMessageQueue.peek();
                if (nextSendMsg != null) {
                    receivedLastSendMsgResponse = false;
                    sendNextMsg();
                }
            }

            String nextString = receiveMessageQueue.poll();
            if (nextString != null && Message.getMessageType(nextString) == Message.MESSAGE_TYPE.SERVER_TO_CLIENT) {
                System.out.println("is connected " + allReplicaSendSockets.get(0).isConnected());
                System.out.println("is closed " + allReplicaSendSockets.get(0).isClosed());
                switch (ServerToClientMsg.getServerToClientType(nextString)) {
                    case ACK:
                        nextMsg = ServerToClientMsg.ServerAckMsg.fromString(nextString);
                        try {
                            PrintWriter leaderPrintWriter = new PrintWriter(allReplicaSendSockets.get(leaderServerID).getOutputStream(), true);
                            new WaitRepeatSend(nextSendMsg, leaderPrintWriter, 5);
                        } catch (Exception e) {
                            e.printStackTrace();
                            System.out.printf("build printWriter failed for index %s", leaderServerID);

                        }
                        break;
                    case NACK:
                        nextMsg = ServerToClientMsg.ServerNackMsg.fromString(nextString);
                        try {
                            leaderServerID = ((ServerToClientMsg.ServerNackMsg) nextMsg).getCurrentLeaderId();
                            PrintWriter printWriterRandom = new PrintWriter(allReplicaSendSockets.get(leaderServerID).getOutputStream(), true);
                            printWriterRandom.println(MsgHello);
                            printWriterRandom.close();
                        } catch (Exception e) {
                            e.printStackTrace();
                            System.out.printf("build printWriter failed for index %s", leaderServerID);
                        }
                        break;
                    case RESPONSE:
                        nextMsg = ServerToClientMsg.ServerResponseMsg.fromString(nextString);
                        if (nextSendMsg.getMessageSequenceNumber() == ((ServerToClientMsg.ServerResponseMsg) nextMsg).getMessageSequenceNumber()) {
                            sendMessageQueue.poll();
                            receivedLastSendMsgResponse = true;
                        } else {
                            throw new IllegalStateException("received inconsistent message response");
                        }

                        break;
                    default:
                        throw new IllegalArgumentException("Can not detect message type in message queue");
                }
            }

        }
    }


    /* define the function to send next msg to leader*/
    //
    //
    //

    public void sendNextMsg() {

        try {
            find_leader();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Find leader failed");
        }


    }

    /*define the function to find leader*/

    public void find_leader() {
//         Random random = new Random();
//         int randomIndex = random.nextInt(allReplicasInfo.size());
//         leaderServerID = randomIndex;


        try {
            PrintWriter printWriterRandom = new PrintWriter(allReplicaSendSockets.get(leaderServerID).getOutputStream(), true);
            printWriterRandom.println(MsgHello.toString());
            printWriterRandom.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.printf("build printwriter failed for index %s", leaderServerID);
        }


    }

    public class WaitRepeatSendTask extends TimerTask {

        ClientToServerMsg.ChatMsg curMsg;
        PrintWriter leaderPrintWriter;

        public WaitRepeatSendTask(ClientToServerMsg.ChatMsg curMsg, PrintWriter leaderPrintWriter) {
            this.curMsg = curMsg;
            this.leaderPrintWriter = leaderPrintWriter;
        }

        @Override
        public void run() {

            if (curMsg == sendMessageQueue.peek()) {
                leaderPrintWriter.println(curMsg.toString());
                System.out.println("is connected " + allReplicaSendSockets.get(0).isConnected());
                System.out.println("is closed " + allReplicaSendSockets.get(0).isClosed());
            } else {
                leaderPrintWriter.close();
                cancel();
            }
        }
    }

    public class WaitRepeatSend {
        Timer timer;

        public WaitRepeatSend(ClientToServerMsg.ChatMsg curMsg, PrintWriter leaderPrintWriter, int seconds) {
            timer = new Timer();
            Calendar calendar = Calendar.getInstance();
            Date time = calendar.getTime();
            timer.schedule(new WaitRepeatSendTask(curMsg, leaderPrintWriter), time, seconds * 10);
        }
    }
}
