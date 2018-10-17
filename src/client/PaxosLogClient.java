package client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class PaxosLogClient {

    private final String serverAddress;
    private final int serverPort;
    private final Socket socket;
    private final PrintWriter printWriter;
    private final BufferedReader bufferedReader;

    public PaxosLogClient(final String serverAddress, final int serverPort) throws IOException {
        this.serverAddress = serverAddress;
        this.serverPort = serverPort;
        this.socket = new Socket(serverAddress, serverPort);
        this.printWriter = new PrintWriter(socket.getOutputStream(), true);
        this.bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        System.out.println("Connecting to the server with address: " + serverAddress +
                " with port number: " + serverPort);
    }

    public void write(final String message) {
        this.printWriter.println(message);
    }

    public String read() throws IOException {
        return this.bufferedReader.readLine();
    }
}
