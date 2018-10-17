package test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class EchoServer1 {
    public static void main(String[] args) throws IOException {

        int portNumber = 3058;

        try (
                ServerSocket serverSocket =
                        new ServerSocket(3058);
                Socket clientSocket = serverSocket.accept();
                PrintWriter out =
                        new PrintWriter(clientSocket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(clientSocket.getInputStream()));
        ) {
            String inputLine;
            System.out.println("getInetAddress" + clientSocket.getInetAddress().getHostAddress()
                    + "getPort" + clientSocket.getPort() + "getLocalPort" + clientSocket.getLocalPort() + "localaddr"
                    + clientSocket.getLocalAddress().getHostAddress());
            while ((inputLine = in.readLine()) != null) {
                out.println(inputLine);
            }
        } catch (IOException e) {
            System.out.println("Exception caught when trying to listen on port "
                    + portNumber + " or listening for a connection");
            System.out.println(e.getMessage());
        }
    }
}
