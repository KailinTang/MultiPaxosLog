package client;

import java.io.IOException;
import java.util.Scanner;

public class ClientLauncher {

    private PaxosLogClient paxosLogClient;

    public void setPaxosLogClient(PaxosLogClient paxosLogClient) {
        this.paxosLogClient = paxosLogClient;
    }

    public PaxosLogClient getPaxosLogClient() {
        return paxosLogClient;
    }

    public static void main(String[] args) {
        final ClientLauncher clientLauncher = new ClientLauncher();
        try {
            clientLauncher.setPaxosLogClient(new PaxosLogClient("127.0.0.1", 3057));
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("PaxosLogClient initialization failure!");
        }
        final Scanner scanner = new Scanner(System.in);
        final PaxosLogClient client = clientLauncher.getPaxosLogClient();
        while (true) {
            client.write(scanner.nextLine());
            try {
                System.out.println(client.read());
            } catch (IOException e) {
                e.printStackTrace();
                System.out.println("Fail to receive message from server!");
            }
        }
    }
}
