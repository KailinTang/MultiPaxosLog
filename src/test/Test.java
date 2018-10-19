package test;

import java.util.Scanner;

public class Test {

    public static void main(String[] args) {
        new Thread(new ScannerHandler()).start();
    }

    public static class ScannerHandler implements Runnable {

        @Override
        public void run() {
            final Scanner scanner = new Scanner(System.in);
            while (true) {
                System.out.println(scanner.nextLine());
            }
        }
    }

}
