package tributary.cli;

import java.util.Scanner;

import tributary.api.TributaryService;
import tributary.core.TributaryCluster;

public class TributaryCLI {
    private static TributaryService tributary = new TributaryCluster();
    private static String commandType;
    private static String classType;

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter input (Ctrl+C to cancel):\n");

        while (scanner.hasNextLine()) {
            String input = scanner.nextLine();
            try {
                handleInput(input);
            } catch (Exception e) {
                System.err.println("Invalid Command\n");
            }
        }
        scanner.close();
        System.out.println("Input cancelled. Program terminated.");

    }

    private static void handleInput(String input) {
        String[] inputArgs = input.split(" ");
        commandType = inputArgs[0];
        classType = inputArgs[1];

        switch (commandType) {
        case "create":
            handleCreateInput(inputArgs);
            break;

        case "show":
            handleShowInput(inputArgs);
            break;

        case "produce":

            break;

        case "consume":

            break;

        case "delete":

            break;
        default:
            break;
        }
        System.out.println();
    }

    private static void handleCreateInput(String[] inputArgs) {
        switch (classType) {
        case "topic":
            String id = inputArgs[2];
            String type = inputArgs[3];
            tributary.createTopic(id, type);
            break;
        case "partition":
            String topicId = inputArgs[2];
            String partitionId = inputArgs[3];
            tributary.createPartition(topicId, partitionId);
            break;
        case "consumer_group":

            break;
        case "consumer":

            break;
        case "producer":

            break;

        default:
            System.err.println("Invalid Command");
            break;
        }
    }

    private static void handleShowInput(String[] inputArgs) {
        switch (classType) {
        case "topic":
            String id = inputArgs[2];
            tributary.showTopic(id);
            break;
        case "consumer_group":

            break;
        default:
            System.err.println("Invalid Command");
            break;
        }
    }
}
