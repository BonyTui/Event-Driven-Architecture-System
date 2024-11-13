package tributary.cli;

import java.util.Scanner;
import java.util.Arrays;

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

        // The config like <topic>, <id>, <type>
        String[] inputConfig = Arrays.copyOfRange(inputArgs, 2, inputArgs.length);

        switch (commandType) {
        case "create":
            handleCreateInput(inputConfig);
            break;

        case "show":
            handleShowInput(inputConfig);
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

    private static void handleCreateInput(String[] inputConfig) {
        String topicId;
        String topicType;
        String partitionId;
        String producerId;
        String producerAllocation;

        switch (classType) {
        case "topic":
            topicId = inputConfig[0];
            topicType = inputConfig[1];
            tributary.createTopic(topicId, topicType);
            break;
        case "partition":
            topicId = inputConfig[0];
            partitionId = inputConfig[1];
            tributary.createPartition(topicId, partitionId);
            break;
        case "consumer_group":

            break;
        case "consumer":

            break;
        case "producer":
            producerId = inputConfig[0];
            topicType = inputConfig[1];
            producerAllocation = inputConfig[2];
            tributary.createProducer(producerId, topicType, producerAllocation);
            break;

        default:
            System.err.println("Invalid Command");
            break;
        }
    }

    private static void handleShowInput(String[] inputConfig) {
        switch (classType) {
        case "topic":
            String id = inputConfig[2];
            tributary.showTopic(id);
            break;
        case "consumer_group":

            break;
        case "all":
            tributary.showAll();
            break;
        default:
            System.err.println("Invalid Command");
            break;
        }
    }
}
