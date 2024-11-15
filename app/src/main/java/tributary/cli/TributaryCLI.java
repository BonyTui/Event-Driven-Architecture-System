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
        if (input.isEmpty()) {
            return;
        }

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
            handleProduceInput(inputConfig);
            break;

        case "consume":
            handleConsumeInput(inputConfig);
            break;

        case "delete":
            handleDeleteInput(inputConfig);
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
        String consumerGroupId;
        String consumerId;
        String consumerGroupRebalancing;

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
            consumerGroupId = inputConfig[0];
            topicId = inputConfig[1];
            consumerGroupRebalancing = inputConfig[2];
            tributary.createConsumerGroup(consumerGroupId, topicId, consumerGroupRebalancing);
            break;
        case "consumer":
            consumerGroupId = inputConfig[0];
            consumerId = inputConfig[1];
            tributary.createConsumer(consumerGroupId, consumerId);
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
            String topicId = inputConfig[0];
            tributary.showTopic(topicId);
            break;
        case "consumer_group":
            String consumerGroupId = inputConfig[0];
            tributary.showConsumerGroup(consumerGroupId);
            break;
        case "all":
            tributary.showAll();
            break;
        default:
            System.err.println("Invalid Command");
            break;
        }
    }

    private static void handleDeleteInput(String[] inputConfig) {
        switch (classType) {
        case "consumer":
            String consumerId = inputConfig[0];
            tributary.deleteConsumer(consumerId);
            break;
        default:
            System.err.println("Invalid Command");
            break;
        }
    }

    private static void handleProduceInput(String[] inputConfig) {
        switch (classType) {
        case "event":
            String producerId = inputConfig[0];
            String topicId = inputConfig[1];
            String eventContent = inputConfig[2];
            String partitionId = inputConfig[3];
            tributary.produceEvent(producerId, topicId, eventContent, partitionId);
            break;
        default:
            System.err.println("Invalid Command");
            break;
        }
    }

    private static void handleConsumeInput(String[] inputConfig) {
        String consumerId;
        String partitionId;
        String numberOfEvents;
        switch (classType) {
        case "event":
            consumerId = inputConfig[0];
            partitionId = inputConfig[1];
            tributary.consumeEvent(consumerId, partitionId);
            break;
        case "events":
            consumerId = inputConfig[0];
            partitionId = inputConfig[1];
            numberOfEvents = inputConfig[2];
            tributary.consumeEvents(consumerId, partitionId, Integer.parseInt(numberOfEvents));
            break;
        default:
            System.err.println("Invalid Command");
            break;
        }
    }
}
