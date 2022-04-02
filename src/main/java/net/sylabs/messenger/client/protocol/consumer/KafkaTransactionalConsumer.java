/*
 * Copyright (c) 2019-2022 Clément Cazaud <clement.cazaud@gmail.com>,
 *                         Hamza Abidi <abidi.hamza84000@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.sylabs.messenger.client.protocol.consumer;

import net.sylabs.messenger.client.protocol.channel.ChannelInterface;
import net.sylabs.messenger.client.protocol.consumer.exception.ConsumerException;
import net.sylabs.messenger.client.protocol.contract.id.ContractId;
import net.sylabs.messenger.client.protocol.contract.resolver.ContractResolverInterface;
import net.sylabs.messenger.client.utils.serializer.SerializerInterface;
import net.sylabs.messenger.message.MessageInterface;
import org.apache.commons.compress.utils.Lists;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

public final class KafkaTransactionalConsumer<MessageType extends MessageInterface> implements TransactionalConsumerInterface<MessageType> {
    private final ChannelInterface channel;
    private final ConsumerConfig config;
    private final SerializerInterface serializer;
    private final String groupId;
    private final String consumerId;
    private final KafkaConsumer<String, String> consumer;
    private final Class<? extends MessageInterface> messageClass;
    private Set<TopicPartition> partitions;
    private List<ConsumerRecord<String, String>> consumedRecords;
    private List<ConsumerRecord<String, String>> consumedRecordsToCommit;

    // TODO: Implement message validation
    public KafkaTransactionalConsumer(
        ChannelInterface channel,
        ConsumerConfig config,
        SerializerInterface serializer,
        ContractResolverInterface contractResolver
    ) throws ConsumerException {
        this.channel = channel;
        this.config = config;
        this.serializer = serializer;

        this.groupId = channel.getConsumerId();
        this.consumerId = String.format(
            "%s-%s",
            channel.getConsumerId(),
            UUID.randomUUID()
        );


        this.consumer = createConsumer();

        try {
            consumer.subscribe(Collections.singletonList(channel.getId()));
        } catch (Throwable exception) {
            throw new ConsumerException(exception.getMessage(), exception);
        }

        try {
            this.messageClass = contractResolver.getContractualizedMessageClass(new ContractId(channel.getContractId()));
        } catch (Throwable exception) {
            throw new ConsumerException(exception.getMessage(), exception);
        }

        this.partitions = new HashSet<>();
        this.consumedRecords = new ArrayList<>();
        this.consumedRecordsToCommit = new ArrayList<>();
    }

    @Override
    public ChannelInterface getChannel() {
        return channel;
    }

    @Override
    public List<MessageType> consume() throws ConsumerException {
        List<String> serializedMessages = poll();
        List<MessageType> deserializedMessages = new ArrayList<>();

        try {
            for (String serializedMessage : serializedMessages) {
                deserializedMessages.add(serializer.deserialize(serializedMessage, messageClass));
            }

            return deserializedMessages;
        } catch (Throwable exception) {
            throw new ConsumerException(exception.getMessage(), exception);
        }
    }

    @Override
    public void commitConsummation() throws ConsumerException {
        try {
            consumer.commitSync();
            consumedRecordsToCommit.clear();
        } catch (Throwable exception) {
            throw new ConsumerException(exception.getMessage(), exception);
        }
    }

    @Override
    public void commitConsummation(int messageIndex) throws ConsumerException {
        try {
            ConsumerRecord<String, String> record = consumedRecords.get(messageIndex);

            for (TopicPartition partition : partitions) {
                if (partition.partition() == record.partition() && consumedRecordsToCommit.contains(record)) {
                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(record.offset() + 1)));

                    consumedRecordsToCommit = consumedRecordsToCommit.subList(
                        consumedRecordsToCommit.indexOf(record) + 1,
                        consumedRecordsToCommit.size()
                    );

                    return;
                }
            }
        } catch (Throwable exception) {
            throw new ConsumerException(exception.getMessage(), exception);
        }
    }

    private List<String> poll() throws ConsumerException {
        if (!consumedRecordsToCommit.isEmpty()) {
            throw new ConsumerException("Consummation of previously returned messages must get confirmed first in order to consume more");
        }

        try {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));

            List<String> serializedMessages = new ArrayList<>();

            for (ConsumerRecord<String, String> record : records) {
                serializedMessages.add(record.value());
            }

            this.partitions = records.partitions();
            this.consumedRecords = Lists.newArrayList(records.iterator());
            this.consumedRecordsToCommit = Lists.newArrayList(records.iterator());

            return serializedMessages;
        } catch (Throwable exception) {
            throw new ConsumerException(exception.getMessage(), exception);
        }
    }

    private KafkaConsumer<String, String> createConsumer() throws ConsumerException {
        try {
            Properties properties = new Properties();

            properties.setProperty(org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(",", config.getServerUris()));
            properties.setProperty(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_ID_CONFIG, consumerId);
            properties.setProperty(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            properties.setProperty(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            properties.setProperty(org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            properties.setProperty(org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
            properties.setProperty(org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

            return new KafkaConsumer<>(properties);
        } catch (Throwable exception) {
            throw new ConsumerException(exception.getMessage(), exception);
        }
    }
}
