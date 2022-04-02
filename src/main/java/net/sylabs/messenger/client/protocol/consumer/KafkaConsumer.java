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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.*;

public final class KafkaConsumer<MessageType extends MessageInterface> implements ConsumerInterface<MessageType> {
    private final ChannelInterface channel;
    private final ConsumerConfig config;
    private final SerializerInterface serializer;
    private final String groupId;
    private final String consumerId;
    private final org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer;
    private final Class<? extends MessageInterface> messageClass;

    // TODO: Implement message validation
    public KafkaConsumer(
        ChannelInterface channel,
        ConsumerConfig config,
        SerializerInterface serializer,
        ContractResolverInterface contractResolver
    ) throws ConsumerException {
        this.channel = channel;
        this.config = config;
        this.serializer = serializer;

        this.consumerId = String.format(
            "%s-%s",
            channel.getConsumerId(),
            UUID.randomUUID()
        );

        if (config.isCollaborativeMode()) {
            this.groupId = channel.getConsumerId();
        } else {
            this.groupId = consumerId;
        }

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

    private List<String> poll() throws ConsumerException {
        try {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));

            List<String> serializedMessages = new ArrayList<>();

            for (ConsumerRecord<String, String> record : records) {
                serializedMessages.add(record.value());
            }

            return serializedMessages;
        } catch (Throwable exception) {
            throw new ConsumerException(exception.getMessage(), exception);
        }
    }

    private org.apache.kafka.clients.consumer.KafkaConsumer<String, String> createConsumer() throws ConsumerException {
        try {
            Properties properties = new Properties();

            properties.setProperty(org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(",", config.getServerUris()));
            properties.setProperty(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(org.apache.kafka.clients.consumer.ConsumerConfig.CLIENT_ID_CONFIG, consumerId);
            properties.setProperty(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            properties.setProperty(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            properties.setProperty(org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

            return new org.apache.kafka.clients.consumer.KafkaConsumer<>(properties);
        } catch (Throwable exception) {
            throw new ConsumerException(exception.getMessage(), exception);
        }
    }
}
