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

package net.sylabs.messenger.client.protocol.producer;

import net.sylabs.messenger.client.protocol.channel.ChannelInterface;
import net.sylabs.messenger.client.protocol.contract.id.ContractId;
import net.sylabs.messenger.client.protocol.contract.resolver.ContractResolverInterface;
import net.sylabs.messenger.client.protocol.producer.exception.ProducerException;
import net.sylabs.messenger.client.utils.serializer.SerializerInterface;
import net.sylabs.messenger.message.MessageInterface;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class KafkaProducer<MessageType extends MessageInterface> implements ProducerInterface<MessageType> {
    private final ChannelInterface channel;
    private final ProducerConfig config;
    private final SerializerInterface serializer;
    private final String producerId;
    private final org.apache.kafka.clients.producer.KafkaProducer<String, String> producer;
    private final Class<? extends MessageInterface> messageClass;

    public KafkaProducer(
        ChannelInterface channel,
        ProducerConfig config,
        SerializerInterface serializer,
        ContractResolverInterface contractResolver
    ) throws ProducerException {
        this.channel = channel;
        this.config = config;
        this.serializer = serializer;
        this.producerId = String.format(
            "%s-%s",
            channel.getProducerId(),
            UUID.randomUUID()
        );

        this.producer = createKafkaProducer();

        try {
            this.messageClass = contractResolver.getContractualizedMessageClass(new ContractId(channel.getContractId()));
        } catch (Throwable exception) {
            throw new ProducerException(exception.getMessage(), exception);
        }
    }

    @Override
    public ChannelInterface getChannel() {
        return channel;
    }

    @Override
    public void produce(List<MessageType> messages) throws ProducerException {
        List<String> serializedMessages = new ArrayList<>();

        for (MessageInterface message : messages) {
            if (!messageClass.equals(message.getClass())) {
                throw new ProducerException(
                    String.format(
                        "First argument must be a list of %s instances.",
                        messageClass.getName()
                    )
                );
            }
        }

        try {
            for (MessageInterface message : messages) {
                serializedMessages.add(serializer.serialize(message));
            }

            send(serializedMessages);
        } catch (Throwable exception) {
            throw new ProducerException(exception.getMessage(), exception);
        }
    }

    private void send(List<String> messages) throws ProducerException {
        try {
            for (String message : messages) {
                ProducerRecord<String, String> record = new ProducerRecord<>(channel.getId(), message);
                producer.send(record);
            }
        } catch (Throwable exception) {
            throw new ProducerException(exception.getMessage(), exception);
        }
    }

    private org.apache.kafka.clients.producer.KafkaProducer<String, String> createKafkaProducer() throws ProducerException {
        try {
            Properties properties = new Properties();

            properties.setProperty(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(",", config.getServerUris()));
            properties.setProperty(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            properties.setProperty(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            properties.setProperty(org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG, "1");

            return new org.apache.kafka.clients.producer.KafkaProducer<>(properties);
        } catch (Throwable exception) {
            throw new ProducerException(exception.getMessage(), exception);
        }
    }
}
