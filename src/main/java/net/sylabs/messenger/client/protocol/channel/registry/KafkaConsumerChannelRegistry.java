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

package net.sylabs.messenger.client.protocol.channel.registry;

import net.sylabs.messenger.client.protocol.channel.ChannelInterface;
import net.sylabs.messenger.client.protocol.channel.factory.ChannelFactoryInterface;
import net.sylabs.messenger.client.protocol.channel.factory.exception.ChannelFactoryException;
import net.sylabs.messenger.client.protocol.channel.registry.exception.ChannelRegistryException;
import net.sylabs.messenger.client.protocol.consumer.id.ConsumerIdInterface;
import net.sylabs.messenger.client.protocol.contract.id.ContractIdInterface;
import net.sylabs.messenger.client.protocol.producer.id.ProducerIdInterface;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class KafkaConsumerChannelRegistry extends Thread implements ChannelRegistryInterface {
    private final ChannelRegistryConfig config;
    private final ChannelFactoryInterface channelFactory;
    private final String consumerId;
    private final Logger logger;
    private final Set<ChannelInterface> channels = new HashSet<>();

    public KafkaConsumerChannelRegistry(ChannelRegistryConfig config, ChannelFactoryInterface channelFactory) throws ChannelRegistryException {
        this.config = config;
        this.channelFactory = channelFactory;
        this.logger = LogManager.getLogger(KafkaConsumerChannelRegistry.class);
        this.consumerId = String.format(
            "%s-%s",
            this.getClass().getName(),
            UUID.randomUUID()
        );

        synchronize();
        start();
    }

    @Override
    public Set<ChannelInterface> getChannels(ConsumerIdInterface consumerId) throws ChannelRegistryException {
        try {
            return channels
                .stream()
                .filter(channel -> consumerId.toString().equals(channel.getConsumerId()))
                .collect(Collectors.toSet());
        } catch (Throwable exception) {
            throw new ChannelRegistryException(exception.getMessage(), exception);
        }
    }

    @Override
    public Set<ChannelInterface> getChannels(ProducerIdInterface producerId) throws ChannelRegistryException {
        try {
            return channels
                .stream()
                .filter(channel -> producerId.toString().equals(channel.getProducerId()))
                .collect(Collectors.toSet());
        } catch (Throwable exception) {
            throw new ChannelRegistryException(exception.getMessage(), exception);
        }
    }

    @Override
    public Set<ChannelInterface> getChannels(ContractIdInterface contractId) throws ChannelRegistryException {
        try {
            return channels
                .stream()
                .filter(channel -> contractId.toString().equals(channel.getContractId()))
                .collect(Collectors.toSet());
        } catch (Throwable exception) {
            throw new ChannelRegistryException(exception.getMessage(), exception);
        }
    }

    @Override
    public void run() {
        //noinspection InfiniteLoopStatement
        while (true) {
            logger.atDebug().log(String.format("Pauses synchronization for %d ms.", config.getSynchronizationIntervalMillis()));

            try {
                //noinspection BusyWait
                Thread.sleep(config.getSynchronizationIntervalMillis());
            } catch (Throwable exception) {
                logger.atError().log("Fails to pause synchronization:", exception);
            }

            logger.atDebug().log("Starts synchronization.");

            try {
                synchronize();
            } catch (Throwable exception) {
                logger.atError().log("Fails to synchronize:", exception);
            }

            logger.atDebug().log("Finishes synchronization.");
        }
    }

    private void synchronize() throws ChannelRegistryException {
        Set<ChannelInterface> channels = getChannels();

        logger.atInfo().log("Registers channels: {}", channels);

        this.channels.addAll(channels);
    }

    private Set<ChannelInterface> getChannels() throws ChannelRegistryException {
        Set<String> topics = getTopics();
        Set<ChannelInterface> channels = new HashSet<>();

        for (String channelId : topics) {
            try {
                channels.add(channelFactory.createChannel(channelId));
            } catch (ChannelFactoryException exception) {
                logger.atInfo().log("Fails to register a topic as a Channel:", exception);
            }
        }

        return channels;
    }

    private Set<String> getTopics() throws ChannelRegistryException {
        KafkaConsumer<String, String> consumer = createConsumer();

        try {
            Set<String> topics = new HashSet<>(consumer.listTopics().keySet());
            consumer.close();

            return topics;
        } catch (Throwable exception) {
            throw new ChannelRegistryException(exception.getMessage());
        }
    }

    private KafkaConsumer<String, String> createConsumer() throws ChannelRegistryException {
        try {
            Properties properties = new Properties();

            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(",", config.getServerUris()));
            properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, consumerId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

            return new KafkaConsumer<>(properties);
        } catch (Throwable exception) {
            throw new ChannelRegistryException(exception.getMessage());
        }
    }
}
