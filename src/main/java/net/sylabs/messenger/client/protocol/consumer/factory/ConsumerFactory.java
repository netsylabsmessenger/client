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

package net.sylabs.messenger.client.protocol.consumer.factory;

import net.sylabs.messenger.client.protocol.channel.ChannelInterface;
import net.sylabs.messenger.client.protocol.consumer.*;
import net.sylabs.messenger.client.protocol.consumer.factory.exception.ConsumerFactoryException;
import net.sylabs.messenger.client.protocol.contract.resolver.ContractResolverInterface;
import net.sylabs.messenger.client.utils.serializer.SerializerInterface;
import net.sylabs.messenger.message.MessageInterface;

public class ConsumerFactory implements ConsumerFactoryInterface {
    ConsumerConfig consumerConfig;
    SerializerInterface serializer;
    ContractResolverInterface contractResolver;

    public ConsumerFactory(
        ConsumerConfig consumerConfig,
        SerializerInterface serializer,
        ContractResolverInterface contractResolver
    ) {
        this.consumerConfig = consumerConfig;
        this.serializer = serializer;
        this.contractResolver = contractResolver;
    }

    // TODO: Implement consumer type resolving
    @Override
    public <MessageType extends MessageInterface> ConsumerInterface<MessageType> createConsumer(
        ChannelInterface channel
    ) throws ConsumerFactoryException {
        try {
            return new KafkaConsumer<>(channel, consumerConfig, serializer, contractResolver);
        } catch (Throwable exception) {
            throw new ConsumerFactoryException(exception.getMessage(), exception);
        }
    }

    // TODO: Implement consumer type resolving
    @Override
    public <MessageType extends MessageInterface> TransactionalConsumerInterface<MessageType> createTransactionalConsumer(
        ChannelInterface channel
    ) throws ConsumerFactoryException {
        try {
            return new KafkaTransactionalConsumer<>(channel, consumerConfig, serializer, contractResolver);
        } catch (Throwable exception) {
            throw new ConsumerFactoryException(exception.getMessage(), exception);
        }
    }
}
