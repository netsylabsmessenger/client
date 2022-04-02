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

package net.sylabs.messenger.client.protocol.channel.id;

import net.sylabs.messenger.client.protocol.consumer.ConsumerConfig;
import net.sylabs.messenger.client.protocol.channel.id.exception.ChannelIdException;
import net.sylabs.messenger.client.protocol.consumer.id.ConsumerId;
import net.sylabs.messenger.client.protocol.contract.id.ContractId;
import net.sylabs.messenger.client.protocol.producer.id.ProducerId;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ChannelId implements ChannelIdInterface {
    private final String string;
    private final String consumerId;
    private final String producerId;
    private final String contractId;

    public ChannelId(String string) throws ChannelIdException {
        Pattern pattern = Pattern.compile(ChannelIdInterface.getRegexPattern());
        Matcher matcher = pattern.matcher(string);

        if (!matcher.find()) {
            throw new ChannelIdException(
                String.format(
                    "Provided string argument \"%s\" does not comply with Channel ID regex pattern %s",
                    string,
                    ChannelIdInterface.getRegexPattern()
                )
            );
        }

        this.string = matcher.group(0);

        try {
            this.consumerId = new ConsumerId(matcher.group(1)).toString();
            this.producerId = new ProducerId(matcher.group(2)).toString();
            this.contractId = new ContractId(matcher.group(3)).toString();
        } catch (Throwable exception) {
            throw new ChannelIdException(exception.getMessage(), exception);
        }
    }

    @Override
    public String toString() {
        return string;
    }

    @Override
    public String getConsumerId() {
        return consumerId;
    }

    @Override
    public String getProducerId() {
        return producerId;
    }

    @Override
    public String getContractId() {
        return contractId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof ChannelId)) {
            return false;
        }

        ChannelId channelId = (ChannelId) o;

        return toString().equals(channelId.toString());
    }

    @Override
    public int hashCode() {
        return Objects.hash(toString());
    }
}
