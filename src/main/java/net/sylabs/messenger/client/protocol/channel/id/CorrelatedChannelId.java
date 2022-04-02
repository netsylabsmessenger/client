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

import net.sylabs.messenger.client.protocol.channel.id.exception.CorrelatedChannelIdException;
import net.sylabs.messenger.client.protocol.consumer.id.ConsumerId;
import net.sylabs.messenger.client.protocol.contract.id.ContractId;
import net.sylabs.messenger.client.protocol.producer.id.ProducerId;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CorrelatedChannelId implements CorrelatedChannelIdInterface {
    private final String string;
    private final String consumerId;
    private final String producerId;
    private final String contractId;
    private final String correlationId;

    public CorrelatedChannelId(String string) throws CorrelatedChannelIdException {
        Pattern pattern = Pattern.compile(CorrelatedChannelIdInterface.getRegexPattern());
        Matcher matcher = pattern.matcher(string);

        if (!matcher.find()) {
            throw new CorrelatedChannelIdException(
                String.format(
                    "Provided string argument \"%s\" does not comply with Correlated Channel ID regex pattern %s",
                    string,
                    CorrelatedChannelIdInterface.getRegexPattern()
                )
            );
        }

        this.string = matcher.group(0);

        try {
            this.consumerId = new ConsumerId(matcher.group(1)).toString();
            this.producerId = new ProducerId(matcher.group(2)).toString();
            this.contractId = new ContractId(matcher.group(3)).toString();
            this.correlationId = new ChannelId(matcher.group(4)).toString();
        } catch (Throwable exception) {
            throw new CorrelatedChannelIdException(exception.getMessage(), exception);
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
    public String getCorrelationId() {
        return correlationId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof CorrelatedChannelId)) {
            return false;
        }

        CorrelatedChannelId that = (CorrelatedChannelId) o;

        return toString().equals(that.toString()) && getCorrelationId().equals(that.getCorrelationId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(toString(), getCorrelationId());
    }
}
