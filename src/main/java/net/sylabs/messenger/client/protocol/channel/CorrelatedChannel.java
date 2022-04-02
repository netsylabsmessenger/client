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

package net.sylabs.messenger.client.protocol.channel;

import net.sylabs.messenger.client.protocol.channel.exception.CorrelatedChannelException;
import net.sylabs.messenger.client.protocol.channel.id.ChannelId;
import net.sylabs.messenger.client.protocol.channel.id.CorrelatedChannelIdInterface;

import java.util.Objects;

public class CorrelatedChannel implements CorrelatedChannelInterface {
    private final String id;
    private final String consumerId;
    private final String producerId;
    private final String contractId;
    private final ChannelInterface correlation;

    public CorrelatedChannel(CorrelatedChannelIdInterface id) throws CorrelatedChannelException {
        this.id = id.toString();
        this.consumerId = id.getConsumerId();
        this.producerId = id.getProducerId();
        this.contractId = id.getContractId();

        try {
            this.correlation = new Channel(new ChannelId(id.getCorrelationId()));
        } catch (Throwable exception) {
            throw new CorrelatedChannelException(exception.getMessage(), exception);
        }

        if (!this.correlation.getConsumerId().equals(this.getProducerId()) ||
            !this.correlation.getProducerId().equals(this.getConsumerId())) {
            throw new CorrelatedChannelException(
                String.format(
                    "Consumer/Producer of a Correlated Channel %s must correspond to the Producer/Consumer of its Correlation Channel %s.",
                    this.getId(),
                    this.correlation.getId()
                )
            );
        }
    }

    @Override
    public String toString() {
        return id;
    }

    @Override
    public String getId() {
        return id;
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
    public ChannelInterface getCorrelation() {
        return correlation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof CorrelatedChannel)) {
            return false;
        }

        CorrelatedChannel that = (CorrelatedChannel) o;

        return getId().equals(that.getId()) && getCorrelation().equals(that.getCorrelation());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId());
    }
}
