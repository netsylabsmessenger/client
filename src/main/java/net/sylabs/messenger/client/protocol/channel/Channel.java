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

import net.sylabs.messenger.client.protocol.channel.id.ChannelIdInterface;

import java.util.Objects;

public class Channel implements ChannelInterface {
    private final String id;
    private final String consumerId;
    private final String producerId;
    private final String contractId;

    public Channel(ChannelIdInterface id) {
        this.id = id.toString();
        this.consumerId = id.getConsumerId();
        this.producerId = id.getProducerId();
        this.contractId = id.getContractId();
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
    public String toString() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof Channel)) {
            return false;
        }

        Channel channel = (Channel) o;

        return getId().equals(channel.getId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId());
    }
}
