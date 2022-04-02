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

package net.sylabs.messenger.client.protocol.channel.factory;

import net.sylabs.messenger.client.protocol.channel.Channel;
import net.sylabs.messenger.client.protocol.channel.ChannelInterface;
import net.sylabs.messenger.client.protocol.channel.CorrelatedChannel;
import net.sylabs.messenger.client.protocol.channel.factory.exception.ChannelFactoryException;
import net.sylabs.messenger.client.protocol.channel.id.ChannelId;
import net.sylabs.messenger.client.protocol.channel.id.CorrelatedChannelId;
import net.sylabs.messenger.client.protocol.channel.id.CorrelatedChannelIdInterface;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ChannelFactory implements ChannelFactoryInterface {

    @Override
    public ChannelInterface createChannel(String channelId) throws ChannelFactoryException {
        try {
            Pattern pattern = Pattern.compile(CorrelatedChannelIdInterface.getRegexPattern());
            Matcher matcher = pattern.matcher(channelId);

            if (matcher.find()) {
                return new CorrelatedChannel(new CorrelatedChannelId(channelId));
            }

            return new Channel(new ChannelId(channelId));
        } catch (Throwable exception) {
            throw new ChannelFactoryException(exception.getMessage(), exception);
        }
    }
}
