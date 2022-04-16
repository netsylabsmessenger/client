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

package net.sylabs.messenger.client.protocol.node.id;

import net.sylabs.messenger.client.protocol.node.id.exception.NodeIdException;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NodeId implements NodeIdInterface {
    private final String string;

    public NodeId(String string) throws NodeIdException {
        Pattern pattern = Pattern.compile(NodeIdInterface.getRegexPattern());
        Matcher matcher = pattern.matcher(string);

        if (!matcher.find()) {
            throw new NodeIdException(
                String.format(
                    "Provided string argument \"%s\" does not comply with Network ID regex pattern %s",
                    string,
                    NodeIdInterface.getRegexPattern()
                )
            );
        }

        this.string = matcher.group(0);
    }

    @Override
    public String toString() {
        return string;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof NodeId)) {
            return false;
        }

        NodeId that = (NodeId) o;

        return toString().equals(that.toString());
    }

    @Override
    public int hashCode() {
        return Objects.hash(toString());
    }
}
