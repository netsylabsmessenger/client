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

package net.sylabs.messenger.client.protocol.contract;

import net.sylabs.messenger.client.protocol.contract.id.ContractIdInterface;
import net.sylabs.messenger.client.protocol.contract.schema.SchemaInterface;

import java.util.Objects;

public class Contract implements ContractInterface {
    private final String id;
    private final String name;
    private final String version;
    private final SchemaInterface schema;

    public Contract(ContractIdInterface id, SchemaInterface schema) {
        this.id = id.toString();
        this.name = id.getName();
        this.version = id.getVersion();
        this.schema = schema;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getVersion() {
        return version;
    }

    @Override
    public SchemaInterface getSchema() {
        return schema;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof Contract)) {
            return false;
        }

        Contract contract = (Contract) o;

        return getId().equals(contract.getId()) && getSchema().equals(contract.getSchema());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId(), getSchema());
    }
}
