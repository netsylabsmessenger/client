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

package net.sylabs.messenger.client.protocol.contract.registry;

import net.sylabs.messenger.client.protocol.contract.Contract;
import net.sylabs.messenger.client.protocol.contract.ContractInterface;
import net.sylabs.messenger.client.protocol.contract.id.ContractIdInterface;
import net.sylabs.messenger.client.protocol.contract.registry.exception.ContractRegistryException;
import net.sylabs.messenger.client.protocol.contract.schema.AvroSchema;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.json.JSONObject;

import java.util.Objects;

// TODO: Finish implementing this class
public class ConfluentSchemaRegistryContractRegistry implements ContractRegistryInterface {
    private final ContractRegistryConfig config;
    private final OkHttpClient httpClient;

    public ConfluentSchemaRegistryContractRegistry(ContractRegistryConfig config) {
        this.config = config;
        this.httpClient = new OkHttpClient();
    }

    @Override
    public ContractInterface getContract(ContractIdInterface id) throws ContractRegistryException {
        try {
            String endpoint = String.format(
                "%s/%s/%s-%s/%s/%s",
                config.getServerUris().toArray()[0],
                "subjects",
                id.getName(),
                "value",
                "versions",
                id.getVersion()
            );

            Request request = new Request.Builder()
                .url(endpoint)
                .addHeader("Content-Type", "application/json")
                .addHeader("Accept", "application/vnd.schemaregistry.v1+json,application/vnd.schemaregistry+json,application/json")
                .build();

            Response response = httpClient.newCall(request).execute();

            String schema = new JSONObject(Objects.requireNonNull(response.body()).string()).getString("schema");

            return new Contract(id, new AvroSchema(schema));
        } catch (Throwable exception) {
            throw new ContractRegistryException(exception.getMessage(), exception);
        }
    }
}
