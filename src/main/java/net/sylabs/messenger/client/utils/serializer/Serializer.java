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

package net.sylabs.messenger.client.utils.serializer;

import com.google.gson.*;
import net.sylabs.messenger.client.utils.serializer.exception.SerializerException;
import net.sylabs.messenger.message.MessageInterface;

import java.lang.reflect.Type;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Serializer implements SerializerInterface {
    private final Gson gson;

    public Serializer() {
        GsonBuilder builder = new GsonBuilder();

        this.gson = builder.serializeNulls()
           .registerTypeAdapter(
               LocalDate.class,
               (JsonSerializer<LocalDate>) (localDate, type, jsonSerializationContext) -> new JsonPrimitive(
                   localDate.format(DateTimeFormatter.ISO_DATE)
               )
           ).registerTypeAdapter(
                LocalDate.class,
                (JsonDeserializer<LocalDate>) (jsonElement, type, jsonDeserializationContext) -> LocalDate.parse(
                    jsonElement.getAsString(),
                    DateTimeFormatter.ISO_DATE
                )
            ).registerTypeAdapter(
                LocalDateTime.class,
                (JsonSerializer<LocalDateTime>) (localDateTime, type, jsonSerializationContext) -> new JsonPrimitive(
                    localDateTime.format(DateTimeFormatter.ISO_DATE_TIME)
                )
            ).registerTypeAdapter(
                LocalDateTime.class,
                (JsonDeserializer<LocalDateTime>) (jsonElement, type, jsonDeserializationContext) -> LocalDateTime.parse(
                    jsonElement.getAsString(),
                    DateTimeFormatter.ISO_DATE_TIME
                )
            ).create();
    }

    @Override
    public String serialize(MessageInterface message) throws SerializerException {
        try {
            return this.gson.toJson(message);
        } catch (Throwable exception) {
            throw new SerializerException(exception.getMessage());
        }
    }

    @Override
    public <MessageType extends MessageInterface> MessageType deserialize(String message, Class<? extends MessageInterface> clazz) throws SerializerException {
        try {
            return this.gson.fromJson(message, (Type) clazz);
        } catch (Throwable exception) {
            throw new SerializerException(exception.getMessage());
        }
    }
}
