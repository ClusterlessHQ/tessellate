/*
 * Copyright (c) 2023 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

package io.clusterless.tessellate.printer;

import cascading.tap.AdaptorTap;
import cascading.tap.Tap;
import cascading.tap.partition.BasePartitionTap;
import cascading.tuple.Fields;
import cascading.tuple.coerce.Coercions;
import cascading.tuple.type.CoercibleType;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.clusterless.tessellate.options.PrintOptions;
import io.clusterless.tessellate.type.WrappedCoercibleType;
import io.clusterless.tessellate.util.json.JSONUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.lang.reflect.Type;
import java.util.*;
import java.util.function.BiConsumer;

public class SchemaPrinter {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaPrinter.class);
    private TypeMap typeMap = new TypeMap(TypeMap.Dialect.athena);

    public static class Schema {
        public static final String NAME = "name";
        public static final String TYPE = "type";
        public static final String COMMENT = "comment";

        @JsonProperty
        protected List<Map<String, String>> columns = new LinkedList<>();

        @JsonProperty
        protected List<Map<String, String>> partitions = new LinkedList<>();

        public void addColumn(String name, String type) {
            addTo(columns, name, type);
        }

        public void addPartition(String name, String type) {
            addTo(partitions, name, type);
        }

        protected void addTo(List<Map<String, String>> value, String name, String type) {
            value.stream()
                    .filter(pos -> pos.get(NAME).equals(name))
                    .findFirst()
                    .ifPresentOrElse(
                            pos -> pos.put(TYPE, type),
                            () -> value.add(map(name, type))
                    );
        }

        private Map<String, String> map(String name, String type) {
            Map<String, String> map = new LinkedHashMap<>();
            map.put(NAME, name);
            map.put(TYPE, type);
            return map;
        }
    }

    private final PrintOptions.PrintFormat printFormat;
    private final Fields fields;
    private final Fields partitionFields;

    public SchemaPrinter(Tap tap, PrintOptions.PrintFormat printFormat) {
        this.printFormat = printFormat;

        if (tap instanceof AdaptorTap) {
            tap = ((AdaptorTap<?, ?, ?, ?, ?, ?>) tap).getOriginal();
        }

        if (tap instanceof BasePartitionTap) {
            Tap parent = ((BasePartitionTap<?, ?, ?>) tap).getParent();
            this.fields = parent.getSinkFields();
            this.partitionFields = ((BasePartitionTap<?, ?, ?>) tap).getPartition().getPartitionFields();
        } else {
            this.fields = tap.getSinkFields();
            this.partitionFields = Fields.NONE;
        }
    }

    public void print(PrintStream out) {
        // todo: update in place by reading an existing schema
        //       note the schema object already supports update of a column without changing the order or comments
        Schema schema = new Schema();

        LOG.info("adding columns: {}, excluding: {}", fields.print(), partitionFields.print());
        addFields(fields, partitionFields, schema::addColumn);

        LOG.info("adding partitions: {}", partitionFields.print());
        addFields(partitionFields, Fields.NONE, schema::addPartition);

        out.print(asString(schema));
    }

    protected String asString(Schema schema) {
        switch (printFormat) {
            case JSON:
                return JSONUtil.writeAsStringSafePretty(schema);
            default:
                throw new UnsupportedOperationException("Unsupported value: " + printFormat);
        }
    }

    protected void addFields(Fields fields, Fields exclude, BiConsumer<String, String> consumer) {
        Iterator<Fields> fieldsIterator = fields.fieldsIterator();

        while (fieldsIterator.hasNext()) {
            Fields next = fieldsIterator.next();

            if (exclude.contains(next)) {
                continue;
            }

            String name = next.get(0).toString();

            Type type = next.getType(0);

            if (type instanceof WrappedCoercibleType) {
                type = ((WrappedCoercibleType<?>) type).wrappedCoercibleType();

                if (type instanceof Coercions.Coerce) {
                    type = ((Coercions.Coerce) type).getCanonicalType();
                }
            }

            if (type instanceof Class && Coercions.primitives.containsKey(type)) {
                type = Coercions.primitives.get(type);
            } else if (type instanceof CoercibleType) {
                type = type.getClass();
            }

            String typeName = typeMap.get(type);

            consumer.accept(name, typeName);
        }
    }
}
