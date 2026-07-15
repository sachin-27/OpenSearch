/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.fields;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.FieldNamesFieldMapper;
import org.opensearch.index.mapper.IndexFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NestedPathFieldMapper;
import org.opensearch.index.mapper.ObjectMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.SourceFieldMapper;
import org.opensearch.parquet.fields.core.data.number.LongParquetField;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * Builds Apache Arrow schemas from OpenSearch MapperService field mappings.
 *
 * <p>Fields under a {@code nested} mapping are folded into {@code LIST<STRUCT<...>>}
 * columns — one list column per top-level nested path, with deeper nested paths becoming
 * sub-lists inside the parent's struct (Dremel repeated-group encoding). Example: a
 * mapping with {@code comments} (nested, fields author/score) containing
 * {@code comments.replies} (nested, field user) yields
 * <pre>
 * comments: LIST&lt;STRUCT&lt;author, score, replies: LIST&lt;STRUCT&lt;user&gt;&gt;&gt;&gt;
 * </pre>
 * Struct children are named relative to their owning nested path ({@code comments.author}
 * → {@code author}); intermediate non-nested objects keep their dotted suffix
 * ({@code comments.meta.score} → {@code meta.score}).
 */
public final class ArrowSchemaBuilder {

    private static final Logger logger = LogManager.getLogger(ArrowSchemaBuilder.class);

    /** Name of the synthetic element field inside every LIST — the struct holding one child. */
    static final String LIST_ELEMENT_NAME = "element";

    private ArrowSchemaBuilder() {}

    /**
     * Creates an Arrow Schema from the MapperService.
     * @param mapperService the mapper service containing field mappings
     * TODO - Get the mapping version while creating the schema
     */
    public static Schema getSchema(MapperService mapperService) {
        Objects.requireNonNull(mapperService, "MapperService cannot be null");
        DocumentMapper documentMapper = mapperService.documentMapperWithAutoCreate().getDocumentMapper();
        return buildSchema(documentMapper);
    }

    /**
     * Core schema construction from a document mapper.
     *
     * @param documentMapper the document mapper, or null for a metadata-only schema
     * @return the Arrow schema
     */
    static Schema buildSchema(DocumentMapper documentMapper) {
        if (documentMapper == null) {
            return buildSchema(null, Map.of(), null);
        }
        return buildSchema(documentMapper.mappers(), documentMapper.objectMappers(), documentMapper);
    }

    /**
     * Testable core: builds the schema from the flat field-mapper iteration and the
     * object-mapper map. Package-private for unit testing without a {@link DocumentMapper}
     * (whose {@code MappingLookup} is final and unmockable).
     *
     * @param mappers all field mappers, or null for none
     * @param objectMappers object mappers by full path (nested paths are discovered here)
     * @param documentMapper optional, used only for multi-field/normalizer handling
     */
    static Schema buildSchema(Iterable<Mapper> mappers, Map<String, ObjectMapper> objectMappers, DocumentMapper documentMapper) {
        List<Field> fields = new ArrayList<>();
        if (mappers != null) {
            // Nested paths sorted so parents precede their children deterministically.
            Set<String> nestedPaths = nestedPaths(objectMappers);
            // Leaf fields grouped by the nested path that owns them, named relative to it.
            Map<String, List<Field>> leavesByNestedPath = new LinkedHashMap<>();

            for (Mapper mapper : mappers) {
                if (isUnsupportedMetadataField(mapper)) {
                    logger.debug("Skipping unsupported metadata field: [{}] of type [{}]", mapper.name(), mapper.typeName());
                    continue;
                }

                ParquetField parquetField = ArrowFieldRegistry.getParquetField(mapper.typeName());
                if (parquetField == null) {
                    logger.debug("No ParquetField registered for field: [{}] of type [{}]", mapper.name(), mapper.typeName());
                    continue;
                }

                String owningPath = owningNestedPath(mapper.name(), nestedPaths);
                if (owningPath == null) {
                    fields.add(new Field(mapper.name(), parquetField.getFieldType(), null));
                    if (documentMapper != null) {
                        handleNormalizedField(mapper, documentMapper, fields, parquetField);
                    }
                } else {
                    String relativeName = mapper.name().substring(owningPath.length() + 1);
                    leavesByNestedPath.computeIfAbsent(owningPath, k -> new ArrayList<>())
                        .add(new Field(relativeName, parquetField.getFieldType(), null));
                }
            }

            // One LIST<STRUCT> column per top-level nested path; deeper paths nest inside.
            for (String path : nestedPaths) {
                if (parentNestedPath(path, nestedPaths) == null) {
                    Field listField = buildListField(path, path, nestedPaths, leavesByNestedPath);
                    if (listField != null) {
                        fields.add(listField);
                    }
                }
            }
        }
        // Add row ID field (long)
        LongParquetField longField = new LongParquetField(false);
        fields.add(new Field(DocumentInput.ROW_ID_FIELD, longField.getFieldType(), null));
        fields.add(new Field(SeqNoFieldMapper.PRIMARY_TERM_NAME, new LongParquetField(false).getFieldType(), null));
        return new Schema(fields);
    }

    /**
     * Recursively builds the {@code LIST<STRUCT<...>>} field for one nested path: the
     * struct's children are the path's own leaf fields plus one sub-list per directly
     * nested child path. Returns null (and logs) when the struct would be empty — a
     * nested mapping with no Parquet-supported fields has nothing to materialize.
     */
    private static Field buildListField(String path, String fieldName, Set<String> nestedPaths, Map<String, List<Field>> leaves) {
        List<Field> structChildren = new ArrayList<>(leaves.getOrDefault(path, List.of()));
        for (String candidate : nestedPaths) {
            if (path.equals(parentNestedPath(candidate, nestedPaths))) {
                Field childList = buildListField(candidate, candidate.substring(path.length() + 1), nestedPaths, leaves);
                if (childList != null) {
                    structChildren.add(childList);
                }
            }
        }
        if (structChildren.isEmpty()) {
            logger.debug("Nested path [{}] has no Parquet-supported fields; omitting its LIST column", path);
            return null;
        }
        Field element = new Field(LIST_ELEMENT_NAME, FieldType.nullable(ArrowType.Struct.INSTANCE), structChildren);
        return new Field(fieldName, FieldType.nullable(new ArrowType.List()), List.of(element));
    }

    /** Returns all nested object-mapper paths, sorted so parents precede children. */
    private static Set<String> nestedPaths(Map<String, ObjectMapper> objectMappers) {
        Set<String> nestedPaths = new TreeSet<>();
        for (Map.Entry<String, ObjectMapper> entry : objectMappers.entrySet()) {
            if (entry.getValue().nested().isNested()) {
                nestedPaths.add(entry.getKey());
            }
        }
        return nestedPaths;
    }

    /**
     * Returns the deepest nested path that owns the given field (the longest nested path
     * that is a proper dotted prefix of the field name), or null for root-owned fields.
     */
    private static String owningNestedPath(String fieldName, Set<String> nestedPaths) {
        String owner = null;
        for (String path : nestedPaths) {
            if (fieldName.startsWith(path + ".") && (owner == null || path.length() > owner.length())) {
                owner = path;
            }
        }
        return owner;
    }

    /** Returns the deepest nested path that encloses the given nested path, or null for top-level paths. */
    private static String parentNestedPath(String nestedPath, Set<String> nestedPaths) {
        return owningNestedPath(nestedPath, nestedPaths);
    }

    private static void handleNormalizedField(Mapper mapper, DocumentMapper documentMapper, List<Field> fields, ParquetField parquetField) {
        if (mapper instanceof KeywordFieldMapper keywordFieldMapper) {
            if (!documentMapper.mappers().isMultiField(mapper.name()) && keywordFieldMapper.getRawValueFieldType() != null) {
                KeywordFieldMapper.KeywordFieldType rawValueField = keywordFieldMapper.getRawValueFieldType();
                fields.add(new Field(rawValueField.name(), parquetField.getFieldType(), null));
            }
        }
    }

    private static boolean isUnsupportedMetadataField(Mapper mapper) {
        return mapper instanceof SourceFieldMapper
            || mapper instanceof FieldNamesFieldMapper
            || mapper instanceof IndexFieldMapper
            || mapper instanceof NestedPathFieldMapper
            || Objects.equals(mapper.typeName(), "_feature")
            || Objects.equals(mapper.typeName(), "_data_stream_timestamp");
    }
}
