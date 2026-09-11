// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.analysis;

import com.google.common.base.Strings;

import java.util.HashMap;
import java.util.Map;

public class InvertedIndexProperties {

    public static String INVERTED_INDEX_PARSER_KEY = "parser";
    public static String INVERTED_INDEX_PARSER_KEY_ALIAS = "built_in_analyzer";
    public static String INVERTED_INDEX_PARSER_UNKNOWN = "unknown";
    public static String INVERTED_INDEX_PARSER_NONE = "none";
    public static String INVERTED_INDEX_PARSER_STANDARD = "standard";
    public static String INVERTED_INDEX_PARSER_UNICODE = "unicode";
    public static String INVERTED_INDEX_PARSER_ENGLISH = "english";
    public static String INVERTED_INDEX_PARSER_CHINESE = "chinese";
    public static String INVERTED_INDEX_PARSER_ICU = "icu";
    public static String INVERTED_INDEX_PARSER_BASIC = "basic";
    public static String INVERTED_INDEX_PARSER_IK = "ik";

    public static String INVERTED_INDEX_PARSER_MODE_KEY = "parser_mode";
    public static String INVERTED_INDEX_PARSER_FINE_GRANULARITY = "fine_grained";
    public static String INVERTED_INDEX_PARSER_COARSE_GRANULARITY = "coarse_grained";
    public static String INVERTED_INDEX_PARSER_SMART = "ik_smart";

    public static String INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE = "char_filter_type";
    public static String INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN = "char_filter_pattern";
    public static String INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT = "char_filter_replacement";

    public static String INVERTED_INDEX_CHAR_FILTER_CHAR_REPLACE = "char_replace";

    public static String INVERTED_INDEX_SUPPORT_PHRASE_KEY = "support_phrase";

    public static String INVERTED_INDEX_PARSER_IGNORE_ABOVE_KEY = "ignore_above";

    public static String INVERTED_INDEX_PARSER_LOWERCASE_KEY = "lower_case";

    public static String INVERTED_INDEX_PARSER_STOPWORDS_KEY = "stopwords";

    public static String INVERTED_INDEX_DICT_COMPRESSION_KEY = "dict_compression";

    public static String INVERTED_INDEX_ANALYZER_NAME_KEY = "analyzer";
    public static String INVERTED_INDEX_NORMALIZER_NAME_KEY = "normalizer";

    public static String INVERTED_INDEX_PARSER_FIELD_PATTERN_KEY = "field_pattern";

    // VARIANT value-first index. `variant_index_scope` selects which terms are written:
    //   paths        -> term(value, path): exact per-path predicates, whole-root / sub-document
    //                   predicates through a dictionary run (default);
    //   values       -> term(value): whole-root equality / MATCH as one exact term lookup;
    //   paths,values -> both.
    // `variant_index_mode` (root / all_values) is the pre-scope spelling and is normalized to
    // paths / values. The format version is stamped by the FE, never user-supplied; a BE that does
    // not know the version treats the index as unusable instead of misreading it.
    public static final String VARIANT_INDEX_SCOPE_KEY = "variant_index_scope";
    public static final String VARIANT_INDEX_SCOPE_PATHS = "paths";
    public static final String VARIANT_INDEX_SCOPE_VALUES = "values";
    public static final String VARIANT_INDEX_SCOPE_PATHS_AND_VALUES = "paths,values";
    public static final String VARIANT_INDEX_EXCLUDE_PATHS_KEY = "variant_index_exclude_paths";
    public static final String VARIANT_INDEX_MODE_KEY = "variant_index_mode";
    public static final String VARIANT_INDEX_MODE_ROOT = "root";
    public static final String VARIANT_INDEX_MODE_ALL_VALUES = "all_values";
    public static final String VARIANT_ROOT_FORMAT_VERSION_KEY = "variant_root_format_version";
    public static final String VARIANT_ROOT_FORMAT_VERSION_V1 = "1";
    public static final String VARIANT_ROOT_FORMAT_VERSION_V2 = "2";
    // Value-first layout [tag][value][sep][path] with typed, order-preserving values.
    public static final String VARIANT_ROOT_FORMAT_VERSION_V3 = "3";

    public static boolean isVariantRootIndexMode(String mode) {
        return VARIANT_INDEX_MODE_ROOT.equals(mode) || VARIANT_INDEX_MODE_ALL_VALUES.equals(mode);
    }

    /** Canonical scope spelling for a scope or legacy mode value, or null when it is neither. */
    public static String normalizeVariantIndexScope(String scopeOrMode) {
        if (scopeOrMode == null) {
            return null;
        }
        String value = scopeOrMode.trim().toLowerCase();
        if (VARIANT_INDEX_MODE_ROOT.equals(value)) {
            return VARIANT_INDEX_SCOPE_PATHS;
        }
        if (VARIANT_INDEX_MODE_ALL_VALUES.equals(value)) {
            return VARIANT_INDEX_SCOPE_VALUES;
        }
        boolean paths = false;
        boolean values = false;
        for (String part : value.split(",")) {
            String item = part.trim();
            if (VARIANT_INDEX_SCOPE_PATHS.equals(item)) {
                paths = true;
            } else if (VARIANT_INDEX_SCOPE_VALUES.equals(item)) {
                values = true;
            } else {
                return null;
            }
        }
        if (paths && values) {
            return VARIANT_INDEX_SCOPE_PATHS_AND_VALUES;
        }
        if (paths) {
            return VARIANT_INDEX_SCOPE_PATHS;
        }
        if (values) {
            return VARIANT_INDEX_SCOPE_VALUES;
        }
        return null;
    }

    /** The canonical scope of a VARIANT index, or null when the properties describe no such index. */
    public static String getVariantIndexScope(Map<String, String> properties) {
        if (properties == null) {
            return null;
        }
        String scope = normalizeVariantIndexScope(properties.get(VARIANT_INDEX_SCOPE_KEY));
        if (scope != null) {
            return scope;
        }
        return normalizeVariantIndexScope(properties.get(VARIANT_INDEX_MODE_KEY));
    }

    public static boolean isVariantRootIndex(Map<String, String> properties) {
        return getVariantIndexScope(properties) != null;
    }

    public static boolean variantIndexScopeHasValues(Map<String, String> properties) {
        String scope = getVariantIndexScope(properties);
        return scope != null && scope.contains(VARIANT_INDEX_SCOPE_VALUES);
    }

    public static boolean variantIndexScopeHasPaths(Map<String, String> properties) {
        String scope = getVariantIndexScope(properties);
        return scope != null && scope.contains(VARIANT_INDEX_SCOPE_PATHS);
    }

    // Default analyzer key constant - matches BE's INVERTED_INDEX_DEFAULT_ANALYZER_KEY
    public static final String INVERTED_INDEX_DEFAULT_ANALYZER_KEY = "__default__";

    public static String getInvertedIndexParser(Map<String, String> properties) {
        if (properties == null) {
            return INVERTED_INDEX_PARSER_NONE;
        }
        String parser = properties.get(INVERTED_INDEX_PARSER_KEY);
        if (parser == null) {
            parser = properties.get(INVERTED_INDEX_PARSER_KEY_ALIAS);
        }
        return parser != null ? parser : INVERTED_INDEX_PARSER_NONE;
    }

    public static String getInvertedIndexParserMode(Map<String, String> properties) {
        if (properties == null) {
            return INVERTED_INDEX_PARSER_COARSE_GRANULARITY;
        }
        String mode = properties.get(INVERTED_INDEX_PARSER_MODE_KEY);
        String parser = properties.get(INVERTED_INDEX_PARSER_KEY);
        if (parser == null) {
            parser = properties.get(INVERTED_INDEX_PARSER_KEY_ALIAS);
        }
        return mode != null ? mode :
            INVERTED_INDEX_PARSER_IK.equals(parser) ? INVERTED_INDEX_PARSER_SMART :
                INVERTED_INDEX_PARSER_COARSE_GRANULARITY;
    }

    public static String getInvertedIndexFieldPattern(Map<String, String> properties) {
        String fieldPattern = properties == null ? null : properties.get(INVERTED_INDEX_PARSER_FIELD_PATTERN_KEY);
        // default is "none" if not set
        return fieldPattern != null ? fieldPattern : "";
    }

    public static String getPreferredAnalyzer(Map<String, String> properties) {
        if (properties == null || properties.isEmpty()) {
            return "";
        }
        // Check analyzer first, then normalizer
        String analyzer = properties.get(INVERTED_INDEX_ANALYZER_NAME_KEY);
        if (analyzer != null && !analyzer.isEmpty()) {
            return analyzer;
        }
        String normalizer = properties.get(INVERTED_INDEX_NORMALIZER_NAME_KEY);
        return normalizer != null ? normalizer : "";
    }

    public static boolean isAnalyzed(Map<String, String> properties) {
        return !Strings.isNullOrEmpty(getPreferredAnalyzer(properties))
                || !INVERTED_INDEX_PARSER_NONE.equalsIgnoreCase(getInvertedIndexParser(properties));
    }

    public static Map<String, String> getInvertedIndexCharFilter(Map<String, String> properties) {
        if (properties == null) {
            return new HashMap<>();
        }

        if (!properties.containsKey(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE)) {
            return new HashMap<>();
        }
        String type = properties.get(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE);

        Map<String, String> charFilterMap = new HashMap<>();
        if (type.equals(INVERTED_INDEX_CHAR_FILTER_CHAR_REPLACE)) {
            // type
            charFilterMap.put(INVERTED_INDEX_PARSER_CHAR_FILTER_TYPE, INVERTED_INDEX_CHAR_FILTER_CHAR_REPLACE);

            // pattern
            if (!properties.containsKey(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN)) {
                return new HashMap<>();
            }
            String pattern = properties.get(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN);
            charFilterMap.put(INVERTED_INDEX_PARSER_CHAR_FILTER_PATTERN, pattern);

            // placement
            String replacement = " ";
            if (properties.containsKey(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT)) {
                replacement = properties.get(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT);
            }
            charFilterMap.put(INVERTED_INDEX_PARSER_CHAR_FILTER_REPLACEMENT, replacement);
        } else {
            return new HashMap<>();
        }

        return charFilterMap;
    }

    public static boolean getInvertedIndexParserLowercase(Map<String, String> properties) {
        String lowercase = properties == null ? null : properties.get(INVERTED_INDEX_PARSER_LOWERCASE_KEY);
        // default is true if not set
        return lowercase == null || Boolean.parseBoolean(lowercase);
    }

    public static String getInvertedIndexParserStopwords(Map<String, String> properties) {
        String stopwrods = properties == null ? null : properties.get(INVERTED_INDEX_PARSER_STOPWORDS_KEY);
        // default is "" if not set
        return stopwrods != null ? stopwrods : "";
    }

    /**
     * Builds the SQL fragment for USING ANALYZER clause.
     * Returns empty string if analyzer is null or empty.
     * Otherwise returns " USING ANALYZER <analyzer>" with proper quoting.
     */
    public static String buildAnalyzerSqlFragment(String analyzer) {
        if (Strings.isNullOrEmpty(analyzer)) {
            return "";
        }
        String trimmed = analyzer.trim();
        if (trimmed.isEmpty()) {
            return "";
        }
        if (trimmed.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            return " USING ANALYZER " + trimmed;
        }
        return " USING ANALYZER '" + trimmed.replace("'", "''") + "'";
    }
}
