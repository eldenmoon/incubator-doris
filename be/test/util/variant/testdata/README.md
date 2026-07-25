# Parquet Variant golden vectors

These fixtures are generated offline with the official Apache Parquet Java artifact
`org.apache.parquet:parquet-variant:1.17.0`.

The required jar SHA-256 is:

```text
daecf8161e7bba63f7ba9fd62c1e8a77730c9a9d76a335191dc9d0a0fcaaec52
```

`regenerate.sh` uses only that jar and the JDK. It does not invoke Maven or access the network. It
compiles `ParquetVariantGolden.java` into a temporary directory and removes all generated classes
on exit. The default mode is read-only: it regenerates both corpora in a temporary directory and
compares them byte for byte with the checked-in files.

```shell
./be/test/util/variant/testdata/regenerate.sh --check
./be/test/util/variant/testdata/regenerate.sh --extended
# Explicitly update checked-in fixtures, then review the diff:
./be/test/util/variant/testdata/regenerate.sh --update
```

Set `PARQUET_VARIANT_JAR` to use a non-default local path. The SHA-256 check remains mandatory.
No jar or class file belongs in this directory.

## Independent directions

`parquet_java_vectors.tsv` contains two explicitly separated provenances:

- `parquet-java-builder`: metadata and value bytes emitted by the official Java builders. Each
  declared typed logical input is read back through the official `Variant` typed accessors before
  the record is written.
- `spec-raw-java-validated`: compact, spec-legal encodings needed for non-minimal 3/4-byte width and
  non-monotonic object-offset coverage. These bytes are not described as builder output. They are
  admitted only after `ImmutableMetadata` and the official `Variant` typed accessors decode the
  declared value.

`doris_java_verified_vectors.tsv` is the reverse oracle. The helper independently creates sorted,
unique unsigned-UTF-8 metadata, passes it through official `ImmutableMetadata`, emits values with
official `VariantBuilder`, and verifies every declared logical input with official typed accessors.
`VariantGoldenVectorTest` freshly builds the same values with Doris `VariantBatchBuilder` and
requires exact metadata and value byte equality with these Java-decoded records. The normal C++
unit test therefore does not require Java while still proving that the current Doris bytes are
bytes actually decoded by parquet-java.

The corpora cover all 21 primitive IDs, short strings at 0 and 63 bytes, long strings at 64 bytes,
UTF-8 strings, objects, arrays, nested containers, unsorted metadata, 255/256-element boundaries,
metadata byte-length boundaries, value-offset boundaries, field-ID widths, and legal object values
whose physical offsets are not monotonic.

Real 3-to-4-byte transitions require more than 16 MiB of dictionary or value bytes. They are
verified in a temporary directory by `--extended` and deliberately are not checked in as large
fixtures. The compact committed width-4 records are spec-legal, non-minimal encodings decoded by
parquet-java. Doris's full 1/2/3/4-byte boundary matrix remains covered by
`VariantValueTest.MetadataWidthsBoundariesAndErrors`,
`VariantValueTest.OffsetWidthsAndElementCountBoundaries`, and the corresponding builder tests.

## Known parquet-java 1.17.0 implementation deviation

VariantEncoding v1 requires object fields to be ordered lexicographically by unsigned UTF-8 bytes.
parquet-java 1.17.0 instead implements `VariantBuilder.FieldEntry.compareTo` with Java
`String.compareTo`, which orders UTF-16 code units. Those orders differ for U+E000 and U+10000:

```text
sign(U+E000 String.compareTo U+10000)       = +1
sign(unsigned-UTF8(U+E000, U+10000))        = -1
metadata                                    = 0102000307ee8080f0908080
value                                       = 0202010000050a14020000001401000000
parquet-java field order                    = U+10000, U+E000
```

The official Java reader can look up both fields in its own output. The bytes are nevertheless
spec-invalid: a conforming reader may rely on unsigned-UTF8 field order, and Doris does so. The
generator reproduces and checks this evidence on every `--check`, `--update`, and `--extended` run,
but the counterexample is not placed in the positive corpus and no C++ test treats lookup failure as
desired behavior. Positive Unicode object vectors use pairs for which UTF-16 and unsigned-UTF8 order
agree.
