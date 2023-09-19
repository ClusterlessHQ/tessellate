# Operation Declaration Syntax

The pipeline declaration, in part, declares the required operation against the schema and tuples being processed.

A field can be dropped or renamed, or the field type can be changed. During processing, the field is physically dropped
from the processing stream (after being used upstream in a calculation), or the value in the field is coerced into a new
type.

Insertion of field values can be done statically (by injecting a literal value), or via a function that takes other
fields as input parameters (`concat(firstName, " ", lastName)`).

Tuples (records) can be filtered and dropped, or a tuple value can be used to generate a set of new tuples (one
for each value in a json list).

Since the tessellate pipeline declaration is simple json, we need to introduce a new syntax that can accommodate the
different types of operations introduced above.

```json
{
  "operations": [
    "op1",
    "op2",
    "etc"
  ]
}
```

# Operations

## Single line syntax

```bnf
operation = field-parameters expression operator field-results

field-paramters = field *( "+" field )

field = (field-name / field-ordinal) [ "|" type ] 

expression = 

``` 

## Transforms

- `=>` - literal assignment
  - `value => intoFieldname|asType`
- `+>` - retain after copy or evaluate
  - `fromFieldname +> toFieldname|asType`
  - `fromField1|type+fromField2|type !{java} +> toFieldname|asType`
  - `fromFieldname @[json pointer] +> toFieldname|asType`
  - `fromFieldname ~/regex matcher/ +> toFieldname|Boolean`
- `->` - discard after copy or evaluate
  - `fieldname ->`
  - `fromFieldname -> toFieldname|asType`
  - `fromField1|type+fromField2|type !{java} -> toFieldname|asType`
  - `fromFieldname @[json pointer] -> toFieldname|asType`
  - `fromFieldname ~/regex matcher/ -> toFieldname|Boolean`
- `[none]`- inplace coercion
  - `fieldname|asType`

## Filters

The expression will be evaluated and if returns `true`, the tuple will be retained, if `false`, the whole tuple is
discarded. Or the pattern will be applied and if a match is found, the tuple will be retained, if not matched, the whole
tuple is discarded.

- java expression -
  - `fromField1|type+fromField2|type !{java}`
- regular expression (regex) -
  - `fromField ~/regex/`
- pointer expression (json pointer) -
  - `fromField @[path]`
  - `fromField @[path] ~/regex/`

## Simple Transforms

- insert - insert a literal value into a field
  - `value=>intoField|type`
- coerce - transform a field to a new type
  - `field|newType`
- copy - copy a field value to a new field, optionally coercing its type
  - `fromField+>toField|type`
- rename - rename a field, optionally coercing its type
  - `fromField->toField|type`
- discard - remove a field
  - `field->`

## Expressions

- `!{java}` - `java` is a java expression
- `~/regex/` - `regex` is a regular expression
- `@[json pointer]` - [JSON Pointer](https://tools.ietf.org/html/draft-ietf-appsawg-json-pointer-03) having support for
  wildcards (`/*/`) and descent (`/**/`).
- `^intrinsic{param1:value1, param2:value2}` - `intrinsic` is a built-in function, with optional parameters

## Intrinsic Transforms

- any - `true` if any input is true, otherwise `false` or `null` if used as a predicate
  - `fromField1+fromField2+fromFieldN ^any{} +> intoField|Boolean`
- all - `true` if all inputs are true, otherwise `false` or `null` if used as a predicate
  - `fromField1+fromField2+fromFieldN ^all{} +> intoField|Boolean`
- tsid - create a unique long id
  - `^tsid{node:...,nodeCount:...,signed:true/false,epoch:...} +> intoField|type`
    - type = long, string
- siphash
  - `fromField1+fromField2+fromFieldN ^siphash{} +> intoField|type`

## Expression Transforms

- java expression -
  - `fromField1|type+fromField2|type !{java} +> toField` // add new derived field
  - `fromField1|type+fromField2|type !{java} -> toField` // add new derived field, discard arguments
- regular expression (regex) -
  - `fromField ~/regex\/ +> toField`
- pointer expression (json pointer) -
  - `fromField @[json pointer] +> toField`
  - `fromField @[json pointer] ~/regex/ +> toField`
