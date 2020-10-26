// Copyright [2019] LinkedIn Corp. Licensed under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with the
// License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

package goavro

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// pcfProcessor is a function type that given a parsed JSON object, returns its
// Parsing Canonical Form according to the Avro specification.
type pcfProcessor func(s interface{}) (string, error)

type parsingContext struct {
	namespace   string
	isFieldName bool
	isType      bool
	typeLookup map[string]string
}

// Returns explicit copy of parent
func (env parsingContext) copy() parsingContext {
	return env
}

func parsingCanonicalForm(schema interface{}) (string, error) {
	return parsingContext{typeLookup: make(map[string]string)}.parsingCanonicalForm(schema)
}

// parsingCanonialForm returns the "Parsing Canonical Form" (pcf) for a parsed
// JSON structure of a valid Avro schema, or an error describing the schema
// error.
func (env parsingContext) parsingCanonicalForm(schema interface{}) (string, error) {
	switch val := schema.(type) {
	case map[string]interface{}:
		// JSON objects are decoded as a map of strings to empty interfaces
		return env.pcfObject(val)
	case []interface{}:
		// JSON arrays are decoded as a slice of empty interfaces
		return env.pcfArray(val)
	case string:
		// JSON string values are decoded as a Go string
		return env.pcfString(val)
	case float64:
		// JSON numerical values are decoded as Go float64
		return env.pcfNumber(val)
	default:
		return "", fmt.Errorf("cannot parse schema with invalid schema type; ought to be map[string]interface{}, []interface{}, string, or float64; received: %T: %v", schema, schema)
	}
}

// pcfNumber returns the parsing canonical form for a numerical value.
func (env parsingContext) pcfNumber(val float64) (string, error) {
	return strconv.FormatFloat(val, 'g', -1, 64), nil
}

func startsWithUpper(val string) bool {
	return val[0:1] == strings.ToUpper(val[0:1])	
}

func (env parsingContext) hasNamespace() bool {
	return env.namespace != ""
}

// pcfString returns the parsing canonical form for a string value.
func (env parsingContext) pcfString(val string) (string, error) {
	if env.isType && startsWithUpper(val) && env.hasNamespace()  {
		val = env.namespace + "." + val
	}
	return `"` + val + `"`, nil
}

// pcfArray returns the parsing canonical form for a JSON array.
func (env parsingContext) pcfArray(val []interface{}) (string, error) {
	items := make([]string, len(val))
	for i, el := range val {
		p, err := env.parsingCanonicalForm(el)
		if err != nil {
			return "", err
		}
		items[i] = p
	}
	return "[" + strings.Join(items, ",") + "]", nil
}

// pcfObject returns the parsing canonical form for a JSON object.
func (env parsingContext) pcfObject(jsonMap map[string]interface{}) (string, error) {
	pairs := make(stringPairs, 0, len(jsonMap))

	if namespaceJSON, ok := jsonMap["namespace"]; ok {
		if namespaceStr, ok := namespaceJSON.(string); ok {
			// and it's value is string (otherwise invalid schema)
			env.namespace = namespaceStr
		}
	}

	for k, v := range jsonMap {
		// Reduce primitive schemas to their simple form.
		if len(jsonMap) == 1 && k == "type" {
			if t, ok := v.(string); ok {
				return "\"" + t + "\"", nil
			}
		}

		// Only keep relevant attributes (strip 'doc', 'alias', 'namespace')
		if _, ok := fieldOrder[k]; !ok {
			continue
		}

		// Add namespace to a non-qualified name.
		if k == "name" && env.hasNamespace() && !env.isFieldName {
			// Check if the name isn't already qualified.
			if t, ok := v.(string); ok && !strings.ContainsRune(t, '.') {
				v = env.namespace + "." + t
			}
		}

		// Only fixed type allows size, and we must convert a string size to a
		// float.
		if k == "size" {
			if s, ok := v.(string); ok {
				s, err := strconv.ParseUint(s, 10, 0)
				if err != nil {
					// should never get here because already validated schema
					return "", fmt.Errorf("Fixed size ought to be number greater than zero: %v", s)
				}
				v = float64(s)
			}
		}

		pk, err := env.parsingCanonicalForm(k)
		if err != nil {
			return "", err
		}
		childParsingContext := env.copy()
		childParsingContext.isFieldName = k == "fields"
		childParsingContext.isType = k == "type" || k == "items" || k == "values"
		pv, err := childParsingContext.parsingCanonicalForm(v)
		if err != nil {
			return "", err
		}

		objectType, ok := jsonMap["type"].(string)
		if env.isType && k == "name" && ok && objectType != "record" && objectType != "enum" {
			continue
		}
		pairs = append(pairs, stringPair{k, pk + ":" + pv})
	}

	// Sort keys by their order in specification.
	sort.Sort(byAvroFieldOrder(pairs))
	return "{" + strings.Join(pairs.Bs(), ",") + "}", nil
}

// stringPair represents a pair of string values.
type stringPair struct {
	A string
	B string
}

// stringPairs is a sortable slice of pairs of strings.
type stringPairs []stringPair

// Bs returns an array of second values of an array of pairs.
func (sp *stringPairs) Bs() []string {
	items := make([]string, len(*sp))
	for i, el := range *sp {
		items[i] = el.B
	}
	return items
}

// fieldOrder defines fields that show up in canonical schema and specifies
// their precedence.
var fieldOrder = map[string]int{
	"name":    1,
	"type":    2,
	"fields":  3,
	"symbols": 4,
	"items":   5,
	"values":  6,
	"size":    7,
}

// byAvroFieldOrder is equipped with a sort order of fields according to the
// specification.
type byAvroFieldOrder []stringPair

func (s byAvroFieldOrder) Len() int {
	return len(s)
}

func (s byAvroFieldOrder) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s byAvroFieldOrder) Less(i, j int) bool {
	return fieldOrder[s[i].A] < fieldOrder[s[j].A]
}
