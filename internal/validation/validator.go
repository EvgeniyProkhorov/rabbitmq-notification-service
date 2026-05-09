package validation

import (
	"reflect"
	"strings"

	"github.com/go-playground/validator/v10"
)

// NewValidator создаёт validator с поддержкой json, query и path тегов для имён полей.
func NewValidator() *validator.Validate {
	validate := validator.New()

	validate.RegisterTagNameFunc(func(field reflect.StructField) string {
		if name := tagName(field, "json"); name != "" {
			return name
		}

		if name := tagName(field, "query"); name != "" {
			return name
		}

		if name := tagName(field, "path"); name != "" {
			return name
		}

		return field.Name
	})

	return validate
}

func tagName(field reflect.StructField, tag string) string {
	value := field.Tag.Get(tag)
	if value == "" {
		return ""
	}

	name := strings.Split(value, ",")[0]
	if name == "-" {
		return ""
	}

	return name
}
