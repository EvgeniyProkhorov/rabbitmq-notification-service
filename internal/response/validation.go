package response

import "github.com/go-playground/validator/v10"

// ValidationFields преобразует ошибки validator в map field -> validation rule.
func ValidationFields(err error) map[string]string {
	fields := make(map[string]string)

	validationErrors, ok := err.(validator.ValidationErrors)

	if !ok {
		return fields
	}

	for _, fieldErr := range validationErrors {
		fields[fieldErr.Field()] = fieldErr.Tag()
	}

	return fields

}
