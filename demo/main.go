package main

import (
	"fmt"
	"strings"
	"unicode"
)

func main() {
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	// contents := `Hello World! This is a test.`
	contents := `adfad123
	`
	// split contents into an array of words.
	words := strings.FieldsFunc(contents, ff)
	for _, word := range words {
		fmt.Println(word)
	}
}
