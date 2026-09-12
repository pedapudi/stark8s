// Package web holds the browser assets the coordinator serves.
//
// The editor is embedded rather than read from disk so that the binary is
// self-contained: an image needs no extra layer, and there is no file path to
// get wrong at run time.
package web

import _ "embed"

// Editor is the graph editor and viewer, a single HTML file with its style
// sheet and script inline. The coordinator serves it unchanged.
//
//go:embed editor.html
var Editor []byte
