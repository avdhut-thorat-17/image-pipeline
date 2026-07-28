// Package web embeds the static dashboard assets for the pipeline UI.
package web

import "embed"

//go:embed index.html style.css app.js
var Assets embed.FS
