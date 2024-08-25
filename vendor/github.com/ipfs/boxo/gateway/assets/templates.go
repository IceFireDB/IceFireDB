package assets

import (
	"bytes"
	"html/template"
	"io"
	"io/fs"
	"net/url"
	"path"
)

// custom template-escaping function to escape a full path, including '#' and '?'
func urlEscape(rawUrl string) string {
	pathURL := url.URL{Path: rawUrl}
	return pathURL.String()
}

// iconFromExt is a helper to guess the icon for a filename according to its extension.
func iconFromExt(filename string) string {
	ext := path.Ext(filename)
	if _, ok := KnownIcons[ext]; ok {
		return "ipfs-" + ext[1:] // Remove first dot.
	}
	return "ipfs-_blank" // Default is blank icon.
}

// args is a helper function to allow sending more than one object to a template.
func args(args ...interface{}) []interface{} {
	return args
}

var funcMap = template.FuncMap{
	"iconFromExt": iconFromExt,
	"urlEscape":   urlEscape,
	"args":        args,
}

func readFile(fs fs.FS, filename string) ([]byte, error) {
	f, err := fs.Open(filename)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(f)
}

func loadStyles(fs fs.FS) ([]byte, error) {
	iconsBytes, err := readFile(fs, "icons.css")
	if err != nil {
		return nil, err
	}

	stylesBytes, err := readFile(fs, "style.css")
	if err != nil {
		return nil, err
	}

	css := []byte("<style>")
	css = append(css, iconsBytes...)
	css = append(css, stylesBytes...)
	css = append(css, []byte("</style>")...)
	css = bytes.ReplaceAll(css, []byte("\t"), []byte{})
	css = bytes.ReplaceAll(css, []byte("\n"), []byte{})
	css = bytes.ReplaceAll(css, []byte("\r"), []byte{})
	return css, nil
}

func BuildTemplate(fs fs.FS, filename string) (*template.Template, error) {
	css, err := loadStyles(fs)
	if err != nil {
		return nil, err
	}

	pageHeader, err := readFile(fs, "header.html")
	if err != nil {
		return nil, err
	}

	templateBytes, err := readFile(fs, filename)
	if err != nil {
		return nil, err
	}

	templateBytes = bytes.Replace(templateBytes, []byte("<link rel=\"stylesheet\">"), css, 1)
	templateBytes = bytes.Replace(templateBytes, []byte("<header></header>"), pageHeader, 1)

	return template.New(filename).Funcs(funcMap).Parse(string(templateBytes))
}
