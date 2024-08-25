package assets

import (
	"embed"
	"html/template"
	"io"
	"io/fs"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/cespare/xxhash/v2"
	"github.com/ipfs/boxo/path"
)

//go:embed *.html *.css
var assets embed.FS

// AssetHash a non-cryptographic hash of all embedded assets
var AssetHash string

var (
	DirectoryTemplate *template.Template
	DagTemplate       *template.Template
	ErrorTemplate     *template.Template
)

func init() {
	tmpls := [...]struct {
		result     **template.Template
		sourceFile string
	}{
		{&DirectoryTemplate, "directory.html"},
		{&DagTemplate, "dag.html"},
		{&ErrorTemplate, "error.html"},
	}

	var wg sync.WaitGroup
	wg.Add(len(tmpls))

	for _, tmpl := range tmpls {
		tmpl := tmpl
		go func() {
			defer wg.Done()
			var err error
			*tmpl.result, err = BuildTemplate(assets, tmpl.sourceFile)
			if err != nil {
				panic(err)
			}
		}()
	}

	initAssetsHash() // reuse the init thread instead of blocking on wg.Wait

	// @Jorropo: this is still waiting because I was too lazy to break the API of this public package.
	// It sounds better if we would use sync.Once and maybe start goroutines in init().
	wg.Wait()
}

func initAssetsHash() {
	sum := xxhash.New()
	err := fs.WalkDir(assets, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			return nil
		}

		file, err := assets.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		_, err = io.Copy(sum, file)
		return err
	})
	if err != nil {
		panic("error creating asset sum: " + err.Error())
	}

	AssetHash = strconv.FormatUint(sum.Sum64(), 32)
}

type MenuItem struct {
	URL   string
	Title string
}

type GlobalData struct {
	Menu       []MenuItem
	GatewayURL string
	DNSLink    bool
}

type DagTemplateData struct {
	GlobalData
	Path      string
	CID       string
	CodecName string
	CodecHex  string
	Node      *ParsedNode
}

type ErrorTemplateData struct {
	GlobalData
	StatusCode int
	StatusText string
	Error      string
}

type DirectoryTemplateData struct {
	GlobalData
	Listing     []DirectoryItem
	Size        string
	Path        string
	Breadcrumbs []Breadcrumb
	BackLink    string
	Hash        string
}

type DirectoryItem struct {
	Size      string
	Name      string
	Path      string
	Hash      string
	ShortHash string
}

type Breadcrumb struct {
	Name string
	Path string
}

func Breadcrumbs(urlPath string, dnslinkOrigin bool) []Breadcrumb {
	var ret []Breadcrumb

	p, err := path.NewPath(urlPath)
	if err != nil {
		// No assets.Breadcrumbs, fallback to bare Path in template
		return ret
	}
	segs := p.Segments()
	contentRoot := segs[1]
	for i, seg := range segs {
		if i == 0 {
			ret = append(ret, Breadcrumb{Name: seg})
		} else {
			ret = append(ret, Breadcrumb{
				Name: seg,
				Path: "/" + strings.Join(segs[0:i+1], "/"),
			})
		}
	}

	// Drop the /ipns/<fqdn> prefix from assets.Breadcrumb Paths when directory
	// listing on a DNSLink website (loaded due to Host header in HTTP
	// request).  Necessary because the hostname most likely won't have a
	// public gateway mounted.
	if dnslinkOrigin {
		prefix := "/ipns/" + contentRoot
		for i, crumb := range ret {
			if strings.HasPrefix(crumb.Path, prefix) {
				ret[i].Path = strings.Replace(crumb.Path, prefix, "", 1)
			}
		}
		// Make contentRoot assets.Breadcrumb link to the website root
		ret[1].Path = "/"
	}

	return ret
}

func ShortHash(hash string) string {
	if len(hash) <= 8 {
		return hash
	}
	return (hash[0:4] + "\u2026" + hash[len(hash)-4:])
}

// helper to detect DNSLink website context
// (when hostname from gwURL is matching /ipns/<fqdn> in path)
func HasDNSLinkOrigin(gwURL string, path string) bool {
	if gwURL != "" {
		fqdn := stripPort(strings.TrimPrefix(gwURL, "//"))
		return strings.HasPrefix(path, "/ipns/"+fqdn)
	}
	return false
}

func stripPort(hostname string) string {
	host, _, err := net.SplitHostPort(hostname)
	if err == nil {
		return host
	}
	return hostname
}
