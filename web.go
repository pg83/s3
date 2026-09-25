package main

import (
	"encoding/json"
	"errors"
	"html/template"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
)

type Web struct {
	store *Store
}

var pageTmpl = template.Must(template.New("page").Parse(`<!doctype html>
<meta charset="utf-8">
<title>s3</title>
<style>
body { font: 14px/1.4 sans-serif; margin: 24px; color: #222; }
table { border-collapse: collapse; }
td, th { padding: 4px 12px 4px 0; text-align: left; border-bottom: 1px solid #eee; }
.mono { font-family: monospace; }
.two { color: #b00; }
</style>
<h1>{{.Title}}</h1>
{{if .Buckets}}<table><tr><th>bucket</th><th>created</th></tr>
{{range .Buckets}}<tr><td><a href="/b/{{.Name}}">{{.Name}}</a></td><td>{{.Created}}</td></tr>{{end}}</table>{{end}}
{{if .Bucket}}<form method="get"><input name="prefix" value="{{.Prefix}}" placeholder="prefix"> <button>list</button></form>
<table><tr><th>key</th><th>size</th><th>modified</th><th>md5</th><th>pieces</th></tr>
{{range .Objects}}<tr><td><a href="/o/{{$.Bucket}}/{{.Key}}">{{.Key}}</a></td><td>{{.Size}}</td><td>{{.Mtime}}</td><td class="mono">{{.Md5}}</td><td{{if lt .Pieces 3}} class="two"{{end}}>{{.Pieces}}</td></tr>{{end}}
</table>{{if .More}}<p>first {{len .Objects}} shown, narrow the prefix</p>{{end}}{{end}}
`))

type WebBucket struct {
	Name    string
	Created string
}

type WebObject struct {
	Key    string
	Size   int64
	Mtime  string
	Md5    string
	Pieces int
}

type WebPage struct {
	Title   string
	Buckets []WebBucket
	Bucket  string
	Prefix  string
	Objects []WebObject
	More    bool
}

func runWeb(cfg *Config, listen string) {
	if listen == "" {
		throwFmt("web: -listen is required")
	}

	w := &Web{store: newStore(cfg)}

	slog.Info("web: serving", "listen", listen)

	throw(http.ListenAndServe(listen, w))
}

func (wb *Web) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	try(func() {
		wb.route(w, r)
	}).catch(func(exc *Exception) {
		slog.Error("web", "path", r.URL.Path, "err", exc.error())
		http.Error(w, exc.error(), http.StatusInternalServerError)
	})
}

func (wb *Web) route(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/":
		page := WebPage{Title: "buckets"}

		found, _ := wb.store.etcd.scan("bkt/", "", listLimit)

		for _, entry := range found {
			page.Buckets = append(page.Buckets, WebBucket{Name: strings.TrimPrefix(entry.key, "bkt/"), Created: string(entry.value)})
		}

		throw(pageTmpl.Execute(w, page))
	case strings.HasPrefix(r.URL.Path, "/b/"):
		bucket := strings.TrimPrefix(r.URL.Path, "/b/")
		prefix := r.URL.Query().Get("prefix")
		page := WebPage{Title: bucket, Bucket: bucket, Prefix: prefix}

		found, more := wb.store.etcd.scan("obj/"+bucket+"/"+prefix, "", listLimit)
		page.More = more

		for _, entry := range found {
			m := Manifest{}

			throw(json.Unmarshal(entry.value, &m))
			page.Objects = append(page.Objects, WebObject{
				Key:    strings.TrimPrefix(entry.key, "obj/"+bucket+"/"),
				Size:   m.Size,
				Mtime:  m.Mtime.UTC().Format(s3Time),
				Md5:    m.Md5,
				Pieces: len(m.Pieces),
			})
		}

		throw(pageTmpl.Execute(w, page))
	case strings.HasPrefix(r.URL.Path, "/o/"):
		bucket, key, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/o/"), "/")
		m, _, err := wb.store.manifest(bucket, key)

		if errors.Is(err, errNoSuchKey) {
			http.NotFound(w, r)

			return
		}

		throw(err)

		data := throw2(wb.store.get(bucket, key, m))

		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.Write(data)
	default:
		http.NotFound(w, r)
	}
}
