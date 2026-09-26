package main

import (
	"errors"
	"fmt"
	"html/template"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

var pageFuncs = template.FuncMap{"clock": clockOf, "stamp": stampOf, "size": sizeOf}

var pageTmpl = template.Must(template.New("page").Funcs(pageFuncs).Parse(`<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="color-scheme" content="light dark">
<title>s3 {{if .Bucket}}{{.Bucket}}{{else}}buckets{{end}}</title>
<style>
:root{font-family:system-ui,-apple-system,"Segoe UI",sans-serif;--bg:#f9f9f7;--rail:#f0f0ed;--text:#0b0b0b;--muted:#898781;--dim:#a4a29b;--line:#e1e0d9;--accent:#2a78d6;--rule:#d9d8d1;--dirrow:rgba(42,120,214,.035);--hover:rgba(11,11,11,.025);--good:#0ca30c;--warn:#ec835a;--bad:#d03b3b;--mono:ui-monospace,SFMono-Regular,"SF Mono",Menlo,Consolas,monospace;color:var(--text);background:var(--bg);font-synthesis:none;color-scheme:light}
@media(prefers-color-scheme:dark){:root{--bg:#0d0d0d;--rail:#161615;--text:#f0f0ed;--muted:#898781;--dim:#5e5d57;--line:#242423;--accent:#3987e5;--rule:#2c2c2a;--dirrow:rgba(57,135,229,.045);--hover:rgba(255,255,255,.025);color-scheme:dark}}
*{box-sizing:border-box}
body{margin:0;display:flex;height:100dvh;overflow:hidden}
button,a{-webkit-tap-highlight-color:transparent}
button{font:inherit;cursor:pointer;color:inherit;border:0;background:none}
button:focus-visible,a:focus-visible{outline:1px solid var(--accent);outline-offset:4px}
.sidebar{width:152px;flex:0 0 152px;display:flex;flex-direction:column;align-items:stretch;padding:24px 16px;background:var(--rail);gap:28px;overflow-y:auto}
.logo{display:flex;align-items:center;gap:11px;flex-shrink:0;font-size:24px;font-weight:650;letter-spacing:-1px;text-decoration:none;color:var(--text)}
.logo-icon{display:block;position:relative;width:35px;height:35px;background:var(--accent);color:#fff;line-height:32px;text-align:center;font-weight:500;font-size:25px;letter-spacing:-1px}
.logo-icon span{position:absolute;font-size:14px;right:3px;top:-4px}
nav{display:flex;flex-direction:column}
nav a{display:flex;align-items:center;gap:6px;min-height:44px;padding:12px 10px;text-align:left;text-decoration:none;border-left:2px solid transparent;font-size:12px;color:var(--muted)}
nav a:hover{color:var(--text)}
nav a.active{color:var(--accent);border-left-color:var(--accent)}
.nav-count{margin-left:auto;font:10px var(--mono);color:var(--dim)}
.active .nav-count{color:var(--accent);opacity:.65}
.kinds{padding:0 0 0 12px;display:flex;flex-direction:column;gap:5px}
.filter{padding:7px 0;display:flex;align-items:center;gap:8px;font-size:11px;text-align:left;color:var(--muted)}
.filter:hover,.filter.selected{color:var(--text)}
.filter .count{margin-left:auto;font:11px var(--mono);font-variant-numeric:tabular-nums}
.filter.selected .count{color:var(--accent)}
.dot{display:inline-block;flex:none;width:5px;height:5px;border-radius:50%;background:var(--accent)}
.dot.file{background:transparent;border:1px solid var(--muted)}
.dot.full{background:var(--good);border:0}.dot.short{background:var(--warn);border:0}
.sidebar-foot{margin-top:auto;padding:0 0 0 12px;font-size:10px;line-height:1.9;color:var(--muted)}
.sidebar-foot .snapshot{display:flex;align-items:center;gap:7px}
.sidebar-foot .dot{background:var(--good)}
.sidebar-foot .dot.stale{background:var(--bad)}
.sidebar-foot time{display:block;padding-left:12px;font:9px/1.9 var(--mono);color:var(--dim)}
main{flex:1;min-width:0;height:100dvh;overflow:auto;scrollbar-color:var(--rule) transparent;background:var(--bg)}
.page{padding:26px 36px 48px;min-height:100%;min-width:720px}
.crumbs{display:block;font:11px/1.6 var(--mono);color:var(--muted);margin-bottom:16px;overflow-wrap:anywhere}
.crumbs a{color:var(--muted);text-decoration:none}.crumbs a:hover{color:var(--text)}
.crumbs .sep{margin:0 8px;color:var(--dim)}
.crumbs .cur{color:var(--text)}
table{border-collapse:collapse;width:100%;table-layout:fixed;text-align:left}
col.name{width:50%}col.size{width:10%}col.time{width:16%}col.md5{width:16%}col.pieces{width:8%}
.buckets col.name{width:70%}.buckets col.time{width:30%}
th{height:34px;padding:0 14px 13px;font:500 9px var(--mono);letter-spacing:1.25px;color:var(--muted);text-transform:uppercase;vertical-align:top;white-space:nowrap;border-bottom:1px solid var(--rule)}
th:first-child{padding-left:16px}th:last-child{padding-right:16px}
td{height:56px;padding:14px;vertical-align:middle;border-bottom:1px solid var(--line);font-size:12px}
td:first-child{padding-left:16px}td:last-child{padding-right:16px}
tr.dir{background:var(--dirrow)}tbody tr:hover{background:var(--hover)}
.entry{display:flex;align-items:center;gap:11px;min-width:0}
.entry-body{min-width:0}
.entry-link{display:block;text-decoration:none;color:inherit}
.entry-link:hover .entry-name{color:var(--accent)}
.entry-name{white-space:pre-wrap;font:12px/1.55 var(--mono);color:var(--text);overflow-wrap:anywhere}
.number{text-align:right;font-variant-numeric:tabular-nums}
td.number{font:11px var(--mono);color:var(--muted)}
td.number.short{color:var(--warn)}
.clock{font:11px var(--mono);color:var(--muted);white-space:nowrap}
.md5{font:10px var(--mono);color:var(--dim);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.dash{font:12px var(--mono);color:var(--dim)}
.empty{height:130px;font:12px var(--mono);color:var(--muted);text-align:center}
.more{display:flex;justify-content:space-between;align-items:center;padding:14px 16px;font:10px var(--mono);color:var(--dim)}
.more a{color:var(--accent);text-decoration:none}
.error{border-left:2px solid var(--bad);padding:10px 14px;margin:0 0 18px;color:var(--bad);font:12px/1.6 var(--mono);white-space:pre-wrap;overflow-wrap:anywhere}
[hidden]{display:none!important}
@media(min-width:1700px){.page{padding-left:44px;padding-right:44px}}
@media(max-width:1000px){.page{padding-left:22px;padding-right:22px}col.name{width:44%}col.size{width:11%}col.time{width:17%}col.md5{width:18%}col.pieces{width:10%}.entry-name{font-size:11px}td{padding-left:10px;padding-right:10px}}
@media(max-width:680px){.sidebar{width:124px;flex-basis:124px;padding:20px 12px;gap:25px}.logo{font-size:21px;gap:8px}.logo-icon{width:30px;height:30px;line-height:28px;font-size:22px}.page{padding:22px 16px 40px;min-width:680px}.kinds,.sidebar-foot{padding-left:12px}}
</style>
</head>
<body>
<aside class="sidebar" aria-label="Navigation">
  <a class="logo" href="/" aria-label="s3 buckets"><span class="logo-icon" aria-hidden="true">s<span>↗</span></span>s3</a>
  <nav aria-label="Views">
    <a href="/"{{if eq .Page "buckets"}} class="active" aria-current="page"{{end}}>Buckets{{if eq .Page "buckets"}} <span class="nav-count">{{len .Buckets}}</span>{{end}}</a>
  </nav>
  {{if eq .Page "bucket"}}
  <div class="kinds" aria-label="Filter entries">
    <button class="filter" data-filter="dir" aria-pressed="false"><span class="dot" aria-hidden="true"></span>Folders<span class="count">{{.Dirs}}</span></button>
    <button class="filter" data-filter="file" aria-pressed="false"><span class="dot file" aria-hidden="true"></span>Files<span class="count">{{.Files}}</span></button>
  </div>
  {{end}}
  <div class="sidebar-foot">
    <span class="snapshot"><span class="dot{{if .Error}} stale{{end}}" aria-hidden="true"></span><span role="status">{{if .Error}}Unavailable{{else}}Snapshot{{end}}</span></span>
    <time datetime="{{.Now}}" title="{{.Now}}">{{clock .Now}} UTC</time>
  </div>
</aside>
<main>
<section class="page" aria-label="{{.Page}}">
  <div class="error" role="alert"{{if not .Error}} hidden{{end}}>{{.Error}}</div>
  {{if eq .Page "buckets"}}
  <table class="buckets" aria-label="Buckets">
    <colgroup><col class="name"></colgroup>
    <thead><tr><th scope="col">Bucket</th></tr></thead>
    <tbody>
    {{range .Buckets}}
      <tr>
        <td><div class="entry"><span class="dot" aria-hidden="true"></span><div class="entry-body"><a class="entry-link" href="/b/{{.Name}}"><div class="entry-name">{{.Name}}</div></a></div></div></td>
      </tr>
    {{else}}
      <tr><td class="empty">no buckets</td></tr>
    {{end}}
    </tbody>
  </table>
  {{else}}
  <div class="crumbs"><a href="/">Buckets</a>{{range .Crumbs}}<span class="sep">/</span>{{if .URL}}<a href="{{.URL}}">{{.Name}}</a>{{else}}<span class="cur">{{.Name}}</span>{{end}}{{end}}</div>
  <table aria-label="Entries">
    <colgroup><col class="name"><col class="size"><col class="time"><col class="md5"><col class="pieces"></colgroup>
    <thead><tr><th scope="col">Name</th><th scope="col" class="number">Size</th><th scope="col" class="number">Modified</th><th scope="col">MD5</th><th scope="col" class="number">Pieces</th></tr></thead>
    <tbody id="rows">
    {{range .Entries}}
      <tr class="{{if .Dir}}dir{{else}}file{{end}}" data-entry>
        <td><div class="entry"><span class="dot{{if not .Dir}} file{{if lt .Pieces 3}} short{{else}} full{{end}}{{end}}" title="{{if .Dir}}Folder{{else if lt .Pieces 3}}{{.Pieces}} of 3 pieces{{else}}3 pieces{{end}}"></span><div class="entry-body"><a class="entry-link" href="{{.URL}}"><div class="entry-name">{{.Name}}</div></a></div></div></td>
        {{if .Dir}}<td class="number"><span class="dash">—</span></td><td class="number"><span class="dash">—</span></td><td><span class="dash">—</span></td><td class="number"><span class="dash">—</span></td>{{else}}<td class="number" title="{{.Size}} bytes">{{size .Size}}</td>
        <td class="number"><time class="clock" datetime="{{.Mtime}}" title="{{.Mtime}}">{{stamp .Mtime}}</time></td>
        <td class="md5" title="{{.Md5}}">{{.Md5}}</td>
        <td class="number{{if lt .Pieces 3}} short{{end}}">{{.Pieces}}</td>{{end}}
      </tr>
    {{end}}
      <tr id="empty-row"{{if .Entries}} hidden{{end}}><td colspan="5" class="empty">{{if .Prefix}}folder is empty{{else}}bucket is empty{{end}}</td></tr>
    </tbody>
  </table>
  <div class="more"><span><span id="shown">{{len .Entries}}</span> entries</span>{{if .Next}}<a href="{{.Next}}">next page ↗</a>{{end}}</div>
  {{end}}
</section>
</main>
{{if eq .Page "bucket"}}
<script>
(function () {
  var rows = document.getElementById('rows');
  var filters = document.querySelectorAll('[data-filter]');
  var filter = 'all';

  function applyFilter() {
    var visible = 0;
    rows.querySelectorAll('[data-entry]').forEach(function (row) {
      row.hidden = filter !== 'all' && !row.classList.contains(filter);
      if (!row.hidden) visible++;
    });
    var empty = document.getElementById('empty-row');
    var total = rows.querySelectorAll('[data-entry]').length;
    empty.hidden = visible !== 0;
    if (total) empty.firstElementChild.textContent = filter === 'dir' ? 'no folders' : 'no files';
    document.getElementById('shown').textContent = visible;
    filters.forEach(function (button) {
      var active = button.dataset.filter === filter;
      button.classList.toggle('selected', active);
      button.setAttribute('aria-pressed', String(active));
    });
  }

  filters.forEach(function (button) {
    button.addEventListener('click', function () {
      filter = filter === button.dataset.filter ? 'all' : button.dataset.filter;
      applyFilter();
    });
  });
})();
</script>
{{end}}
</body>
</html>
`))

const pageLimit = 500

type Web struct {
	store *Store
}

type WebBucket struct {
	Name string
}

type WebCrumb struct {
	Name string
	URL  string
}

type WebEntry struct {
	Name   string
	URL    string
	Dir    bool
	Size   int64
	Mtime  string
	Md5    string
	Pieces int
}

type WebPage struct {
	Page    string
	Now     string
	Error   string
	Buckets []WebBucket
	Bucket  string
	Prefix  string
	Crumbs  []WebCrumb
	Entries []WebEntry
	Dirs    int
	Files   int
	Next    string
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
	switch {
	case r.URL.Path == "/":
		wb.page(w, r, wb.buckets)
	case strings.HasPrefix(r.URL.Path, "/b/"):
		wb.page(w, r, wb.bucket)
	case strings.HasPrefix(r.URL.Path, "/o/"):
		wb.object(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (wb *Web) page(w http.ResponseWriter, r *http.Request, fill func(*http.Request, *WebPage)) {
	page := &WebPage{Now: time.Now().UTC().Format(time.RFC3339)}

	try(func() {
		fill(r, page)
	}).catch(func(exc *Exception) {
		slog.Error("web", "path", r.URL.Path, "err", exc.error())
		page.Error = exc.error()
	})

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = pageTmpl.Execute(w, page)
}

func (wb *Web) buckets(r *http.Request, page *WebPage) {
	page.Page = "buckets"

	for _, b := range wb.store.buckets {
		page.Buckets = append(page.Buckets, WebBucket{Name: b})
	}
}

func (wb *Web) bucket(r *http.Request, page *WebPage) {
	page.Page = "bucket"
	page.Bucket = strings.TrimPrefix(r.URL.Path, "/b/")
	page.Prefix = r.URL.Query().Get("prefix")
	page.Crumbs = crumbsOf(page.Bucket, page.Prefix)

	if page.Bucket == "" || strings.Contains(page.Bucket, "/") {
		throwFmt("no such bucket")
	}

	if !wb.store.known[page.Bucket] {
		throwFmt("no such bucket: %s", page.Bucket)
	}

	found := wb.store.list(page.Bucket, page.Prefix, "/", r.URL.Query().Get("after"), pageLimit)

	for _, cp := range found.Dirs {
		page.Entries = append(page.Entries, WebEntry{Name: strings.TrimPrefix(cp, page.Prefix), URL: dirURL(page.Bucket, cp), Dir: true})
	}

	for _, o := range found.Objects {
		page.Entries = append(page.Entries, WebEntry{
			Name:   strings.TrimPrefix(o.Key, page.Prefix),
			URL:    objURL(page.Bucket, o.Key),
			Size:   o.Size,
			Mtime:  o.Mtime.UTC().Format(time.RFC3339),
			Md5:    o.Md5,
			Pieces: len(o.Pieces),
		})
	}

	page.Dirs = len(found.Dirs)
	page.Files = len(found.Objects)

	if found.Next != "" {
		page.Next = dirURL(page.Bucket, page.Prefix) + "&after=" + url.QueryEscape(found.Next)
	}
}

func (wb *Web) object(w http.ResponseWriter, r *http.Request) {
	try(func() {
		bucket, key, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/o/"), "/")
		m, _, err := wb.store.manifest(bucket, key)

		if errors.Is(err, errNoSuchKey) {
			http.NotFound(w, r)

			return
		}

		throw(err)

		data, err := wb.store.get(r.Context().Done(), bucket, key, m)

		if errors.Is(err, errClientGone) {
			return
		}

		throw(err)

		kind := m.ContentType

		if kind == "" {
			kind = "application/octet-stream"
		}

		w.Header().Set("Content-Type", kind)
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.Header().Set("ETag", `"`+m.Md5+`"`)
		w.Write(data)
	}).catch(func(exc *Exception) {
		slog.Error("web", "path", r.URL.Path, "err", exc.error())
		http.Error(w, exc.error(), http.StatusInternalServerError)
	})
}

func crumbsOf(bucket, prefix string) []WebCrumb {
	crumbs := []WebCrumb{{Name: bucket, URL: dirURL(bucket, "")}}
	sofar := ""

	for _, seg := range strings.Split(strings.TrimSuffix(prefix, "/"), "/") {
		if prefix == "" {
			break
		}

		sofar += seg + "/"
		crumbs = append(crumbs, WebCrumb{Name: seg, URL: dirURL(bucket, sofar)})
	}

	crumbs[len(crumbs)-1].URL = ""

	return crumbs
}

func dirURL(bucket, prefix string) string {
	return "/b/" + url.PathEscape(bucket) + "?prefix=" + url.QueryEscape(prefix)
}

func objURL(bucket, key string) string {
	segs := strings.Split(key, "/")

	for i, seg := range segs {
		segs[i] = url.PathEscape(seg)
	}

	return "/o/" + url.PathEscape(bucket) + "/" + strings.Join(segs, "/")
}

func clockOf(timestamp string) string {
	t, err := time.Parse(time.RFC3339Nano, timestamp)

	if err != nil {
		return timestamp
	}

	return t.UTC().Format("15:04:05")
}

func stampOf(timestamp string) string {
	t, err := time.Parse(time.RFC3339Nano, timestamp)

	if err != nil {
		return timestamp
	}

	return t.UTC().Format("2006-01-02 15:04:05")
}

func sizeOf(n int64) string {
	units := []string{"B", "KB", "MB", "GB", "TB"}
	v := float64(n)
	i := 0

	for v >= 1024 && i < len(units)-1 {
		v /= 1024
		i++
	}

	if i == 0 {
		return fmt.Sprintf("%d B", n)
	}

	return fmt.Sprintf("%.1f %s", v, units[i])
}
