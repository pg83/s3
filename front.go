package main

import (
	"bufio"
	"encoding/xml"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

var bucketSubresources = []string{
	"accelerate", "acl", "analytics", "cors", "delete", "encryption", "intelligent-tiering", "inventory",
	"lifecycle", "location", "logging", "metrics", "notification", "object-lock", "ownershipControls",
	"policy", "policyStatus", "publicAccessBlock", "replication", "requestPayment", "tagging",
	"versioning", "versions", "website",
}

var objectSubresources = []string{"acl", "attributes", "legal-hold", "retention", "select", "tagging", "torrent"}

const (
	listLimit      = 1000
	s3Time         = "2006-01-02T15:04:05.000Z"
	bodiesInFlight = 32
)

type Front struct {
	store *Store
	slots chan struct{}
}

func runFront(cfg *Config, listen string) {
	if listen == "" {
		throwFmt("front: -listen is required")
	}

	f := &Front{store: newStore(cfg), slots: make(chan struct{}, bodiesInFlight)}

	slog.Info("front: serving S3", "listen", listen)

	throw(http.ListenAndServe(listen, f))
}

type S3Error struct {
	XMLName  xml.Name `xml:"Error"`
	Code     string   `xml:"Code"`
	Message  string   `xml:"Message"`
	Resource string   `xml:"Resource"`
}

type Owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type BucketInfo struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

type ListAllMyBucketsResult struct {
	XMLName xml.Name     `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListAllMyBucketsResult"`
	Owner   Owner        `xml:"Owner"`
	Buckets []BucketInfo `xml:"Buckets>Bucket"`
}

type Object struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type CommonPrefix struct {
	Prefix string `xml:"Prefix"`
}

type ListBucketResult struct {
	XMLName               xml.Name       `xml:"http://s3.amazonaws.com/doc/2006-03-01/ ListBucketResult"`
	Name                  string         `xml:"Name"`
	Prefix                string         `xml:"Prefix"`
	Delimiter             string         `xml:"Delimiter,omitempty"`
	MaxKeys               int            `xml:"MaxKeys"`
	KeyCount              int            `xml:"KeyCount"`
	IsTruncated           bool           `xml:"IsTruncated"`
	EncodingType          string         `xml:"EncodingType,omitempty"`
	ContinuationToken     string         `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string         `xml:"NextContinuationToken,omitempty"`
	Marker                string         `xml:"Marker,omitempty"`
	NextMarker            string         `xml:"NextMarker,omitempty"`
	Contents              []Object       `xml:"Contents"`
	CommonPrefixes        []CommonPrefix `xml:"CommonPrefixes"`
}

type DeleteRequest struct {
	XMLName xml.Name `xml:"Delete"`
	Quiet   bool     `xml:"Quiet"`
	Objects []struct {
		Key string `xml:"Key"`
	} `xml:"Object"`
}

type DeletedObject struct {
	Key string `xml:"Key"`
}

type DeleteResult struct {
	XMLName xml.Name        `xml:"http://s3.amazonaws.com/doc/2006-03-01/ DeleteResult"`
	Deleted []DeletedObject `xml:"Deleted"`
}

type LocationConstraint struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ LocationConstraint"`
}

type VersioningConfiguration struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ VersioningConfiguration"`
}

func subresource(q url.Values, names []string) string {
	for _, name := range names {
		if q.Has(name) {
			return name
		}
	}

	return ""
}

func writeXml(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	w.Write([]byte(xml.Header))
	throw(xml.NewEncoder(w).Encode(v))
}

func s3Fail(w http.ResponseWriter, status int, code, message, resource string) {
	writeXml(w, status, S3Error{Code: code, Message: message, Resource: resource})
}

func (f *Front) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	try(func() {
		f.route(w, r)
	}).catch(func(exc *Exception) {
		slog.Error("front", "method", r.Method, "path", r.URL.Path, "err", exc.error())
		s3Fail(w, http.StatusInternalServerError, "InternalError", exc.error(), r.URL.Path)
	})
}

func (f *Front) route(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	bucket, key, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")

	if q.Has("uploads") || q.Has("uploadId") {
		s3Fail(w, http.StatusNotImplemented, "NotImplemented", "multipart uploads are not implemented", r.URL.Path)

		return
	}

	switch {
	case bucket == "":
		if r.Method != http.MethodGet {
			s3Fail(w, http.StatusMethodNotAllowed, "MethodNotAllowed", r.Method, "/")

			return
		}

		f.listBuckets(w)
	case key == "":
		f.bucketOp(w, r, bucket)
	default:
		f.objectOp(w, r, bucket, key)
	}
}

func (f *Front) listBuckets(w http.ResponseWriter) {
	out := ListAllMyBucketsResult{Owner: Owner{ID: "s3", DisplayName: "s3"}}
	from := ""

	for {
		found, more := f.store.etcd.scan("bkt/", from, listLimit)

		for _, entry := range found {
			from = entry.key + "\x00"
			out.Buckets = append(out.Buckets, BucketInfo{Name: strings.TrimPrefix(entry.key, "bkt/"), CreationDate: string(entry.value)})
		}

		if !more {
			break
		}
	}

	writeXml(w, http.StatusOK, out)
}

func (f *Front) bucketExists(bucket string) bool {
	_, found := f.store.etcd.get(bucketKey(bucket))

	return found
}

func (f *Front) bucketOp(w http.ResponseWriter, r *http.Request, bucket string) {
	if sub := subresource(r.URL.Query(), bucketSubresources); sub != "" {
		f.bucketSub(w, r, bucket, sub)

		return
	}

	switch r.Method {
	case http.MethodPut:
		if f.bucketExists(bucket) {
			s3Fail(w, http.StatusConflict, "BucketAlreadyOwnedByYou", "bucket exists", "/"+bucket)

			return
		}

		f.store.etcd.put(bucketKey(bucket), []byte(time.Now().UTC().Format(s3Time)))
		w.Header().Set("Location", "/"+bucket)
		w.WriteHeader(http.StatusOK)
	case http.MethodHead:
		if !f.bucketExists(bucket) {
			w.WriteHeader(http.StatusNotFound)

			return
		}

		w.WriteHeader(http.StatusOK)
	case http.MethodDelete:
		if !f.bucketExists(bucket) {
			s3Fail(w, http.StatusNotFound, "NoSuchBucket", "no such bucket", "/"+bucket)

			return
		}

		if found, _ := f.store.etcd.scan("obj/"+bucket+"/", "", 1); len(found) > 0 {
			s3Fail(w, http.StatusConflict, "BucketNotEmpty", "bucket is not empty", "/"+bucket)

			return
		}

		f.store.etcd.del(bucketKey(bucket))
		w.WriteHeader(http.StatusNoContent)
	case http.MethodGet:
		if !f.bucketExists(bucket) {
			s3Fail(w, http.StatusNotFound, "NoSuchBucket", "no such bucket", "/"+bucket)

			return
		}

		f.listObjects(w, r, bucket)
	default:
		s3Fail(w, http.StatusMethodNotAllowed, "MethodNotAllowed", r.Method, "/"+bucket)
	}
}

func (f *Front) bucketSub(w http.ResponseWriter, r *http.Request, bucket, sub string) {
	resource := "/" + bucket

	if !f.bucketExists(bucket) {
		s3Fail(w, http.StatusNotFound, "NoSuchBucket", "no such bucket", resource)

		return
	}

	switch {
	case sub == "delete" && r.Method == http.MethodPost:
		f.deleteObjects(w, r, bucket)
	case sub == "location" && r.Method == http.MethodGet:
		writeXml(w, http.StatusOK, LocationConstraint{})
	case sub == "versioning" && r.Method == http.MethodGet:
		writeXml(w, http.StatusOK, VersioningConfiguration{})
	case sub == "policy" && r.Method == http.MethodGet:
		s3Fail(w, http.StatusNotFound, "NoSuchBucketPolicy", "the bucket has no policy", resource)
	default:
		s3Fail(w, http.StatusNotImplemented, "NotImplemented", sub+" is not implemented", resource)
	}
}

func (f *Front) deleteObjects(w http.ResponseWriter, r *http.Request, bucket string) {
	req := DeleteRequest{}

	throw(xml.Unmarshal(readBody(r), &req))

	if len(req.Objects) > listLimit {
		s3Fail(w, http.StatusBadRequest, "MalformedXML", "at most 1000 keys per request", "/"+bucket)

		return
	}

	out := DeleteResult{}

	for _, o := range req.Objects {
		f.store.etcd.del(objKey(bucket, o.Key))

		if !req.Quiet {
			out.Deleted = append(out.Deleted, DeletedObject{Key: o.Key})
		}
	}

	writeXml(w, http.StatusOK, out)
}

func (f *Front) listObjects(w http.ResponseWriter, r *http.Request, bucket string) {
	q := r.URL.Query()
	prefix := q.Get("prefix")
	delimiter := q.Get("delimiter")
	maxKeys := listLimit
	v2 := q.Get("list-type") == "2"

	if v := q.Get("max-keys"); v != "" {
		maxKeys = throw2(strconv.Atoi(v))
	}

	after := q.Get("marker")

	if v2 {
		after = q.Get("start-after")

		if token := q.Get("continuation-token"); token != "" {
			after = token
		}
	}

	out := ListBucketResult{Name: bucket, Prefix: prefix, Delimiter: delimiter, MaxKeys: maxKeys, EncodingType: q.Get("encoding-type")}
	found := f.store.list(bucket, prefix, delimiter, after, maxKeys)

	for _, cp := range found.Dirs {
		out.CommonPrefixes = append(out.CommonPrefixes, CommonPrefix{Prefix: encodeKey(cp, out.EncodingType)})
	}

	for _, o := range found.Objects {
		out.Contents = append(out.Contents, Object{
			Key:          encodeKey(o.Key, out.EncodingType),
			LastModified: o.Mtime.UTC().Format(s3Time),
			ETag:         `"` + o.Md5 + `"`,
			Size:         o.Size,
			StorageClass: "STANDARD",
		})
	}

	out.KeyCount = len(found.Dirs) + len(found.Objects)

	if found.Next != "" {
		out.IsTruncated = true

		if v2 {
			out.NextContinuationToken = found.Next
		} else {
			out.NextMarker = found.Next
		}
	}

	if v2 {
		out.ContinuationToken = q.Get("continuation-token")
	} else {
		out.Marker = q.Get("marker")
	}

	writeXml(w, http.StatusOK, out)
}

func encodeKey(key, encoding string) string {
	if encoding == "url" {
		return url.QueryEscape(key)
	}

	return key
}

func (f *Front) objectOp(w http.ResponseWriter, r *http.Request, bucket, key string) {
	resource := "/" + bucket + "/" + key

	if !f.bucketExists(bucket) {
		s3Fail(w, http.StatusNotFound, "NoSuchBucket", "no such bucket", resource)

		return
	}

	if sub := subresource(r.URL.Query(), objectSubresources); sub != "" {
		s3Fail(w, http.StatusNotImplemented, "NotImplemented", sub+" is not implemented", resource)

		return
	}

	switch r.Method {
	case http.MethodPut:
		if r.Header.Get("x-amz-copy-source") != "" {
			s3Fail(w, http.StatusNotImplemented, "NotImplemented", "copy is not implemented", resource)

			return
		}

		f.slots <- struct{}{}

		defer func() { <-f.slots }()

		start := time.Now()
		data := readBody(r)

		slog.Debug("front: body", "key", key, "size", len(data), "read", time.Since(start))

		m, err := f.store.put(r.Context().Done(), bucket, key, data, r.Header.Get("Content-Type"))

		if errors.Is(err, errClientGone) {
			return
		}

		if errors.Is(err, errTooFewCells) {
			s3Fail(w, http.StatusServiceUnavailable, "SlowDown", err.Error(), resource)

			return
		}

		throw(err)
		w.Header().Set("ETag", `"`+m.Md5+`"`)
		w.WriteHeader(http.StatusOK)
	case http.MethodHead, http.MethodGet:
		m, _, err := f.store.manifest(bucket, key)

		if errors.Is(err, errNoSuchKey) {
			if r.Method == http.MethodHead {
				w.WriteHeader(http.StatusNotFound)
			} else {
				s3Fail(w, http.StatusNotFound, "NoSuchKey", "no such key", resource)
			}

			return
		}

		throw(err)

		h := w.Header()

		h.Set("ETag", `"`+m.Md5+`"`)
		h.Set("Last-Modified", m.Mtime.UTC().Format(http.TimeFormat))
		h.Set("Accept-Ranges", "bytes")

		if m.ContentType != "" {
			h.Set("Content-Type", m.ContentType)
		} else {
			h.Set("Content-Type", "application/octet-stream")
		}

		if r.Method == http.MethodHead {
			h.Set("Content-Length", strconv.FormatInt(m.Size, 10))
			w.WriteHeader(http.StatusOK)

			return
		}

		f.slots <- struct{}{}

		defer func() { <-f.slots }()

		data, err := f.store.get(r.Context().Done(), bucket, key, m)

		if errors.Is(err, errClientGone) {
			return
		}

		if errors.Is(err, errUnreadable) {
			s3Fail(w, http.StatusInternalServerError, "InternalError", err.Error(), resource)

			return
		}

		throw(err)

		start, end, partial, ok := byteRange(r.Header.Get("Range"), m.Size)

		if !ok {
			h.Set("Content-Range", "bytes */"+strconv.FormatInt(m.Size, 10))
			s3Fail(w, http.StatusRequestedRangeNotSatisfiable, "InvalidRange", "range not satisfiable", resource)

			return
		}

		h.Set("Content-Length", strconv.FormatInt(end-start, 10))

		if partial {
			h.Set("Content-Range", "bytes "+strconv.FormatInt(start, 10)+"-"+strconv.FormatInt(end-1, 10)+"/"+strconv.FormatInt(m.Size, 10))
			w.WriteHeader(http.StatusPartialContent)
		} else {
			w.WriteHeader(http.StatusOK)
		}

		w.Write(data[start:end])
	case http.MethodDelete:
		f.store.etcd.del(objKey(bucket, key))
		w.WriteHeader(http.StatusNoContent)
	default:
		s3Fail(w, http.StatusMethodNotAllowed, "MethodNotAllowed", r.Method, resource)
	}
}

func byteRange(spec string, size int64) (int64, int64, bool, bool) {
	if spec == "" {
		return 0, size, false, true
	}

	spec, ok := strings.CutPrefix(spec, "bytes=")

	if !ok || strings.Contains(spec, ",") {
		return 0, 0, false, false
	}

	first, last, _ := strings.Cut(spec, "-")

	if first == "" {
		n := throw2(strconv.ParseInt(last, 10, 64))

		if n <= 0 {
			return 0, 0, false, false
		}

		return max(0, size-n), size, true, true
	}

	start := throw2(strconv.ParseInt(first, 10, 64))
	end := size

	if last != "" {
		end = min(size, throw2(strconv.ParseInt(last, 10, 64))+1)
	}

	if start >= size || start >= end {
		return 0, 0, false, false
	}

	return start, end, true, true
}

func readBody(r *http.Request) []byte {
	defer r.Body.Close()

	chunked := strings.HasPrefix(r.Header.Get("x-amz-content-sha256"), "STREAMING-") ||
		strings.Contains(r.Header.Get("Content-Encoding"), "aws-chunked")

	if !chunked {
		return throw2(io.ReadAll(r.Body))
	}

	var out []byte

	br := bufio.NewReader(r.Body)

	for {
		line := throw2(br.ReadString('\n'))
		sizeHex, _, _ := strings.Cut(strings.TrimRight(line, "\r\n"), ";")
		n := throw2(strconv.ParseInt(strings.TrimSpace(sizeHex), 16, 64))

		if n == 0 {
			break
		}

		chunk := make([]byte, n)

		throw2(io.ReadFull(br, chunk))
		out = append(out, chunk...)

		throw2(br.ReadString('\n'))
	}

	return out
}
