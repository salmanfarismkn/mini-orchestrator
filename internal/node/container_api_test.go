package node

import (
	"net/http"
	"net/url"
	"testing"
)

func TestContainerIDFromPath(t *testing.T) {
	for _, tc := range []struct {
		name string
		path string
		want string
	}{
		{name: "start route", path: "/containers/abc123/start", want: "abc123"},
		{name: "inspect route", path: "/containers/abc123", want: "abc123"},
		{name: "list route", path: "/containers", want: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &http.Request{URL: &url.URL{Path: tc.path}}
			if got := containerIDFromPath(r); got != tc.want {
				t.Fatalf("containerIDFromPath(%q) = %q; want %q", tc.path, got, tc.want)
			}
		})
	}
}
