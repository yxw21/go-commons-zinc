package go_commons_zinc

import (
	"fmt"
	"os"
	"testing"
)

func TestZinc(t *testing.T) {
	zinc := NewClient("test")
	resp, _, err := zinc.DocumentIndex(map[string]interface{}{
		"name": "tom",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = fmt.Fprintf(os.Stdout, "Response from `Document.Index`: %v\n", resp.GetId())
}
