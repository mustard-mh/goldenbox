package goldenbox

import (
	"fmt"
	"os"
	"testing"

	"github.com/mustard-mh/goldenbox/golden"
)

// TestMain dogfoods the golden update lifecycle on the library's own goldens:
// a full -update-golden run rewrites every golden below and deletes orphans
// left behind by renamed cases, exactly as consumers do. Core's goldens live in
// testdata/normalize (drivers get testdata/mysql, testdata/redis) so each
// component's orphan sweep stays scoped to its own subtree.
func TestMain(m *testing.M) {
	code := m.Run()
	if code == 0 {
		for _, f := range golden.CleanObsolete("testdata/normalize") {
			fmt.Println("removed orphan golden:", f)
		}
	}
	os.Exit(code)
}
