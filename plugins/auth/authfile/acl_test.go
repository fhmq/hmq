//+build test

package acl

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOrigAcls(t *testing.T) {
	pwd, _ := os.Getwd()
	os.Chdir("../../../")
	aclOrig := Init()
	os.Chdir(pwd)

	// rule: allow      ip          127.0.0.1      2         $SYS/#
	origAllowed := aclOrig.CheckACL(PUB, "dummyClientID", "dummyUser", "127.0.0.1", "$SYS/something")
	assert.True(t, origAllowed)
	origAllowed = aclOrig.CheckACL(SUB, "dummyClientID", "dummyUser", "127.0.0.1", "$SYS/something")
	assert.False(t, origAllowed)
}
