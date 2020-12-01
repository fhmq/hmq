//+build test

package acl

import (
	"bufio"
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

func TestInitWithConfig(t *testing.T) {
	// lets create the same config (default) and the one created separately and check that the support similar auth procedures

	pwd, _ := os.Getwd()
	os.Chdir("../../../")
	aclOrig := Init()
	os.Chdir(pwd)

	// read the same from the file (for testing purpose) and use the newly offered way to read config from the reader (= from memory)
	f, err := os.Open("./acl.conf")
	if assert.NoError(t, err) {
		buf := bufio.NewReader(f)

		aclConfig := &ACLConfig{}
		aclConfig.PraseFromReader(buf)
		aclNew := InitWithConfig(aclConfig)

		assert.Equal(t, aclOrig.CheckConnect("dummyClientID", "dummyUser", "dummyPsw"), aclNew.CheckConnect("dummyClientID", "dummyUser", "dummyPsw"))
		origAllowed := aclOrig.CheckACL(PUB, "dummyClientID", "dummyUser", "127.0.0.1", "toDevice/dummyClientID")
		newAllowed := aclNew.CheckACL(PUB, "dummyClientID", "dummyUser", "127.0.0.1", "toDevice/dummyClientID")
		assert.Equal(t, origAllowed, newAllowed)
		origAllowed = aclOrig.CheckACL(SUB, "dummyClientID", "dummyUser", "127.0.0.1", "toDevice/dummyClientID")
		newAllowed = aclNew.CheckACL(SUB, "dummyClientID", "dummyUser", "127.0.0.1", "toDevice/dummyClientID")
		assert.Equal(t, origAllowed, newAllowed)
	}
}
