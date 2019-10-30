package acl

type aclAuth struct {
	config *ACLConfig
}

func Init() *aclAuth {
	aclConfig, err := AclConfigLoad("./plugins/auth/authfile/acl.conf")
	if err != nil {
		panic(err)
	}
	return &aclAuth{
		config: aclConfig,
	}
}

func (a *aclAuth) CheckConnect(clientID, username, password string) bool {
	return true
}

func (a *aclAuth) CheckACL(action, clientID, username, ip, topic string) bool {
	return checkTopicAuth(a.config, action, username, ip, clientID, topic)
}
