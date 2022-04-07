package acl

type aclAuth struct {
	config *ACLConfig
}

func Init(confFile string) *aclAuth {
	aclConfig, err := AclConfigLoad(confFile)
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
	return checkTopicAuth(a.config, action, ip, username, clientID, topic)
}
