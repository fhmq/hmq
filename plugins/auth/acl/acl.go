package acl

type aclAuth struct {
	config *ACLConfig
}

func Init() *aclAuth {
	aclConfig, err := AclConfigLoad("./plugins/auth/authhttp/http.json")
	if err != nil {
		panic(err)
	}
	return &aclAuth{
		config: aclConfig,
	}
}

func (a *aclAuth) CheckConnect(clientID, username, password string) bool {
	return checkTopicAuth(a.config, clientID, username, password)
}

func (a *aclAuth) CheckACL(action, username, topic string) bool {
	return checkTopicAuth(a.config, action, clientID, username, password)
}

// type Auth interface {
// 	CheckACL(action, username, topic string) bool
// 	CheckConnect(clientID, username, password string) bool
// }
