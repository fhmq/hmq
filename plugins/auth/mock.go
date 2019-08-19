package auth

type mockAuth struct{}

func (m *mockAuth) CheckACL(action, username, topic string) bool {
	return true
}

func (m *mockAuth) CheckConnect(clientID, username, password string) bool {
	return true
}
