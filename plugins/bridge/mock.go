package bridge

type mockMQ struct{}

func (m *mockMQ) Publish(e *Elements) (bool, error) {
	return false, nil
}
