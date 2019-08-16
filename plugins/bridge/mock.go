package bridge

type mockMQ struct{}

func (m *mockMQ) Publish(e *Elements) error {
	return nil
}
