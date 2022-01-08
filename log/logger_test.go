package log

func InitLog() {
	var config log.Config

	config.MaxLogSize = 100
	config.ServiceName = "test"

	log.Init(&config)
}

func TestLogger(t *testing.T) {
	InitLog()
	log.Info("test")
}
