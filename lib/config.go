package workerpool

type Config struct {
	MaxWorker     int   `mapstructure:"MAX_WORKER"`
	MaxWorkerIdle int64 `mapstructure:"MAX_WORKER_IDLE"`
	MaxQueue      int   `mapstructure:"MAX_QUEUE"`
}
