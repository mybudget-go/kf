package tasks

import (
	"context"
	"fmt"
	"github.com/gmbyapa/kstream/kafka"
	"github.com/gmbyapa/kstream/pkg/async"
	"github.com/gmbyapa/kstream/pkg/errors"
	"github.com/gmbyapa/kstream/streams/topology"
	"github.com/tryfix/log"
	"github.com/tryfix/metrics"
	"sync"
)

type TaskManager interface {
	NewTaskId(prefix string, tp kafka.TopicPartition) TaskID
	AddTask(ctx topology.BuilderContext, id TaskID, topology topology.SubTopologyBuilder, session kafka.GroupSession) (Task, error)
	AddGlobalTask(ctx topology.BuilderContext, id TaskID, topology topology.SubTopologyBuilder) (Task, error)
	RemoveTask(id TaskID) error
	Task(id TaskID) (Task, error)
	StoreInstances(name string) []topology.StateStore
	StopAll() error
}

type taskManager struct {
	logger            log.Logger
	tasks             map[string]Task
	partitionConsumer kafka.PartitionConsumer
	builderCtx        topology.BuilderContext
	transactional     bool
	topicConfigs      map[string]*kafka.Topic

	ctxCancel context.CancelFunc
	mu        sync.Mutex
	taskOpts  []TaskOpt
}

func NewTaskManager(
	builderCtx topology.BuilderContext,
	logger log.Logger,
	partitionConsumer kafka.PartitionConsumer,
	topologies topology.SubTopologyBuilders,
	transactional bool,
	taskOpts ...TaskOpt,
) (TaskManager, error) {
	// Get topic meta for all the topics
	tpConfigs, err := builderCtx.Admin().FetchInfo(topologies.Topics())
	if err != nil {
		return nil, errors.Wrapf(err, `Topic meta fetch failed for %v`, topologies.Topics())
	}

	return &taskManager{
		tasks:             map[string]Task{},
		builderCtx:        builderCtx,
		partitionConsumer: partitionConsumer,
		logger:            logger,
		transactional:     transactional,
		topicConfigs:      tpConfigs,
		taskOpts:          taskOpts,
	}, nil
}

func (t *taskManager) AddTask(ctx topology.BuilderContext, id TaskID, tp topology.SubTopologyBuilder, session kafka.GroupSession) (Task, error) {
	return t.addTask(ctx, id, tp, session)
}

func (t *taskManager) AddGlobalTask(ctx topology.BuilderContext, id TaskID, tp topology.SubTopologyBuilder) (Task, error) {
	return t.addGlobalTask(ctx, id, tp)
}

func (t *taskManager) addTask(ctx topology.BuilderContext, id TaskID, subTopology topology.SubTopologyBuilder, session kafka.GroupSession) (Task, error) {
	logger := t.logger.NewLog(log.Prefixed(id.String()))
	producer, err := ctx.ProducerBuilder()(func(config *kafka.ProducerConfig) {
		txID := fmt.Sprintf(`%s-%s`, ctx.ApplicationId(), id.UniqueID())
		config.Id = txID
		config.Transactional.Id = txID
		config.Logger = logger
	})
	if err != nil {
		return nil, errors.Wrap(err, `task build failed`)
	}

	topologyCtx := topology.NewSubTopologyContext(
		context.Background(),
		id.Partition(),
		ctx,
		producer,
		t.partitionConsumer,
		logger,
		t.topicConfigs,
	)
	subTp, err := subTopology.Build(topologyCtx)
	if err != nil {
		return nil, err
	}

	taskOpts := new(taskOptions)
	taskOpts.setDefault()
	taskOpts.failedMessageHandler = func(err error, record kafka.Record) {
		t.logger.Error(fmt.Sprintf(`Message %s failed due to %s`, record, err))
	}
	taskOpts.apply(t.taskOpts...)

	tsk := &task{
		id:          id,
		logger:      logger,
		session:     session,
		subTopology: subTp,
		producer:    producer,
		stopping:    make(chan struct{}),
		//stopSync:    make(chan struct{}, 1),
		runGroup: async.NewRunGroup(logger),
		options:  taskOpts,
	}

	tsk.buffer = newBuffer(tsk.options.buffer, tsk.onFlush, logger.NewLog(log.Prefixed(`Buffer`)))

	tsk.metrics.reporter = ctx.MetricsReporter().Reporter(metrics.ReporterConf{
		Subsystem: "task_manager_task",
		ConstLabels: map[string]string{
			`type`:    `task`,
			`task_id`: id.String(),
		},
	})

	var task Task = tsk

	if t.transactional {
		tsk.metrics.reporter = ctx.MetricsReporter().Reporter(metrics.ReporterConf{
			Subsystem: "task_manager_task",
			ConstLabels: map[string]string{
				`type`:    `transactional_task`,
				`task_id`: id.String(),
			},
		})
		kTransactionalTask := &transactionalTask{
			task:     tsk,
			producer: tsk.producer.(kafka.TransactionalProducer),
		}
		kTransactionalTask.buffer = newBuffer(kTransactionalTask.options.buffer, kTransactionalTask.onFlush, logger.NewLog(log.Prefixed(`Buffer`)))
		task = kTransactionalTask
	}

	if err := task.Init(topologyCtx); err != nil {
		return nil, err
	}

	t.mu.Lock()
	t.tasks[id.String()] = task
	t.mu.Unlock()

	return task, nil
}

func (t *taskManager) addGlobalTask(ctx topology.BuilderContext, id TaskID, subTpB topology.SubTopologyBuilder) (Task, error) {
	logger := t.logger.NewLog(log.Prefixed(id.String()))

	txManageCtx, cancel := context.WithCancel(context.Background())
	t.ctxCancel = cancel

	topologyCtx := topology.NewSubTopologyContext(
		txManageCtx,
		id.Partition(),
		ctx,
		nil,
		t.partitionConsumer,
		logger,
		t.topicConfigs,
	)
	subTp, err := subTpB.Build(topologyCtx)
	if err != nil {
		return nil, err
	}

	taskOpts := new(taskOptions)
	taskOpts.setDefault()
	taskOpts.failedMessageHandler = func(err error, record kafka.Record) {
		t.logger.Error(fmt.Sprintf(`Message %s failed due to %s`, record, err))
	}
	taskOpts.apply(t.taskOpts...)

	tsk := &task{
		id:          id,
		logger:      logger,
		subTopology: subTp,
		global:      true,
		ctx:         topologyCtx,
		stopping:    make(chan struct{}, 1),
		runGroup:    async.NewRunGroup(logger),
		options:     taskOpts,
	}

	tsk.metrics.reporter = ctx.MetricsReporter().Reporter(metrics.ReporterConf{
		Subsystem: "task_manager_task",
		ConstLabels: map[string]string{
			`type`:    `task`,
			`task_id`: id.String(),
		},
	})

	globalKTask := &globalTask{tsk}

	if err := globalKTask.Init(topologyCtx); err != nil {
		return nil, err
	}

	t.mu.Lock()
	t.tasks[id.String()] = globalKTask
	t.mu.Unlock()

	return globalKTask, nil
}

func (t *taskManager) RemoveTask(id TaskID) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	task, ok := t.tasks[id.String()]
	if !ok {
		return errors.Errorf(`task [%s] doesn't exists`, id)
	}

	if err := task.Stop(); err != nil {
		return errors.Wrap(err, `task stop failed`)
	}

	delete(t.tasks, id.String())

	t.logger.Info(fmt.Sprintf(`%s successfully removed`, id))

	return nil
}

func (t *taskManager) Task(id TaskID) (Task, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	tsk, ok := t.tasks[id.String()]
	if !ok {
		return nil, errors.Errorf(`task [%s] doesn't exists`, id)
	}

	return tsk, nil
}

func (t *taskManager) StopAll() error {
	t.logger.Info(`Running tasks stopping...`)
	defer t.logger.Info(`Running tasks stopped`)

	wg := &sync.WaitGroup{}
	wg.Add(len(t.tasks))
	for _, tsk := range t.tasks {
		go func(wg *sync.WaitGroup, tsk Task) {
			defer async.LogPanicTrace(t.logger)

			if err := tsk.Stop(); err != nil {
				panic(err)
			}

			wg.Done()
		}(wg, tsk)
	}

	wg.Wait()

	return nil
}

func (t *taskManager) NewTaskId(prefix string, tp kafka.TopicPartition) TaskID {
	if prefix != `` {
		prefix = fmt.Sprintf(`%s-`, prefix)
	}

	t.mu.Lock()
	defer t.mu.Unlock()
	return taskId{
		prefix:    fmt.Sprintf(`%sTask(%s)`, prefix, tp),
		partition: tp.Partition,
	}
}

func (t *taskManager) StoreInstances(name string) []topology.StateStore {
	var stors []topology.StateStore
	for _, task := range t.tasks {
		if stor := task.Store(name); stor != nil {
			stors = append(stors, stor)
		}
	}

	return stors
}
