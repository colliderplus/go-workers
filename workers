package workers

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"
)

type Job func(w *Worker)

type Jobs interface {
	start()
}

type Worker struct {
	//Jobs
	Method Job
	Quit   chan os.Signal
	Name   string
}

func NewWorker(method Job, name string) *Worker {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
	)
	return &Worker{Method: method, Quit: ch, Name: name}
}

type ScheduleWorker struct {
	//Jobs
	*Worker
	time time.Duration
}

type DayHourWorker struct {
	*ScheduleWorker
	hour int
	day  int
}

func (w *DayHourWorker) startTime() time.Duration {
	d := time.Now()
	var runDate time.Time
	if w.day != 0 {
		runDate = time.Date(d.Year(), d.Month(), w.day, w.hour, 0, 10, 0, d.Location())
		if runDate.Nanosecond() < d.Nanosecond() {
			runDate = runDate.AddDate(0, 1, 0)
		}
	} else {
		runDate = time.Date(d.Year(), d.Month(), d.Day(), w.hour, 0, 10, 0, d.Location())
		if runDate.Nanosecond() < d.Nanosecond() {
			runDate = runDate.AddDate(0, 0, 1)
		}
	}
	return runDate.Sub(d)
}

func (w *DayHourWorker) start() {
	defer recovery(w)
	for {
		ticker := time.NewTimer(w.time)
		layout := "2006-01-02 15:04:05"
		d := time.Now().Add(w.time)
		log.Println(fmt.Sprintf("Next starting date %s %s", d.Format(layout), w.Name))
		select {
		case <-w.Quit:
			os.Exit(0)
			log.Println(fmt.Sprintf("Finish %s worker", w.Name))
			break
		case <-ticker.C:
			log.Println(fmt.Sprintf("Start work: %s", w.Name))
			w.Method(w.Worker)
			w.time = w.startTime()
			log.Println(fmt.Sprintf("End work: %s", w.Name))
		}
	}
}

func NewDayHourWorker(method Job, day, hour int, name string) *DayHourWorker {
	w := DayHourWorker{hour: hour, day: day}
	sheduler := NewSheduleWorker(method, w.startTime(), name)
	w.ScheduleWorker = sheduler
	return &w
}

func NewSheduleWorker(method Job, time time.Duration, name string) *ScheduleWorker {
	return &ScheduleWorker{Worker: NewWorker(method, name), time: time}
}

func (w *ScheduleWorker) start() {
	ticker := time.NewTicker(w.time)
	defer recovery(w)
	for {
		select {
		case <-w.Quit:
			os.Exit(0)
			log.Println(fmt.Sprintf("Finish %s worker", w.Name))
			break
		case <-ticker.C:
			log.Println(fmt.Sprintf("Start work: %s", w.Name))
			w.Method(w.Worker)
			log.Println(fmt.Sprintf("End work: %s", w.Name))
		}
	}

}

func (w *Worker) start() {
	defer recovery(w)
	for {
		select {
		case <-w.Quit:
			os.Exit(0)
			log.Println(fmt.Sprintf("Finish %s worker", w.Name))
			break
		default:
			log.Println(fmt.Sprintf("Start work: %s", w.Name))
			w.Method(w)
			log.Println(fmt.Sprintf("End work: %s", w.Name))
		}
	}
}

type Workers []Jobs
type Runner struct {
	Workers Workers
}

func NewRunner(workers ...Jobs) *Runner {
	return &Runner{Workers: workers}
}

func (r *Runner)Append(j Jobs) {
	r.Workers = append(r.Workers, j)
}

func (r Runner) Run() {
	if len(r.Workers) == 0 {
		return
	}
	quit := make(chan os.Signal)
	signal.Notify(quit,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
	)
	for _, w := range r.Workers {
		if w == nil {
			continue
		}
		go w.start()
	}
	<-quit
}

func recovery(worker Jobs) {
	if err := recover(); err != nil {
		stack := stack(3)
		log.Println(err)
		log.Println(string(stack))
		go worker.start()
	}
}

func stack(skip int) []byte {
	buf := new(bytes.Buffer)
	var lines [][]byte
	var lastFile string
	for i := skip; ; i++ {
		pc, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}
		fmt.Fprintf(buf, "%s:%d (0x%x)\n", file, line, pc)
		if file != lastFile {
			data, err := ioutil.ReadFile(file)
			if err != nil {
				continue
			}
			lines = bytes.Split(data, []byte{'\n'})
			lastFile = file
		}
		fmt.Fprintf(buf, "\t%s: %s\n", function(pc), source(lines, line))
	}
	return buf.Bytes()
}

func source(lines [][]byte, n int) []byte {
	n--
	if n < 0 || n >= len(lines) {
		return dunno
	}
	return bytes.TrimSpace(lines[n])
}

func function(pc uintptr) []byte {
	fn := runtime.FuncForPC(pc)
	if fn == nil {
		return dunno
	}
	name := []byte(fn.Name())
	if lastSlash := bytes.LastIndex(name, slash); lastSlash >= 0 {
		name = name[lastSlash+1:]
	}
	if period := bytes.Index(name, dot); period >= 0 {
		name = name[period+1:]
	}
	name = bytes.Replace(name, centerDot, dot, -1)
	return name
}

var (
	dunno     = []byte("???")
	centerDot = []byte("·")
	dot       = []byte(".")
	slash     = []byte("/")
)
