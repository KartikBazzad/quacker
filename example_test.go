package quacker

import (
	"context"
	"fmt"
	"time"
)

// Example is the smallest useful program: define a task, enqueue it, await the
// result.
func Example() {
	q, _ := Open(WithStorage(Memory()))
	defer q.Close(context.Background())

	greet := NewTask("greet", func(ctx context.Context, name string) (string, error) {
		return "hello " + name, nil
	})

	h, _ := Enqueue(context.Background(), q, greet, "world")
	out, _ := h.Result(context.Background())
	fmt.Println(out)
	// Output: hello world
}

// ExampleNewWorkflow runs a two-step DAG where the second step reads the first
// step's output.
func ExampleNewWorkflow() {
	q, _ := Open(WithStorage(Memory()))
	defer q.Close(context.Background())

	charge := NewTask("charge", func(ctx context.Context, in string) (string, error) {
		return "charged:" + in, nil
	})
	ship := NewTask("ship", func(ctx context.Context, in string) (string, error) {
		rec, err := DepOutput[string](ctx, "charge")
		if err != nil {
			return "", err
		}
		return "shipped(" + rec + ")", nil
	})
	wf := NewWorkflow[string]("fulfill",
		Step("charge", charge),
		Step("ship", ship, "charge"),
	)

	h, _ := EnqueueWorkflow[string](context.Background(), q, wf, "o-1")
	out, _ := h.Result(context.Background())
	fmt.Println(out)
	// Output: shipped(charged:o-1)
}

// ExampleSleepDurable suspends a run without holding a worker slot and resumes
// it; RunOnce makes the side effect happen once across the replay.
func ExampleSleepDurable() {
	q, _ := Open(WithStorage(Memory()), WithPollInterval(time.Millisecond))
	defer q.Close(context.Background())

	task := NewTask("job", func(ctx context.Context, in string) (string, error) {
		reservations, err := RunOnce(ctx, "reserve", func() (int, error) { return 1, nil })
		if err != nil {
			return "", err
		}
		if err := SleepDurable(ctx, time.Millisecond); err != nil {
			return "", err
		}
		return fmt.Sprintf("done %s (%d)", in, reservations), nil
	})

	h, _ := Enqueue(context.Background(), q, task, "x")
	out, _ := h.Result(context.Background())
	fmt.Println(out)
	// Output: done x (1)
}

type examplePlugin struct{ finished chan string }

func (p examplePlugin) Name() string { return "example" }

func (p examplePlugin) Hooks() Hooks {
	return Hooks{
		OnRunFinished: func(ctx context.Context, r RunInfo) { p.finished <- r.Status },
	}
}

// ExampleWithPlugin registers a plugin that observes run completion.
func ExampleWithPlugin() {
	finished := make(chan string, 1)
	q, _ := Open(WithStorage(Memory()), WithPlugin(examplePlugin{finished: finished}))
	defer q.Close(context.Background())

	task := NewTask("greet", func(ctx context.Context, in string) (string, error) {
		return "hi " + in, nil
	})
	h, _ := Enqueue(context.Background(), q, task, "world")
	out, _ := h.Result(context.Background())
	fmt.Println(out, <-finished) // OnRunFinished fires around Result
	// Output: hi world SUCCEEDED
}
