package backup

import (
	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/events"
)

type eventsProcessorStdio struct {
	done chan struct{}
}

func startEventsProcessorStdio(ctx *appcontext.AppContext, quiet bool) eventsProcessorStdio {
	done := make(chan struct{})
	ep := eventsProcessorStdio{done: done}

	go func() {
		for event := range ctx.Events().Listen() {
			switch event := event.(type) {
			case events.PathError:
				ctx.GetLogger().Stderr("%x: KO %s %s: %s", event.SnapshotID[:4], crossMark, event.Pathname, event.Message)
			case events.DirectoryOK:
				if !quiet {
					ctx.GetLogger().Stdout("%x: OK %s %s", event.SnapshotID[:4], checkMark, event.Pathname)
				}
			case events.FileOK:
				if !quiet {
					ctx.GetLogger().Stdout("%x: OK %s %s", event.SnapshotID[:4], checkMark, event.Pathname)
				}
			case events.Done:
				done <- struct{}{}
			default:
				//ctx.GetLogger().Warn("unknown event: %T", event)
			}
		}
	}()

	return ep
}

func (ep eventsProcessorStdio) Close() {
	<-ep.done
}
