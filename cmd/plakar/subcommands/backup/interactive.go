package backup

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/events"
	tea "github.com/charmbracelet/bubbletea"
)

type tickMsg struct{}

// tick command sends a message after a few ms to update the elapsed time
func tick() tea.Cmd {
	return tea.Tick(100*time.Millisecond, func(time.Time) tea.Msg {
		return tickMsg{}
	})
}

type Model struct {
	basepath string

	startTime time.Time
	elapsed   time.Duration

	forceQuit bool

	lastLog string

	countFilesOk     uint64
	countFilesErrors uint64

	countDirsOk     uint64
	countDirsErrors uint64

	importerSize uint64
	backupSize   uint64
}

func (m Model) Init() tea.Cmd {
	return tick()
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch event := msg.(type) {
	case tickMsg:
		m.elapsed = time.Since(m.startTime)
		return m, tick()

	case events.FileOK:
		m.countFilesOk++
		m.lastLog = fmt.Sprintf("%x: %s", event.SnapshotID[:4], event.Pathname)
		if event.Size > 0 {
			m.backupSize += uint64(event.Size)
		}

	case events.FileError, events.PathError:
		m.countFilesErrors++

	case events.DirectoryOK:
		// When we backup a subdirectory, eg. /home/user/xxx, we get events for
		// the parent directories: /home/user, /home and /.
		// Let's avoid reporting them.
		if len(event.Pathname) < len(m.basepath) {
			break
		}
		m.countDirsOk++
		m.lastLog = fmt.Sprintf("%x: %s", event.SnapshotID[:4], event.Pathname)

	case events.DirectoryError:
		m.countDirsErrors++

	case tea.KeyMsg:
		switch event.String() {
		case "ctrl+c":
			m.forceQuit = true
			return m, tea.Quit
		}

	case events.Done:
		m.lastLog = "Done!"

	case events.DoneImporter:
		m.importerSize = event.Size

	case tea.QuitMsg:
		m.lastLog = "Aborted"
		return m, tea.Quit
	}
	return m, nil
}

func (m Model) View() string {
	// If nothing was backed up (for example when the directory to backup
	// doesn't exist), don't show anything.
	if m.countFilesOk == 0 && m.countFilesErrors == 0 && m.countDirsOk == 0 && m.countDirsErrors == 0 {
		return ""
	}

	if m.forceQuit {
		return fmt.Sprintf("%s Backup aborted\n", crossMark)
	}

	var s strings.Builder

	fmt.Fprintf(&s, "Duration: %ds", int64(m.elapsed.Seconds()))

	if m.importerSize > 0 {
		fmt.Fprintf(&s, " (%d%%)", m.backupSize*100/m.importerSize)
	}

	fmt.Fprintf(&s, "\n")

	fmt.Fprintf(&s, "Directories: %s %d", checkMark, m.countDirsOk)
	if m.countDirsErrors > 0 {
		fmt.Fprintf(&s, " %s %d", crossMark, m.countDirsErrors)
	}
	fmt.Fprintf(&s, "\n")

	fmt.Fprintf(&s, "      Files: %s %d", checkMark, m.countFilesOk)
	if m.countFilesErrors > 0 {
		fmt.Fprintf(&s, " %s %d", crossMark, m.countFilesErrors)
	}
	fmt.Fprintf(&s, "\n")

	fmt.Fprintf(&s, "%s\n", m.lastLog)
	return s.String()
}

type eventsProcessorInteractive struct {
	program *tea.Program
	done    chan struct{}
}

func startEventsProcessorInteractive(ctx *appcontext.AppContext, basepath string) eventsProcessorInteractive {
	ep := eventsProcessorInteractive{
		program: tea.NewProgram(Model{
			basepath:  basepath,
			startTime: time.Now(),
		}),
		done: make(chan struct{}),
	}

	// Start the Bubble Tea program in the background.
	go func() {
		model, err := ep.program.Run()
		if err != nil {
			ctx.GetLogger().Error("error starting Bubble Tea: %v", err)
			return
		}

		// If the Bubble Tea program was exited with a ctrl+c, forward the
		// signal to the main process.
		if model.(Model).forceQuit {
			p := os.Process{Pid: os.Getpid()}
			_ = p.Signal(os.Interrupt)
		}
	}()

	// Start a goroutine to listen for events and send them to the bubble Tea program.
	go func() {
		for event := range ctx.Events().Listen() {
			ep.program.Send(event)

			switch event.(type) {
			case events.Done:
				ep.done <- struct{}{}
			}
		}
	}()
	return ep
}

func (ep eventsProcessorInteractive) Close() {
	<-ep.done
	ep.program.Quit()
	ep.program.Wait()
}
