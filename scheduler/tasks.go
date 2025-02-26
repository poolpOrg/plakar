package scheduler

import (
	"fmt"
	"time"

	"github.com/PlakarKorp/plakar/appcontext"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands/backup"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands/check"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands/cleanup"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands/restore"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands/rm"
	"github.com/PlakarKorp/plakar/cmd/plakar/subcommands/sync"
	"github.com/PlakarKorp/plakar/repository"
	"github.com/PlakarKorp/plakar/storage"
)

func (s *Scheduler) backupTask(taskset TaskSet, task BackupConfig) error {
	interval, err := stringToDuration(task.Interval)
	if err != nil {
		return err
	}

	var retention time.Duration
	if task.Retention != "" {
		retention, err = stringToDuration(task.Retention)
		if err != nil {
			return err
		}
	}

	backupSubcommand := &backup.Backup{}
	backupSubcommand.RepositoryLocation = taskset.Repository.URL
	if taskset.Repository.Passphrase != "" {
		backupSubcommand.RepositorySecret = []byte(taskset.Repository.Passphrase)
		_ = backupSubcommand.RepositorySecret
	}
	backupSubcommand.Silent = true
	backupSubcommand.Job = taskset.Name
	backupSubcommand.Path = task.Path
	backupSubcommand.Quiet = true
	if task.Check {
		backupSubcommand.OptCheck = true
	}

	rmSubcommand := &rm.Rm{}
	rmSubcommand.RepositoryLocation = taskset.Repository.URL
	if taskset.Repository.Passphrase != "" {
		rmSubcommand.RepositorySecret = []byte(taskset.Repository.Passphrase)
		_ = rmSubcommand.RepositorySecret
	}
	rmSubcommand.OptJob = task.Name

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		firstRun := true
		for {
			if firstRun {
				firstRun = false
			} else {
				time.Sleep(interval)
			}

			store, config, err := storage.Open(map[string]string{"location": backupSubcommand.RepositoryLocation})
			if err != nil {
				s.ctx.GetLogger().Error("Error opening storage: %s", err)
				continue
			}

			newCtx := appcontext.NewAppContextFrom(s.ctx)

			repo, err := repository.New(newCtx, store, config)
			if err != nil {
				s.ctx.GetLogger().Error("Error opening repository: %s", err)
				store.Close()
				continue
			}

			backupCtx := appcontext.NewAppContextFrom(newCtx)
			retval, err := backupSubcommand.Execute(backupCtx, repo)
			if err != nil || retval != 0 {
				s.ctx.GetLogger().Error("Error creating backup: %s", err)
				backupCtx.Close()
				goto close
			}
			backupCtx.Close()

			if task.Retention != "" {
				rmCtx := appcontext.NewAppContextFrom(newCtx)
				rmSubcommand.OptBefore = time.Now().Add(-retention)
				retval, err = rmSubcommand.Execute(rmCtx, repo)
				if err != nil || retval != 0 {
					s.ctx.GetLogger().Error("Error removing obsolete backups: %s", err)
				}
				rmCtx.Close()
			}

		close:
			newCtx.Close()
			repo.Close()
			store.Close()
		}
	}()

	return nil
}

func (s *Scheduler) checkTask(taskset TaskSet, task CheckConfig) error {
	interval, err := stringToDuration(task.Interval)
	if err != nil {
		return err
	}

	checkSubcommand := &check.Check{}
	checkSubcommand.RepositoryLocation = taskset.Repository.URL
	if taskset.Repository.Passphrase != "" {
		checkSubcommand.RepositorySecret = []byte(taskset.Repository.Passphrase)
		_ = checkSubcommand.RepositorySecret
	}
	checkSubcommand.OptJob = taskset.Name
	checkSubcommand.OptLatest = task.Latest
	checkSubcommand.Silent = true
	if task.Path != "" {
		checkSubcommand.Snapshots = []string{":" + task.Path}
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		firstRun := true
		for {
			if firstRun {
				firstRun = false
			} else {
				time.Sleep(interval)
			}

			store, config, err := storage.Open(map[string]string{"location": checkSubcommand.RepositoryLocation})
			if err != nil {
				s.ctx.GetLogger().Error("Error opening storage: %s", err)
				continue
			}

			newCtx := appcontext.NewAppContextFrom(s.ctx)

			repo, err := repository.New(newCtx, store, config)
			if err != nil {
				s.ctx.GetLogger().Error("Error opening repository: %s", err)
				store.Close()
				continue
			}

			retval, err := checkSubcommand.Execute(newCtx, repo)
			if err != nil || retval != 0 {
				s.ctx.GetLogger().Error("Error executing check: %s", err)
			}

			newCtx.Close()
			repo.Close()
			store.Close()
		}
	}()

	return nil
}

func (s *Scheduler) restoreTask(taskset TaskSet, task RestoreConfig) error {
	interval, err := stringToDuration(task.Interval)
	if err != nil {
		return err
	}

	restoreSubcommand := &restore.Restore{}
	restoreSubcommand.RepositoryLocation = taskset.Repository.URL
	if taskset.Repository.Passphrase != "" {
		restoreSubcommand.RepositorySecret = []byte(taskset.Repository.Passphrase)
		_ = restoreSubcommand.RepositorySecret
	}
	restoreSubcommand.OptJob = taskset.Name
	restoreSubcommand.Target = task.Target
	restoreSubcommand.Silent = true
	if task.Path != "" {
		restoreSubcommand.Snapshots = []string{":" + task.Path}
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		firstRun := true
		for {
			if firstRun {
				firstRun = false
			} else {
				time.Sleep(interval)
			}

			store, config, err := storage.Open(map[string]string{"location": restoreSubcommand.RepositoryLocation})
			if err != nil {
				s.ctx.GetLogger().Error("Error opening storage: %s", err)
				continue
			}

			newCtx := appcontext.NewAppContextFrom(s.ctx)

			repo, err := repository.New(newCtx, store, config)
			if err != nil {
				s.ctx.GetLogger().Error("Error opening repository: %s", err)
				store.Close()
				continue
			}

			retval, err := restoreSubcommand.Execute(newCtx, repo)
			if err != nil || retval != 0 {
				s.ctx.GetLogger().Error("Error executing restore: %s", err)
			}

			newCtx.Close()
			repo.Close()
			store.Close()
		}
	}()

	return nil
}

func (s *Scheduler) syncTask(taskset TaskSet, task SyncConfig) error {
	interval, err := stringToDuration(task.Interval)
	if err != nil {
		return err
	}

	syncSubcommand := &sync.Sync{}
	syncSubcommand.SourceRepositoryLocation = taskset.Repository.URL
	if taskset.Repository.Passphrase != "" {
		syncSubcommand.SourceRepositorySecret = []byte(taskset.Repository.Passphrase)
		_ = syncSubcommand.SourceRepositorySecret
	}

	syncSubcommand.PeerRepositoryLocation = task.Peer
	if task.Direction == "to" {
		syncSubcommand.Direction = "to"
	} else if task.Direction == "from" {
		syncSubcommand.Direction = "from"
	} else if task.Direction == "with" || task.Direction == "" {
		syncSubcommand.Direction = "with"
	} else {
		return fmt.Errorf("invalid sync direction: %s", task.Direction)
	}
	//	if taskset.Repository.Passphrase != "" {
	//		syncSubcommand.DestinationRepositorySecret = []byte(taskset.Repository.Passphrase)
	//		_ = syncSubcommand.DestinationRepositorySecret

	//	syncSubcommand.OptJob = taskset.Name
	//	syncSubcommand.Target = task.Target
	//	syncSubcommand.Silent = true

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		firstRun := true
		for {
			if firstRun {
				firstRun = false
			} else {
				time.Sleep(interval)
			}

			store, config, err := storage.Open(map[string]string{"location": syncSubcommand.SourceRepositoryLocation})
			if err != nil {
				s.ctx.GetLogger().Error("sync: error opening storage: %s", err)
				continue
			}

			newCtx := appcontext.NewAppContextFrom(s.ctx)

			repo, err := repository.New(newCtx, store, config)
			if err != nil {
				s.ctx.GetLogger().Error("sync: error opening repository: %s", err)
				store.Close()
				continue
			}

			retval, err := syncSubcommand.Execute(newCtx, repo)
			if err != nil || retval != 0 {
				s.ctx.GetLogger().Error("sync: %s", err)
			} else {
				s.ctx.GetLogger().Info("sync: synchronization succeeded")
			}

			newCtx.Close()
			repo.Close()
			store.Close()
		}
	}()

	return nil
}

func (s *Scheduler) cleanupTask(task CleanupConfig) error {
	interval, err := stringToDuration(task.Interval)
	if err != nil {
		return err
	}

	cleanupSubcommand := &cleanup.Cleanup{}
	cleanupSubcommand.RepositoryLocation = task.Repository.URL
	if task.Repository.Passphrase != "" {
		cleanupSubcommand.RepositorySecret = []byte(task.Repository.Passphrase)
		_ = cleanupSubcommand.RepositorySecret
	}

	rmSubcommand := &rm.Rm{}
	rmSubcommand.RepositoryLocation = task.Repository.URL
	if task.Repository.Passphrase != "" {
		rmSubcommand.RepositorySecret = []byte(task.Repository.Passphrase)
		_ = rmSubcommand.RepositorySecret
	}

	var retention time.Duration
	if task.Retention != "" {
		retention, err = stringToDuration(task.Retention)
		if err != nil {
			return err
		}
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		firstRun := true
		for {
			if firstRun {
				firstRun = false
			} else {
				time.Sleep(interval)
			}

			store, config, err := storage.Open(map[string]string{"location": cleanupSubcommand.RepositoryLocation})
			if err != nil {
				s.ctx.GetLogger().Error("Error opening storage: %s", err)
				continue
			}

			newCtx := appcontext.NewAppContextFrom(s.ctx)

			repo, err := repository.New(newCtx, store, config)
			if err != nil {
				s.ctx.GetLogger().Error("Error opening repository: %s", err)
				store.Close()
				continue
			}

			retval, err := cleanupSubcommand.Execute(newCtx, repo)
			if err != nil || retval != 0 {
				s.ctx.GetLogger().Error("Error executing cleanup: %s", err)
			} else {
				s.ctx.GetLogger().Info("maintenance of repository %s succeeded", cleanupSubcommand.RepositoryLocation)
			}

			if task.Retention != "" {
				rmCtx := appcontext.NewAppContextFrom(newCtx)
				rmSubcommand.OptBefore = time.Now().Add(-retention)
				retval, err = rmSubcommand.Execute(rmCtx, repo)
				if err != nil || retval != 0 {
					s.ctx.GetLogger().Error("Error removing obsolete backups: %s", err)
				} else {
					s.ctx.GetLogger().Info("Retention purge succeeded")
				}
				rmCtx.Close()
			}

			newCtx.Close()
			repo.Close()
			store.Close()
		}
	}()

	return nil
}
