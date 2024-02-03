package main

import (
	"context"
	"fmt"
	"os"
	"time"
)

type fileStatProvider interface {
	Stat(name string) (os.FileInfo, error)
}

type StatProvider struct{}

func (p *StatProvider) Stat(name string) (os.FileInfo, error) {
	return os.Stat(name)
}

type FileMonitor struct {
	FileStatProvider fileStatProvider
	Path             string
	LastModified     time.Time
}

func (m *FileMonitor) StartMonitor(ctx context.Context, doneCh chan bool) {

	defer func() {
		doneCh <- true
	}()
	stat, err := m.FileStatProvider.Stat(m.Path)
	if err != nil {
		fmt.Printf("Could not start file monitor for %s: %v", m.Path, err)
		return
	}
	m.LastModified = stat.ModTime()

	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := m.innerMonitor(); err != nil {
				return
			}
		}
	}
}

func (m *FileMonitor) innerMonitor() error {
	stat, err := m.FileStatProvider.Stat(m.Path)
	if err != nil {
		fmt.Printf("Error reading file stats for %s: %v\n", m.Path, err)
		return err
	}
	if stat.ModTime().After(m.LastModified) {
		fmt.Printf("File %s modified at %s\n", stat.Name(), stat.ModTime())
		m.LastModified = stat.ModTime()
	}
	return nil
}

func main() {

	if len(os.Args) != 2 {
		fmt.Println("Usage: <command> path/to/file.ext")
		os.Exit(1)
	}
	m := &FileMonitor{
		Path:             os.Args[1],
		FileStatProvider: &StatProvider{},
	}

	doneCh := make(chan bool)
	defer close(doneCh)

	ctx, cancel := context.WithCancel(context.Background())
	go m.StartMonitor(ctx, doneCh)
	<-doneCh
	cancel()
}
