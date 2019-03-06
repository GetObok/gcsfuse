package gcsx

import (
	"github.com/jacobsa/gcloud/gcs"

	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
	"sync"
)

type tempFileStat struct {
	Name       string
	Synced     bool
	Generation int64
}

type TempFileState struct {
	mu        sync.Mutex
	stateFile string

	syncer Syncer
}

func NewTempFileState(cacheDir string, syncer Syncer) *TempFileState {
	return &TempFileState{
		stateFile: path.Join(cacheDir, "status.json"),
		syncer:    syncer,
	}
}

func (p *TempFileState) getStatusFile() (*os.File, map[string]tempFileStat, error) {
	file, err := os.OpenFile(p.stateFile, os.O_CREATE|os.O_RDWR, 0700)
	if err != nil {
		return nil, nil, err
	}

	bytes, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, nil, err
	}

	tmpFileStats := map[string]tempFileStat{}
	if len(bytes) > 0 {
		if err := json.Unmarshal(bytes, &tmpFileStats); err != nil {
			return nil, nil, err
		}
	}
	return file, tmpFileStats, nil
}

func (p *TempFileState) writeStatusFile(file *os.File, st map[string]tempFileStat) error {
	bytes, err := json.Marshal(st)
	if err != nil {
		return err
	}

	file.Truncate(0)
	file.Seek(0, 0)

	if _, err = file.Write(bytes); err != nil {
		return err
	}
	file.WriteString("\n")
	return nil
}

func (p *TempFileState) MarkForUpload(tmpFile, dstPath string, generation int64) error {
	return p.update(func(m map[string]tempFileStat) {
		m[tmpFile] = tempFileStat{
			Name:       dstPath,
			Generation: generation,
		}
	})
}

func (p *TempFileState) MarkUploaded(tmpFile string) error {
	return p.update(func(m map[string]tempFileStat) {
		s := m[tmpFile]
		s.Synced = true
		m[tmpFile] = s
	})
}

func (p *TempFileState) DeleteFileStatus(tmpFile string) error {
	return p.update(func(m map[string]tempFileStat) {
		delete(m, tmpFile)
	})
}

func (p *TempFileState) UpdatePaths(oldPath, newPath string) error {
	return p.update(func(m map[string]tempFileStat) {
		for t, s := range m {
			if strings.HasPrefix(s.Name, oldPath) {
				p2 := strings.TrimPrefix(s.Name, oldPath)
				newName := path.Join(newPath, p2)
				if strings.HasSuffix(t, "/") {
					newName = newName + "/"
				}
				s.Name = newName
				m[t] = s
			}
		}
	})
}

func (p *TempFileState) update(update func(m map[string]tempFileStat)) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	file, st, err := p.getStatusFile()
	if err != nil {
		return err
	}
	defer file.Close()
	update(st)

	return p.writeStatusFile(file, st)
}

func (p *TempFileState) UploadUnsynced(ctx context.Context) error {
	p.mu.Lock()
	file, st, err := p.getStatusFile()
	if err != nil {
		p.mu.Unlock()
		return err
	}
	p.mu.Unlock()
	defer file.Close()

	go func() {
		for t, f := range st {
			tfile, err := NewTempFileRO(t)
			if err != nil && !os.IsNotExist(err) {
				continue
			} else if err == nil {
				if !f.Synced {
					log.Println("local cache file sync.", t, f.Name)
					if err := p.uploadTmpFile(ctx, tfile, f); err != nil {
						log.Println("local cache file sync failed.", t, f.Name, err)
						continue
					}
					log.Println("local cache file sync done.", t, f.Name)
				} else {
					log.Println("local cache file already synced.", t, f.Name)
				}
				tfile.Destroy()
			}

			p.mu.Lock()
			file, st, err := p.getStatusFile()
			if err != nil {
				log.Println("failed to open cache state file.", t, f.Name)
				p.mu.Unlock()
				continue
			}
			delete(st, t)
			if err = p.writeStatusFile(file, st); err != nil {
				log.Println("failed to write cache state file.", t, f.Name)
			}
			file.Close()
			p.mu.Unlock()
		}
	}()
	return nil
}

func (p *TempFileState) CreateIfEmpty() error {
	csf, err := os.OpenFile(p.stateFile, os.O_CREATE|os.O_RDWR, 0700)
	if err != nil {
		return err
	}
	defer csf.Close()
	size, err := csf.Seek(0, 2)
	if err != nil {
		return err
	}
	if size == 0 {
		_, err := csf.WriteString("{}\n")
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *TempFileState) uploadTmpFile(ctx context.Context, tfile TempFileRO, f tempFileStat) error {
	obj := gcs.Object{
		Name:       f.Name,
		Generation: f.Generation,
	}

	_, err := p.syncer.SyncObject(ctx, &obj, tfile)
	return err
}
